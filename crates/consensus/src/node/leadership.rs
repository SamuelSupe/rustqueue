use super::*;

const CATCH_UP_ATTEMPTS: usize = 50;
const ELECTION_ATTEMPTS: usize = 4;
const ELECTION_POLL_ATTEMPTS: usize = 25;
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

struct HeartbeatPause<'a> {
    raft: &'a Raft,
}

impl<'a> HeartbeatPause<'a> {
    fn new(raft: &'a Raft) -> Self {
        raft.runtime_config().heartbeat(false);
        Self { raft }
    }
}

impl Drop for HeartbeatPause<'_> {
    fn drop(&mut self) {
        self.raft.runtime_config().heartbeat(true);
    }
}

impl ConsensusNode {
    pub async fn transfer_leadership(&self, target: NodeId) -> anyhow::Result<()> {
        let metrics = self.raft.metrics().borrow().clone();
        if !metrics.membership_config.voter_ids().any(|id| id == target) {
            anyhow::bail!("target node {target} is not a voter");
        }
        if metrics.current_leader == Some(target) {
            return Ok(());
        }
        let leader = metrics
            .current_leader
            .ok_or_else(|| anyhow::anyhow!("cluster has no elected leader"))?;
        if leader != self.node_id {
            return self.forward_leadership_transfer(leader, target).await;
        }

        let gate = self.leadership_gate.write().await;
        let leader = self.current_leader()?;
        if leader == target {
            return Ok(());
        }
        if leader != self.node_id {
            drop(gate);
            return self.forward_leadership_transfer(leader, target).await;
        }

        self.ensure_quorum_local_unlocked().await?;
        self.wait_for_replication(target).await?;
        let quiet_period = std::time::Duration::from_millis(
            self.raft
                .config()
                .election_timeout_max
                .saturating_add(self.raft.config().heartbeat_interval * 2),
        );
        let _heartbeat = HeartbeatPause::new(&self.raft);
        tokio::time::sleep(quiet_period).await;

        for _ in 0..ELECTION_ATTEMPTS {
            self.request_target_election(target).await?;
            if self.wait_for_target_leader(target).await? {
                tracing::info!(
                    audit_event = "leader_transfer_completed",
                    group_key = %self.group_key,
                    from_node = self.node_id,
                    to_node = target,
                    "Raft leadership transferred"
                );
                return Ok(());
            }
        }
        anyhow::bail!(
            "leadership transfer for group {} to node {target} timed out",
            self.group_key
        )
    }

    async fn forward_leadership_transfer(
        &self,
        leader: NodeId,
        target: NodeId,
    ) -> anyhow::Result<()> {
        let node = self
            .node(leader)
            .ok_or_else(|| anyhow::anyhow!("leader {leader} is not in configured nodes"))?;
        let _timer = self.latency.forward.timer();
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{}/transfer/{target}",
                node.addr.trim_end_matches('/'),
                self.group_key
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    async fn wait_for_replication(&self, target: NodeId) -> anyhow::Result<()> {
        for _ in 0..CATCH_UP_ATTEMPTS {
            let metrics = self.raft.metrics().borrow().clone();
            if metrics.current_leader != Some(self.node_id) {
                anyhow::bail!("leadership changed while preparing transfer; retry the operation");
            }
            let last = metrics.last_log_index.unwrap_or_default();
            let matched = metrics
                .replication
                .as_ref()
                .and_then(|replication| replication.get(&target))
                .and_then(|log_id| log_id.as_ref())
                .map(|log_id| log_id.index)
                .unwrap_or_default();
            if matched >= last {
                return Ok(());
            }
            self.raft
                .trigger()
                .heartbeat()
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        anyhow::bail!("leadership target node {target} did not catch up")
    }

    async fn request_target_election(&self, target: NodeId) -> anyhow::Result<()> {
        let node = self
            .node(target)
            .ok_or_else(|| anyhow::anyhow!("node {target} is not present in cluster.nodes"))?;
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{}/elect",
                node.addr.trim_end_matches('/'),
                self.group_key
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    async fn wait_for_target_leader(&self, target: NodeId) -> anyhow::Result<bool> {
        let node = self
            .node(target)
            .ok_or_else(|| anyhow::anyhow!("node {target} is not present in cluster.nodes"))?;
        for _ in 0..ELECTION_POLL_ATTEMPTS {
            if self.raft.metrics().borrow().current_leader == Some(target) {
                return Ok(true);
            }
            if let Ok(response) = self
                .client
                .get(format!(
                    "{}/raft/groups/{}/health",
                    node.addr.trim_end_matches('/'),
                    self.group_key
                ))
                .send()
                .await
            {
                if response.status().is_success() {
                    let health: serde_json::Value = response.json().await?;
                    if health["current_leader"].as_u64() == Some(target) {
                        return Ok(true);
                    }
                    if health["current_leader"]
                        .as_u64()
                        .is_some_and(|leader| leader != self.node_id && leader != target)
                    {
                        return Ok(false);
                    }
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        Ok(false)
    }
}
