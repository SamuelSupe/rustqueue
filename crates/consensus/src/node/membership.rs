use super::*;

impl ConsensusNode {
    pub async fn add_learner(&self, node_id: NodeId) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        let leader = self.current_leader()?;
        if leader != self.node_id {
            let leader = self
                .node(leader)
                .ok_or_else(|| anyhow::anyhow!("learner leader is not configured"))?;
            let _timer = self.latency.forward.timer();
            let response: OperationResponse = self
                .client
                .post(format!(
                    "{}/raft/groups/{}/learners/{node_id}",
                    leader.addr.trim_end_matches('/'),
                    self.group_key
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            return response
                .error
                .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)));
        }
        self.add_learner_unlocked(node_id, true).await
    }

    pub async fn add_learner_local(&self, node_id: NodeId) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        self.add_learner_unlocked(node_id, true).await
    }

    pub(crate) async fn add_learner_nonblocking_local(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        self.add_learner_unlocked(node_id, false).await
    }

    async fn add_learner_unlocked(&self, node_id: NodeId, blocking: bool) -> anyhow::Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow::anyhow!("node {node_id} is not present in cluster.nodes"))?;
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            self.raft.add_learner(node_id, node, blocking),
        )
        .await
        .map_err(|_| anyhow::anyhow!("learner catch-up timed out"))??;
        Ok(())
    }

    pub async fn change_membership(
        &self,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        let leader = self.current_leader()?;
        if leader != self.node_id {
            let leader = self
                .node(leader)
                .ok_or_else(|| anyhow::anyhow!("membership leader is not configured"))?;
            let _timer = self.latency.forward.timer();
            let response: OperationResponse = self
                .client
                .post(format!(
                    "{}/raft/groups/{}/membership",
                    leader.addr.trim_end_matches('/'),
                    self.group_key
                ))
                .json(&crate::ChangeMembershipRequest {
                    voters,
                    retain_removed_as_learners,
                })
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            return response
                .error
                .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)));
        }
        self.change_membership_unlocked(voters, retain_removed_as_learners)
            .await
    }

    pub async fn change_membership_local(
        &self,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        self.change_membership_unlocked(voters, retain_removed_as_learners)
            .await
    }

    async fn change_membership_unlocked(
        &self,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> anyhow::Result<()> {
        if !matches!(voters.len(), 3 | 5) {
            anyhow::bail!("membership must contain exactly three or five voters");
        }
        for node_id in &voters {
            if self.node(*node_id).is_none() {
                anyhow::bail!("node {node_id} is not present in cluster.nodes");
            }
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.raft
                .change_membership(voters, retain_removed_as_learners),
        )
        .await
        .map_err(|_| anyhow::anyhow!("joint consensus change timed out"))??;
        Ok(())
    }

    pub(crate) async fn change_membership_for_repair(
        &self,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        if voters.is_empty() {
            anyhow::bail!("repair membership cannot be empty");
        }
        for node_id in &voters {
            if self.node(*node_id).is_none() {
                anyhow::bail!("node {node_id} is not present in cluster.nodes");
            }
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.raft.change_membership(voters, true),
        )
        .await
        .map_err(|_| anyhow::anyhow!("repair membership change timed out"))??;
        Ok(())
    }

    pub async fn build_snapshot(&self) -> anyhow::Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub(crate) async fn compact_partition_storage(
        &self,
        topic: &str,
        partition: u16,
    ) -> anyhow::Result<usize> {
        let Some(target) = self.raft.metrics().borrow().last_applied else {
            return Ok(0);
        };
        self.build_snapshot().await?;
        wait_for_log_metric(&self.raft, target.index, |metrics| metrics.snapshot).await?;
        self.raft
            .trigger()
            .purge_log(target.index)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        wait_for_log_metric(&self.raft, target.index, |metrics| metrics.purged).await?;

        let broker = Arc::clone(&self.broker);
        let topic = topic.to_owned();
        let retained = tokio::task::spawn_blocking(move || {
            broker
                .partition_payload_paths(&topic, partition)
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        })
        .await??;
        let segments = self.log_store.gc_purged_segments(&retained).await?;
        // During emergency disk pressure the newly installed generation has
        // already been fully verified and fsynced. Keeping only that current
        // generation releases hard-link references held by the fallback copy.
        let generations = self.state_machine.prune_snapshot_generations(1).await?;
        Ok(segments.saturating_add(generations))
    }

    pub async fn scrub(&self) -> anyhow::Result<usize> {
        self.log_store.scrub().await.map_err(anyhow::Error::from)
    }

    pub(super) fn current_leader(&self) -> anyhow::Result<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .current_leader
            .ok_or_else(|| anyhow::anyhow!("cluster has no elected leader"))
    }
}

async fn wait_for_log_metric(
    raft: &Raft,
    target: u64,
    read: impl Fn(&openraft::RaftMetrics<NodeId, BasicNode>) -> Option<openraft::LogId<NodeId>>,
) -> anyhow::Result<()> {
    let mut metrics = raft.metrics();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if read(&metrics.borrow()).is_some_and(|log_id| log_id.index >= target) {
                return Ok(());
            }
            metrics
                .changed()
                .await
                .map_err(|_| anyhow::anyhow!("Raft metrics channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for Raft storage compaction"))?
}
