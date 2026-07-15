use super::*;

impl ClusterRuntime {
    pub async fn scrub_and_repair(&self) -> anyhow::Result<ScrubResult> {
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        let mut result = ScrubResult::default();
        for group in groups {
            match group.scrub().await {
                Ok(records) => result.records_checked += records,
                Err(error) if !matches!(group.group_key(), GroupKey::Partition(_)) => {
                    group.isolate().await;
                    tracing::error!(
                        group_key = %group.group_key(),
                        node_id = self.node_id,
                        audit_event = "replica_isolated",
                        %error,
                        "metadata replica failed scrub and was isolated"
                    );
                    anyhow::bail!(
                        "metadata replica is corrupt and requires an offline restore: {error}"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        group_key = %group.group_key(),
                        node_id = self.node_id,
                        %error,
                        "partition replica failed scrub and will be rebuilt"
                    );
                    let group_key = group.group_key();
                    let group_id = group_key
                        .partition_id()
                        .expect("partition branch has a partition key");
                    let (_, partition) = self
                        .metadata
                        .partition(group_id)
                        .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
                    let Some(mut leader) = group.raft().metrics().borrow().current_leader else {
                        group.isolate().await;
                        anyhow::bail!("corrupt group has no elected leader");
                    };
                    if leader == self.node_id {
                        let Some(target) = partition
                            .replicas
                            .iter()
                            .copied()
                            .find(|candidate| *candidate != self.node_id)
                        else {
                            group.isolate().await;
                            anyhow::bail!("corrupt leader has no transfer target");
                        };
                        if let Err(error) = group.transfer_leadership(target).await {
                            group.isolate().await;
                            return Err(error);
                        }
                        if let Err(error) = self.wait_for_leader_change(&group, self.node_id).await
                        {
                            group.isolate().await;
                            return Err(error);
                        }
                        leader = group
                            .raft()
                            .metrics()
                            .borrow()
                            .current_leader
                            .unwrap_or(target);
                    }
                    group.isolate().await;
                    tracing::error!(
                        group_id = %group_id,
                        node_id = self.node_id,
                        audit_event = "replica_isolated",
                        "corrupt partition replica stopped voting and serving"
                    );
                    self.forward_repair_to(group_id, leader, self.node_id)
                        .await?;
                    result.replicas_repaired += 1;
                }
            }
        }
        self.scrub_records
            .fetch_add(result.records_checked as u64, Ordering::Relaxed);
        Ok(result)
    }
}
