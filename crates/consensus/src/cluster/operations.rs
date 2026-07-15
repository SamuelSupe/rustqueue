use super::*;

impl ClusterRuntime {
    pub async fn rebalance_group(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        let (_, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        let leader = self.partition_leader(&partition).await?;
        if leader != self.node_id {
            return self.forward_rebalance_to(group_id, leader, voters).await;
        }
        self.rebalance_group_local(group_id, voters).await
    }

    pub(super) async fn partition_leader(
        &self,
        partition: &PartitionDescriptor,
    ) -> anyhow::Result<NodeId> {
        self.partition_leader_among(partition, &partition.replicas)
            .await
    }

    pub(super) async fn partition_leader_among(
        &self,
        partition: &PartitionDescriptor,
        eligible: &BTreeSet<NodeId>,
    ) -> anyhow::Result<NodeId> {
        if let Some(group) = self.group(partition.group_key()).await {
            if let Some(leader) = group
                .raft()
                .metrics()
                .borrow()
                .current_leader
                .filter(|leader| eligible.contains(leader))
            {
                return Ok(leader);
            }
        }
        let candidates: BTreeSet<_> = partition.replicas.union(eligible).copied().collect();
        for node_id in candidates {
            let Some(node) = self.node(node_id) else {
                continue;
            };
            let response = self
                .client
                .get(format!(
                    "{}/raft/groups/{}/health",
                    node.addr.trim_end_matches('/'),
                    partition.group_key()
                ))
                .send()
                .await;
            let Ok(response) = response else { continue };
            let Ok(response) = response.error_for_status() else {
                continue;
            };
            let health: serde_json::Value = response.json().await?;
            if let Some(leader) = health["current_leader"].as_u64() {
                if eligible.contains(&leader) {
                    return Ok(leader);
                }
            }
        }
        anyhow::bail!(
            "partition group {} has no elected leader",
            partition.global_id()
        )
    }

    async fn forward_rebalance_to(
        &self,
        group_id: crate::GlobalGroupId,
        leader: NodeId,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        let leader = self
            .node(leader)
            .ok_or_else(|| anyhow::anyhow!("rebalance leader is not configured"))?;
        let response: OperationResponse = crate::post_binary(
            &self.client,
            format!(
                "{}/raft/groups/{group_key}/rebalance",
                leader.addr.trim_end_matches('/')
            ),
            &RebalanceGroupRequest { voters },
        )
        .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    pub async fn rebalance_group_local(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        let (topic, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        if voters.len() != partition.replication_factor as usize {
            anyhow::bail!(
                "partition group {group_id} requires {} voters",
                partition.replication_factor
            );
        }
        if voters.iter().any(|node| self.node(*node).is_none()) {
            anyhow::bail!("requested replica is not configured on this node");
        }
        let group = self
            .partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} is not hosted locally"))?;
        let current: BTreeSet<_> = group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        if current == voters {
            if partition.replicas != voters {
                let response = self
                    .metadata_group()
                    .write(QueueCommand::UpdatePartitionReplicas {
                        group_id,
                        replicas: voters,
                    })
                    .await?;
                ensure_response(&response)?;
            }
            return Ok(());
        }

        let desired = PartitionDescriptor {
            replicas: voters.clone(),
            ..partition.clone()
        };
        for node_id in voters.difference(&current) {
            self.ensure_replica_host(&topic, &desired, *node_id).await?;
            group.add_learner(*node_id).await?;
        }
        let removed_leader = group
            .raft()
            .metrics()
            .borrow()
            .current_leader
            .filter(|leader| !voters.contains(leader));
        group.change_membership(voters.clone(), false).await?;
        if let Some(leader) = removed_leader {
            self.wait_for_leader_change(&group, leader).await?;
        }
        let response = self
            .metadata_group()
            .write(QueueCommand::UpdatePartitionReplicas {
                group_id,
                replicas: voters.clone(),
            })
            .await?;
        ensure_response(&response)?;
        for removed in current.difference(&voters).copied().collect::<Vec<_>>() {
            self.retire_replica(group_id, removed).await?;
        }
        Ok(())
    }

    pub async fn drain_group_replica(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<NodeId> {
        let voters = self.plan_group_replica_removal(group_id, node_id)?;
        let replacement = *voters
            .iter()
            .find(|candidate| {
                self.metadata
                    .partition(group_id)
                    .is_some_and(|(_, partition)| !partition.replicas.contains(candidate))
            })
            .ok_or_else(|| anyhow::anyhow!("group {group_id} has no replacement node"))?;
        self.rebalance_group(group_id, voters).await?;
        Ok(replacement)
    }

    pub fn plan_group_replica_removal(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<BTreeSet<NodeId>> {
        let (_, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        if !partition.replicas.contains(&node_id) {
            anyhow::bail!("node {node_id} is not a replica of group {group_id}");
        }
        let snapshot = self.metadata.snapshot();
        let replacement = snapshot
            .nodes
            .values()
            .filter(|node| {
                !snapshot.drained_nodes.contains(&node.id)
                    && !partition.replicas.contains(&node.id)
                    && snapshot
                        .node_health
                        .get(&node.id)
                        .is_some_and(|health| health.available && health.storage_eligible)
            })
            .min_by_key(|node| {
                let same_domain = partition
                    .replicas
                    .iter()
                    .filter(|replica| **replica != node_id)
                    .any(|replica| snapshot.nodes[replica].failure_domain == node.failure_domain);
                (usize::from(same_domain), node.id)
            })
            .ok_or_else(|| anyhow::anyhow!("group {group_id} has no replacement node"))?
            .id;
        let mut voters = partition.replicas;
        voters.remove(&node_id);
        voters.insert(replacement);
        Ok(voters)
    }

    pub async fn repair_replica(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let _timer = self.repair_latency.timer();
        let (_, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        if !partition.replicas.contains(&node_id) {
            anyhow::bail!("node {node_id} is not a replica of group {group_id}");
        }
        let Some(group) = self.partition_group(group_id).await else {
            let response: OperationResponse = self
                .post_to_replicas(&partition, "repair", &RepairReplicaRequest { node_id })
                .await?;
            return response
                .error
                .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)));
        };

        group.ensure_quorum().await?;
        let current_leader = group
            .raft()
            .metrics()
            .borrow()
            .current_leader
            .ok_or_else(|| anyhow::anyhow!("repair requires an elected leader"))?;
        if current_leader == node_id {
            let target = partition
                .replicas
                .iter()
                .copied()
                .find(|candidate| *candidate != node_id)
                .ok_or_else(|| anyhow::anyhow!("repair has no healthy leadership target"))?;
            group.transfer_leadership(target).await?;
            self.wait_for_leader_change(&group, node_id).await?;
            return self.forward_repair_to(group_id, target, node_id).await;
        }
        if current_leader != self.node_id {
            return self
                .forward_repair_to(group_id, current_leader, node_id)
                .await;
        }

        let _repair_guard = self.repair_lock.lock().await;
        if group.raft().metrics().borrow().current_leader != Some(self.node_id) {
            anyhow::bail!("repair leadership changed; retry the operation");
        }
        let expected_index = group
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |log_id| log_id.index);
        let current_voters: BTreeSet<_> = group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        if current_voters.contains(&node_id) {
            let mut healthy_voters = current_voters;
            healthy_voters.remove(&node_id);
            group.change_membership_for_repair(healthy_voters).await?;
        }

        let node = self.node(node_id).expect("replica is configured");
        let group_key = partition.group_key();
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{group_key}/reset",
                node.addr.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(error) = response.error {
            anyhow::bail!(error);
        }

        group.add_learner(node_id).await?;
        self.wait_replica_caught_up(&partition, node_id, expected_index)
            .await?;
        group
            .change_membership(partition.replicas.clone(), true)
            .await?;
        self.replica_repairs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(super) async fn forward_repair_to(
        &self,
        group_id: crate::GlobalGroupId,
        leader: NodeId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        let leader = self
            .node(leader)
            .ok_or_else(|| anyhow::anyhow!("repair leader is not configured"))?;
        let response: OperationResponse = crate::post_binary(
            &self.client,
            format!(
                "{}/raft/groups/{group_key}/repair",
                leader.addr.trim_end_matches('/')
            ),
            &RepairReplicaRequest { node_id },
        )
        .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    pub async fn reset_replica_local(&self, group_id: crate::GlobalGroupId) -> anyhow::Result<()> {
        let (topic, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        let group_key = partition.group_key();
        if !partition.replicas.contains(&self.node_id) {
            anyhow::bail!("local node is not assigned to group {group_id}");
        }
        let group = self
            .groups
            .write()
            .await
            .remove(&group_key)
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted locally"))?;
        if !group.is_isolated() {
            group.raft().shutdown().await?;
        }
        let component = group_key.storage_component();
        let source = self.directory.join("groups").join(&component);
        let quarantine = self.directory.join("quarantine");
        crate::store::blocking_io::run(move || {
            std::fs::create_dir_all(&quarantine)?;
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            std::fs::rename(
                &source,
                quarantine.join(format!("group-{component}-{stamp}")),
            )
        })
        .await?;
        self.ensure_partition_local(EnsureGroupRequest { topic, partition })
            .await
    }
}
