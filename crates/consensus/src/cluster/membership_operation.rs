use super::operation_error::{OperationAttempt, OperationAttemptError};
use super::*;
use crate::{MaintenanceOperation, OperationPhase, OperationState};

impl ClusterRuntime {
    pub(super) async fn run_rebalance_operation(
        &self,
        operation: &MaintenanceOperation,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
    ) -> OperationAttempt<bool> {
        self.validate_rebalance_target(group_id, &voters)?;
        let (step, next) = match operation.phase {
            OperationPhase::Planned => {
                self.persist_membership_phase(operation.id, OperationPhase::TransferLeader)
                    .await?;
                return Ok(false);
            }
            OperationPhase::TransferLeader => {
                (OperationPhase::TransferLeader, OperationPhase::AddLearner)
            }
            OperationPhase::AddLearner => (OperationPhase::AddLearner, OperationPhase::CatchUp),
            OperationPhase::CatchUp => (OperationPhase::CatchUp, OperationPhase::JointConsensus),
            OperationPhase::JointConsensus => {
                (OperationPhase::JointConsensus, OperationPhase::RemoveOld)
            }
            OperationPhase::RemoveOld => (OperationPhase::RemoveOld, OperationPhase::Retire),
            OperationPhase::Retire => {
                self.apply_rebalance_step(group_id, voters, OperationPhase::Retire)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.complete_operation(operation.id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                return Ok(true);
            }
            OperationPhase::Completed => return Ok(true),
            phase => {
                return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "rebalance operation has invalid phase {phase:?}"
                )))
            }
        };
        self.apply_rebalance_step(group_id, voters, step)
            .await
            .map_err(OperationAttemptError::retryable)?;
        self.persist_membership_phase(operation.id, next).await?;
        Ok(false)
    }

    pub(super) async fn run_metadata_replacement_operation(
        &self,
        operation: &MaintenanceOperation,
        removed: NodeId,
        replacement: NodeId,
    ) -> OperationAttempt<bool> {
        if self.node(replacement).is_none() {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "metadata replacement node {replacement} is not configured"
            )));
        }
        let group = self.metadata_group();
        let current: BTreeSet<_> = group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        let mut target = current.clone();
        target.remove(&removed);
        target.insert(replacement);
        if !matches!(target.len(), 3 | 5) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "metadata replacement would create an invalid voter count"
            )));
        }
        match operation.phase {
            OperationPhase::Planned => {
                self.persist_membership_phase(operation.id, OperationPhase::TransferLeader)
                    .await?;
            }
            OperationPhase::TransferLeader => {
                if group.raft().metrics().borrow().current_leader == Some(removed) {
                    let target_leader = current
                        .iter()
                        .copied()
                        .find(|node| *node != removed)
                        .ok_or_else(|| {
                            OperationAttemptError::needs_operator(anyhow::anyhow!(
                                "metadata group has no safe leadership target"
                            ))
                        })?;
                    group
                        .transfer_leadership(target_leader)
                        .await
                        .map_err(OperationAttemptError::retryable)?;
                }
                self.persist_membership_phase(operation.id, OperationPhase::AddLearner)
                    .await?;
            }
            OperationPhase::AddLearner => {
                group
                    .add_learner_nonblocking_local(replacement)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.persist_membership_phase(operation.id, OperationPhase::CatchUp)
                    .await?;
            }
            OperationPhase::CatchUp => {
                group
                    .add_learner_local(replacement)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.persist_membership_phase(operation.id, OperationPhase::JointConsensus)
                    .await?;
            }
            OperationPhase::JointConsensus => {
                group
                    .change_membership(target.clone(), true)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.persist_membership_phase(operation.id, OperationPhase::RemoveOld)
                    .await?;
            }
            OperationPhase::RemoveOld => {
                group
                    .change_membership(target, false)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.persist_membership_phase(operation.id, OperationPhase::Retire)
                    .await?;
            }
            OperationPhase::Retire => {
                self.complete_operation(operation.id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                return Ok(true);
            }
            OperationPhase::Completed => return Ok(true),
            phase => {
                return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "metadata replacement has invalid phase {phase:?}"
                )))
            }
        }
        Ok(false)
    }

    async fn persist_membership_phase(
        &self,
        operation_id: u64,
        phase: OperationPhase,
    ) -> OperationAttempt<()> {
        self.update_operation(operation_id, phase, OperationState::Running, None)
            .await
            .map_err(OperationAttemptError::retryable)
    }

    pub(super) fn validate_rebalance_target(
        &self,
        group_id: crate::GlobalGroupId,
        voters: &BTreeSet<NodeId>,
    ) -> OperationAttempt<()> {
        let (_, partition) = self.metadata.partition(group_id).ok_or_else(|| {
            OperationAttemptError::needs_operator(anyhow::anyhow!(
                "partition group {group_id} no longer exists"
            ))
        })?;
        if voters.len() != partition.replication_factor as usize {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "partition group {group_id} requires {} voters",
                partition.replication_factor
            )));
        }
        if let Some(node_id) = voters.iter().find(|node| self.node(**node).is_none()) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "rebalance target node {node_id} is not configured"
            )));
        }
        let snapshot = self.metadata.snapshot();
        for node_id in voters.difference(&partition.replicas) {
            if snapshot.drained_nodes.contains(node_id) {
                return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "rebalance target node {node_id} is drained"
                )));
            }
            if super::automation_plan::maintenance_active(
                &snapshot,
                *node_id,
                super::automation_plan::now_i64(),
            ) {
                return Err(OperationAttemptError::retryable(anyhow::anyhow!(
                    "rebalance target node {node_id} is in maintenance"
                )));
            }
            if !snapshot
                .node_health
                .get(node_id)
                .is_some_and(|health| health.available && health.storage_eligible)
            {
                return Err(OperationAttemptError::retryable(anyhow::anyhow!(
                    "rebalance target node {node_id} is temporarily unavailable"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_transfer_target(
        &self,
        group: GroupKey,
        node_id: NodeId,
    ) -> OperationAttempt<()> {
        let voters = if group == self.metadata_group().group_key() {
            self.metadata_group()
                .raft()
                .metrics()
                .borrow()
                .membership_config
                .voter_ids()
                .collect()
        } else {
            let group_id = group.partition_id().ok_or_else(|| {
                OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "unsupported leadership target {group}"
                ))
            })?;
            self.metadata
                .partition(group_id)
                .ok_or_else(|| {
                    OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "partition group {group_id} no longer exists"
                    ))
                })?
                .1
                .replicas
        };
        if !voters.contains(&node_id) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "leadership target node {node_id} is not a voter of group {group}"
            )));
        }
        let snapshot = self.metadata.snapshot();
        if snapshot.drained_nodes.contains(&node_id) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "leadership target node {node_id} is drained"
            )));
        }
        if !snapshot
            .node_health
            .get(&node_id)
            .is_some_and(|health| health.available)
        {
            return Err(OperationAttemptError::retryable(anyhow::anyhow!(
                "leadership target node {node_id} is temporarily unavailable"
            )));
        }
        Ok(())
    }

    pub(super) fn validate_repair_target(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> OperationAttempt<()> {
        let (_, partition) = self.metadata.partition(group_id).ok_or_else(|| {
            OperationAttemptError::needs_operator(anyhow::anyhow!(
                "partition group {group_id} no longer exists"
            ))
        })?;
        if !partition.replicas.contains(&node_id) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "repair node {node_id} is not a replica of group {group_id}"
            )));
        }
        Ok(())
    }

    pub(crate) async fn apply_rebalance_step(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
        phase: OperationPhase,
    ) -> anyhow::Result<()> {
        let (_, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        let eligible: BTreeSet<_> = partition.replicas.union(&voters).copied().collect();
        let leader = self.partition_leader_among(&partition, &eligible).await?;
        if leader != self.node_id {
            let leader = self
                .node(leader)
                .ok_or_else(|| anyhow::anyhow!("rebalance leader is not configured"))?;
            let _timer = self.forward_latency.timer();
            let response: OperationResponse = crate::post_binary(
                &self.client,
                format!(
                    "{}/raft/groups/{}/rebalance-step",
                    leader.addr.trim_end_matches('/'),
                    partition.group_key(),
                ),
                &RebalanceStepRequest { voters, phase },
            )
            .await?;
            return response
                .error
                .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)));
        }
        self.apply_rebalance_step_local(group_id, voters, phase)
            .await
    }

    pub async fn apply_rebalance_step_local(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
        phase: OperationPhase,
    ) -> anyhow::Result<()> {
        let (topic, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        let group = self
            .partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} is not hosted locally"))?;
        let added: Vec<_> = voters.difference(&partition.replicas).copied().collect();
        let removed: Vec<_> = partition.replicas.difference(&voters).copied().collect();
        match phase {
            OperationPhase::TransferLeader => {
                let current_leader = group.raft().metrics().borrow().current_leader;
                if current_leader.is_some_and(|leader| !voters.contains(&leader)) {
                    let target = partition
                        .replicas
                        .intersection(&voters)
                        .copied()
                        .next()
                        .ok_or_else(|| {
                            anyhow::anyhow!("rebalance has no safe leadership target")
                        })?;
                    group.transfer_leadership(target).await?;
                }
            }
            OperationPhase::AddLearner => {
                let desired = PartitionDescriptor {
                    replicas: voters.clone(),
                    ..partition.clone()
                };
                for node_id in added {
                    self.ensure_replica_host(&topic, &desired, node_id).await?;
                    group.add_learner_nonblocking_local(node_id).await?;
                }
            }
            OperationPhase::CatchUp => {
                for node_id in added {
                    group.add_learner_local(node_id).await?;
                }
            }
            OperationPhase::JointConsensus => {
                let membership = group
                    .raft()
                    .metrics()
                    .borrow()
                    .membership_config
                    .membership()
                    .clone();
                let configs = membership.get_joint_config();
                if configs.len() == 1 && configs[0] != voters {
                    group.change_membership_local(voters, true).await?;
                } else if configs.len() > 1 && configs.last() != Some(&voters) {
                    anyhow::bail!("rebalance found an unrelated joint membership");
                }
            }
            OperationPhase::RemoveOld => {
                let membership = group
                    .raft()
                    .metrics()
                    .borrow()
                    .membership_config
                    .membership()
                    .clone();
                let configs = membership.get_joint_config();
                if configs.len() > 1 {
                    if configs.last() != Some(&voters) {
                        anyhow::bail!("rebalance found an unrelated joint membership");
                    }
                    group.change_membership_local(voters, false).await?;
                } else if configs.first() != Some(&voters) {
                    anyhow::bail!("rebalance target membership was not committed");
                }
            }
            OperationPhase::Retire => {
                for node_id in removed {
                    self.retire_replica(group_id, node_id).await?;
                }
                let response = self
                    .metadata_group()
                    .write(QueueCommand::UpdatePartitionReplicas {
                        group_id,
                        replicas: voters,
                    })
                    .await?;
                ensure_response(&response)?;
            }
            _ => anyhow::bail!("phase {phase:?} is not a rebalance step"),
        }
        Ok(())
    }
}
