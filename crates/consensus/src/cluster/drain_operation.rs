use super::operation_error::{OperationAttempt, OperationAttemptError};
use super::*;
use crate::{
    DrainGroupPlan, DrainProgress, MaintenanceOperation, OperationPhase, OperationProgress,
    OperationState,
};

impl ClusterRuntime {
    pub(super) async fn run_drain_operation(
        &self,
        operation: &MaintenanceOperation,
        node_id: NodeId,
    ) -> OperationAttempt<bool> {
        let progress = match &operation.progress {
            OperationProgress::None => {
                let progress = self.plan_node_drain(node_id)?;
                return self
                    .initialize_node_drain(operation.id, node_id, progress)
                    .await;
            }
            OperationProgress::Drain(progress) => progress.clone(),
        };

        if let Some(plan) = progress.groups.get(progress.current).cloned() {
            self.validate_rebalance_target(plan.group_id, &plan.voters)?;
            return self
                .run_drain_partition_step(operation, progress, plan)
                .await;
        }
        if let Some(replacement) = progress
            .metadata_replacement
            .filter(|_| !progress.metadata_completed)
        {
            return self
                .run_drain_metadata_step(operation, node_id, replacement, progress)
                .await;
        }
        self.finish_drain(operation.id, progress).await
    }

    fn plan_node_drain(&self, node_id: NodeId) -> OperationAttempt<DrainProgress> {
        let snapshot = self.metadata.snapshot();
        if !snapshot.nodes.contains_key(&node_id) {
            return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "drain node {node_id} is not configured"
            )));
        }
        let mut loads: BTreeMap<NodeId, usize> = snapshot.nodes.keys().map(|id| (*id, 0)).collect();
        for partition in snapshot.topics.values().flat_map(|topic| &topic.partitions) {
            for replica in &partition.replicas {
                *loads.entry(*replica).or_default() += 1;
            }
        }
        let mut partitions: Vec<_> = snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .filter(|partition| partition.replicas.contains(&node_id))
            .cloned()
            .collect();
        partitions.sort_by_key(PartitionDescriptor::global_id);
        let mut groups = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let mut candidates: Vec<_> = snapshot
                .nodes
                .values()
                .filter(|candidate| {
                    candidate.id != node_id
                        && !snapshot.drained_nodes.contains(&candidate.id)
                        && !partition.replicas.contains(&candidate.id)
                        && snapshot
                            .node_health
                            .get(&candidate.id)
                            .is_some_and(|health| health.available && health.storage_eligible)
                        && !super::automation_plan::maintenance_active(
                            &snapshot,
                            candidate.id,
                            super::automation_plan::now_i64(),
                        )
                })
                .collect();
            candidates.sort_by_key(|candidate| {
                let duplicate_domain = partition
                    .replicas
                    .iter()
                    .filter(|replica| **replica != node_id)
                    .any(|replica| {
                        snapshot.nodes[replica].failure_domain == candidate.failure_domain
                    });
                (
                    usize::from(duplicate_domain),
                    loads.get(&candidate.id).copied().unwrap_or_default(),
                    candidate.id,
                )
            });
            let replacement = candidates
                .into_iter()
                .find(|candidate| {
                    partition.replication_factor != 5
                        || partition
                            .replicas
                            .iter()
                            .filter(|replica| **replica != node_id)
                            .all(|replica| {
                                snapshot.nodes[replica].failure_domain != candidate.failure_domain
                            })
                })
                .ok_or_else(|| {
                    OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "partition group {} has no healthy replacement for drain",
                        partition.global_id()
                    ))
                })?;
            let group_id = partition.global_id();
            let mut voters = partition.replicas;
            voters.remove(&node_id);
            voters.insert(replacement.id);
            *loads.entry(replacement.id).or_default() += 1;
            groups.push(DrainGroupPlan { group_id, voters });
        }

        let metadata_voters: BTreeSet<_> = self
            .metadata_group()
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        let metadata_replacement = if metadata_voters.contains(&node_id) {
            snapshot
                .nodes
                .values()
                .filter(|candidate| {
                    !snapshot.drained_nodes.contains(&candidate.id)
                        && !metadata_voters.contains(&candidate.id)
                        && !super::automation_plan::maintenance_active(
                            &snapshot,
                            candidate.id,
                            super::automation_plan::now_i64(),
                        )
                        && snapshot
                            .node_health
                            .get(&candidate.id)
                            .is_some_and(|health| health.available && health.storage_eligible)
                        && (metadata_voters.len() != 5
                            || metadata_voters
                                .iter()
                                .filter(|voter| **voter != node_id)
                                .all(|voter| {
                                    snapshot.nodes[voter].failure_domain != candidate.failure_domain
                                }))
                })
                .min_by_key(|candidate| candidate.id)
                .map(|candidate| candidate.id)
                .ok_or_else(|| {
                    OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "metadata group has no healthy replacement for drain"
                    ))
                })?
                .into()
        } else {
            None
        };
        Ok(DrainProgress {
            groups,
            current: 0,
            metadata_replacement,
            metadata_completed: false,
        })
    }

    async fn initialize_node_drain(
        &self,
        operation_id: u64,
        node_id: NodeId,
        progress: DrainProgress,
    ) -> OperationAttempt<bool> {
        let complete = progress.groups.is_empty() && progress.metadata_replacement.is_none();
        let phase = if complete {
            OperationPhase::Completed
        } else {
            OperationPhase::TransferLeader
        };
        let state = if complete {
            OperationState::Completed
        } else {
            OperationState::Running
        };
        let response = self
            .metadata_group()
            .write(QueueCommand::Batch {
                commands: vec![
                    QueueCommand::SetNodeDrained {
                        node_id,
                        drained: true,
                    },
                    QueueCommand::UpdateOperation {
                        operation_id,
                        phase,
                        state,
                        now_ms: super::automation_plan::now_i64(),
                        error: None,
                        progress: Some(OperationProgress::Drain(progress)),
                    },
                ],
            })
            .await
            .map_err(OperationAttemptError::retryable)?;
        ensure_response(&response).map_err(OperationAttemptError::retryable)?;
        Ok(complete)
    }

    async fn run_drain_partition_step(
        &self,
        operation: &MaintenanceOperation,
        mut progress: DrainProgress,
        plan: DrainGroupPlan,
    ) -> OperationAttempt<bool> {
        if operation.phase == OperationPhase::Retire {
            self.apply_rebalance_step(plan.group_id, plan.voters, OperationPhase::Retire)
                .await
                .map_err(OperationAttemptError::retryable)?;
            progress.current = progress.current.saturating_add(1);
            return self.advance_drain_item(operation.id, progress).await;
        }
        let next = next_membership_phase(operation.phase)?;
        self.apply_rebalance_step(plan.group_id, plan.voters, operation.phase)
            .await
            .map_err(OperationAttemptError::retryable)?;
        self.persist_drain(operation.id, next, OperationState::Running, progress)
            .await?;
        Ok(false)
    }

    async fn run_drain_metadata_step(
        &self,
        operation: &MaintenanceOperation,
        removed: NodeId,
        replacement: NodeId,
        mut progress: DrainProgress,
    ) -> OperationAttempt<bool> {
        if operation.phase == OperationPhase::Retire {
            progress.metadata_completed = true;
            return self.finish_drain(operation.id, progress).await;
        }
        let next = next_membership_phase(operation.phase)?;
        self.apply_metadata_membership_step(removed, replacement, operation.phase)
            .await?;
        self.persist_drain(operation.id, next, OperationState::Running, progress)
            .await?;
        Ok(false)
    }

    async fn apply_metadata_membership_step(
        &self,
        removed: NodeId,
        replacement: NodeId,
        phase: OperationPhase,
    ) -> OperationAttempt<()> {
        let group = self.metadata_group();
        let membership = group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .clone();
        let current: BTreeSet<_> = membership.voter_ids().collect();
        let mut target = current.clone();
        target.remove(&removed);
        target.insert(replacement);
        match phase {
            OperationPhase::TransferLeader => {
                if group.raft().metrics().borrow().current_leader == Some(removed) {
                    let leader = current
                        .iter()
                        .copied()
                        .find(|node| *node != removed)
                        .ok_or_else(|| {
                            OperationAttemptError::needs_operator(anyhow::anyhow!(
                                "metadata group has no safe leadership target"
                            ))
                        })?;
                    group
                        .transfer_leadership(leader)
                        .await
                        .map_err(OperationAttemptError::retryable)?;
                }
            }
            OperationPhase::AddLearner => group
                .add_learner_nonblocking_local(replacement)
                .await
                .map_err(OperationAttemptError::retryable)?,
            OperationPhase::CatchUp => group
                .add_learner_local(replacement)
                .await
                .map_err(OperationAttemptError::retryable)?,
            OperationPhase::JointConsensus => {
                let configs = membership.get_joint_config();
                if configs.len() == 1 && configs[0] != target {
                    group
                        .change_membership(target, true)
                        .await
                        .map_err(OperationAttemptError::retryable)?;
                } else if configs.len() > 1 && configs.last() != Some(&target) {
                    return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "metadata drain found an unrelated joint membership"
                    )));
                }
            }
            OperationPhase::RemoveOld => {
                let configs = membership.get_joint_config();
                if configs.len() > 1 {
                    if configs.last() != Some(&target) {
                        return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                            "metadata drain found an unrelated joint membership"
                        )));
                    }
                    group
                        .change_membership(target, false)
                        .await
                        .map_err(OperationAttemptError::retryable)?;
                } else if configs.first() != Some(&target) {
                    return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "metadata drain target membership was not committed"
                    )));
                }
            }
            _ => {
                return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "phase {phase:?} is not a metadata drain step"
                )))
            }
        }
        Ok(())
    }

    async fn advance_drain_item(
        &self,
        operation_id: u64,
        progress: DrainProgress,
    ) -> OperationAttempt<bool> {
        if progress.current < progress.groups.len()
            || progress
                .metadata_replacement
                .is_some_and(|_| !progress.metadata_completed)
        {
            self.persist_drain(
                operation_id,
                OperationPhase::TransferLeader,
                OperationState::Running,
                progress,
            )
            .await?;
            Ok(false)
        } else {
            self.finish_drain(operation_id, progress).await
        }
    }

    async fn finish_drain(
        &self,
        operation_id: u64,
        progress: DrainProgress,
    ) -> OperationAttempt<bool> {
        self.persist_drain(
            operation_id,
            OperationPhase::Completed,
            OperationState::Completed,
            progress,
        )
        .await?;
        Ok(true)
    }

    async fn persist_drain(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        state: OperationState,
        progress: DrainProgress,
    ) -> OperationAttempt<()> {
        self.update_operation_progress(
            operation_id,
            phase,
            state,
            None,
            OperationProgress::Drain(progress),
        )
        .await
        .map_err(OperationAttemptError::retryable)
    }
}

fn next_membership_phase(phase: OperationPhase) -> OperationAttempt<OperationPhase> {
    match phase {
        OperationPhase::TransferLeader => Ok(OperationPhase::AddLearner),
        OperationPhase::AddLearner => Ok(OperationPhase::CatchUp),
        OperationPhase::CatchUp => Ok(OperationPhase::JointConsensus),
        OperationPhase::JointConsensus => Ok(OperationPhase::RemoveOld),
        OperationPhase::RemoveOld => Ok(OperationPhase::Retire),
        _ => Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
            "drain has invalid membership phase {phase:?}"
        ))),
    }
}
