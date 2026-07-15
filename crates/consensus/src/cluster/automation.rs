use super::automation_plan::*;
use super::operation_error::{OperationAttempt, OperationAttemptError};
use super::*;
use crate::{
    MaintenanceOperation, OperationKind, OperationPhase, OperationState, PartitionLifecycle,
};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct RebalancePlanItem {
    pub group_id: crate::GlobalGroupId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub voters: BTreeSet<NodeId>,
}

impl ClusterRuntime {
    pub(super) async fn persist_health_observations(
        &self,
        healthy_nodes: &BTreeSet<NodeId>,
        disk_statuses: &BTreeMap<NodeId, super::disk::DiskStatus>,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        if self
            .metadata_group()
            .raft()
            .metrics()
            .borrow()
            .current_leader
            != Some(self.node_id)
        {
            return Ok(());
        }
        let commands = self
            .metadata
            .snapshot()
            .nodes
            .keys()
            .map(|node_id| {
                let disk = disk_statuses
                    .get(node_id)
                    .copied()
                    .unwrap_or(super::disk::DiskStatus {
                        used_percent: 100,
                        free_bytes: 0,
                        eligible: false,
                    });
                QueueCommand::ObserveNodeHealth {
                    node_id: *node_id,
                    healthy: healthy_nodes.contains(node_id),
                    disk_used_percent: disk.used_percent,
                    disk_free_bytes: disk.free_bytes,
                    storage_eligible: disk.eligible,
                    now_ms,
                }
            })
            .collect();
        let response = self
            .metadata_group()
            .write(QueueCommand::Batch { commands })
            .await?;
        ensure_response(&response)
    }

    pub(super) async fn reconcile_automation(&self) -> anyhow::Result<usize> {
        if self
            .metadata_group()
            .raft()
            .metrics()
            .borrow()
            .current_leader
            != Some(self.node_id)
        {
            return Ok(0);
        }
        let snapshot = self.metadata.snapshot();
        if self.automation.enabled && snapshot.automation_enabled {
            let now = now_i64();
            self.schedule_offline_replacements(&snapshot, now).await?;
            self.schedule_replica_balance(now).await?;
            self.schedule_leader_balance(now).await?;
        }
        // Pausing automation prevents new membership work. Already persisted
        // operations continue so a joint-consensus change reaches a safe edge.
        self.reconcile_maintenance_operations().await
    }

    async fn schedule_offline_replacements(
        &self,
        snapshot: &crate::ClusterMetadata,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        let grace_ms = seconds_ms(self.automation.node_down_grace_seconds);
        let failed: Vec<_> = snapshot
            .node_health
            .iter()
            .filter(|(node_id, health)| {
                !snapshot.drained_nodes.contains(node_id)
                    && !maintenance_active(snapshot, **node_id, now_ms)
                    && health
                        .unavailable_since_ms
                        .is_some_and(|since| now_ms.saturating_sub(since) >= grace_ms)
            })
            .map(|(node_id, _)| *node_id)
            .collect();
        for failed_node in failed {
            for partition in snapshot
                .topics
                .values()
                .flat_map(|topic| &topic.partitions)
                .filter(|partition| {
                    partition.lifecycle == PartitionLifecycle::Active
                        && partition.replicas.contains(&failed_node)
                })
            {
                if operation_for_group(snapshot, partition.global_id()) {
                    continue;
                }
                let replacement = replacement_candidate(
                    snapshot,
                    partition,
                    failed_node,
                    now_ms,
                    self.automation.node_stabilization_seconds,
                );
                let operation = OperationKind::ReplaceOfflineReplica {
                    group_id: partition.global_id(),
                    node_id: failed_node,
                    replacement,
                };
                let created = self.create_operation(operation, now_ms).await?;
                if replacement.is_none() {
                    self.update_operation(
                        created.id,
                        OperationPhase::Planned,
                        OperationState::NeedsOperator,
                        Some(
                            "no healthy replacement satisfies RF and failure-domain policy".into(),
                        ),
                    )
                    .await?;
                }
            }
            if self.automation.auto_replace_metadata {
                self.schedule_metadata_replacement(snapshot, failed_node, now_ms)
                    .await?;
            }
        }
        Ok(())
    }

    async fn schedule_metadata_replacement(
        &self,
        snapshot: &crate::ClusterMetadata,
        failed_node: NodeId,
        now_ms: i64,
    ) -> anyhow::Result<()> {
        let voters: BTreeSet<_> = self
            .metadata_group()
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        if !voters.contains(&failed_node)
            || snapshot.operations.values().any(|operation| {
                matches!(
                    operation.kind,
                    OperationKind::ReplaceMetadataVoter { node_id, .. }
                        if node_id == failed_node
                ) && !terminal(operation.state)
            })
        {
            return Ok(());
        }
        let replacement =
            eligible_nodes(snapshot, now_ms, self.automation.node_stabilization_seconds)
                .into_iter()
                .find(|candidate| !voters.contains(candidate));
        let operation = self
            .create_operation(
                OperationKind::ReplaceMetadataVoter {
                    node_id: failed_node,
                    replacement,
                },
                now_ms,
            )
            .await?;
        if replacement.is_none() {
            self.update_operation(
                operation.id,
                OperationPhase::Planned,
                OperationState::NeedsOperator,
                Some("metadata group has no stable replacement voter".into()),
            )
            .await?;
        }
        Ok(())
    }

    async fn schedule_replica_balance(&self, now_ms: i64) -> anyhow::Result<()> {
        let snapshot = self.metadata.snapshot();
        let Some(item) = build_rebalance_plan(
            &snapshot,
            now_ms,
            self.automation.node_stabilization_seconds,
            self.automation.group_cooldown_seconds,
        )
        .into_iter()
        .next() else {
            return Ok(());
        };
        self.create_operation(
            OperationKind::RebalanceGroup {
                group_id: item.group_id,
                voters: item.voters,
            },
            now_ms,
        )
        .await?;
        Ok(())
    }

    async fn schedule_leader_balance(&self, now_ms: i64) -> anyhow::Result<()> {
        let snapshot = self.metadata.snapshot();
        let eligible: BTreeSet<_> = eligible_nodes(
            &snapshot,
            now_ms,
            self.automation.node_stabilization_seconds,
        )
        .into_iter()
        .collect();
        if eligible.len() < 2 {
            return Ok(());
        }
        let mut leaders = Vec::new();
        let mut loads: BTreeMap<_, usize> = eligible.iter().map(|node| (*node, 0)).collect();
        for partition in snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
        {
            if let Ok(leader) = self.partition_leader(partition).await {
                if let Some(load) = loads.get_mut(&leader) {
                    *load += 1;
                    leaders.push((partition, leader));
                }
            }
        }
        let Some(max_load) = loads.values().max().copied() else {
            return Ok(());
        };
        let cooldown_ms = seconds_ms(self.automation.group_cooldown_seconds);
        for (partition, leader) in leaders {
            if loads[&leader] != max_load
                || operation_for_group(&snapshot, partition.global_id())
                || group_in_cooldown(&snapshot, partition.global_id(), now_ms, cooldown_ms)
            {
                continue;
            }
            let target = partition
                .replicas
                .iter()
                .copied()
                .filter(|node| eligible.contains(node))
                .min_by_key(|node| (loads.get(node).copied().unwrap_or(usize::MAX), *node));
            let Some(target) = target else { continue };
            if target != leader && loads[&leader] > loads[&target] + 1 {
                self.create_operation(
                    OperationKind::TransferLeader {
                        group: partition.group_key(),
                        node_id: target,
                    },
                    now_ms,
                )
                .await?;
                break;
            }
        }
        Ok(())
    }

    async fn reconcile_maintenance_operations(&self) -> anyhow::Result<usize> {
        let mut snapshot = self.metadata.snapshot();
        let recovered: Vec<_> = snapshot
            .operations
            .values()
            .filter(|operation| operation.state == OperationState::NeedsOperator)
            .filter_map(|operation| match &operation.kind {
                OperationKind::ReplaceOfflineReplica {
                    node_id,
                    replacement: None,
                    ..
                }
                | OperationKind::ReplaceMetadataVoter {
                    node_id,
                    replacement: None,
                } if snapshot
                    .node_health
                    .get(node_id)
                    .is_some_and(|health| health.available) =>
                {
                    Some(operation.id)
                }
                _ => None,
            })
            .collect();
        for operation_id in recovered {
            self.complete_operation(operation_id).await?;
            tracing::info!(
                audit_event = "offline_operation_recovered",
                operation_id,
                "node returned before a safe replacement became available"
            );
        }
        snapshot = self.metadata.snapshot();
        let mut node_loads = BTreeMap::<NodeId, usize>::new();
        let mut operations = Vec::new();
        for operation in snapshot.operations.values().filter(|operation| {
            !matches!(operation.kind, OperationKind::ExpandPartitions { .. })
                && operation.state == OperationState::Running
        }) {
            if operations.len() >= self.automation.max_concurrent_migrations {
                break;
            }
            let nodes = operation_nodes(&snapshot, operation);
            if nodes.iter().any(|node| {
                node_loads.get(node).copied().unwrap_or_default()
                    >= self.automation.max_migrations_per_node
            }) {
                continue;
            }
            for node in nodes {
                *node_loads.entry(node).or_default() += 1;
            }
            operations.push(operation.clone());
        }
        let mut completed = 0;
        for operation in operations {
            match self.execute_maintenance_operation(&operation).await {
                Ok(true) => completed += 1,
                Ok(false) => {}
                Err(error) => {
                    let state = if error.is_retryable() {
                        OperationState::Running
                    } else {
                        OperationState::NeedsOperator
                    };
                    self.update_operation(
                        operation.id,
                        operation.phase,
                        state,
                        Some(error.to_string()),
                    )
                    .await?;
                    tracing::warn!(
                        audit_event = "operation_attempt_failed",
                        operation_id = operation.id,
                        ?state,
                        %error,
                        "maintenance operation attempt failed"
                    );
                }
            }
        }
        Ok(completed)
    }

    async fn execute_maintenance_operation(
        &self,
        operation: &MaintenanceOperation,
    ) -> OperationAttempt<bool> {
        match &operation.kind {
            OperationKind::ReplaceOfflineReplica {
                group_id,
                node_id,
                replacement: Some(replacement),
            } => {
                let (_, partition) = self.metadata.partition(*group_id).ok_or_else(|| {
                    OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "partition group no longer exists"
                    ))
                })?;
                let mut voters = partition.replicas;
                voters.remove(node_id);
                voters.insert(*replacement);
                self.run_rebalance_operation(operation, *group_id, voters)
                    .await
            }
            OperationKind::RebalanceGroup { group_id, voters } => {
                self.run_rebalance_operation(operation, *group_id, voters.clone())
                    .await
            }
            OperationKind::ReplaceMetadataVoter {
                node_id,
                replacement: Some(replacement),
            } => {
                self.run_metadata_replacement_operation(operation, *node_id, *replacement)
                    .await
            }
            OperationKind::TransferLeader { group, node_id } => {
                self.validate_transfer_target(*group, *node_id)?;
                self.transfer_group_leadership(*group, *node_id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.complete_operation(operation.id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                Ok(true)
            }
            OperationKind::RepairReplica { group_id, node_id } => {
                self.validate_repair_target(*group_id, *node_id)?;
                self.repair_replica(*group_id, *node_id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                self.complete_operation(operation.id)
                    .await
                    .map_err(OperationAttemptError::retryable)?;
                Ok(true)
            }
            OperationKind::DrainNode { node_id } => {
                self.run_drain_operation(operation, *node_id).await
            }
            OperationKind::ReplaceOfflineReplica {
                replacement: None, ..
            }
            | OperationKind::ReplaceMetadataVoter {
                replacement: None, ..
            } => Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                "operation needs an eligible replacement node"
            ))),
            OperationKind::ExpandPartitions { .. } => Ok(false),
        }
    }
}
