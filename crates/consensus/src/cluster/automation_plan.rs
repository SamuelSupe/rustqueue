use super::automation::RebalancePlanItem;
use super::*;
use crate::{MaintenanceOperation, OperationKind, OperationState, PartitionLifecycle};

pub(super) fn build_rebalance_plan(
    snapshot: &crate::ClusterMetadata,
    now_ms: i64,
    stabilization_seconds: u64,
    cooldown_seconds: u64,
) -> Vec<RebalancePlanItem> {
    let eligible = eligible_nodes(snapshot, now_ms, stabilization_seconds);
    if eligible.len() < 3 {
        return Vec::new();
    }
    let mut loads: BTreeMap<NodeId, usize> = eligible.iter().map(|node| (*node, 0)).collect();
    let mut assignments: BTreeMap<crate::GlobalGroupId, BTreeSet<NodeId>> = BTreeMap::new();
    for partition in snapshot
        .topics
        .values()
        .flat_map(|topic| &topic.partitions)
        .filter(|partition| partition.lifecycle != PartitionLifecycle::Retired)
    {
        assignments.insert(partition.global_id(), partition.replicas.clone());
        for replica in &partition.replicas {
            if let Some(load) = loads.get_mut(replica) {
                *load += 1;
            }
        }
    }
    let cooldown_ms = seconds_ms(cooldown_seconds);
    let mut used = BTreeSet::new();
    let mut plan = Vec::new();
    loop {
        let Some((&high, &high_load)) = loads.iter().max_by_key(|(node, load)| (*load, *node))
        else {
            break;
        };
        let Some((&low, &low_load)) = loads.iter().min_by_key(|(node, load)| (*load, *node)) else {
            break;
        };
        if high_load <= low_load + 1 {
            break;
        }
        let candidate = snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .filter(|partition| {
                partition.lifecycle == PartitionLifecycle::Active
                    && !used.contains(&partition.global_id())
                    && !operation_for_group(snapshot, partition.global_id())
                    && !group_in_cooldown(snapshot, partition.global_id(), now_ms, cooldown_ms)
            })
            .find(|partition| {
                let voters = &assignments[&partition.global_id()];
                voters.contains(&high)
                    && !voters.contains(&low)
                    && replacement_preserves_domains(snapshot, partition, high, low)
            });
        let Some(partition) = candidate else {
            break;
        };
        let group_id = partition.global_id();
        let voters = assignments.get_mut(&group_id).unwrap();
        voters.remove(&high);
        voters.insert(low);
        *loads.get_mut(&high).unwrap() -= 1;
        *loads.get_mut(&low).unwrap() += 1;
        used.insert(group_id);
        plan.push(RebalancePlanItem {
            group_id,
            from_node: high,
            to_node: low,
            voters: voters.clone(),
        });
    }
    plan
}

pub(super) fn validate_operation(
    snapshot: &crate::ClusterMetadata,
    kind: &OperationKind,
) -> anyhow::Result<()> {
    match kind {
        OperationKind::RebalanceGroup { group_id, voters } => {
            let partition = snapshot
                .topics
                .values()
                .flat_map(|topic| &topic.partitions)
                .find(|partition| partition.global_id() == *group_id)
                .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
            if voters.len() != partition.replication_factor as usize
                || voters.iter().any(|node| !snapshot.nodes.contains_key(node))
            {
                anyhow::bail!("rebalance voters do not satisfy the partition RF or allowlist");
            }
            if voters
                .iter()
                .any(|node| snapshot.drained_nodes.contains(node))
            {
                anyhow::bail!("rebalance voters contain a drained node");
            }
        }
        OperationKind::DrainNode { node_id } => {
            if !snapshot.nodes.contains_key(node_id) {
                anyhow::bail!("drain node is not in the static allowlist");
            }
        }
        OperationKind::RepairReplica { group_id, node_id } => {
            let partition = snapshot
                .topics
                .values()
                .flat_map(|topic| &topic.partitions)
                .find(|partition| partition.global_id() == *group_id)
                .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
            if !partition.replicas.contains(node_id) {
                anyhow::bail!("repair node is not a replica of the group");
            }
        }
        OperationKind::TransferLeader {
            group: GroupKey::Partition(group_id),
            node_id,
        } => {
            let partition = snapshot
                .topics
                .values()
                .flat_map(|topic| &topic.partitions)
                .find(|partition| partition.global_id() == *group_id)
                .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
            if !partition.replicas.contains(node_id) {
                anyhow::bail!("leadership target is not a group replica");
            }
            if snapshot.drained_nodes.contains(node_id) {
                anyhow::bail!("leadership target is drained");
            }
        }
        OperationKind::TransferLeader {
            group: GroupKey::CellMetadata { .. },
            node_id,
        } if snapshot.drained_nodes.contains(node_id) => {
            anyhow::bail!("metadata leadership target is drained");
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn operation_conflicts(snapshot: &crate::ClusterMetadata, kind: &OperationKind) -> bool {
    snapshot
        .operations
        .values()
        .filter(|operation| !terminal(operation.state))
        .any(|operation| match (kind, &operation.kind) {
            (
                OperationKind::DrainNode { node_id: left },
                OperationKind::DrainNode { node_id: right },
            ) => left == right,
            (OperationKind::DrainNode { node_id }, other)
            | (other, OperationKind::DrainNode { node_id }) => {
                kind_nodes(snapshot, other).contains(node_id)
            }
            _ => operation_group(kind)
                .zip(operation_group(&operation.kind))
                .is_some_and(|(left, right)| left == right),
        })
}

pub(super) fn operation_group(kind: &OperationKind) -> Option<GroupKey> {
    match kind {
        OperationKind::RebalanceGroup { group_id, .. }
        | OperationKind::RepairReplica { group_id, .. }
        | OperationKind::ReplaceOfflineReplica { group_id, .. } => {
            Some(GroupKey::Partition(*group_id))
        }
        OperationKind::TransferLeader { group, .. } => Some(*group),
        _ => None,
    }
}

pub(super) fn operation_nodes(
    snapshot: &crate::ClusterMetadata,
    operation: &MaintenanceOperation,
) -> BTreeSet<NodeId> {
    if let crate::OperationProgress::Drain(progress) = &operation.progress {
        let mut nodes = kind_nodes(snapshot, &operation.kind);
        if let Some(plan) = progress.groups.get(progress.current) {
            nodes.extend(plan.voters.iter().copied());
        }
        if let Some(replacement) = progress.metadata_replacement {
            nodes.insert(replacement);
        }
        return nodes;
    }
    kind_nodes(snapshot, &operation.kind)
}

fn kind_nodes(snapshot: &crate::ClusterMetadata, kind: &OperationKind) -> BTreeSet<NodeId> {
    match kind {
        OperationKind::RebalanceGroup { group_id, voters } => snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.global_id() == *group_id)
            .map(|partition| partition.replicas.union(voters).copied().collect())
            .unwrap_or_else(|| voters.clone()),
        OperationKind::RepairReplica { group_id, node_id } => snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.global_id() == *group_id)
            .map(|partition| {
                partition
                    .replicas
                    .iter()
                    .copied()
                    .chain(std::iter::once(*node_id))
                    .collect()
            })
            .unwrap_or_else(|| BTreeSet::from([*node_id])),
        OperationKind::TransferLeader { group, node_id } => group
            .partition_id()
            .and_then(|group_id| {
                snapshot
                    .topics
                    .values()
                    .flat_map(|topic| &topic.partitions)
                    .find(|partition| partition.global_id() == group_id)
            })
            .map(|partition| {
                partition
                    .replicas
                    .iter()
                    .copied()
                    .chain(std::iter::once(*node_id))
                    .collect()
            })
            .unwrap_or_else(|| BTreeSet::from([*node_id])),
        OperationKind::DrainNode { node_id } => BTreeSet::from([*node_id]),
        OperationKind::ReplaceOfflineReplica {
            group_id,
            node_id,
            replacement,
        } => snapshot
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.global_id() == *group_id)
            .map(|partition| {
                partition
                    .replicas
                    .iter()
                    .copied()
                    .chain(std::iter::once(*node_id))
                    .chain(replacement.iter().copied())
                    .collect()
            })
            .unwrap_or_else(|| {
                std::iter::once(*node_id)
                    .chain(replacement.iter().copied())
                    .collect()
            }),
        OperationKind::ReplaceMetadataVoter {
            node_id,
            replacement,
        } => std::iter::once(*node_id)
            .chain(replacement.iter().copied())
            .collect(),
        OperationKind::ExpandPartitions { .. } => BTreeSet::new(),
    }
}

pub(super) fn replacement_candidate(
    snapshot: &crate::ClusterMetadata,
    partition: &PartitionDescriptor,
    failed: NodeId,
    now_ms: i64,
    stabilization_seconds: u64,
) -> Option<NodeId> {
    let mut loads: BTreeMap<NodeId, usize> = snapshot.nodes.keys().map(|node| (*node, 0)).collect();
    for replica in snapshot
        .topics
        .values()
        .flat_map(|topic| &topic.partitions)
        .filter(|partition| partition.lifecycle != PartitionLifecycle::Retired)
        .flat_map(|partition| &partition.replicas)
    {
        *loads.entry(*replica).or_default() += 1;
    }
    let mut candidates: Vec<_> = eligible_nodes(snapshot, now_ms, stabilization_seconds)
        .into_iter()
        .filter(|candidate| !partition.replicas.contains(candidate))
        .filter(|candidate| replacement_preserves_domains(snapshot, partition, failed, *candidate))
        .collect();
    candidates.sort_by_key(|candidate| {
        (
            loads.get(candidate).copied().unwrap_or_default(),
            *candidate,
        )
    });
    candidates.into_iter().next()
}

pub(super) fn eligible_nodes(
    snapshot: &crate::ClusterMetadata,
    now_ms: i64,
    stabilization_seconds: u64,
) -> Vec<NodeId> {
    let stabilization = seconds_ms(stabilization_seconds);
    snapshot
        .nodes
        .keys()
        .filter(|node| {
            !snapshot.drained_nodes.contains(node)
                && !maintenance_active(snapshot, **node, now_ms)
                && snapshot.node_health.get(node).is_some_and(|health| {
                    health.available
                        && health.storage_eligible
                        && health
                            .stable_since_ms
                            .is_some_and(|since| now_ms.saturating_sub(since) >= stabilization)
                })
        })
        .copied()
        .collect()
}

pub(super) fn replacement_preserves_domains(
    snapshot: &crate::ClusterMetadata,
    partition: &PartitionDescriptor,
    removed: NodeId,
    added: NodeId,
) -> bool {
    if partition.replication_factor != 5 {
        return true;
    }
    let domains: BTreeSet<_> = partition
        .replicas
        .iter()
        .copied()
        .filter(|node| *node != removed)
        .chain(std::iter::once(added))
        .map(|node| snapshot.nodes[&node].failure_domain.as_str())
        .collect();
    domains.len() == 5
}

pub(super) fn operation_for_group(
    snapshot: &crate::ClusterMetadata,
    group_id: crate::GlobalGroupId,
) -> bool {
    snapshot.operations.values().any(|operation| {
        let matches_group = match &operation.kind {
            OperationKind::RebalanceGroup {
                group_id: group, ..
            }
            | OperationKind::RepairReplica {
                group_id: group, ..
            }
            | OperationKind::ReplaceOfflineReplica {
                group_id: group, ..
            } => *group == group_id,
            OperationKind::TransferLeader {
                group: GroupKey::Partition(group),
                ..
            } => *group == group_id,
            OperationKind::ExpandPartitions {
                partition_groups, ..
            } => partition_groups.contains(&group_id),
            OperationKind::DrainNode { node_id } => {
                let planned = match &operation.progress {
                    crate::OperationProgress::Drain(progress) => {
                        progress.groups.iter().any(|plan| plan.group_id == group_id)
                    }
                    crate::OperationProgress::None => false,
                };
                planned
                    || snapshot
                        .topics
                        .values()
                        .flat_map(|topic| &topic.partitions)
                        .find(|partition| partition.global_id() == group_id)
                        .is_some_and(|partition| partition.replicas.contains(node_id))
            }
            _ => false,
        };
        matches_group && !terminal(operation.state)
    })
}

pub(super) fn group_in_cooldown(
    snapshot: &crate::ClusterMetadata,
    group_id: crate::GlobalGroupId,
    now_ms: i64,
    cooldown_ms: i64,
) -> bool {
    snapshot.operations.values().any(|operation| {
        operation.state == OperationState::Completed
            && now_ms.saturating_sub(operation.updated_at_ms) < cooldown_ms
            && match operation.kind {
                OperationKind::RebalanceGroup {
                    group_id: group, ..
                }
                | OperationKind::RepairReplica {
                    group_id: group, ..
                }
                | OperationKind::ReplaceOfflineReplica {
                    group_id: group, ..
                } => group == group_id,
                OperationKind::TransferLeader {
                    group: GroupKey::Partition(group),
                    ..
                } => group == group_id,
                _ => false,
            }
    })
}

pub(super) fn maintenance_active(
    snapshot: &crate::ClusterMetadata,
    node_id: NodeId,
    now_ms: i64,
) -> bool {
    snapshot
        .maintenance_nodes
        .get(&node_id)
        .is_some_and(|lease| lease.expires_at_ms > now_ms)
}

pub(super) fn terminal(state: OperationState) -> bool {
    matches!(state, OperationState::Completed | OperationState::Cancelled)
}

pub(super) fn now_i64() -> i64 {
    wall_time_ms().min(i64::MAX as u64) as i64
}

pub(super) fn seconds_ms(seconds: u64) -> i64 {
    seconds.saturating_mul(1000).min(i64::MAX as u64) as i64
}

#[cfg(test)]
#[path = "automation_plan_tests.rs"]
mod tests;
