use super::*;
use crate::{MaintenanceLease, MetadataCatalog, NodeDescriptor, NodeHealthRecord, OperationPhase};

fn snapshot(node_count: u64) -> crate::ClusterMetadata {
    let nodes = (1..=node_count)
        .map(|id| {
            (
                id,
                NodeDescriptor {
                    id,
                    raft_address: format!("https://node-{id}:4250"),
                    broadcast_address: format!("node-{id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    tls_server_name: format!("node-{id}"),
                    failure_domain: format!("zone-{id}"),
                    peer_id: None,
                    cell_id: crate::CellId::BOOTSTRAP,
                    federation_router: false,
                },
            )
        })
        .collect();
    let catalog = MetadataCatalog::new(nodes, 1, 3).unwrap();
    catalog.ensure_topic("events", Some(1), Some(3)).unwrap();
    let mut snapshot = catalog.snapshot();
    for health in snapshot.node_health.values_mut() {
        *health = NodeHealthRecord {
            available: true,
            stable_since_ms: Some(0),
            last_observed_ms: 0,
            ..NodeHealthRecord::default()
        };
    }
    snapshot
}

#[test]
fn drain_conflicts_with_any_active_operation_on_the_node() {
    let mut snapshot = snapshot(4);
    let group_id = snapshot.topics["events"].partitions[0].global_id();
    let drain_node = *snapshot.topics["events"].partitions[0]
        .replicas
        .iter()
        .next()
        .unwrap();
    snapshot.operations.insert(
        1,
        MaintenanceOperation {
            id: 1,
            kind: OperationKind::RebalanceGroup {
                group_id,
                voters: BTreeSet::from([2, 3, 4]),
            },
            state: OperationState::Running,
            phase: OperationPhase::CatchUp,
            created_at_ms: 0,
            updated_at_ms: 0,
            error: None,
            progress: crate::OperationProgress::None,
        },
    );
    assert!(operation_conflicts(
        &snapshot,
        &OperationKind::DrainNode {
            node_id: drain_node
        }
    ));
    assert!(!operation_conflicts(
        &snapshot,
        &OperationKind::DrainNode { node_id: 9 }
    ));

    snapshot.operations.clear();
    snapshot.operations.insert(
        2,
        MaintenanceOperation {
            id: 2,
            kind: OperationKind::DrainNode {
                node_id: drain_node,
            },
            state: OperationState::Running,
            phase: OperationPhase::Planned,
            created_at_ms: 0,
            updated_at_ms: 0,
            error: None,
            progress: crate::OperationProgress::None,
        },
    );
    assert!(operation_for_group(&snapshot, group_id));
}

#[test]
fn eligibility_uses_virtual_time_disk_and_maintenance_ttl() {
    let mut metadata = snapshot(4);
    metadata.node_health.get_mut(&3).unwrap().storage_eligible = false;
    metadata.maintenance_nodes.insert(
        2,
        MaintenanceLease {
            expires_at_ms: 200_000,
            reason: "planned restart".into(),
        },
    );
    assert_eq!(eligible_nodes(&metadata, 100_000, 60), vec![1, 4]);
    assert_eq!(eligible_nodes(&metadata, 200_000, 60), vec![1, 2, 4]);
}

#[test]
fn replacement_is_deterministic_and_rejects_disk_pressure() {
    let mut metadata = snapshot(5);
    let partition = metadata.topics["events"].partitions[0].clone();
    let failed = *partition.replicas.iter().next().unwrap();
    let candidates: Vec<_> = metadata
        .nodes
        .keys()
        .copied()
        .filter(|node| !partition.replicas.contains(node))
        .collect();
    assert_eq!(
        replacement_candidate(&metadata, &partition, failed, 100_000, 60),
        Some(candidates[0])
    );
    metadata
        .node_health
        .get_mut(&candidates[0])
        .unwrap()
        .storage_eligible = false;
    assert_eq!(
        replacement_candidate(&metadata, &partition, failed, 100_000, 60),
        Some(candidates[1])
    );
}

#[test]
fn completed_group_operation_enforces_exact_cooldown_boundary() {
    let mut metadata = snapshot(4);
    let partition = &metadata.topics["events"].partitions[0];
    let group_id = partition.global_id();
    let group = partition.group_key();
    metadata.operations.insert(
        1,
        MaintenanceOperation {
            id: 1,
            kind: OperationKind::TransferLeader { group, node_id: 2 },
            state: OperationState::Completed,
            phase: OperationPhase::Completed,
            created_at_ms: 100,
            updated_at_ms: 100,
            error: None,
            progress: crate::OperationProgress::None,
        },
    );
    assert!(group_in_cooldown(&metadata, group_id, 1_099, 1_000));
    assert!(!group_in_cooldown(&metadata, group_id, 1_100, 1_000));
}
