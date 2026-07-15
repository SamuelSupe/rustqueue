use super::*;

fn nodes(count: u64) -> BTreeMap<NodeId, NodeDescriptor> {
    (1..=count)
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
        .collect()
}

#[test]
fn four_nodes_rotate_three_replica_partitions() {
    let catalog = MetadataCatalog::new(nodes(4), 4, 3).unwrap();
    let topic = catalog.ensure_topic("events", None, None).unwrap();
    assert!(topic
        .partitions
        .iter()
        .all(|partition| partition.replicas.len() == 3));
    let used: BTreeSet<_> = topic
        .partitions
        .iter()
        .flat_map(|partition| &partition.replicas)
        .copied()
        .collect();
    assert_eq!(used, BTreeSet::from([1, 2, 3, 4]));
}

#[test]
fn five_replica_topic_uses_every_node() {
    let catalog = MetadataCatalog::new(nodes(5), 1, 3).unwrap();
    let topic = catalog.ensure_topic("critical", Some(2), Some(5)).unwrap();
    assert!(topic
        .partitions
        .iter()
        .all(|partition| partition.replicas == BTreeSet::from([1, 2, 3, 4, 5])));
}

#[test]
fn rejects_four_replicas() {
    assert!(MetadataCatalog::new(nodes(5), 1, 4).is_err());
}

#[test]
fn discovered_node_registration_is_idempotent_and_starts_unavailable() {
    let catalog = MetadataCatalog::new(nodes(3), 1, 3).unwrap();
    let mut descriptor = nodes(4).remove(&4).unwrap();
    descriptor.peer_id = Some("12D3KooWdiscovered".into());
    assert!(catalog.register_node(descriptor.clone()).unwrap());
    assert!(!catalog.register_node(descriptor.clone()).unwrap());
    let snapshot = catalog.snapshot();
    assert_eq!(snapshot.nodes.len(), 4);
    assert!(!snapshot.node_health[&4].available);

    descriptor.peer_id = Some("12D3KooWdifferent".into());
    assert!(catalog.register_node(descriptor).is_err());
}

#[test]
fn placement_stays_balanced_from_three_through_nine_nodes() {
    for count in 3..=9 {
        let catalog = MetadataCatalog::new(nodes(count), 1, 3).unwrap();
        let topic = catalog
            .ensure_topic("balanced", Some((count * 4) as u16), Some(3))
            .unwrap();
        let mut loads = BTreeMap::from_iter((1..=count).map(|node| (node, 0usize)));
        for partition in topic.partitions {
            assert_eq!(partition.replicas.len(), 3);
            for replica in partition.replicas {
                *loads.get_mut(&replica).unwrap() += 1;
            }
        }
        let minimum = loads.values().min().copied().unwrap();
        let maximum = loads.values().max().copied().unwrap();
        assert!(maximum - minimum <= 1, "unbalanced {count}-node placement");
    }
}

#[test]
fn five_replica_placement_uses_distinct_failure_domains() {
    for count in 5..=9 {
        let catalog = MetadataCatalog::new(nodes(count), 2, 3).unwrap();
        let topic = catalog.ensure_topic("critical", Some(4), Some(5)).unwrap();
        let snapshot = catalog.snapshot();
        for partition in topic.partitions {
            let domains: BTreeSet<_> = partition
                .replicas
                .iter()
                .map(|node| snapshot.nodes[node].failure_domain.clone())
                .collect();
            assert_eq!(domains.len(), 5);
        }
    }
}

#[test]
fn v4_catalog_bucket_cutover_updates_the_hot_route_snapshot() {
    let catalog = MetadataCatalog::new(nodes(3), 2, 3).unwrap();
    let topic = catalog.ensure_topic("events", Some(2), Some(3)).unwrap();
    let target = &topic.partitions[1];
    let global = crate::GlobalGroupId {
        cell: target.home_cell,
        local: target.group_id,
    };
    catalog
        .activate_bucket_move("events", 0, u16::MAX, global, 1)
        .unwrap();
    let selected = catalog
        .topic_route("events")
        .unwrap()
        .select_partition(0, None, Some(b"stable-key"))
        .unwrap();
    assert_eq!(selected.group_id, target.group_id);
}

#[test]
fn v6_topic_local_wire_identities_survive_metadata_round_trip() {
    let catalog = MetadataCatalog::new(nodes(3), 2, 3).unwrap();
    let topic = catalog.ensure_topic("events", Some(2), Some(3)).unwrap();
    let encoded = serde_json::to_vec(&catalog.snapshot()).unwrap();
    let restored: ClusterMetadata = serde_json::from_slice(&encoded).unwrap();
    let restored_topic = restored.topics.get("events").unwrap();
    assert_eq!(restored_topic.partitions, topic.partitions);
    assert!(restored_topic
        .partitions
        .iter()
        .all(|partition| partition.slot == partition.number + 1));
    assert!(restored.federation_root.generator_leases.is_empty());
    catalog.replace(restored).unwrap();
}

#[test]
fn wire_slots_are_topic_local_but_internal_group_ids_are_not_reused() {
    let catalog = MetadataCatalog::new(nodes(4), 2, 3).unwrap();
    let first = catalog.ensure_topic("first", Some(2), None).unwrap();
    let first_groups: BTreeSet<_> = first
        .partitions
        .iter()
        .map(PartitionDescriptor::global_id)
        .collect();
    catalog.delete_topic("first");
    let second = catalog.ensure_topic("second", Some(2), None).unwrap();
    assert_eq!(second.partitions[0].slot, 1);
    assert!(second
        .partitions
        .iter()
        .all(|partition| !first_groups.contains(&partition.global_id())));
}

#[test]
fn prepared_topic_deletion_fences_routing_until_completion() {
    let catalog = MetadataCatalog::new(nodes(3), 2, 3).unwrap();
    catalog.ensure_topic("events", None, None).unwrap();
    assert_eq!(2, catalog.topic_route("events").unwrap().active_count());

    let deleting = catalog.prepare_delete_topic("events").unwrap();
    assert_eq!(TopicState::Deleting, deleting.state);
    assert_eq!(0, catalog.topic_route("events").unwrap().active_count());
    assert!(catalog.ensure_topic("events", None, None).is_err());
    assert!(catalog.prepare_channel("events", "workers").is_err());
    assert_eq!(1, catalog.deleting_topics().len());

    catalog.delete_topic("events");
    assert!(catalog.topic("events").is_none());
}

#[test]
fn drained_nodes_receive_no_new_partitions() {
    let catalog = MetadataCatalog::new(nodes(5), 2, 3).unwrap();
    catalog.set_node_drained(1, true).unwrap();
    let topic = catalog
        .ensure_topic("after-drain", Some(8), Some(3))
        .unwrap();
    assert!(topic
        .partitions
        .iter()
        .all(|partition| !partition.replicas.contains(&1)));
    assert!(catalog.ensure_topic("critical", Some(1), Some(5)).is_err());
}

#[test]
fn ephemeral_channel_expires_only_after_every_gateway_lease() {
    let catalog = MetadataCatalog::new(nodes(3), 1, 3).unwrap();
    catalog.ensure_topic("events", None, None).unwrap();
    let generation = catalog
        .prepare_channel("events", "workers#ephemeral")
        .unwrap()
        .unwrap();
    catalog
        .renew_ephemeral_lease("events", "workers#ephemeral", 10, 100)
        .unwrap();
    catalog
        .activate_channel("events", "workers#ephemeral", generation)
        .unwrap();
    assert!(catalog.expired_ephemeral_channels(50).is_empty());

    catalog
        .renew_ephemeral_lease("events", "workers#ephemeral", 20, 200)
        .unwrap();
    assert!(catalog.expired_ephemeral_channels(150).is_empty());

    catalog
        .release_ephemeral_lease("events", "workers#ephemeral", 20)
        .unwrap();
    assert_eq!(
        catalog.expired_ephemeral_channels(150),
        vec![("events".into(), "workers#ephemeral".into())]
    );
}

#[test]
fn stale_channel_generation_commands_are_idempotently_fenced() {
    let catalog = MetadataCatalog::new(nodes(3), 1, 3).unwrap();
    catalog.ensure_topic("events", None, None).unwrap();

    let first = catalog
        .prepare_channel("events", "workers")
        .unwrap()
        .unwrap();
    catalog
        .activate_channel("events", "workers", first)
        .unwrap();
    assert_eq!(
        catalog.prepare_delete_channel("events", "workers").unwrap(),
        Some(first)
    );
    assert_eq!(catalog.prepare_channel("events", "workers").unwrap(), None);
    catalog
        .activate_channel("events", "workers", first)
        .unwrap();
    assert_eq!(
        catalog.topic("events").unwrap().channels["workers"].state,
        ChannelLifecycle::Deleting
    );
    catalog
        .complete_delete_channel("events", "workers", first)
        .unwrap();

    let second = catalog
        .prepare_channel("events", "workers")
        .unwrap()
        .unwrap();
    assert!(second > first);
    catalog
        .activate_channel("events", "workers", first)
        .unwrap();
    catalog
        .complete_delete_channel("events", "workers", first)
        .unwrap();

    let descriptor = catalog.topic("events").unwrap().channels["workers"].clone();
    assert_eq!(descriptor.generation, second);
    assert_eq!(descriptor.state, ChannelLifecycle::Preparing);
    assert!(catalog
        .activate_channel("events", "workers", second + 1)
        .unwrap_err()
        .contains("generation mismatch"));
    assert!(catalog
        .complete_delete_channel("events", "workers", second + 1)
        .unwrap_err()
        .contains("generation mismatch"));
}

#[test]
fn expansion_reserves_permanent_slots_and_activates_atomically() {
    let catalog = MetadataCatalog::new(nodes(4), 2, 3).unwrap();
    let original = catalog.ensure_topic("events", Some(2), None).unwrap();
    let original_key_slots = original.key_routing_slots.clone();
    let operation = catalog
        .reserve_partition_expansion("events", 5, 1024, 10)
        .unwrap();
    let preparing = catalog.topic("events").unwrap();
    assert_eq!(
        preparing
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
            .count(),
        2
    );
    assert_eq!(
        preparing
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Preparing)
            .count(),
        3
    );
    assert_eq!(preparing.key_routing_slots, original_key_slots);

    catalog
        .activate_partition_expansion(operation.id, preparing.channel_catalog_revision, 20)
        .unwrap();
    let active = catalog.topic("events").unwrap();
    assert_eq!(catalog.active_partitions("events").len(), 5);
    assert_eq!(active.topology_generation, 2);
    assert_eq!(active.key_routing_slots, original_key_slots);
    assert_eq!(
        catalog.operation(operation.id).unwrap().state,
        OperationState::Completed
    );
}

#[test]
fn expansion_activation_retries_when_channel_catalog_changes() {
    let catalog = MetadataCatalog::new(nodes(4), 1, 3).unwrap();
    catalog.ensure_topic("events", Some(1), None).unwrap();
    let operation = catalog
        .reserve_partition_expansion("events", 2, 1024, 10)
        .unwrap();
    let stale_revision = catalog.topic("events").unwrap().channel_catalog_revision;
    catalog.prepare_channel("events", "workers").unwrap();
    assert!(catalog
        .activate_partition_expansion(operation.id, stale_revision, 20)
        .unwrap_err()
        .contains("channel catalog changed"));
    assert_eq!(catalog.active_partitions("events").len(), 1);
}

#[test]
fn blocked_or_paused_expansion_requires_explicit_resume() {
    let catalog = MetadataCatalog::new(nodes(4), 1, 3).unwrap();
    catalog.ensure_topic("events", Some(1), None).unwrap();
    let operation = catalog
        .reserve_partition_expansion("events", 2, 1024, 10)
        .unwrap();
    catalog
        .advance_partition_expansion(
            operation.id,
            OperationPhase::CreateGroups,
            OperationState::NeedsOperator,
            20,
            Some("placement policy changed".into()),
        )
        .unwrap();
    assert!(catalog.pending_partition_expansions().is_empty());

    catalog.set_operation_paused(operation.id, false).unwrap();
    assert_eq!(catalog.pending_partition_expansions().len(), 1);
    catalog.set_operation_paused(operation.id, true).unwrap();
    assert!(catalog.pending_partition_expansions().is_empty());
}

#[test]
fn cancelled_expansion_never_reuses_slots() {
    let catalog = MetadataCatalog::new(nodes(4), 1, 3).unwrap();
    catalog.ensure_topic("events", Some(1), None).unwrap();
    let cancelled = catalog
        .reserve_partition_expansion("events", 3, 1024, 10)
        .unwrap();
    let reserved_high = catalog
        .topic("events")
        .unwrap()
        .partitions
        .iter()
        .map(|partition| partition.slot)
        .max()
        .unwrap();
    catalog
        .cancel_partition_expansion(cancelled.id, 20)
        .unwrap();
    let replacement = catalog
        .reserve_partition_expansion("events", 2, 1024, 30)
        .unwrap();
    let replacement_slot = catalog
        .topic("events")
        .unwrap()
        .partitions
        .iter()
        .find(|partition| partition.operation_id == Some(replacement.id))
        .unwrap()
        .slot;
    assert!(replacement_slot > reserved_high);
}

#[test]
fn drain_cursor_and_group_plan_survive_serialization_and_transient_updates() {
    let catalog = MetadataCatalog::new(nodes(4), 1, 3).unwrap();
    let operation = catalog
        .create_operation(OperationKind::DrainNode { node_id: 1 }, 10, 100)
        .unwrap();
    let progress = OperationProgress::Drain(DrainProgress {
        groups: vec![
            DrainGroupPlan {
                group_id: crate::GlobalGroupId {
                    cell: crate::CellId::BOOTSTRAP,
                    local: 7,
                },
                voters: BTreeSet::from([2, 3, 4]),
            },
            DrainGroupPlan {
                group_id: crate::GlobalGroupId {
                    cell: crate::CellId::BOOTSTRAP,
                    local: 8,
                },
                voters: BTreeSet::from([2, 3, 4]),
            },
        ],
        current: 1,
        metadata_replacement: Some(4),
        metadata_completed: false,
    });
    catalog
        .update_operation(
            operation.id,
            OperationPhase::CatchUp,
            OperationState::Running,
            20,
            None,
            Some(progress.clone()),
        )
        .unwrap();
    catalog
        .update_operation(
            operation.id,
            OperationPhase::CatchUp,
            OperationState::Running,
            30,
            Some("retry".into()),
            None,
        )
        .unwrap();

    assert_eq!(catalog.operation(operation.id).unwrap().progress, progress);
    let restored: ClusterMetadata =
        serde_json::from_slice(&serde_json::to_vec(&catalog.snapshot()).unwrap()).unwrap();
    assert_eq!(restored.operations[&operation.id].progress, progress);
}

#[test]
fn feature_level_is_monotonic_and_survives_metadata_serialization() {
    let catalog = MetadataCatalog::standalone(1);
    assert_eq!(
        catalog.snapshot().active_feature_level,
        crate::FEATURE_LEVEL_BASELINE
    );
    catalog
        .activate_feature_level(crate::FEATURE_LEVEL_LARGE_MESSAGES)
        .unwrap();
    catalog
        .activate_feature_level(crate::FEATURE_LEVEL_BASELINE)
        .unwrap();
    let snapshot = catalog.snapshot();
    assert_eq!(
        snapshot.active_feature_level,
        crate::FEATURE_LEVEL_LARGE_MESSAGES
    );
    let restored: ClusterMetadata =
        serde_json::from_slice(&serde_json::to_vec(&snapshot).unwrap()).unwrap();
    assert_eq!(restored.active_feature_level, snapshot.active_feature_level);
}
#[test]
fn route_snapshot_rebuilds_only_for_routing_changes() {
    let catalog = MetadataCatalog::new(nodes(4), 1, 3).unwrap();
    let topic = catalog.ensure_topic("events", Some(1), None).unwrap();
    let cached = catalog.topic_route("events").unwrap();

    catalog.prepare_channel("events", "workers").unwrap();
    let after_channel = catalog.topic_route("events").unwrap();
    assert!(Arc::ptr_eq(&cached, &after_channel));

    let operation = catalog
        .reserve_partition_expansion("events", 3, 1024, 10)
        .unwrap();
    let preparing = catalog.topic_route("events").unwrap();
    assert!(!Arc::ptr_eq(&cached, &preparing));
    assert_eq!(preparing.active_count(), 1);

    let revision = catalog.topic("events").unwrap().channel_catalog_revision;
    catalog
        .activate_partition_expansion(operation.id, revision, 20)
        .unwrap();
    assert_eq!(catalog.topic_route("events").unwrap().active_count(), 3);

    let group_id = topic.partitions[0].global_id();
    catalog
        .update_partition_replicas(group_id, BTreeSet::from([2, 3, 4]))
        .unwrap();
    assert_eq!(
        catalog.partition_route(group_id).unwrap().1.replicas,
        BTreeSet::from([2, 3, 4])
    );
}

#[test]
fn three_replica_clusters_require_three_failure_domains() {
    let mut colocated = nodes(3);
    for node in colocated.values_mut() {
        node.failure_domain = "same-rack".into();
    }
    let error = MetadataCatalog::new(colocated, 1, 3).err().unwrap();
    assert!(error.contains("distinct failure domains"));
}
