use super::*;

impl MetadataCatalog {
    pub fn upsert_federated_partition(
        &self,
        template: &TopicDescriptor,
        partition: PartitionDescriptor,
    ) -> Result<TopicDescriptor, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if partition.home_cell != state.cell_id
            || partition.replicas.is_empty()
            || !partition
                .replicas
                .iter()
                .all(|node| state.nodes.contains_key(node))
        {
            return Err("federated partition is not assigned to this Cell".into());
        }
        let topic = state
            .topics
            .entry(template.name.clone())
            .or_insert_with(|| {
                let mut topic = template.clone();
                topic.partitions.clear();
                topic.channels.clear();
                topic.key_routing_slots = vec![partition.slot];
                topic.state = TopicState::Active;
                topic
            });
        if topic.replication_factor != partition.replication_factor {
            return Err("federated partition replication factor does not match topic".into());
        }
        if topic.partitions.iter().any(|existing| {
            existing.global_id() != partition.global_id()
                && (existing.number == partition.number || existing.slot == partition.slot)
        }) {
            return Err("federated partition number or wire slot collides in target Cell".into());
        }
        if let Some(existing) = topic
            .partitions
            .iter_mut()
            .find(|existing| existing.global_id() == partition.global_id())
        {
            *existing = partition;
        } else {
            topic.partitions.push(partition);
            topic.partitions.sort_by_key(|partition| partition.number);
        }
        topic.topology_generation = topic.topology_generation.max(template.topology_generation);
        let descriptor = topic.clone();
        state.epoch = state.epoch.saturating_add(1);
        super::routes::bump_routing_epoch(&mut state);
        drop(state);
        self.invalidate_routes();
        Ok(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell_two_nodes() -> BTreeMap<NodeId, NodeDescriptor> {
        (4..=6)
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
                        cell_id: CellId(2),
                        federation_router: false,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn target_cell_keeps_the_global_group_origin() {
        let catalog =
            MetadataCatalog::new_federated_in_cell(CellId(2), cell_two_nodes(), 1, 3, 128).unwrap();
        let partition = PartitionDescriptor {
            group_id: 9,
            origin_cell: CellId(1),
            number: 0,
            slot: 1,
            replication_factor: 3,
            replicas: BTreeSet::from([4, 5, 6]),
            leader_hint: None,
            lifecycle: PartitionLifecycle::Preparing,
            operation_id: None,
            home_cell: CellId(2),
            wire_incarnation: 1,
        };
        let template = TopicDescriptor {
            name: "events".into(),
            state: TopicState::Active,
            replication_factor: 3,
            partitions: vec![partition.clone()],
            channels: BTreeMap::new(),
            next_channel_generation: 1,
            paused: false,
            topology_generation: 1,
            key_routing_slots: vec![1],
            channel_catalog_revision: 0,
        };
        let stored = catalog
            .upsert_federated_partition(&template, partition)
            .unwrap();
        assert_eq!(stored.partitions[0].global_id().cell, CellId(1));
        assert_eq!(stored.partitions[0].home_cell, CellId(2));
    }

    #[test]
    fn global_route_index_keeps_equal_local_ids_from_different_cells() {
        let catalog =
            MetadataCatalog::new_federated_in_cell(CellId(2), cell_two_nodes(), 1, 3, 128).unwrap();
        let local = catalog
            .ensure_topic("local", Some(1), Some(3))
            .unwrap()
            .partitions[0]
            .clone();
        let remote = PartitionDescriptor {
            group_id: local.group_id,
            origin_cell: CellId(1),
            number: 0,
            slot: 1,
            replication_factor: 3,
            replicas: BTreeSet::from([4, 5, 6]),
            leader_hint: None,
            lifecycle: PartitionLifecycle::Active,
            operation_id: None,
            home_cell: CellId(2),
            wire_incarnation: 1,
        };
        let template = TopicDescriptor {
            name: "remote".into(),
            state: TopicState::Active,
            replication_factor: 3,
            partitions: vec![remote.clone()],
            channels: BTreeMap::new(),
            next_channel_generation: 1,
            paused: false,
            topology_generation: 1,
            key_routing_slots: vec![1],
            channel_catalog_revision: 0,
        };
        catalog
            .upsert_federated_partition(&template, remote.clone())
            .unwrap();

        assert_eq!(catalog.partition(local.global_id()).unwrap().0, "local");
        assert_eq!(catalog.partition(remote.global_id()).unwrap().0, "remote");
        assert_ne!(local.global_id(), remote.global_id());
    }
}
