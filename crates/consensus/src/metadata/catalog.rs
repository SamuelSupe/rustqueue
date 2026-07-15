use super::placement::{
    choose_replicas, replica_loads, validate_cluster, validate_cluster_state,
    validate_failure_domains, validate_replication_factor,
};
use super::*;

fn same_node_endpoint(left: &NodeDescriptor, right: &NodeDescriptor) -> bool {
    left.id == right.id
        && left.raft_address == right.raft_address
        && left.broadcast_address == right.broadcast_address
        && left.tcp_port == right.tcp_port
        && left.http_port == right.http_port
        && left.tls_server_name == right.tls_server_name
        && left.failure_domain == right.failure_domain
}

impl MetadataCatalog {
    pub fn max_home_cells_per_topic(&self) -> usize {
        self.max_home_cells_per_topic
    }

    pub fn new_control_plane(
        nodes: BTreeMap<NodeId, NodeDescriptor>,
        root_voters: BTreeSet<NodeId>,
        max_home_cells_per_topic: usize,
    ) -> Result<Self, String> {
        if !matches!(root_voters.len(), 3 | 5)
            || !root_voters.iter().all(|node| nodes.contains_key(node))
        {
            return Err("control-plane voters must contain 3 or 5 configured nodes".into());
        }
        if max_home_cells_per_topic == 0 || nodes.is_empty() {
            return Err("control-plane nodes and Home Cell limit are required".into());
        }
        let mut by_cell = BTreeMap::<CellId, Vec<NodeDescriptor>>::new();
        for node in nodes.values() {
            by_cell.entry(node.cell_id).or_default().push(node.clone());
        }
        if by_cell.contains_key(&CellId(0)) {
            return Err("control-plane nodes require non-zero Cell IDs".into());
        }
        let cells = by_cell
            .iter()
            .map(|(cell_id, members)| {
                let mut routers: BTreeSet<_> = members
                    .iter()
                    .filter(|node| node.federation_router)
                    .map(|node| node.id)
                    .take(3)
                    .collect();
                if routers.is_empty() {
                    routers.extend(members.iter().map(|node| node.id).take(3));
                }
                (
                    *cell_id,
                    crate::CellDescriptor {
                        id: *cell_id,
                        nodes: members.iter().map(|node| node.id).collect(),
                        routers,
                        lifecycle: crate::CellLifecycle::Active,
                        feature_level: crate::FEATURE_LEVEL_BASELINE,
                        created_at_ms: 0,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let federation_nodes = nodes
            .values()
            .map(|node| {
                (
                    node.id,
                    crate::FederationNode {
                        id: node.id,
                        failure_domain: node.failure_domain.clone(),
                        placement: crate::NodePlacement::Member(node.cell_id),
                        stable_since_ms: 0,
                        available: true,
                        protocol_version: 1,
                        feature_level: crate::FEATURE_LEVEL_BASELINE,
                    },
                )
            })
            .collect();
        let seed_cell = *cells.keys().next().expect("nodes produced a Cell");
        let local_nodes: BTreeMap<_, _> = nodes
            .values()
            .filter(|node| node.cell_id == seed_cell)
            .map(|node| (node.id, node.clone()))
            .collect();
        let root = crate::FederationRoot {
            epoch: 1,
            next_cell_id: cells.keys().map(|cell| cell.0).max().unwrap_or(1),
            cells,
            nodes: federation_nodes,
            catalog_shards: BTreeMap::from([(
                1,
                crate::CatalogShardDescriptor {
                    id: 1,
                    hash_start: 0,
                    hash_end: u64::MAX,
                    voters: root_voters.clone(),
                    epoch: 1,
                },
            )]),
            catalog_splits: BTreeMap::new(),
            root_voters,
            generator_leases: BTreeMap::new(),
            generator_ranges: BTreeMap::new(),
            next_generator_incarnation: 1,
            min_protocol_version: 1,
            max_protocol_version: 1,
        };
        Ok(Self {
            state: RwLock::new(ClusterMetadata {
                cell_id: seed_cell,
                node_health: local_nodes
                    .keys()
                    .map(|node| (*node, NodeHealthRecord::default()))
                    .collect(),
                nodes: local_nodes,
                drained_nodes: BTreeSet::new(),
                topics: BTreeMap::new(),
                next_group_id: FIRST_GROUP_ID,
                next_slot: FIRST_SLOT,
                epoch: 0,
                routing_epoch: 0,
                next_operation_id: 1,
                operations: BTreeMap::new(),
                automation_enabled: true,
                maintenance_nodes: BTreeMap::new(),
                active_feature_level: crate::FEATURE_LEVEL_BASELINE,
                federation_root: root,
                catalog: crate::CatalogState {
                    shard_id: 1,
                    ..crate::CatalogState::default()
                },
                scoped_feature_levels: crate::ScopedFeatureLevels::default(),
            }),
            routes: RwLock::new(MetadataRoutes::default()),
            default_partitions: 1,
            default_replication_factor: 1,
            max_home_cells_per_topic,
        })
    }

    pub fn new(
        nodes: BTreeMap<NodeId, NodeDescriptor>,
        default_partitions: u16,
        default_replication_factor: u8,
    ) -> Result<Self, String> {
        Self::new_in_cell(
            CellId::BOOTSTRAP,
            nodes,
            default_partitions,
            default_replication_factor,
        )
    }

    pub fn new_in_cell(
        cell_id: CellId,
        nodes: BTreeMap<NodeId, NodeDescriptor>,
        default_partitions: u16,
        default_replication_factor: u8,
    ) -> Result<Self, String> {
        Self::new_federated_in_cell(
            cell_id,
            nodes,
            default_partitions,
            default_replication_factor,
            128,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_federated_in_cell(
        cell_id: CellId,
        nodes: BTreeMap<NodeId, NodeDescriptor>,
        default_partitions: u16,
        default_replication_factor: u8,
        max_home_cells_per_topic: usize,
    ) -> Result<Self, String> {
        if cell_id.0 == 0 || nodes.values().any(|node| node.cell_id != cell_id) {
            return Err("metadata nodes must belong to one non-zero Cell".into());
        }
        if max_home_cells_per_topic == 0 {
            return Err("max Home Cells per topic must be greater than zero".into());
        }
        validate_cluster(&nodes, default_replication_factor)?;
        if default_partitions == 0 {
            return Err("default partition count must be greater than zero".into());
        }
        let mut routers: BTreeSet<_> = nodes
            .values()
            .filter(|node| node.federation_router)
            .map(|node| node.id)
            .take(3)
            .collect();
        if routers.len() < 3 {
            routers.extend(nodes.keys().copied().take(3));
        }
        let voters: BTreeSet<_> = nodes.keys().copied().take(3).collect();
        let federation_nodes = nodes
            .values()
            .map(|node| {
                (
                    node.id,
                    crate::FederationNode {
                        id: node.id,
                        failure_domain: node.failure_domain.clone(),
                        placement: crate::NodePlacement::Member(cell_id),
                        stable_since_ms: 0,
                        available: true,
                        protocol_version: 1,
                        feature_level: crate::FEATURE_LEVEL_BASELINE,
                    },
                )
            })
            .collect();
        let federation_root = crate::FederationRoot {
            epoch: 1,
            next_cell_id: cell_id.0,
            cells: BTreeMap::from([(
                cell_id,
                crate::CellDescriptor {
                    id: cell_id,
                    nodes: nodes.keys().copied().collect(),
                    routers,
                    lifecycle: crate::CellLifecycle::Active,
                    feature_level: crate::FEATURE_LEVEL_BASELINE,
                    created_at_ms: 0,
                },
            )]),
            nodes: federation_nodes,
            catalog_shards: BTreeMap::from([(
                1,
                crate::CatalogShardDescriptor {
                    id: 1,
                    hash_start: 0,
                    hash_end: u64::MAX,
                    voters: voters.clone(),
                    epoch: 1,
                },
            )]),
            catalog_splits: BTreeMap::new(),
            root_voters: voters,
            generator_leases: BTreeMap::new(),
            generator_ranges: BTreeMap::from([(
                cell_id,
                crate::GeneratorSlotRange {
                    start: 1,
                    end: u16::MAX,
                },
            )]),
            next_generator_incarnation: 1,
            min_protocol_version: 1,
            max_protocol_version: 1,
        };
        Ok(Self {
            state: RwLock::new(ClusterMetadata {
                cell_id,
                node_health: nodes
                    .keys()
                    .map(|node| {
                        (
                            *node,
                            NodeHealthRecord {
                                available: true,
                                stable_since_ms: Some(0),
                                ..NodeHealthRecord::default()
                            },
                        )
                    })
                    .collect(),
                nodes,
                drained_nodes: BTreeSet::new(),
                topics: BTreeMap::new(),
                next_group_id: FIRST_GROUP_ID,
                next_slot: FIRST_SLOT,
                epoch: 0,
                routing_epoch: 0,
                next_operation_id: 1,
                operations: BTreeMap::new(),
                automation_enabled: true,
                maintenance_nodes: BTreeMap::new(),
                active_feature_level: crate::FEATURE_LEVEL_BASELINE,
                federation_root,
                catalog: crate::CatalogState {
                    shard_id: 1,
                    ..crate::CatalogState::default()
                },
                scoped_feature_levels: crate::ScopedFeatureLevels::default(),
            }),
            routes: RwLock::new(MetadataRoutes::default()),
            default_partitions,
            default_replication_factor,
            max_home_cells_per_topic,
        })
    }

    pub fn standalone(default_partitions: u16) -> Self {
        let node = NodeDescriptor {
            id: 1,
            raft_address: String::new(),
            broadcast_address: "127.0.0.1".into(),
            tcp_port: 4150,
            http_port: 4151,
            tls_server_name: "localhost".into(),
            failure_domain: "local".into(),
            peer_id: None,
            cell_id: CellId::BOOTSTRAP,
            federation_router: false,
        };
        Self::new(BTreeMap::from([(1, node)]), default_partitions, 1)
            .expect("standalone metadata is valid")
    }

    pub fn snapshot(&self) -> ClusterMetadata {
        self.state.read().expect("metadata lock poisoned").clone()
    }

    pub fn root_snapshot(&self) -> crate::FederationRoot {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .federation_root
            .clone()
    }

    pub fn catalog_snapshot(&self) -> crate::CatalogState {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .catalog
            .clone()
    }

    pub fn replace_root(&self, root: crate::FederationRoot) {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state.federation_root = root;
        state.epoch = state.epoch.saturating_add(1);
    }

    pub fn replace_catalog(&self, catalog: crate::CatalogState) {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state.catalog = catalog;
        state.epoch = state.epoch.saturating_add(1);
        drop(state);
        self.invalidate_routes();
    }

    pub fn activate_feature_level(&self, feature_level: u64) -> Result<(), String> {
        if !(crate::FEATURE_LEVEL_BASELINE..=crate::CURRENT_FEATURE_LEVEL).contains(&feature_level)
        {
            return Err("feature level is not supported by this binary".into());
        }
        let mut state = self.state.write().expect("metadata lock poisoned");
        if feature_level <= state.active_feature_level {
            return Ok(());
        }
        let cell_id = state.cell_id;
        let protocol_floor = state.federation_root.min_protocol_version;
        state.scoped_feature_levels.activate(
            crate::FeatureActivation {
                scope: crate::FeatureScope::Cell(cell_id),
                feature_level,
                activated_at_ms: 0,
                minimum_protocol_version: 1,
            },
            protocol_floor,
        )?;
        state.active_feature_level = feature_level;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn node(&self, node_id: NodeId) -> Option<NodeDescriptor> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .nodes
            .get(&node_id)
            .cloned()
    }

    pub fn register_node(&self, descriptor: NodeDescriptor) -> Result<bool, String> {
        super::placement::validate_node_descriptor(descriptor.id, &descriptor)?;
        if !descriptor.raft_address.starts_with("https://") {
            return Err("discovered node Raft address must use https://".into());
        }
        let peer_id = descriptor
            .peer_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "discovered node must provide a peer ID".to_owned())?;
        let mut state = self.state.write().expect("metadata lock poisoned");
        if descriptor.cell_id != state.cell_id {
            return Err("node belongs to a different Cell".into());
        }
        if let Some(existing) = state.nodes.get_mut(&descriptor.id) {
            if !same_node_endpoint(existing, &descriptor) {
                return Err("node ID is already registered with a different endpoint".into());
            }
            match existing.peer_id.as_deref() {
                Some(current) if current != peer_id => {
                    return Err("node ID is already bound to another peer ID".into())
                }
                Some(_) => return Ok(false),
                None => existing.peer_id = Some(peer_id.to_owned()),
            }
            state.epoch = state.epoch.saturating_add(1);
            return Ok(true);
        }
        if state.nodes.len() >= 9 {
            return Err("cluster already contains the maximum of 9 nodes".into());
        }
        if state.nodes.values().any(|node| {
            node.raft_address == descriptor.raft_address || node.peer_id.as_deref() == Some(peer_id)
        }) {
            return Err("node endpoint or peer ID is already registered".into());
        }
        state
            .node_health
            .insert(descriptor.id, NodeHealthRecord::default());
        let cell_id = state.cell_id;
        state.federation_root.register_node(crate::FederationNode {
            id: descriptor.id,
            failure_domain: descriptor.failure_domain.clone(),
            placement: crate::NodePlacement::Member(cell_id),
            stable_since_ms: 0,
            available: false,
            protocol_version: 1,
            feature_level: crate::FEATURE_LEVEL_BASELINE,
        })?;
        let cell = state
            .federation_root
            .cells
            .get_mut(&cell_id)
            .expect("local Cell exists in federation root");
        cell.nodes.insert(descriptor.id);
        if descriptor.federation_router && cell.routers.len() < 3 {
            cell.routers.insert(descriptor.id);
        }
        state.nodes.insert(descriptor.id, descriptor);
        state.epoch = state.epoch.saturating_add(1);
        Ok(true)
    }

    pub fn replace(&self, state: ClusterMetadata) -> Result<(), String> {
        validate_cluster_state(&state)?;
        *self.state.write().expect("metadata lock poisoned") = state;
        self.invalidate_routes();
        Ok(())
    }

    pub fn ensure_topic(
        &self,
        name: &str,
        partitions: Option<u16>,
        replication_factor: Option<u8>,
    ) -> Result<TopicDescriptor, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if let Some(existing) = state.topics.get(name) {
            if existing.state == TopicState::Deleting {
                return Err("topic is being deleted".into());
            }
            let requested_partitions = partitions.unwrap_or(existing.partitions.len() as u16);
            let requested_rf = replication_factor.unwrap_or(existing.replication_factor);
            if requested_partitions != existing.partitions.len() as u16
                || requested_rf != existing.replication_factor
            {
                return Err("topic partition count and replication factor are immutable".into());
            }
            return Ok(existing.clone());
        }
        let partitions = partitions.unwrap_or(self.default_partitions);
        let replication_factor = replication_factor.unwrap_or(self.default_replication_factor);
        let available_nodes: BTreeMap<_, _> = state
            .nodes
            .iter()
            .filter(|(id, _)| {
                !state.drained_nodes.contains(id)
                    && state
                        .node_health
                        .get(id)
                        .is_some_and(|health| health.available && health.storage_eligible)
            })
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        validate_replication_factor(replication_factor, available_nodes.len())?;
        validate_failure_domains(&available_nodes, replication_factor)?;
        if partitions == 0 {
            return Err("partition count must be greater than zero".into());
        }
        let mut loads = replica_loads(&state);
        let mut descriptors = Vec::with_capacity(partitions as usize);
        let cell_id = state.cell_id;
        for number in 0..partitions {
            let group_id = state.next_group_id;
            // NSQ message IDs only need to be unambiguous inside a topic: a
            // consumer connection subscribes to exactly one topic and ACK
            // routing already carries that topic. Reusing this local namespace
            // across topics and Cells removes the old global 65,535 partition
            // allocation ceiling.
            let slot = number
                .checked_add(1)
                .ok_or_else(|| "topic partition wire slot space exhausted".to_owned())?;
            let replicas = choose_replicas(
                &available_nodes,
                &loads,
                replication_factor as usize,
                slot as usize,
            );
            for node in &replicas {
                *loads.entry(*node).or_default() += 1;
            }
            descriptors.push(PartitionDescriptor {
                group_id,
                origin_cell: cell_id,
                number,
                slot,
                replication_factor,
                replicas,
                leader_hint: None,
                lifecycle: PartitionLifecycle::Active,
                operation_id: None,
                home_cell: cell_id,
                wire_incarnation: 1,
            });
            state.next_group_id = state.next_group_id.saturating_add(1);
            state.next_slot = state.next_slot.max(u32::from(slot).saturating_add(1));
        }
        let key_routing_slots = descriptors.iter().map(|partition| partition.slot).collect();
        let topic = TopicDescriptor {
            name: name.to_owned(),
            state: TopicState::Active,
            replication_factor,
            partitions: descriptors,
            channels: BTreeMap::new(),
            next_channel_generation: 1,
            paused: false,
            topology_generation: 1,
            key_routing_slots,
            channel_catalog_revision: 0,
        };
        state.topics.insert(name.to_owned(), topic.clone());
        let catalog_partitions = topic
            .partitions
            .iter()
            .map(|partition| crate::PartitionHome {
                id: partition.global_id(),
                number: u32::from(partition.number),
                wire_slot: partition.slot,
                wire_incarnation: partition.wire_incarnation,
                home_cell: partition.home_cell,
                lifecycle: crate::PartitionHomeLifecycle::Active,
                routing_epoch: 1,
            })
            .collect();
        let feature_level = state.active_feature_level;
        state.catalog.create_topic(
            name,
            catalog_partitions,
            crate::RoutingMode::Elastic,
            feature_level,
            self.max_home_cells_per_topic,
        )?;
        super::routes::bump_routing_epoch(&mut state);
        state.epoch = state.epoch.saturating_add(1);
        Ok(topic)
    }

    pub fn delete_topic(&self, name: &str) {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if state.topics.remove(name).is_some() {
            state.catalog.topics.remove(name);
            state.catalog.epoch = state.catalog.epoch.saturating_add(1);
            state.epoch = state.epoch.saturating_add(1);
            super::routes::bump_routing_epoch(&mut state);
        }
    }

    pub fn prepare_delete_topic(&self, name: &str) -> Option<TopicDescriptor> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let topic = state.topics.get_mut(name)?;
        if topic.state != TopicState::Deleting {
            topic.state = TopicState::Deleting;
            state.epoch = state.epoch.saturating_add(1);
            super::routes::bump_routing_epoch(&mut state);
        }
        state.topics.get(name).cloned()
    }

    pub fn deleting_topics(&self) -> Vec<TopicDescriptor> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .values()
            .filter(|topic| topic.state == TopicState::Deleting)
            .cloned()
            .collect()
    }

    pub fn topic(&self, name: &str) -> Option<TopicDescriptor> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .get(name)
            .cloned()
    }

    pub fn topic_is_active(&self, name: &str) -> bool {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .topics
            .get(name)
            .is_some_and(|topic| topic.state == TopicState::Active)
    }

    pub fn partition(
        &self,
        group_id: crate::GlobalGroupId,
    ) -> Option<(String, PartitionDescriptor)> {
        self.partition_route(group_id)
            .map(|(topic, partition)| (topic, (*partition).clone()))
    }

    pub fn update_partition_replicas(
        &self,
        group_id: crate::GlobalGroupId,
        replicas: BTreeSet<NodeId>,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if replicas.iter().any(|node| !state.nodes.contains_key(node)) {
            return Err("replica set contains an unknown node".into());
        }
        let expected_rf = state
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.global_id() == group_id)
            .map(|partition| partition.replication_factor as usize)
            .ok_or_else(|| "partition group not found".to_owned())?;
        if replicas.len() != expected_rf {
            return Err(format!("partition requires exactly {expected_rf} replicas"));
        }
        if expected_rf >= 3 {
            let domains: BTreeSet<_> = replicas
                .iter()
                .map(|node| state.nodes[node].failure_domain.as_str())
                .collect();
            if domains.len() != expected_rf {
                return Err(format!(
                    "RF={expected_rf} requires distinct failure domains"
                ));
            }
        }
        let partition = state
            .topics
            .values_mut()
            .flat_map(|topic| &mut topic.partitions)
            .find(|partition| partition.global_id() == group_id)
            .expect("partition was found above");
        if partition.replicas != replicas {
            partition.replicas = replicas;
            partition.leader_hint = None;
            state.epoch = state.epoch.saturating_add(1);
            super::routes::bump_routing_epoch(&mut state);
        }
        Ok(())
    }

    pub fn set_node_drained(&self, node_id: NodeId, drained: bool) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if !state.nodes.contains_key(&node_id) {
            return Err("cannot drain an unknown node".into());
        }
        let changed = if drained {
            state.drained_nodes.insert(node_id)
        } else {
            state.drained_nodes.remove(&node_id)
        };
        if changed {
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(())
    }
}
