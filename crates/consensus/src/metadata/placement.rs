use super::*;

pub fn validate_replication_factor(
    replication_factor: u8,
    node_count: usize,
) -> Result<(), String> {
    if !matches!(replication_factor, 1 | 3 | 5) {
        return Err("replication factor must be 3 or 5".into());
    }
    if replication_factor == 1 && node_count != 1 {
        return Err("replication factor 1 is reserved for standalone mode".into());
    }
    if replication_factor as usize > node_count {
        return Err("replication factor exceeds healthy node count".into());
    }
    Ok(())
}

pub(super) fn validate_cluster(
    nodes: &BTreeMap<NodeId, NodeDescriptor>,
    replication_factor: u8,
) -> Result<(), String> {
    if replication_factor == 1 && nodes.len() == 1 {
        return Ok(());
    }
    if !(3..=9).contains(&nodes.len()) {
        return Err("cluster must contain 3 to 9 nodes".into());
    }
    for (id, node) in nodes {
        validate_node_descriptor(*id, node)?;
    }
    validate_replication_factor(replication_factor, nodes.len())?;
    validate_failure_domains(nodes, replication_factor)
}

pub(super) fn validate_cluster_state(state: &ClusterMetadata) -> Result<(), String> {
    if state.cell_id.0 == 0
        || state
            .nodes
            .values()
            .any(|node| node.cell_id != state.cell_id)
    {
        return Err("metadata contains nodes outside its Cell".into());
    }
    if state.next_group_id < FIRST_GROUP_ID
        || state.next_slot < FIRST_SLOT
        || state.next_slot > MAX_SLOT + 1
    {
        return Err("invalid metadata allocation boundary".into());
    }
    if state
        .drained_nodes
        .iter()
        .any(|node| !state.nodes.contains_key(node))
    {
        return Err("drained node set contains an unknown node".into());
    }
    if !(1..=9).contains(&state.nodes.len()) {
        return Err("cluster metadata must contain 1 to 9 nodes".into());
    }
    for (id, node) in &state.nodes {
        validate_node_descriptor(*id, node)?;
        if !state
            .federation_root
            .nodes
            .get(id)
            .is_some_and(|root_node| {
                root_node.placement == crate::NodePlacement::Member(state.cell_id)
            })
        {
            return Err("metadata node is missing from the federation Root view".into());
        }
    }
    for topic in state.topics.values() {
        validate_replication_factor(topic.replication_factor, state.nodes.len())?;
        validate_failure_domains(&state.nodes, topic.replication_factor)?;
        for partition in &topic.partitions {
            if partition.replicas.len() != partition.replication_factor as usize
                || partition.home_cell != state.cell_id
                || !partition
                    .replicas
                    .iter()
                    .all(|node| state.nodes.contains_key(node))
            {
                return Err("partition contains an invalid replica set".into());
            }
            if partition.slot != partition.number.saturating_add(1)
                || partition.wire_incarnation == 0
            {
                return Err("partition has an invalid topic-local wire identity".into());
            }
        }
        if !state.catalog.topics.contains_key(&topic.name) {
            return Err("topic is missing from its Catalog shard".into());
        }
        if topic.key_routing_slots.is_empty()
            || topic.key_routing_slots.iter().any(|slot| {
                !topic.partitions.iter().any(|partition| {
                    partition.slot == *slot && partition.lifecycle == PartitionLifecycle::Active
                })
            })
        {
            return Err("topic contains an invalid key routing slot set".into());
        }
    }
    Ok(())
}

pub(super) fn validate_node_descriptor(id: NodeId, node: &NodeDescriptor) -> Result<(), String> {
    if id == 0 || node.id != id || node.cell_id.0 == 0 {
        return Err("node IDs must be non-zero and match their map key".into());
    }
    if !node.raft_address.is_empty() && !node.raft_address.starts_with("https://") {
        return Err("node Raft address must use https://".into());
    }
    if node.broadcast_address.trim().is_empty()
        || node.failure_domain.trim().is_empty()
        || node.tls_server_name.trim().is_empty()
        || node.tcp_port == 0
        || node.http_port == 0
    {
        return Err("node addresses, ports, TLS name, and failure domain are required".into());
    }
    if node.raft_address.len() > 512
        || node.broadcast_address.len() > 255
        || node.tls_server_name.len() > 253
        || node.failure_domain.len() > 128
        || node.peer_id.as_ref().is_some_and(|peer| peer.len() > 128)
    {
        return Err("node descriptor contains an oversized field".into());
    }
    Ok(())
}

pub(super) fn validate_failure_domains(
    nodes: &BTreeMap<NodeId, NodeDescriptor>,
    replication_factor: u8,
) -> Result<(), String> {
    if replication_factor >= 3 {
        let domains = nodes
            .values()
            .map(|node| &node.failure_domain)
            .collect::<BTreeSet<_>>()
            .len();
        if domains < replication_factor as usize {
            return Err(format!(
                "RF={replication_factor} requires {replication_factor} distinct failure domains"
            ));
        }
    }
    Ok(())
}

pub(super) fn replica_loads(state: &ClusterMetadata) -> BTreeMap<NodeId, usize> {
    let mut loads: BTreeMap<_, _> = state.nodes.keys().map(|id| (*id, 0)).collect();
    for partition in state
        .topics
        .values()
        .flat_map(|topic| &topic.partitions)
        .filter(|partition| partition.lifecycle != PartitionLifecycle::Retired)
    {
        for node in &partition.replicas {
            *loads.entry(*node).or_default() += 1;
        }
    }
    loads
}

pub(super) fn choose_replicas(
    nodes: &BTreeMap<NodeId, NodeDescriptor>,
    loads: &BTreeMap<NodeId, usize>,
    count: usize,
    seed: usize,
) -> BTreeSet<NodeId> {
    let node_ids: Vec<_> = nodes.keys().copied().collect();
    let mut selected = BTreeSet::new();
    let mut domains = BTreeSet::new();
    while selected.len() < count {
        let mut candidates: Vec<_> = nodes
            .values()
            .filter(|node| !selected.contains(&node.id))
            .collect();
        candidates.sort_by_key(|node| {
            let position = node_ids
                .iter()
                .position(|id| *id == node.id)
                .expect("candidate belongs to node list");
            (
                usize::from(domains.contains(&node.failure_domain)),
                loads.get(&node.id).copied().unwrap_or_default(),
                (position + node_ids.len() - seed % node_ids.len()) % node_ids.len(),
                node.id,
            )
        });
        let selected_node = candidates[0];
        selected.insert(selected_node.id);
        domains.insert(selected_node.failure_domain.clone());
    }
    selected
}
