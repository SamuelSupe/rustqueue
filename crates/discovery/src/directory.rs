use crate::{BrokerEndpoint, BrokerRegistry, BrokerRegistryHead, DiscoveryMetrics, Producer};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

const KODO_BROKER_COUNT: usize = 3;

#[derive(Clone, Default)]
pub struct Directory {
    inner: Arc<RwLock<State>>,
    metrics: DiscoveryMetrics,
}

#[derive(Default)]
struct State {
    endpoints: BTreeSet<BrokerEndpoint>,
    brokers: BTreeMap<BrokerEndpoint, Observation>,
    topic_owners: BTreeMap<String, BTreeSet<BrokerEndpoint>>,
    channel_owners: BTreeMap<String, BTreeMap<String, usize>>,
    publishers: BTreeSet<BrokerEndpoint>,
    consumers: BTreeSet<BrokerEndpoint>,
    kodo_compatibility_enabled: bool,
    kodo_gateways: Vec<Producer>,
    kodo_cleanup_enabled: bool,
    source_stale_after: Option<Duration>,
    last_source_success: Option<Instant>,
}

struct Observation {
    registry: BrokerRegistry,
    seen_at: Instant,
}

impl Directory {
    pub fn metrics(&self) -> &DiscoveryMetrics {
        &self.metrics
    }

    pub fn configure_kodo(&self, gateways: Vec<Producer>, cleanup_enabled: bool) {
        let mut state = self.inner.write();
        state.kodo_compatibility_enabled = true;
        state.kodo_gateways = gateways;
        state.kodo_cleanup_enabled = cleanup_enabled;
    }

    pub fn configure_source_health(&self, stale_after: Duration) {
        self.inner.write().source_stale_after = Some(stale_after);
    }

    pub fn mark_source_success(&self) {
        self.inner.write().last_source_success = Some(Instant::now());
    }

    pub fn source_ready(&self) -> bool {
        let state = self.inner.read();
        source_ready(&state)
    }

    pub fn lookup_ready(&self) -> bool {
        let state = self.inner.read();
        source_ready(&state)
            && (!state.kodo_compatibility_enabled
                || (state.kodo_gateways.len() == KODO_BROKER_COUNT
                    && kodo_broker_inventory_ready(&state)))
    }

    pub fn replace_endpoints(&self, endpoints: BTreeSet<BrokerEndpoint>) {
        let mut state = self.inner.write();
        let removed: Vec<_> = state
            .brokers
            .keys()
            .filter(|endpoint| !endpoints.contains(*endpoint))
            .cloned()
            .collect();
        for endpoint in removed {
            remove(&mut state, &endpoint);
        }
        state.endpoints = endpoints;
    }

    pub fn endpoints(&self) -> Vec<BrokerEndpoint> {
        self.inner.read().endpoints.iter().cloned().collect()
    }

    pub fn observe(&self, endpoint: BrokerEndpoint, registry: BrokerRegistry) {
        if registry.format != 7 {
            return;
        }
        let mut state = self.inner.write();
        if state.brokers.get_mut(&endpoint).is_some_and(|observation| {
            if observation.registry == registry {
                observation.seen_at = Instant::now();
                true
            } else {
                false
            }
        }) {
            return;
        }
        remove(&mut state, &endpoint);
        add(&mut state, endpoint, registry);
    }

    pub fn observe_head(&self, endpoint: &BrokerEndpoint, head: &BrokerRegistryHead) -> bool {
        if head.format != 7 {
            return true;
        }
        let mut state = self.inner.write();
        let Some(observation) = state.brokers.get(endpoint) else {
            return true;
        };
        if observation.registry.revision != head.revision
            || observation.registry.node_id != head.node_id
        {
            return true;
        }
        if observation.registry.ready == head.ready
            && observation.registry.publish_ready == head.publish_ready
            && observation.registry.consume_ready == head.consume_ready
        {
            state.brokers.get_mut(endpoint).unwrap().seen_at = Instant::now();
            return false;
        }
        let mut registry = state.brokers.remove(endpoint).unwrap().registry;
        deindex(&mut state, endpoint, &registry);
        registry.ready = head.ready;
        registry.publish_ready = head.publish_ready;
        registry.consume_ready = head.consume_ready;
        add(&mut state, endpoint.clone(), registry);
        false
    }

    pub fn expire(&self, max_age: Duration) {
        let mut state = self.inner.write();
        let expired: Vec<_> = state
            .brokers
            .iter()
            .filter(|(endpoint, observation)| {
                !state.endpoints.contains(*endpoint) || observation.seen_at.elapsed() > max_age
            })
            .map(|(endpoint, _)| endpoint.clone())
            .collect();
        for endpoint in expired {
            remove(&mut state, &endpoint);
        }
    }

    pub fn revision(&self) -> u64 {
        routing_revision(&self.inner.read())
    }

    pub fn topics(&self) -> Vec<String> {
        self.inner.read().topic_owners.keys().cloned().collect()
    }

    pub fn channels(&self, topic: &str) -> Vec<String> {
        self.inner
            .read()
            .channel_owners
            .get(topic)
            .map(|channels| channels.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn producers(&self, topic: Option<&str>) -> Vec<Producer> {
        let state = self.inner.read();
        let endpoints = match topic {
            Some(topic) => state.topic_owners.get(topic),
            None => Some(&state.consumers),
        };
        let Some(endpoints) = endpoints else {
            return Vec::new();
        };
        let mut producers = unique_producers(
            endpoints
                .iter()
                .filter_map(|endpoint| state.brokers.get(endpoint))
                .map(|item| &item.registry),
        );
        if state.kodo_cleanup_enabled {
            for producer in &mut producers {
                producer.http_port = 4152;
            }
        }
        producers
    }

    pub fn publishers(&self) -> Vec<Producer> {
        let state = self.inner.read();
        publisher_producers(&state)
    }

    pub fn brokers(&self) -> Vec<Producer> {
        broker_producers(&self.inner.read())
    }

    pub fn publisher_snapshot(&self) -> (u64, Vec<Producer>) {
        let state = self.inner.read();
        (routing_revision(&state), publisher_producers(&state))
    }

    pub fn broker_snapshot(&self) -> (u64, Vec<Producer>) {
        let state = self.inner.read();
        (routing_revision(&state), broker_producers(&state))
    }

    pub fn publisher_head(&self) -> (u64, usize) {
        let state = self.inner.read();
        (routing_revision(&state), publisher_producers(&state).len())
    }

    pub fn node_producers(&self) -> Vec<Producer> {
        let state = self.inner.read();
        if state.kodo_compatibility_enabled {
            return state.kodo_gateways.clone();
        }
        publisher_producers(&state)
    }

    pub fn kodo_nodes_ready(&self) -> bool {
        let state = self.inner.read();
        !state.kodo_compatibility_enabled || !state.kodo_gateways.is_empty()
    }

    pub fn kodo_cleanup_enabled(&self) -> bool {
        self.inner.read().kodo_cleanup_enabled
    }

    pub fn broker_count(&self) -> usize {
        let state = self.inner.read();
        consumer_node_ids(&state).len()
    }

    pub fn publisher_count(&self) -> usize {
        let state = self.inner.read();
        publisher_producers(&state).len()
    }
}

fn source_ready(state: &State) -> bool {
    state
        .last_source_success
        .zip(state.source_stale_after)
        .is_some_and(|(last_success, stale_after)| last_success.elapsed() <= stale_after)
}

fn publisher_producers(state: &State) -> Vec<Producer> {
    unique_producers(
        state
            .publishers
            .iter()
            .filter_map(|endpoint| state.brokers.get(endpoint))
            .map(|item| &item.registry),
    )
}

fn broker_producers(state: &State) -> Vec<Producer> {
    unique_producers(state.brokers.values().map(|item| &item.registry))
}

fn unique_producers<'a>(registries: impl IntoIterator<Item = &'a BrokerRegistry>) -> Vec<Producer> {
    let mut producers = BTreeMap::new();
    for registry in registries {
        producers
            .entry(registry.node_id)
            .or_insert_with(|| Producer::from_registry(registry));
    }
    producers.into_values().collect()
}

fn consumer_node_ids(state: &State) -> BTreeSet<u64> {
    state
        .consumers
        .iter()
        .filter_map(|endpoint| state.brokers.get(endpoint))
        .map(|item| item.registry.node_id)
        .collect()
}

fn kodo_broker_inventory_ready(state: &State) -> bool {
    let node_ids = consumer_node_ids(state);
    if node_ids.len() != KODO_BROKER_COUNT || node_ids.contains(&0) {
        return false;
    }
    node_ids
        .iter()
        .map(|node_id| (node_id - 1) % KODO_BROKER_COUNT as u64)
        .collect::<BTreeSet<_>>()
        .len()
        == KODO_BROKER_COUNT
}

fn routing_revision(state: &State) -> u64 {
    let mut hasher = Sha256::new();
    for producer in publisher_producers(state) {
        hash_producer(&mut hasher, b'P', &producer);
    }
    for producer in broker_producers(state) {
        hash_producer(&mut hasher, b'B', &producer);
    }
    let digest = hasher.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix")).max(1)
}

fn hash_producer(hasher: &mut Sha256, role: u8, producer: &Producer) {
    hasher.update([role]);
    hasher.update(producer.node_id.to_be_bytes());
    hasher.update((producer.broadcast_address.len() as u64).to_be_bytes());
    hasher.update(producer.broadcast_address.as_bytes());
    hasher.update(producer.tcp_port.to_be_bytes());
    hasher.update(producer.http_port.to_be_bytes());
}

fn add(state: &mut State, endpoint: BrokerEndpoint, registry: BrokerRegistry) {
    index(state, &endpoint, &registry);
    state.brokers.insert(
        endpoint,
        Observation {
            registry,
            seen_at: Instant::now(),
        },
    );
}

fn remove(state: &mut State, endpoint: &BrokerEndpoint) {
    let Some(observation) = state.brokers.remove(endpoint) else {
        return;
    };
    deindex(state, endpoint, &observation.registry);
}

fn index(state: &mut State, endpoint: &BrokerEndpoint, registry: &BrokerRegistry) {
    if registry.publish_ready {
        state.publishers.insert(endpoint.clone());
    }
    if !consume_ready(registry) {
        return;
    }
    state.consumers.insert(endpoint.clone());
    for topic in &registry.topics {
        state
            .topic_owners
            .entry(topic.name.clone())
            .or_default()
            .insert(endpoint.clone());
        let channels = state.channel_owners.entry(topic.name.clone()).or_default();
        for channel in &topic.channels {
            *channels.entry(channel.clone()).or_default() += 1;
        }
    }
}

fn deindex(state: &mut State, endpoint: &BrokerEndpoint, registry: &BrokerRegistry) {
    state.publishers.remove(endpoint);
    state.consumers.remove(endpoint);
    if !consume_ready(registry) {
        return;
    }
    for topic in &registry.topics {
        if let Some(owners) = state.topic_owners.get_mut(&topic.name) {
            owners.remove(endpoint);
            if owners.is_empty() {
                state.topic_owners.remove(&topic.name);
            }
        }
        if let Some(channels) = state.channel_owners.get_mut(&topic.name) {
            for channel in &topic.channels {
                if let Some(owners) = channels.get_mut(channel) {
                    *owners = owners.saturating_sub(1);
                    if *owners == 0 {
                        channels.remove(channel);
                    }
                }
            }
            if channels.is_empty() {
                state.channel_owners.remove(&topic.name);
            }
        }
    }
}

fn consume_ready(registry: &BrokerRegistry) -> bool {
    registry.consume_ready || registry.ready
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RegistryTopic;
    use std::net::{IpAddr, Ipv4Addr};

    fn endpoint(index: usize) -> BrokerEndpoint {
        BrokerEndpoint {
            address: IpAddr::V4(Ipv4Addr::new(
                10,
                (index / 250) as u8,
                (index % 250) as u8,
                1,
            )),
            http_port: 4151,
        }
    }

    fn registry(index: usize) -> BrokerRegistry {
        BrokerRegistry {
            format: 7,
            revision: 1,
            node_id: index as u64 + 1,
            ready: true,
            publish_ready: true,
            consume_ready: true,
            broadcast_address: format!("broker-{index}.rustqueue"),
            tcp_port: 4150,
            http_port: 4151,
            stored_messages: 1,
            depth: 1,
            in_flight: 0,
            topics: vec![RegistryTopic {
                name: "events".into(),
                paused: false,
                channels: vec!["workers".into()],
                stored_messages: 1,
            }],
            compatibility: None,
        }
    }

    #[test]
    fn indexes_five_hundred_brokers_without_query_time_scans() {
        let directory = Directory::default();
        let endpoints: BTreeSet<_> = (0..500).map(endpoint).collect();
        directory.replace_endpoints(endpoints.clone());
        for (index, endpoint) in endpoints.into_iter().enumerate() {
            directory.observe(endpoint, registry(index));
        }
        assert_eq!(directory.producers(Some("events")).len(), 500);
        assert_eq!(directory.broker_count(), 500);
        assert_eq!(directory.topics(), vec!["events"]);
        assert_eq!(directory.channels("events"), vec!["workers"]);
    }

    #[test]
    fn duplicate_registry_endpoints_do_not_inflate_broker_inventory() {
        let directory = Directory::default();
        let first = endpoint(1);
        let duplicate = endpoint(2);
        directory.replace_endpoints([first.clone(), duplicate.clone()].into_iter().collect());
        let registry = registry(0);
        directory.observe(first, registry.clone());
        directory.observe(duplicate, registry);

        assert_eq!(directory.producers(Some("events")).len(), 1);
        assert_eq!(directory.publishers().len(), 1);
        assert_eq!(directory.brokers().len(), 1);
        assert_eq!(directory.broker_count(), 1);
        assert_eq!(directory.publisher_count(), 1);
        assert_eq!(directory.publisher_head().1, 1);
    }

    #[test]
    fn kodo_lookup_requires_one_unique_broker_per_stats_shard() {
        let directory = Directory::default();
        directory.configure_source_health(Duration::from_secs(5));
        directory.configure_kodo(
            (0..KODO_BROKER_COUNT)
                .map(|ordinal| Producer::gateway("gateway".into(), ordinal))
                .collect(),
            false,
        );
        for (index, node_id) in [1, 2, 4].into_iter().enumerate() {
            let endpoint = endpoint(index);
            let mut registry = registry(index);
            registry.node_id = node_id;
            directory.observe(endpoint, registry);
        }
        directory.mark_source_success();
        assert!(!directory.lookup_ready());

        let endpoint = endpoint(2);
        let mut registry = registry(2);
        registry.node_id = 3;
        directory.observe(endpoint, registry);
        assert!(directory.lookup_ready());
    }

    #[test]
    fn unchanged_head_refreshes_liveness_without_reindexing() {
        let directory = Directory::default();
        let endpoint = endpoint(1);
        directory.replace_endpoints([endpoint.clone()].into_iter().collect());
        directory.observe(endpoint.clone(), registry(1));
        let revision = directory.revision();
        assert!(!directory.observe_head(
            &endpoint,
            &BrokerRegistryHead {
                format: 7,
                revision: 1,
                node_id: 2,
                ready: true,
                publish_ready: true,
                consume_ready: true,
            }
        ));
        assert_eq!(directory.revision(), revision);
    }

    #[test]
    fn routing_revision_is_content_based_across_discovery_replicas() {
        let first = Directory::default();
        let second = Directory::default();
        let first_endpoint = endpoint(1);
        let second_endpoint = endpoint(2);
        first.replace_endpoints([first_endpoint.clone()].into_iter().collect());
        second.replace_endpoints([second_endpoint.clone()].into_iter().collect());
        first.observe(first_endpoint, registry(1));
        second.observe(second_endpoint, registry(2));

        assert_ne!(first.revision(), second.revision());

        let replica = Directory::default();
        let first_endpoint = endpoint(1);
        replica.replace_endpoints([first_endpoint.clone()].into_iter().collect());
        replica.observe(first_endpoint, registry(1));
        assert_eq!(first.revision(), replica.revision());
    }

    #[test]
    fn staged_kodo_mode_never_falls_back_to_direct_broker_publishers() {
        let directory = Directory::default();
        let endpoint = endpoint(1);
        directory.replace_endpoints([endpoint.clone()].into_iter().collect());
        directory.observe(endpoint, registry(1));
        assert_eq!(directory.node_producers().len(), 1);

        directory.configure_kodo(Vec::new(), false);

        assert!(directory.node_producers().is_empty());
        assert_eq!(directory.producers(Some("events")).len(), 1);
    }

    #[test]
    fn nodes_exclude_a_broker_that_can_consume_but_cannot_publish() {
        let directory = Directory::default();
        let endpoint = endpoint(1);
        let mut draining = registry(1);
        draining.publish_ready = false;
        directory.replace_endpoints([endpoint.clone()].into_iter().collect());
        directory.observe(endpoint, draining);

        assert!(directory.node_producers().is_empty());
        assert_eq!(directory.producers(Some("events")).len(), 1);
    }

    #[test]
    fn cleanup_uses_the_network_isolated_broker_compatibility_port() {
        let directory = Directory::default();
        let endpoint = endpoint(1);
        directory.replace_endpoints([endpoint.clone()].into_iter().collect());
        directory.observe(endpoint, registry(1));
        directory.configure_kodo(Vec::new(), true);

        let producer = directory.producers(Some("events")).remove(0);
        assert_eq!(producer.broadcast_address, "broker-1.rustqueue");
        assert_eq!(producer.tcp_port, 4150);
        assert_eq!(producer.http_port, 4152);
        assert!(directory.node_producers().is_empty());
    }
}
