use crate::{BrokerEndpoint, BrokerRegistry, BrokerRegistryHead, DiscoveryMetrics, Producer};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    revision: u64,
}

struct Observation {
    registry: BrokerRegistry,
    seen_at: Instant,
}

impl Directory {
    pub fn metrics(&self) -> &DiscoveryMetrics {
        &self.metrics
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
        self.inner.read().revision
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
        endpoints
            .iter()
            .filter_map(|endpoint| state.brokers.get(endpoint))
            .map(|item| Producer::from_registry(&item.registry))
            .collect()
    }

    pub fn publishers(&self) -> Vec<Producer> {
        let state = self.inner.read();
        state
            .publishers
            .iter()
            .filter_map(|endpoint| state.brokers.get(endpoint))
            .map(|item| Producer::from_registry(&item.registry))
            .collect()
    }

    pub fn broker_count(&self) -> usize {
        self.inner.read().consumers.len()
    }

    pub fn publisher_count(&self) -> usize {
        self.inner.read().publishers.len()
    }
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
    state.revision = state.revision.wrapping_add(1).max(1);
}

fn remove(state: &mut State, endpoint: &BrokerEndpoint) {
    let Some(observation) = state.brokers.remove(endpoint) else {
        return;
    };
    deindex(state, endpoint, &observation.registry);
    state.revision = state.revision.wrapping_add(1).max(1);
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
        assert_eq!(directory.topics(), vec!["events"]);
        assert_eq!(directory.channels("events"), vec!["workers"]);
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
}
