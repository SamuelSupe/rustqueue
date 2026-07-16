use crate::{BrokerEndpoint, BrokerRegistry, Producer};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct Directory {
    inner: Arc<RwLock<State>>,
}

#[derive(Default)]
struct State {
    endpoints: BTreeSet<BrokerEndpoint>,
    brokers: BTreeMap<BrokerEndpoint, Observation>,
}

struct Observation {
    registry: BrokerRegistry,
    seen_at: Instant,
}

impl Directory {
    pub fn replace_endpoints(&self, endpoints: BTreeSet<BrokerEndpoint>) {
        self.inner.write().endpoints = endpoints;
    }

    pub fn endpoints(&self) -> Vec<BrokerEndpoint> {
        self.inner.read().endpoints.iter().cloned().collect()
    }

    pub fn observe(&self, endpoint: BrokerEndpoint, registry: BrokerRegistry) {
        if registry.format != 7 {
            return;
        }
        self.inner.write().brokers.insert(
            endpoint,
            Observation {
                registry,
                seen_at: Instant::now(),
            },
        );
    }

    pub fn expire(&self, max_age: Duration) {
        let mut state = self.inner.write();
        let endpoints = state.endpoints.clone();
        state.brokers.retain(|endpoint, observation| {
            endpoints.contains(endpoint) && observation.seen_at.elapsed() <= max_age
        });
    }

    pub fn topics(&self) -> Vec<String> {
        let mut topics = BTreeSet::new();
        for observation in self
            .inner
            .read()
            .brokers
            .values()
            .filter(|item| item.registry.consume_ready || item.registry.ready)
        {
            topics.extend(
                observation
                    .registry
                    .topics
                    .iter()
                    .map(|topic| topic.name.clone()),
            );
        }
        topics.into_iter().collect()
    }

    pub fn channels(&self, topic: &str) -> Vec<String> {
        let mut channels = BTreeSet::new();
        for observation in self
            .inner
            .read()
            .brokers
            .values()
            .filter(|item| item.registry.consume_ready || item.registry.ready)
        {
            if let Some(found) = observation
                .registry
                .topics
                .iter()
                .find(|item| item.name == topic)
            {
                channels.extend(found.channels.iter().cloned());
            }
        }
        channels.into_iter().collect()
    }

    pub fn producers(&self, topic: Option<&str>) -> Vec<Producer> {
        self.inner
            .read()
            .brokers
            .values()
            .filter(|item| item.registry.consume_ready || item.registry.ready)
            .filter(|item| {
                topic.is_none_or(|name| item.registry.topics.iter().any(|topic| topic.name == name))
            })
            .map(|item| Producer::from_registry(&item.registry))
            .collect()
    }

    pub fn publishers(&self) -> Vec<Producer> {
        self.inner
            .read()
            .brokers
            .values()
            .filter(|item| item.registry.publish_ready)
            .map(|item| Producer::from_registry(&item.registry))
            .collect()
    }

    pub fn broker_count(&self) -> usize {
        self.inner
            .read()
            .brokers
            .values()
            .filter(|item| item.registry.consume_ready || item.registry.ready)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RegistryTopic;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn indexes_five_hundred_brokers_without_global_metadata() {
        let directory = Directory::default();
        let endpoints: BTreeSet<_> = (0..500)
            .map(|index| BrokerEndpoint {
                address: IpAddr::V4(Ipv4Addr::new(
                    10,
                    (index / 250) as u8,
                    (index % 250) as u8,
                    1,
                )),
                http_port: 4151,
            })
            .collect();
        directory.replace_endpoints(endpoints.clone());
        for (index, endpoint) in endpoints.into_iter().enumerate() {
            directory.observe(
                endpoint,
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
                },
            );
        }
        assert_eq!(directory.producers(Some("events")).len(), 500);
        assert_eq!(directory.topics(), vec!["events"]);
        assert_eq!(directory.channels("events"), vec!["workers"]);
    }
}
