use crate::metadata::TopicRoute;
use crate::{GlobalGroupId, PartitionDescriptor};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

// Keep empty-topic work proportional to consumer demand. Claims are shared by
// every consumer of the topic/channel, so two probes per fetch still sweep
// 1024 partitions in about 1.6s with 32 consumers and a 100ms long poll while
// bounding internal readiness RPCs to two per external fetch.
pub(super) const MAX_READY_PROBES: usize = 2;
const MAX_SCHEDULER_STATES: usize = 4096;

#[derive(Hash, PartialEq, Eq)]
struct FetchKey {
    topic: String,
    channel: String,
}

#[derive(Default)]
pub(super) struct FetchScheduler {
    states: Mutex<HashMap<FetchKey, Arc<FetchState>>>,
}

#[derive(Default)]
struct FetchStateInner {
    cursor: usize,
    claimed: HashSet<GlobalGroupId>,
    ready: VecDeque<GlobalGroupId>,
    ready_set: HashSet<GlobalGroupId>,
}

pub(super) struct FetchState {
    inner: Mutex<FetchStateInner>,
    available: tokio::sync::Notify,
}

pub(super) struct PartitionClaim {
    state: Arc<FetchState>,
    pub(super) partition: Arc<PartitionDescriptor>,
}

impl FetchScheduler {
    pub(super) fn state(&self, topic: &str, channel: &str) -> Arc<FetchState> {
        let key = FetchKey {
            topic: topic.to_owned(),
            channel: channel.to_owned(),
        };
        let mut states = self.states.lock();
        if let Some(state) = states.get(&key) {
            return Arc::clone(state);
        }
        if states.len() >= MAX_SCHEDULER_STATES {
            if let Some(stale) = states.keys().next().map(|key| FetchKey {
                topic: key.topic.clone(),
                channel: key.channel.clone(),
            }) {
                states.remove(&stale);
            }
        }
        let state = Arc::new(FetchState {
            inner: Mutex::new(FetchStateInner::default()),
            available: tokio::sync::Notify::new(),
        });
        states.insert(key, Arc::clone(&state));
        state
    }
}

impl FetchState {
    pub(super) fn claim(self: &Arc<Self>, route: &TopicRoute) -> Vec<PartitionClaim> {
        let active = route.active_partitions();
        if active.is_empty() {
            return Vec::new();
        }
        let mut inner = self.inner.lock();
        let mut selected = Vec::with_capacity(MAX_READY_PROBES);

        let ready_count = inner.ready.len();
        for _ in 0..ready_count {
            let group_id = inner.ready.pop_front().expect("ready length was captured");
            let Some(partition) = route.partition_by_group(group_id) else {
                inner.ready_set.remove(&group_id);
                continue;
            };
            if inner.claimed.insert(group_id) {
                inner.ready_set.remove(&group_id);
                selected.push(PartitionClaim {
                    state: Arc::clone(self),
                    partition,
                });
                if selected.len() == MAX_READY_PROBES {
                    return selected;
                }
            } else {
                inner.ready.push_back(group_id);
            }
        }

        let start = inner.cursor % active.len();
        inner.cursor = inner.cursor.wrapping_add(MAX_READY_PROBES);
        for offset in 0..active.len() {
            if selected.len() == MAX_READY_PROBES {
                break;
            }
            let partition = Arc::clone(&active[(start + offset) % active.len()]);
            if inner.claimed.insert(partition.global_id()) {
                selected.push(PartitionClaim {
                    state: Arc::clone(self),
                    partition,
                });
            }
        }
        selected
    }

    pub(super) fn mark_ready(&self, group_id: GlobalGroupId) {
        let mut inner = self.inner.lock();
        if inner.ready_set.insert(group_id) {
            inner.ready.push_back(group_id);
        }
        drop(inner);
        self.available.notify_waiters();
    }

    pub(super) async fn wait_for_claim(&self, wait: Duration) {
        let _ = tokio::time::timeout(wait, self.available.notified()).await;
    }
}

impl Drop for PartitionClaim {
    fn drop(&mut self) {
        self.state
            .inner
            .lock()
            .claimed
            .remove(&self.partition.global_id());
        self.state.available.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataCatalog, NodeDescriptor};
    use std::collections::{BTreeMap, HashSet};

    fn route(partitions: u16) -> Arc<TopicRoute> {
        let nodes: BTreeMap<_, _> = (1..=3)
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
        let catalog = MetadataCatalog::new(nodes, partitions, 3).unwrap();
        catalog
            .ensure_topic("events", Some(partitions), Some(3))
            .unwrap();
        catalog.topic_route("events").unwrap()
    }

    #[test]
    fn shared_claims_sweep_sixty_four_partitions_in_thirty_two_rounds() {
        let route = route(64);
        let state = Arc::new(FetchState {
            inner: Mutex::new(FetchStateInner::default()),
            available: tokio::sync::Notify::new(),
        });
        let mut visited = HashSet::new();
        for _ in 0..32 {
            let claims = state.claim(&route);
            assert_eq!(claims.len(), MAX_READY_PROBES);
            visited.extend(claims.iter().map(|claim| claim.partition.global_id()));
            drop(claims);
        }
        assert_eq!(visited.len(), 64);
    }

    #[test]
    fn ready_partition_preempts_the_round_robin_cursor() {
        let route = route(8);
        let state = Arc::new(FetchState {
            inner: Mutex::new(FetchStateInner::default()),
            available: tokio::sync::Notify::new(),
        });
        let ready = route.active_partitions()[7].global_id();
        state.mark_ready(ready);
        let claims = state.claim(&route);
        assert!(claims
            .iter()
            .any(|claim| claim.partition.global_id() == ready));
    }
}
