use parking_lot::RwLock;
use rand::seq::SliceRandom;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Backend {
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub node_id: u64,
}

impl Backend {
    pub fn tcp_address(&self) -> String {
        if self.broadcast_address.contains(':') && !self.broadcast_address.starts_with('[') {
            format!("[{}]:{}", self.broadcast_address, self.tcp_port)
        } else {
            format!("{}:{}", self.broadcast_address, self.tcp_port)
        }
    }
    pub fn http_origin(&self) -> String {
        if self.broadcast_address.contains(':') && !self.broadcast_address.starts_with('[') {
            format!("http://[{}]:{}", self.broadcast_address, self.http_port)
        } else {
            format!("http://{}:{}", self.broadcast_address, self.http_port)
        }
    }
}

#[derive(Clone, Default)]
pub struct BackendPool {
    inner: Arc<RwLock<PoolState>>,
}

#[derive(Default)]
struct PoolState {
    backends: Vec<Backend>,
    active_connections: BTreeMap<u64, usize>,
    invalidation: BTreeMap<u64, watch::Sender<u64>>,
    next_tie: usize,
}

pub struct BackendLease {
    backend: Backend,
    pool: BackendPool,
    invalidation: watch::Receiver<u64>,
}

impl Deref for BackendLease {
    type Target = Backend;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl Drop for BackendLease {
    fn drop(&mut self) {
        let mut state = self.pool.inner.write();
        if let Some(active) = state.active_connections.get_mut(&self.backend.node_id) {
            *active = active.saturating_sub(1);
            if *active == 0
                && !state
                    .backends
                    .iter()
                    .any(|backend| backend.node_id == self.backend.node_id)
            {
                state.active_connections.remove(&self.backend.node_id);
            }
        }
    }
}

impl BackendPool {
    pub fn replace(&self, backends: Vec<Backend>) {
        let mut unique = BTreeMap::new();
        for backend in backends {
            unique.insert(backend.node_id, backend);
        }
        let mut state = self.inner.write();
        let next: BTreeMap<_, _> = unique.iter().map(|(id, backend)| (*id, backend)).collect();
        let changed: Vec<_> = state
            .backends
            .iter()
            .filter(|backend| next.get(&backend.node_id).copied() != Some(*backend))
            .map(|backend| backend.node_id)
            .collect();
        for node_id in changed {
            if let Some(sender) = state.invalidation.get(&node_id) {
                sender.send_modify(|generation| *generation = generation.wrapping_add(1));
            }
        }
        state.backends = unique.into_values().collect();
        let node_ids: std::collections::BTreeSet<_> = state
            .backends
            .iter()
            .map(|backend| backend.node_id)
            .collect();
        state
            .active_connections
            .retain(|node_id, active| *active > 0 || node_ids.contains(node_id));
        state
            .invalidation
            .retain(|node_id, _| node_ids.contains(node_id));
        for node_id in node_ids {
            state
                .invalidation
                .entry(node_id)
                .or_insert_with(|| watch::channel(0).0);
        }
    }

    pub fn shuffled(&self, limit: usize) -> Vec<Backend> {
        let mut backends = self.inner.read().backends.clone();
        backends.shuffle(&mut rand::thread_rng());
        backends.truncate(limit.min(backends.len()));
        backends
    }

    pub fn lease(&self) -> Option<BackendLease> {
        let mut state = self.inner.write();
        let minimum = state
            .backends
            .iter()
            .map(|backend| {
                state
                    .active_connections
                    .get(&backend.node_id)
                    .copied()
                    .unwrap_or_default()
            })
            .min()?;
        let candidates: Vec<_> = state
            .backends
            .iter()
            .filter(|backend| {
                state
                    .active_connections
                    .get(&backend.node_id)
                    .copied()
                    .unwrap_or_default()
                    == minimum
            })
            .cloned()
            .collect();
        let backend = candidates[state.next_tie % candidates.len()].clone();
        state.next_tie = state.next_tie.wrapping_add(1);
        *state.active_connections.entry(backend.node_id).or_default() += 1;
        let invalidation = state
            .invalidation
            .entry(backend.node_id)
            .or_insert_with(|| watch::channel(0).0)
            .subscribe();
        Some(BackendLease {
            backend,
            pool: self.clone(),
            invalidation,
        })
    }

    pub fn all(&self) -> Vec<Backend> {
        self.inner.read().backends.clone()
    }

    pub fn len(&self) -> usize {
        self.inner.read().backends.len()
    }
}

impl BackendLease {
    pub fn invalidation(&self) -> watch::Receiver<u64> {
        self.invalidation.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_addresses_bracket_ipv6_literals() {
        let backend = Backend {
            broadcast_address: "2001:db8::1".into(),
            tcp_port: 4150,
            http_port: 4151,
            node_id: 1,
        };
        assert_eq!(backend.tcp_address(), "[2001:db8::1]:4150");
        assert_eq!(backend.http_origin(), "http://[2001:db8::1]:4151");
    }

    #[test]
    fn pool_deduplicates_five_hundred_discovered_brokers() {
        let pool = BackendPool::default();
        let mut backends = Vec::new();
        for node_id in 1..=500 {
            backends.push(Backend {
                broadcast_address: format!("broker-{node_id}"),
                tcp_port: 4150,
                http_port: 4151,
                node_id,
            });
        }
        backends.push(backends[0].clone());
        pool.replace(backends);
        assert_eq!(pool.len(), 500);
        assert_eq!(pool.shuffled(64).len(), 64);
    }

    #[test]
    fn tcp_leases_spread_before_reusing_a_broker() {
        let pool = BackendPool::default();
        pool.replace(
            (1..=8)
                .map(|node_id| Backend {
                    broadcast_address: format!("broker-{node_id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    node_id,
                })
                .collect(),
        );
        let leases: Vec<_> = (0..8).map(|_| pool.lease().unwrap()).collect();
        let nodes: std::collections::BTreeSet<_> =
            leases.iter().map(|lease| lease.node_id).collect();
        assert_eq!(nodes.len(), 8);
        assert_eq!(pool.lease().unwrap().node_id, 1);
    }

    #[test]
    fn sequential_reconnections_rotate_equal_backends() {
        let pool = BackendPool::default();
        pool.replace(
            (1..=3)
                .map(|node_id| Backend {
                    broadcast_address: format!("broker-{node_id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    node_id,
                })
                .collect(),
        );
        let first = pool.lease().unwrap().node_id;
        let second = pool.lease().unwrap().node_id;
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn removing_a_backend_invalidates_its_active_lease() {
        let pool = BackendPool::default();
        pool.replace(vec![Backend {
            broadcast_address: "broker-1".into(),
            tcp_port: 4150,
            http_port: 4151,
            node_id: 1,
        }]);
        let lease = pool.lease().unwrap();
        let mut invalidation = lease.invalidation();
        pool.replace(Vec::new());
        assert!(invalidation.changed().await.is_ok() || invalidation.has_changed().is_err());
    }
}
