use parking_lot::RwLock;
use rand::seq::SliceRandom;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Backend {
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub node_id: u64,
}

impl Backend {
    pub fn tcp_address(&self) -> String {
        format!("{}:{}", self.broadcast_address, self.tcp_port)
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
    updated_at: Option<Instant>,
}

impl BackendPool {
    pub fn replace(&self, backends: Vec<Backend>) {
        let mut unique = BTreeMap::new();
        for backend in backends {
            unique.insert(backend.node_id, backend);
        }
        let mut state = self.inner.write();
        state.backends = unique.into_values().collect();
        state.updated_at = Some(Instant::now());
    }

    pub fn clear_if_stale(&self, maximum_age: Duration) {
        let mut state = self.inner.write();
        if state
            .updated_at
            .is_none_or(|updated| updated.elapsed() > maximum_age)
        {
            state.backends.clear();
        }
    }

    pub fn shuffled(&self, limit: usize) -> Vec<Backend> {
        let mut backends = self.inner.read().backends.clone();
        backends.shuffle(&mut rand::thread_rng());
        backends.truncate(limit.min(backends.len()));
        backends
    }

    pub fn len(&self) -> usize {
        self.inner.read().backends.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
