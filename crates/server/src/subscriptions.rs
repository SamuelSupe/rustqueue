use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClientIdentity {
    pub client_id: String,
    pub hostname: String,
    pub remote_address: String,
    pub user_agent: String,
    pub sample_rate: u8,
    pub tls: bool,
    pub deflate: bool,
    pub snappy: bool,
    pub authed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ClientSnapshot {
    pub client_id: String,
    pub hostname: String,
    pub remote_address: String,
    pub user_agent: String,
    pub state: i32,
    pub ready_count: u64,
    pub in_flight_count: u64,
    pub message_count: u64,
    pub finish_count: u64,
    pub requeue_count: u64,
    pub connect_ts: i64,
    pub sample_rate: u8,
    pub tls: bool,
    pub deflate: bool,
    pub snappy: bool,
    pub authed: bool,
}

#[derive(Clone, Default)]
pub(crate) struct SubscriptionRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

#[derive(Default)]
struct RegistryState {
    next_id: u64,
    channels: BTreeMap<ChannelKey, BTreeMap<u64, Arc<ClientRuntime>>>,
    deleting: BTreeSet<ChannelKey>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChannelKey {
    topic: String,
    channel: String,
}

struct ClientRuntime {
    identity: ClientIdentity,
    connected_at: i64,
    ready_count: AtomicU64,
    in_flight_count: AtomicU64,
    message_count: AtomicU64,
    finish_count: AtomicU64,
    requeue_count: AtomicU64,
}

pub(crate) struct SubscriptionLease {
    registry: SubscriptionRegistry,
    key: ChannelKey,
    id: u64,
    client: Arc<ClientRuntime>,
}

pub(crate) struct DeletePermit {
    registry: SubscriptionRegistry,
    key: ChannelKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteBlocked {
    ActiveClients(usize),
    InProgress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegisterBlocked {
    DeleteInProgress,
}

impl SubscriptionRegistry {
    pub fn register(
        &self,
        topic: &str,
        channel: &str,
        identity: ClientIdentity,
    ) -> Result<SubscriptionLease, RegisterBlocked> {
        let key = ChannelKey {
            topic: topic.into(),
            channel: channel.into(),
        };
        let mut state = self.inner.lock();
        if state.deleting.contains(&key) {
            return Err(RegisterBlocked::DeleteInProgress);
        }
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        let client = Arc::new(ClientRuntime {
            identity,
            connected_at: unix_seconds(),
            ready_count: AtomicU64::new(0),
            in_flight_count: AtomicU64::new(0),
            message_count: AtomicU64::new(0),
            finish_count: AtomicU64::new(0),
            requeue_count: AtomicU64::new(0),
        });
        state
            .channels
            .entry(key.clone())
            .or_default()
            .insert(id, Arc::clone(&client));
        Ok(SubscriptionLease {
            registry: self.clone(),
            key,
            id,
            client,
        })
    }

    pub fn clients(&self, topic: &str, channel: &str) -> Vec<ClientSnapshot> {
        let key = ChannelKey {
            topic: topic.into(),
            channel: channel.into(),
        };
        self.inner
            .lock()
            .channels
            .get(&key)
            .into_iter()
            .flat_map(BTreeMap::values)
            .map(|client| client.snapshot())
            .collect()
    }

    pub fn client_count(&self, topic: &str, channel: &str) -> usize {
        let key = ChannelKey {
            topic: topic.into(),
            channel: channel.into(),
        };
        self.inner
            .lock()
            .channels
            .get(&key)
            .map_or(0, BTreeMap::len)
    }

    pub fn begin_delete(&self, topic: &str, channel: &str) -> Result<DeletePermit, DeleteBlocked> {
        let key = ChannelKey {
            topic: topic.into(),
            channel: channel.into(),
        };
        let mut state = self.inner.lock();
        if state.deleting.contains(&key) {
            return Err(DeleteBlocked::InProgress);
        }
        let clients = state.channels.get(&key).map_or(0, BTreeMap::len);
        if clients > 0 {
            return Err(DeleteBlocked::ActiveClients(clients));
        }
        state.deleting.insert(key.clone());
        Ok(DeletePermit {
            registry: self.clone(),
            key,
        })
    }
}

impl SubscriptionLease {
    pub fn update_flow(&self, ready: u64, in_flight: usize) {
        self.client.ready_count.store(ready, Ordering::Relaxed);
        self.client
            .in_flight_count
            .store(in_flight as u64, Ordering::Relaxed);
    }

    pub fn observe_delivery(&self) {
        self.client.message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_finish(&self) {
        self.client.finish_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_requeue(&self) {
        self.client.requeue_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        let mut state = self.registry.inner.lock();
        let remove_channel = state.channels.get_mut(&self.key).is_some_and(|clients| {
            clients.remove(&self.id);
            clients.is_empty()
        });
        if remove_channel {
            state.channels.remove(&self.key);
        }
    }
}

impl Drop for DeletePermit {
    fn drop(&mut self) {
        self.registry.inner.lock().deleting.remove(&self.key);
    }
}

impl ClientRuntime {
    fn snapshot(&self) -> ClientSnapshot {
        ClientSnapshot {
            client_id: self.identity.client_id.clone(),
            hostname: self.identity.hostname.clone(),
            remote_address: self.identity.remote_address.clone(),
            user_agent: self.identity.user_agent.clone(),
            state: 3,
            ready_count: self.ready_count.load(Ordering::Relaxed),
            in_flight_count: self.in_flight_count.load(Ordering::Relaxed),
            message_count: self.message_count.load(Ordering::Relaxed),
            finish_count: self.finish_count.load(Ordering::Relaxed),
            requeue_count: self.requeue_count.load(Ordering::Relaxed),
            connect_ts: self.connected_at,
            sample_rate: self.identity.sample_rate,
            tls: self.identity.tls,
            deflate: self.identity.deflate,
            snappy: self.identity.snappy,
            authed: self.identity.authed,
        }
    }
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_barrier_blocks_new_subscriptions_and_counts_live_clients() {
        let registry = SubscriptionRegistry::default();
        let lease = registry
            .register("events", "workers", ClientIdentity::default())
            .unwrap();
        assert_eq!(registry.clients("events", "workers").len(), 1);
        assert!(matches!(
            registry.begin_delete("events", "workers"),
            Err(DeleteBlocked::ActiveClients(1))
        ));
        drop(lease);

        let permit = registry.begin_delete("events", "workers").unwrap();
        assert!(matches!(
            registry.register("events", "workers", ClientIdentity::default()),
            Err(RegisterBlocked::DeleteInProgress)
        ));
        drop(permit);
        assert!(registry
            .register("events", "workers", ClientIdentity::default())
            .is_ok());
    }
}
