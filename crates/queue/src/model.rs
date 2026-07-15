use rustqueue_storage::PayloadRef;
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct StoredMessage {
    pub id: u64,
    pub timestamp_ns: i64,
    pub available_at_ms: i64,
    pub log_index: u64,
    pub batch_ordinal: u32,
    pub payload: PayloadRef,
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: u64,
    pub timestamp_ns: i64,
    pub attempts: u16,
    pub body: Arc<[u8]>,
}

#[derive(Clone, Debug)]
pub(crate) struct InFlight {
    pub deadline_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadingDelivery {
    pub token: u64,
    pub deadline_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ReservedDelivery {
    pub message_id: u64,
    pub timestamp_ns: i64,
    pub payload: PayloadRef,
    pub token: u64,
    pub timeout_ms: i64,
}

#[derive(Clone, Debug, Default)]
#[allow(clippy::box_collection)] // Keeps the cold ChannelState inline footprint bounded.
pub(crate) struct LazySet(Option<Box<BTreeSet<u64>>>);

impl LazySet {
    pub fn contains(&self, value: &u64) -> bool {
        self.0.as_ref().is_some_and(|set| set.contains(value))
    }

    pub fn insert(&mut self, value: u64) -> bool {
        self.0.get_or_insert_with(Default::default).insert(value)
    }

    pub fn remove(&mut self, value: &u64) -> bool {
        let removed = self.0.as_mut().is_some_and(|set| set.remove(value));
        if self.0.as_ref().is_some_and(|set| set.is_empty()) {
            self.0 = None;
        }
        removed
    }

    pub fn clear(&mut self) {
        self.0 = None;
    }

    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |set| set.len())
    }

    pub fn iter(&self) -> impl Iterator<Item = &u64> {
        self.0.iter().flat_map(|set| set.iter())
    }

    pub fn retain(&mut self, keep: impl FnMut(&u64) -> bool) {
        if let Some(set) = self.0.as_mut() {
            set.retain(keep);
            if set.is_empty() {
                self.0 = None;
            }
        }
    }
}

impl From<BTreeSet<u64>> for LazySet {
    fn from(values: BTreeSet<u64>) -> Self {
        Self((!values.is_empty()).then(|| Box::new(values)))
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::box_collection)] // Sparse state pays the extra allocation only when populated.
pub(crate) struct LazyMap<V>(Option<Box<HashMap<u64, V>>>);

impl<V> Default for LazyMap<V> {
    fn default() -> Self {
        Self(None)
    }
}

impl<V> LazyMap<V> {
    pub fn get(&self, key: &u64) -> Option<&V> {
        self.0.as_ref()?.get(key)
    }

    pub fn get_mut(&mut self, key: &u64) -> Option<&mut V> {
        self.0.as_mut()?.get_mut(key)
    }

    pub fn contains_key(&self, key: &u64) -> bool {
        self.0.as_ref().is_some_and(|map| map.contains_key(key))
    }

    pub fn insert(&mut self, key: u64, value: V) -> Option<V> {
        self.0
            .get_or_insert_with(Default::default)
            .insert(key, value)
    }

    pub fn remove(&mut self, key: &u64) -> Option<V> {
        let removed = self.0.as_mut()?.remove(key);
        if self.0.as_ref().is_some_and(|map| map.is_empty()) {
            self.0 = None;
        }
        removed
    }

    pub fn clear(&mut self) {
        self.0 = None;
    }

    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |map| map.len())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &V)> {
        self.0.iter().flat_map(|map| map.iter())
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&u64, &mut V) -> bool) {
        if let Some(map) = self.0.as_mut() {
            map.retain(|key, value| keep(key, value));
            if map.is_empty() {
                self.0 = None;
            }
        }
    }

    pub fn entry(&mut self, key: u64) -> hash_map::Entry<'_, u64, V> {
        self.0.get_or_insert_with(Default::default).entry(key)
    }
}

impl<V> From<HashMap<u64, V>> for LazyMap<V> {
    fn from(values: HashMap<u64, V>) -> Self {
        Self((!values.is_empty()).then(|| Box::new(values)))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChannelState {
    pub barrier: usize,
    pub cursor: usize,
    pub ack_floor: usize,
    pub acknowledged: LazySet,
    pub in_flight: LazyMap<InFlight>,
    pub loading: LazyMap<LoadingDelivery>,
    pub requeued_until: LazyMap<i64>,
    pub attempts: LazyMap<u16>,
    pub paused: bool,
    pub ephemeral: bool,
    pub delivery_blocked_until_ms: i64,
    pub retention_cursor: usize,
    max_ack_gap: usize,
}

impl ChannelState {
    pub fn new(barrier: usize, ephemeral: bool, max_ack_gap: usize) -> Self {
        Self {
            barrier,
            cursor: barrier,
            ack_floor: barrier,
            acknowledged: LazySet::default(),
            in_flight: LazyMap::default(),
            loading: LazyMap::default(),
            requeued_until: LazyMap::default(),
            attempts: LazyMap::default(),
            paused: false,
            ephemeral,
            delivery_blocked_until_ms: 0,
            retention_cursor: barrier,
            max_ack_gap,
        }
    }

    pub fn is_acknowledged(&self, position: usize, message_id: u64) -> bool {
        position < self.ack_floor || self.acknowledged.contains(&message_id)
    }

    pub fn can_deliver(&self, position: usize) -> bool {
        position < self.ack_floor.saturating_add(self.max_ack_gap)
    }

    pub fn acknowledge(&mut self, position: usize, messages: &[StoredMessage]) {
        if position < self.ack_floor {
            return;
        }
        self.acknowledged.insert(messages[position].id);
        while self.ack_floor < messages.len()
            && self.acknowledged.remove(&messages[self.ack_floor].id)
        {
            self.ack_floor += 1;
        }
        if self.retention_cursor < self.ack_floor {
            self.retention_cursor = self.ack_floor;
        }
        self.delivery_blocked_until_ms = 0;
    }

    pub fn empty_through(&mut self, position: usize) {
        self.ack_floor = position;
        self.acknowledged.clear();
        self.cursor = position;
        self.retention_cursor = position;
        self.delivery_blocked_until_ms = 0;
    }

    pub fn ack_gap(&self) -> usize {
        self.acknowledged.len()
    }

    pub fn depth(&self, message_count: usize) -> usize {
        message_count
            .saturating_sub(self.ack_floor.max(self.barrier))
            .saturating_sub(self.acknowledged.len())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BrokerStats {
    pub topics: Vec<TopicStats>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TopicStats {
    pub name: String,
    pub paused: bool,
    pub message_count: u64,
    pub partitions: Vec<PartitionStats>,
    pub channels: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartitionStats {
    pub partition: u16,
    pub slot: u16,
    pub message_count: u64,
    pub log_records: u64,
    pub channels: Vec<ChannelStats>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChannelStats {
    pub name: String,
    pub depth: u64,
    pub in_flight_count: u64,
    pub deferred_count: u64,
    pub paused: bool,
    pub ephemeral: bool,
    pub ack_cursor: u64,
    pub ack_gap: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(count: usize) -> Vec<StoredMessage> {
        (0..count)
            .map(|index| StoredMessage {
                id: index as u64 + 1,
                timestamp_ns: 0,
                available_at_ms: 0,
                log_index: index as u64,
                batch_ordinal: 0,
                payload: PayloadRef {
                    path: std::sync::Arc::new(std::path::PathBuf::new()),
                    offset: 0,
                    len: 0,
                    crc32c: 0,
                },
            })
            .collect()
    }

    #[test]
    fn sparse_ack_window_is_bounded_and_collapses_contiguously() {
        let messages = messages(10);
        let mut channel = ChannelState::new(0, false, 3);
        assert!(channel.can_deliver(2));
        assert!(!channel.can_deliver(3));

        channel.acknowledge(2, &messages);
        channel.acknowledge(1, &messages);
        assert_eq!(channel.ack_floor, 0);
        assert_eq!(channel.ack_gap(), 2);

        channel.acknowledge(0, &messages);
        assert_eq!(channel.ack_floor, 3);
        assert_eq!(channel.ack_gap(), 0);
        assert!(channel.can_deliver(5));
        assert!(!channel.can_deliver(6));
    }

    #[test]
    fn disk_first_message_metadata_stays_fixed_and_small() {
        assert!(std::mem::size_of::<StoredMessage>() <= 64);
        assert!(std::mem::size_of::<PayloadRef>() <= 32);
    }

    #[test]
    fn cold_channel_state_keeps_sparse_collections_out_of_line() {
        assert!(std::mem::size_of::<ChannelState>() <= 104);
        let channel = ChannelState::new(0, false, 65_536);
        assert!(channel.acknowledged.0.is_none());
        assert!(channel.in_flight.0.is_none());
        assert!(channel.loading.0.is_none());
        assert!(channel.requeued_until.0.is_none());
        assert!(channel.attempts.0.is_none());
    }

    #[test]
    fn channel_depth_uses_cursors_instead_of_scanning_messages() {
        let messages = messages(8);
        let mut channel = ChannelState::new(2, false, 65_536);
        channel.acknowledge(4, &messages);
        assert_eq!(5, channel.depth(messages.len()));
        channel.acknowledge(2, &messages);
        assert_eq!(4, channel.depth(messages.len()));
    }
}
