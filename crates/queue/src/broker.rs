use crate::batch;
use crate::catalog::{Catalog, CatalogStore, PartitionDefinition, TopicDefinition};
use crate::dedup::{DedupCache, DedupKey};
use crate::model::{
    BrokerStats, ChannelState, ChannelStats, Delivery, InFlight, LoadingDelivery, PartitionStats,
    ReservedDelivery, StoredMessage, TopicStats,
};
use crate::payload_reader::PayloadReader;
use crate::projection::{PartitionProjection, ProjectedChannel, ProjectedMessage};
use parking_lot::{Mutex, RwLock};
use rustqueue_protocol::validate_name;
use rustqueue_storage::{
    ensure_data_format, Record, RecordKind, SegmentLog, StorageError, MAX_RECORD_BYTES,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const MAX_PARTITION_SLOT: u32 = u16::MAX as u32;

#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub data_path: PathBuf,
    pub default_partitions: u16,
    pub max_segment_bytes: u64,
    pub max_message_bytes: usize,
    pub message_timeout: Duration,
    pub max_ack_gap: usize,
    pub max_backlog_messages_per_partition: usize,
    pub projection_only: bool,
    pub entry_cache_bytes: usize,
    pub payload_read_workers: usize,
    pub payload_read_queue: usize,
    pub dedup_max_entries: usize,
    pub dedup_ttl: Duration,
    pub cell_id: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("data"),
            default_partitions: 1,
            max_segment_bytes: 100 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            message_timeout: Duration::from_secs(60),
            max_ack_gap: 65_536,
            max_backlog_messages_per_partition: 10_000_000,
            projection_only: false,
            entry_cache_bytes: 64 * 1024 * 1024,
            payload_read_workers: 0,
            payload_read_queue: 4096,
            dedup_max_entries: 1_000_000,
            dedup_ttl: Duration::from_secs(600),
            cell_id: 1,
        }
    }
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("topic not found")]
    TopicNotFound,
    #[error("channel not found")]
    ChannelNotFound,
    #[error("partition not found")]
    PartitionNotFound,
    #[error("message not in flight")]
    MessageNotInFlight,
    #[error("message exceeds configured maximum")]
    MessageTooLarge,
    #[error("batch exceeds storage record maximum")]
    BatchTooLarge,
    #[error("partition backlog quota exceeded")]
    BacklogLimit,
    #[error("partition slot space exhausted")]
    SlotExhausted,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid durable record: {0}")]
    InvalidRecord(String),
    #[error("invalid topic name")]
    InvalidTopic,
    #[error("invalid channel name")]
    InvalidChannel,
}

pub struct Broker {
    config: BrokerConfig,
    catalog_store: CatalogStore,
    catalog: Mutex<Catalog>,
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    payload_reader: Arc<PayloadReader>,
    dedup: Mutex<DedupCache>,
}

struct Topic {
    name: String,
    partitions: RwLock<PartitionRoutes>,
    key_routing_slots: Vec<u16>,
    next_partition: AtomicUsize,
    paused: AtomicBool,
}

struct PartitionRoutes {
    ordered: Vec<Arc<Mutex<Partition>>>,
    by_number: HashMap<u16, Arc<Mutex<Partition>>>,
    by_slot: HashMap<u16, Arc<Mutex<Partition>>>,
}

struct Partition {
    number: u16,
    slot: u16,
    group_id: u64,
    cell_id: u64,
    wire_incarnation: u32,
    base_sequence: u64,
    next_sequence: u64,
    log: SegmentLog,
    messages: Vec<StoredMessage>,
    channels: HashMap<String, ChannelState>,
    durable_appends: bool,
    dirty: bool,
    max_ack_gap: usize,
    max_backlog_messages: usize,
    persist_wal: bool,
    projection_index: u64,
    next_delivery_token: u64,
    delivery_wake: tokio::sync::watch::Sender<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChannelCommand {
    channel: String,
    #[serde(default)]
    message_id: u64,
    #[serde(default)]
    available_at_ms: i64,
    #[serde(default)]
    paused: bool,
}

mod api;
mod batch_delivery;
mod channel_delivery;
mod partition_apply;
mod partition_channel;
mod partition_snapshot;
mod partition_storage;
mod projection;
mod protective_eviction;
mod topic;

pub use protective_eviction::ProtectiveEvictionCandidate;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLayout {
    pub number: u16,
    pub slot: u16,
    pub cell_id: u64,
    pub group_id: u64,
    pub wire_incarnation: u32,
}

fn topic_path(data_path: &Path, name: &str) -> PathBuf {
    data_path.join("topics").join(name)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

fn duration_ms(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_broker(path: &Path) -> Arc<Broker> {
        Broker::open(BrokerConfig {
            data_path: path.to_path_buf(),
            default_partitions: 2,
            max_segment_bytes: 1024,
            max_message_bytes: 1024,
            message_timeout: Duration::from_millis(20),
            max_ack_gap: 65_536,
            max_backlog_messages_per_partition: 10_000_000,
            projection_only: false,
            entry_cache_bytes: 1024 * 1024,
            payload_read_workers: 1,
            payload_read_queue: 16,
            dedup_max_entries: 1024,
            dedup_ttl: Duration::from_secs(60),
            cell_id: 7,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn internal_message_identity_survives_partial_batch_gc_and_restart() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker
            .ensure_topic_layout_v4(
                "events",
                &[PartitionLayout {
                    number: 0,
                    slot: 19,
                    cell_id: 7,
                    group_id: 42,
                    wire_incarnation: 3,
                }],
                &[19],
            )
            .unwrap();
        broker.create_channel("events", "workers").unwrap();
        let wire_ids = broker
            .publish(
                "events",
                vec![b"first".to_vec(), b"second".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        let first = broker.internal_message_id("events", wire_ids[0]).unwrap();
        let second = broker.internal_message_id("events", wire_ids[1]).unwrap();
        assert_eq!(first.group.cell, rustqueue_protocol::CellId(7));
        assert_eq!(first.group.local, 42);
        assert_eq!(first.log_index, second.log_index);
        assert_eq!((first.ordinal, second.ordinal), (0, 1));
        assert_eq!(first.incarnation, 3);
        let mut cursor = 0;
        let delivery = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.id, wire_ids[0]);
        broker.finish("events", "workers", delivery.id).unwrap();
        assert_eq!(broker.compact_partition_projection("events", 0).unwrap(), 1);
        assert_eq!(
            broker.internal_message_id("events", wire_ids[1]).unwrap(),
            second
        );
        drop(broker);

        let broker = open_broker(directory.path());
        assert_eq!(
            broker.internal_message_id("events", wire_ids[1]).unwrap(),
            second
        );
    }

    #[tokio::test]
    async fn fanout_and_restart_preserve_acknowledgements() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "one").unwrap();
        broker.create_channel("events", "two").unwrap();
        let ids = broker
            .publish(
                "events",
                vec![b"hello".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        let mut cursor = 0;
        let first = broker
            .next_message("events", "one", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, ids[0]);
        broker.finish("events", "one", first.id).unwrap();
        drop(broker);

        let broker = open_broker(directory.path());
        let mut cursor = 0;
        assert!(broker
            .next_message("events", "one", &mut cursor, None)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            broker
                .next_message("events", "two", &mut cursor, None)
                .await
                .unwrap()
                .unwrap()
                .body
                .as_ref(),
            b"hello"
        );
    }

    #[test]
    fn concurrent_first_publish_creates_one_runtime_topic() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let mut workers = Vec::new();
        for worker in 0..16 {
            let broker = Arc::clone(&broker);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                for message in 0..100 {
                    broker
                        .publish(
                            "first-publish-race",
                            vec![format!("{worker}-{message}").into_bytes()],
                            Duration::ZERO,
                            None,
                            None,
                        )
                        .unwrap();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let count: u64 = broker
            .stats()
            .topics
            .iter()
            .find(|topic| topic.name == "first-publish-race")
            .unwrap()
            .partitions
            .iter()
            .map(|partition| partition.message_count)
            .sum();
        assert_eq!(count, 1_600);
    }

    #[tokio::test]
    async fn timeout_redelivers_at_least_once() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("jobs", "workers").unwrap();
        broker
            .publish("jobs", vec![b"job".to_vec()], Duration::ZERO, Some(0), None)
            .unwrap();
        let mut cursor = 0;
        let first = broker
            .next_message(
                "jobs",
                "workers",
                &mut cursor,
                Some(Duration::from_millis(1)),
            )
            .await
            .unwrap()
            .unwrap();
        std::thread::sleep(Duration::from_millis(3));
        let second = broker
            .next_message("jobs", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(second.attempts, 2);
    }

    #[tokio::test]
    async fn channel_starts_after_creation_barrier() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker
            .publish(
                "events",
                vec![b"old".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        broker.create_channel("events", "new").unwrap();
        broker
            .publish(
                "events",
                vec![b"new".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        let mut cursor = 0;
        let delivery = broker
            .next_message("events", "new", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.body.as_ref(), b"new");
    }

    #[tokio::test]
    async fn payload_reservation_does_not_hold_the_partition_lock() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let ids = broker
            .publish(
                "events",
                vec![b"one".to_vec(), b"two".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        let mut cursor = 0;
        let first = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, ids[0]);

        let partition = broker.partition("events", 0).unwrap();
        let reservation = partition
            .lock()
            .reserve_next_message("workers", Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(reservation.message_id, ids[1]);
        assert!(partition.try_lock().is_some());
        broker.finish("events", "workers", first.id).unwrap();
        partition.lock().cancel_delivery("workers", &reservation);
    }

    #[tokio::test]
    async fn replicated_publish_is_runtime_idempotent_but_cache_loss_can_duplicate() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let timestamp = 123_456_789;
        let ids = broker
            .publish_replicated(
                77,
                "events",
                vec![b"once".to_vec()],
                timestamp,
                0,
                Some(0),
                None,
            )
            .unwrap();
        let repeated = broker
            .publish_replicated(
                77,
                "events",
                vec![b"once".to_vec()],
                timestamp,
                0,
                Some(0),
                None,
            )
            .unwrap();
        assert_eq!(ids, repeated);
        drop(broker);

        let broker = open_broker(directory.path());
        let recovered = broker
            .publish_replicated(
                77,
                "events",
                vec![b"once".to_vec()],
                timestamp,
                0,
                Some(0),
                None,
            )
            .unwrap();
        assert_ne!(ids, recovered);
        let mut cursor = 0;
        let delivery = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.timestamp_ns, timestamp);
        assert_eq!(broker.stats().topics[0].message_count, 2);
    }

    #[tokio::test]
    async fn topic_pause_survives_restart_and_blocks_delivery() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        broker
            .publish(
                "events",
                vec![b"queued".to_vec()],
                Duration::ZERO,
                None,
                None,
            )
            .unwrap();
        broker.set_topic_paused("events", true).unwrap();
        drop(broker);

        let broker = open_broker(directory.path());
        let mut cursor = 0;
        assert!(broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .is_none());
        broker.set_topic_paused("events", false).unwrap();
        assert_eq!(
            broker
                .next_message("events", "workers", &mut cursor, None)
                .await
                .unwrap()
                .unwrap()
                .body
                .as_ref(),
            b"queued"
        );
    }

    #[tokio::test]
    async fn snapshot_compaction_keeps_sequence_mapping_after_prefix_gc() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let ids = broker
            .publish(
                "events",
                vec![b"one".to_vec(), b"two".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        for id in &ids {
            let mut cursor = 0;
            broker
                .next_message("events", "workers", &mut cursor, None)
                .await
                .unwrap()
                .unwrap();
            broker.finish("events", "workers", *id).unwrap();
        }
        assert_eq!(broker.compact_partition_projection("events", 0).unwrap(), 2);
        let next = broker
            .publish(
                "events",
                vec![b"three".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        assert_eq!(next[0] & ((1u64 << 48) - 1), 3);
        let mut cursor = 0;
        assert_eq!(
            broker
                .next_message("events", "workers", &mut cursor, None)
                .await
                .unwrap()
                .unwrap()
                .id,
            next[0]
        );
    }

    #[test]
    fn rejects_names_that_can_escape_storage_paths() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        assert!(matches!(
            broker.publish("../escape", vec![b"x".to_vec()], Duration::ZERO, None, None),
            Err(BrokerError::InvalidTopic)
        ));
        assert!(matches!(
            broker.create_channel("safe", "bad/channel"),
            Err(BrokerError::InvalidChannel)
        ));
    }

    #[test]
    fn partition_backlog_quota_rejects_before_allocating_more_ids() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: directory.path().to_path_buf(),
            max_backlog_messages_per_partition: 1,
            max_segment_bytes: 1024 * 1024,
            ..BrokerConfig::default()
        })
        .unwrap();
        let first = broker
            .publish(
                "events",
                vec![b"one".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();
        assert_eq!(1, first.len());
        assert!(matches!(
            broker.publish(
                "events",
                vec![b"two".to_vec()],
                Duration::ZERO,
                Some(0),
                None
            ),
            Err(BrokerError::BacklogLimit)
        ));
    }
}
