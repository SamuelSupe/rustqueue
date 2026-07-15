use crate::latency::GroupLatencyMetrics;
use crate::{ClusterMetadata, MetadataCatalog, NodeId, QueueCommand, QueueResponse, TypeConfig};
use openraft::storage::{
    LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
    Snapshot,
};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex as BlockingMutex;
use rustqueue_queue::{Broker, BrokerError, PartitionProjection};
use rustqueue_storage::{
    GenerationStore, PayloadRef, Record, RecordKind, RecordLocation, SegmentLog,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

const APPLIED_CHECKPOINT_ENTRIES: u64 = 64;

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<BlockingMutex<LogStateData>>,
    directory: Arc<PathBuf>,
}

#[derive(Clone)]
struct LogEntryPointer {
    log_id: LogId<NodeId>,
    location: RecordLocation,
}

struct LogStateData {
    vote: Option<Vote<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    entries: BTreeMap<u64, LogEntryPointer>,
    segments: SegmentLog,
    pending_flush: Vec<LogFlushed<TypeConfig>>,
    flush_scheduled: bool,
    latency: Arc<GroupLatencyMetrics>,
}

pub struct StateMachineStore {
    broker: Arc<Broker>,
    metadata: Arc<MetadataCatalog>,
    directory: PathBuf,
    state: RwLock<StateMachineData>,
    operation_lock: tokio::sync::Mutex<()>,
    generations: GenerationStore,
    snapshot_index: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    checkpoint_pending: AtomicU64,
    role: StateMachineRole,
    payload_log: Option<LogStore>,
    latency: Arc<GroupLatencyMetrics>,
}

#[derive(Clone, Debug)]
pub enum StateMachineRole {
    All,
    Root,
    Catalog { shard: u64 },
    CellMetadata,
    Partition { topic: String, partition: u16 },
}

impl StateMachineRole {
    fn command_scope(&self) -> Option<u8> {
        match self {
            Self::All => None,
            Self::Root => Some(crate::COMMAND_SCOPE_ROOT),
            Self::Catalog { .. } => Some(crate::COMMAND_SCOPE_CATALOG),
            Self::CellMetadata => Some(crate::COMMAND_SCOPE_CELL_METADATA),
            Self::Partition { .. } => Some(crate::COMMAND_SCOPE_PARTITION),
        }
    }

    fn validate_envelope(&self, envelope: &crate::CommandEnvelope) -> io::Result<()> {
        envelope.validate().map_err(io::Error::other)?;
        self.command_scope()
            .is_none_or(|scope| envelope.command.is_scoped_to(scope))
            .then_some(())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Raft command scope does not match the state-machine role",
                )
            })
    }

    fn carries_cell_metadata(&self) -> bool {
        matches!(self, Self::All | Self::CellMetadata)
    }

    fn carries_root(&self) -> bool {
        matches!(self, Self::All | Self::Root)
    }

    fn carries_catalog(&self) -> bool {
        matches!(self, Self::All | Self::Catalog { .. })
    }
}

fn command_error(message: impl Into<String>) -> QueueResponse {
    QueueResponse {
        message_ids: Vec::new(),
        error: Some(message.into()),
        results: Vec::new(),
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    #[serde(default)]
    commands: Vec<QueueCommand>,
    #[serde(default)]
    metadata: Option<ClusterMetadata>,
    #[serde(default)]
    root: Option<crate::FederationRoot>,
    #[serde(default)]
    catalog: Option<crate::CatalogState>,
    #[serde(default)]
    projection: Option<PartitionProjection>,
}

impl StateMachineStore {
    fn capture_control_state(&self, state: &mut StateMachineData) {
        state.metadata = self
            .role
            .carries_cell_metadata()
            .then(|| self.metadata.snapshot());
        state.root = self
            .role
            .carries_root()
            .then(|| self.metadata.root_snapshot());
        state.catalog = self
            .role
            .carries_catalog()
            .then(|| self.metadata.catalog_snapshot());
    }
}

#[derive(Clone, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    directory: PathBuf,
}

mod applied_boundary;
mod batch;
pub(crate) mod blocking_io;
mod entry_codec;
mod log;
mod maintenance;
mod persistence;
mod raft;
mod recovery;
mod snapshot_files;
mod state;
mod state_roles;

use applied_boundary::{read_applied_state, write_applied_state};
use persistence::{
    read_binary_optional, read_json_optional, write_binary_atomic, write_json_atomic,
};
use recovery::{
    ensure_broker_topic, is_fatal_queue_error, rebuild_broker_projection,
    rebuild_broker_projection_refs, reconcile_broker_topics, recovery_tail, replay_metadata,
};

pub fn read_applied_boundary_index(path: impl AsRef<Path>) -> io::Result<Option<u64>> {
    read_applied_state(path.as_ref()).map(|boundary| boundary.map(|log_id| log_id.index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::{StoreBuilder, Suite};
    use rustqueue_queue::BrokerConfig;
    use tempfile::TempDir;

    struct Builder;

    impl StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>, TempDir> for Builder {
        async fn build(
            &self,
        ) -> Result<(TempDir, LogStore, Arc<StateMachineStore>), StorageError<NodeId>> {
            let directory = TempDir::new().unwrap();
            let broker = Broker::open(BrokerConfig {
                data_path: directory.path().join("queue"),
                ..BrokerConfig::default()
            })
            .unwrap();
            let log = LogStore::open(directory.path().join("log"), 1024 * 1024).unwrap();
            let state = StateMachineStore::open(directory.path().join("state"), broker).unwrap();
            Ok((directory, log, state))
        }
    }

    #[test]
    fn passes_openraft_storage_suite() {
        Suite::test_all(Builder).unwrap();
    }

    #[tokio::test]
    async fn unified_log_recovers_applied_boundary_and_snapshot_state() {
        let directory = TempDir::new().unwrap();
        let queue_path = directory.path().join("queue");
        let state_path = directory.path().join("state");
        let broker = Broker::open(BrokerConfig {
            data_path: queue_path.clone(),
            ..BrokerConfig::default()
        })
        .unwrap();
        let mut state = StateMachineStore::open(&state_path, broker).unwrap();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(2, 1), 0);
        let entry = Entry {
            log_id,
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::CreateTopic {
                topic: "unified".into(),
                partitions: Some(2),
                replication_factor: Some(1),
            })),
        };
        state.apply([entry.clone()]).await.unwrap();
        drop(state);

        let broker = Broker::open(BrokerConfig {
            data_path: queue_path,
            ..BrokerConfig::default()
        })
        .unwrap();
        let metadata = Arc::new(MetadataCatalog::standalone(1));
        let mut recovered = StateMachineStore::open_for_group_with_entries(
            &state_path,
            broker,
            Arc::clone(&metadata),
            StateMachineRole::All,
            vec![entry],
        )
        .unwrap();
        assert_eq!(recovered.applied_state().await.unwrap().0, Some(log_id));
        assert!(metadata.snapshot().topics.contains_key("unified"));
        recovered.build_snapshot().await.unwrap();
        let active = recovered.generations.active().unwrap().unwrap();
        let data: StateMachineData = read_binary_optional(&active.join("snapshot-state.bin"))
            .unwrap()
            .unwrap();
        assert!(data.commands.is_empty());
    }

    #[tokio::test]
    async fn partition_snapshot_round_trip_uses_streamed_projection() {
        let directory = TempDir::new().unwrap();
        let queue_path = directory.path().join("queue");
        let state_path = directory.path().join("state");
        let log_path = directory.path().join("log");
        let broker = Broker::open(BrokerConfig {
            data_path: queue_path.clone(),
            projection_only: true,
            ..BrokerConfig::default()
        })
        .unwrap();
        broker
            .ensure_topic_layout("events", &[(0, 7)], &[7])
            .unwrap();

        let log = LogStore::open(&log_path, 1024 * 1024).unwrap();
        let entry = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 0),
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::Publish {
                operation_id: 1,
                topic: "events".into(),
                bodies: vec![
                    bytes::Bytes::from_static(b"one"),
                    bytes::Bytes::from_static(b"two"),
                ],
                timestamp_ns: 10,
                available_at_ms: 0,
                partition: Some(0),
                routing_key: None,
            })),
        };
        append_test_entry(&log, &entry);
        let metadata = Arc::new(MetadataCatalog::standalone(1));
        let mut state = StateMachineStore::open_for_group_with_log(
            &state_path,
            Arc::clone(&broker),
            Arc::clone(&metadata),
            StateMachineRole::Partition {
                topic: "events".into(),
                partition: 0,
            },
            Vec::new(),
            log.clone(),
        )
        .unwrap();
        broker
            .create_channel_partition("events", "workers", 0)
            .unwrap();
        state.apply([entry]).await.unwrap();
        state.build_snapshot().await.unwrap();
        let active = state.generations.active().unwrap().unwrap();
        assert!(active.join("snapshot-state.bin").is_file());
        assert!(active.join("partition-projection.bin").is_file());
        let persisted: StateMachineData = read_binary_optional(&active.join("snapshot-state.bin"))
            .unwrap()
            .unwrap();
        assert!(persisted.projection.is_none());
        drop(state);
        drop(broker);

        let broker = Broker::open(BrokerConfig {
            data_path: queue_path,
            projection_only: true,
            ..BrokerConfig::default()
        })
        .unwrap();
        let log = LogStore::open(log_path, 1024 * 1024).unwrap();
        let recovered = log.recovered_entries_with_payloads().await.unwrap();
        let _state = StateMachineStore::open_for_group_with_log(
            state_path,
            Arc::clone(&broker),
            metadata,
            StateMachineRole::Partition {
                topic: "events".into(),
                partition: 0,
            },
            recovered,
            log,
        )
        .unwrap();
        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            2
        );
        let delivery = broker
            .next_message_partition("events", "workers", 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.body.as_ref(), b"one");
    }

    #[tokio::test]
    async fn protective_eviction_survives_snapshot_restart() {
        let directory = TempDir::new().unwrap();
        let queue_path = directory.path().join("queue");
        let state_path = directory.path().join("state");
        let log_path = directory.path().join("log");
        let broker = Broker::open(BrokerConfig {
            data_path: queue_path.clone(),
            projection_only: true,
            ..BrokerConfig::default()
        })
        .unwrap();
        broker
            .ensure_topic_layout("events", &[(0, 7)], &[7])
            .unwrap();
        let log = LogStore::open(&log_path, 1024 * 1024).unwrap();
        let publish = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 0),
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::Publish {
                operation_id: 1,
                topic: "events".into(),
                bodies: vec![
                    bytes::Bytes::from_static(b"one"),
                    bytes::Bytes::from_static(b"two"),
                ],
                timestamp_ns: 10,
                available_at_ms: 0,
                partition: Some(0),
                routing_key: None,
            })),
        };
        let eviction = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(
                QueueCommand::ProtectiveEvict {
                    operation_id: 2,
                    topic: "events".into(),
                    partition: 0,
                    through_message_id: (7u64 << 48) | 1,
                },
            )),
        };
        append_test_entry(&log, &publish);
        append_test_entry(&log, &eviction);
        let metadata = Arc::new(MetadataCatalog::standalone(1));
        let mut state = StateMachineStore::open_for_group_with_log(
            &state_path,
            Arc::clone(&broker),
            Arc::clone(&metadata),
            StateMachineRole::Partition {
                topic: "events".into(),
                partition: 0,
            },
            Vec::new(),
            log.clone(),
        )
        .unwrap();
        broker
            .create_channel_partition("events", "workers", 0)
            .unwrap();
        state.apply([publish, eviction]).await.unwrap();
        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            1
        );
        let projection = broker.export_partition_projection("events", 0).unwrap();
        assert_eq!(projection.messages.len(), 1);
        assert_eq!(projection.channels["workers"].ack_floor, 0);
        assert!(!projection.channels["workers"].ephemeral);
        state.build_snapshot().await.unwrap();
        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            1
        );
        drop(state);
        drop(broker);

        let broker = Broker::open(BrokerConfig {
            data_path: queue_path,
            projection_only: true,
            ..BrokerConfig::default()
        })
        .unwrap();
        let log = LogStore::open(log_path, 1024 * 1024).unwrap();
        let recovered = log.recovered_entries_with_payloads().await.unwrap();
        let _state = StateMachineStore::open_for_group_with_log(
            state_path,
            Arc::clone(&broker),
            metadata,
            StateMachineRole::Partition {
                topic: "events".into(),
                partition: 0,
            },
            recovered,
            log,
        )
        .unwrap();
        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            1
        );
        let delivery = broker
            .next_message_partition("events", "workers", 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.body.as_ref(), b"two");
    }

    fn append_test_entry(log: &LogStore, entry: &Entry<TypeConfig>) {
        let encoded = entry_codec::encode(entry).unwrap();
        let mut inner = log.inner.lock();
        let location = inner
            .segments
            .append_at_with_location(
                Record {
                    kind: RecordKind::PublishBatch,
                    flags: 0,
                    term: entry.log_id.leader_id.term,
                    index: entry.log_id.index,
                    timestamp_ns: 0,
                    message_id: 0,
                    payload: encoded.bytes,
                },
                true,
            )
            .unwrap();
        inner.entries.insert(
            entry.log_id.index,
            LogEntryPointer {
                log_id: entry.log_id,
                location,
            },
        );
    }
}
