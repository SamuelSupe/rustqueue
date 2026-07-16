#[path = "broker/group_commit.rs"]
mod group_commit;
#[path = "broker/io.rs"]
mod io;
#[path = "broker/maintenance.rs"]
mod maintenance;

use crate::metadata::{
    load_optional, load_topic_manifest, store_atomic, topic_directory, BrokerMeta,
};
use crate::model::BrokerStats;
use crate::payload_reader::PayloadReader;
use crate::telemetry::QueueMetrics;
use crate::topic::TopicHandle;
use parking_lot::{Mutex, RwLock};
use rustqueue_protocol::validate_name;
use rustqueue_storage::{
    binary_capabilities, ensure_data_format, prepare_compatibility, BinaryCapabilities,
    CompatibilityState, StorageError, BASE_STORAGE_FEATURE_LEVEL,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub data_path: PathBuf,
    pub node_id: u64,
    pub max_segment_bytes: u64,
    pub max_message_bytes: usize,
    pub message_timeout: Duration,
    pub bootstrap_retention: Duration,
    pub max_ack_gap: usize,
    pub max_backlog_messages: usize,
    pub entry_cache_bytes: usize,
    pub payload_read_workers: usize,
    pub payload_read_queue: usize,
    pub storage_feature_level: u32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_path: "data".into(),
            node_id: 1,
            max_segment_bytes: 100 * 1024 * 1024,
            max_message_bytes: 20 * 1024 * 1024,
            message_timeout: Duration::from_secs(60),
            bootstrap_retention: Duration::from_secs(30),
            max_ack_gap: 65_536,
            max_backlog_messages: 10_000_000,
            entry_cache_bytes: 64 * 1024 * 1024,
            payload_read_workers: 0,
            payload_read_queue: 4096,
            storage_feature_level: BASE_STORAGE_FEATURE_LEVEL,
        }
    }
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("topic is invalid")]
    InvalidTopic,
    #[error("channel is invalid")]
    InvalidChannel,
    #[error("topic does not exist")]
    TopicNotFound,
    #[error("topic deletion is still draining in-process readers")]
    TopicRetiring,
    #[error("channel does not exist")]
    ChannelNotFound,
    #[error("message does not exist")]
    MessageNotFound,
    #[error("message is not in flight")]
    MessageNotInFlight,
    #[error("message exceeds configured limit")]
    MessageTooLarge,
    #[error("publish batch is invalid or too large")]
    BatchTooLarge,
    #[error("topic backlog limit reached")]
    BacklogLimit,
    #[error("message ID sequence exhausted")]
    SequenceExhausted,
    #[error("local storage is isolated after an earlier failure; restart is required")]
    StorageUnavailable,
    #[error("invalid durable record: {0}")]
    InvalidRecord(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    config: BrokerConfig,
    topics_root: PathBuf,
    meta_path: PathBuf,
    meta: Mutex<BrokerMeta>,
    sequence: Mutex<SequenceState>,
    topic_lifecycle: Mutex<()>,
    topics: RwLock<HashMap<String, Arc<TopicHandle>>>,
    retired_topics: Mutex<HashMap<String, Arc<TopicHandle>>>,
    payload_reader: Arc<PayloadReader>,
    registry_revision: AtomicU64,
    storage_healthy: AtomicBool,
    publish_groups: group_commit::PublishGroups,
    metrics: QueueMetrics,
    compatibility: CompatibilityState,
}

struct SequenceState {
    next: u64,
    reserved_exclusive: u64,
}

impl Broker {
    pub fn open(config: BrokerConfig) -> Result<Self, BrokerError> {
        if config.node_id == 0 || config.node_id > u16::MAX as u64 {
            return Err(BrokerError::InvalidRecord(
                "node ID must fit 16 bits".into(),
            ));
        }
        ensure_data_format(&config.data_path)
            .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
        let compatibility = prepare_compatibility(&config.data_path, config.storage_feature_level)
            .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
        let topics_root = config.data_path.join("topics");
        std::fs::create_dir_all(&topics_root)?;
        std::fs::create_dir_all(config.data_path.join("dlq-outbox"))?;
        std::fs::create_dir_all(config.data_path.join("audit"))?;
        let meta_path = config.data_path.join("broker.meta");
        let meta = match load_optional::<BrokerMeta>(&meta_path)? {
            Some(meta) if meta.format == 7 && meta.node_id == config.node_id => meta,
            Some(_) => {
                return Err(BrokerError::InvalidRecord(
                    "broker.meta identity or format mismatch".into(),
                ))
            }
            None => {
                let meta = BrokerMeta {
                    format: 7,
                    node_id: config.node_id,
                    next_sequence: 1,
                    registry_revision: 1,
                };
                store_atomic(&meta_path, &meta)?;
                meta
            }
        };
        let metrics = QueueMetrics::default();
        let payload_reader = PayloadReader::new(
            config.entry_cache_bytes,
            config.payload_read_workers,
            config.payload_read_queue,
            Arc::clone(&metrics.payload_read),
        );
        let mut topics = HashMap::new();
        for entry in std::fs::read_dir(&topics_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() || !entry.path().join("manifest").exists() {
                continue;
            }
            let manifest = load_topic_manifest(&entry.path().join("manifest"))?
                .ok_or_else(|| BrokerError::InvalidRecord("topic manifest disappeared".into()))?;
            if manifest.deleted {
                std::fs::remove_dir_all(entry.path())?;
                continue;
            }
            let handle = TopicHandle::open(
                &entry.path(),
                config.max_segment_bytes,
                config.max_backlog_messages,
                config.max_ack_gap,
                compatibility.active_writer_feature_level,
            )?;
            let name = handle.state.lock().name.clone();
            topics.insert(name, handle);
        }
        let revision = meta.registry_revision;
        let next_sequence = meta.next_sequence;
        let broker = Self {
            inner: Arc::new(BrokerInner {
                config,
                topics_root,
                meta_path,
                meta: Mutex::new(meta),
                sequence: Mutex::new(SequenceState {
                    next: next_sequence,
                    reserved_exclusive: next_sequence,
                }),
                topic_lifecycle: Mutex::new(()),
                topics: RwLock::new(topics),
                retired_topics: Mutex::new(HashMap::new()),
                payload_reader,
                registry_revision: AtomicU64::new(revision),
                storage_healthy: AtomicBool::new(true),
                publish_groups: group_commit::PublishGroups::default(),
                metrics,
                compatibility,
            }),
        };
        broker.recover_outbox()?;
        Ok(broker)
    }

    pub fn capabilities(&self) -> (BinaryCapabilities, CompatibilityState) {
        (binary_capabilities(), self.inner.compatibility.clone())
    }

    pub async fn create_topic(&self, name: &str) -> Result<(), BrokerError> {
        let broker = self.clone();
        let name = name.to_owned();
        self.storage_task(move || broker.get_or_create_topic(&name).map(|_| ()))
            .await
    }

    pub async fn delete_topic(&self, name: &str) -> Result<(), BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        let broker = self.clone();
        let name = name.to_owned();
        self.storage_task(move || {
            let _lifecycle = broker.inner.topic_lifecycle.lock();
            let handle = broker
                .inner
                .topics
                .read()
                .get(&name)
                .cloned()
                .ok_or(BrokerError::TopicNotFound)?;
            handle.state.lock().mark_deleted()?;
            broker.inner.topics.write().remove(&name);
            let directory = topic_directory(&broker.inner.config.data_path, &name);
            if Arc::strong_count(&handle) == 1
                && !broker.inner.payload_reader.has_active_under(&directory)
            {
                std::fs::remove_dir_all(directory)?;
                std::fs::File::open(&broker.inner.topics_root)?.sync_all()?;
            } else {
                broker
                    .inner
                    .retired_topics
                    .lock()
                    .insert(name.clone(), handle);
            }
            broker.bump_registry()?;
            Ok(())
        })
        .await
    }

    pub async fn create_channel(&self, topic: &str, channel: &str) -> Result<(), BrokerError> {
        validate_channel(channel)?;
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            let handle = broker.get_or_create_topic(&topic)?;
            if handle
                .state
                .lock()
                .create_channel(&channel, broker.inner.config.bootstrap_retention)?
            {
                broker.bump_registry()?;
            }
            handle.signal();
            Ok(())
        })
        .await
    }

    pub async fn delete_channel(&self, topic: &str, channel: &str) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            broker
                .topic(&topic)?
                .state
                .lock()
                .delete_channel(&channel)?;
            broker.bump_registry()?;
            Ok(())
        })
        .await
    }

    pub async fn set_topic_paused(&self, topic: &str, paused: bool) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        self.storage_task(move || broker.topic(&topic)?.state.lock().set_paused(paused))
            .await
    }

    pub async fn set_channel_paused(
        &self,
        topic: &str,
        channel: &str,
        paused: bool,
    ) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            broker
                .topic(&topic)?
                .state
                .lock()
                .set_channel_paused(&channel, paused)
        })
        .await
    }

    pub async fn empty_topic(&self, topic: &str) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        self.storage_task(move || broker.topic(&topic)?.state.lock().empty_topic())
            .await
    }

    pub async fn empty_channel(&self, topic: &str, channel: &str) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || broker.topic(&topic)?.state.lock().empty_channel(&channel))
            .await
    }

    pub fn topic_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.inner.topics.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn channel_names(&self, topic: &str) -> Result<Vec<String>, BrokerError> {
        Ok(self.topic(topic)?.state.lock().channel_names())
    }

    pub fn stats(&self) -> BrokerStats {
        let mut topics: Vec<_> = self
            .inner
            .topics
            .read()
            .values()
            .map(|topic| topic.state.lock().stats())
            .collect();
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        BrokerStats {
            publish_group_commit: self.inner.publish_groups.stats(),
            latency: self.inner.metrics.snapshot(),
            topics,
        }
    }

    pub fn registry_revision(&self) -> u64 {
        self.inner.registry_revision.load(Ordering::Acquire)
    }

    pub fn storage_healthy(&self) -> bool {
        self.inner.storage_healthy.load(Ordering::Acquire)
    }

    pub(crate) fn ensure_storage_healthy(&self) -> Result<(), BrokerError> {
        if self.storage_healthy() {
            Ok(())
        } else {
            Err(BrokerError::StorageUnavailable)
        }
    }

    pub(crate) fn observe_storage_result<T>(
        &self,
        result: Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        if result.as_ref().is_err_and(|error| {
            matches!(
                error,
                BrokerError::StorageUnavailable | BrokerError::Storage(_) | BrokerError::Io(_)
            )
        }) {
            self.inner.storage_healthy.store(false, Ordering::Release);
        }
        result
    }

    async fn storage_task<T: Send + 'static>(
        &self,
        task: impl FnOnce() -> Result<T, BrokerError> + Send + 'static,
    ) -> Result<T, BrokerError> {
        self.ensure_storage_healthy()?;
        let result = blocking(task).await;
        self.observe_storage_result(result)
    }

    fn get_or_create_topic(&self, name: &str) -> Result<Arc<TopicHandle>, BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        if let Some(topic) = self.inner.topics.read().get(name).cloned() {
            return Ok(topic);
        }
        let _lifecycle = self.inner.topic_lifecycle.lock();
        self.cleanup_retired_topic(name)?;
        let mut topics = self.inner.topics.write();
        if let Some(topic) = topics.get(name).cloned() {
            return Ok(topic);
        }
        let directory = topic_directory(&self.inner.config.data_path, name);
        let topic = TopicHandle::create(
            &directory,
            name,
            self.inner.config.max_segment_bytes,
            self.inner.config.max_backlog_messages,
            self.inner.config.max_ack_gap,
            self.inner.compatibility.active_writer_feature_level,
        )?;
        topics.insert(name.into(), Arc::clone(&topic));
        drop(topics);
        self.bump_registry()?;
        Ok(topic)
    }

    fn cleanup_retired_topic(&self, name: &str) -> Result<(), BrokerError> {
        let mut retired = self.inner.retired_topics.lock();
        let Some(handle) = retired.get(name) else {
            return Ok(());
        };
        if Arc::strong_count(handle) > 1 {
            return Err(BrokerError::TopicRetiring);
        }
        let directory = topic_directory(&self.inner.config.data_path, name);
        if self.inner.payload_reader.has_active_under(&directory) {
            return Err(BrokerError::TopicRetiring);
        }
        let handle = retired.remove(name).expect("retired topic still exists");
        drop(handle);
        if directory.exists() {
            std::fs::remove_dir_all(directory)?;
        }
        std::fs::File::open(&self.inner.topics_root)?.sync_all()?;
        Ok(())
    }

    fn topic(&self, name: &str) -> Result<Arc<TopicHandle>, BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        self.inner
            .topics
            .read()
            .get(name)
            .cloned()
            .ok_or(BrokerError::TopicNotFound)
    }

    fn bump_registry(&self) -> Result<(), BrokerError> {
        let revision = self
            .inner
            .registry_revision
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        let mut meta = self.inner.meta.lock();
        meta.registry_revision = revision;
        store_atomic(&self.inner.meta_path, &*meta)?;
        Ok(())
    }
}

fn validate_channel(channel: &str) -> Result<(), BrokerError> {
    validate_name(channel).map_err(|_| BrokerError::InvalidChannel)
}

async fn blocking<T: Send + 'static>(
    task: impl FnOnce() -> Result<T, BrokerError> + Send + 'static,
) -> Result<T, BrokerError> {
    tokio::task::spawn_blocking(task).await.map_err(|error| {
        BrokerError::InvalidRecord(format!("blocking storage task failed: {error}"))
    })?
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
