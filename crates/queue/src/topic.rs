#[path = "topic/delivery.rs"]
pub(crate) mod delivery;
#[path = "topic/index.rs"]
pub(crate) mod index;
#[path = "topic/index_cache.rs"]
pub(crate) mod index_cache;
#[path = "topic/maintenance.rs"]
mod maintenance;
#[path = "topic/recovery.rs"]
mod recovery;

use crate::batch::{self, EncodedBatch};
use crate::channel::{ChannelCommand, ChannelRuntime, ChannelState};
use crate::channel_store::{checkpoint_paths, ChannelStore};
use crate::metadata::{load_topic_manifest, store_atomic, TopicManifest};
use crate::BrokerError;
use index::{MessageIndex, MessageIndexCache, MetadataReservation};
use parking_lot::Mutex;
use rustqueue_storage::{RecordHeader, RecordKind, SegmentLog};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const MAX_CHANNELS_PER_TOPIC: usize = 1_024;

pub(crate) struct TopicHandle {
    pub state: Mutex<Topic>,
    pub wake: tokio::sync::watch::Sender<u64>,
}

pub(crate) struct Topic {
    pub name: String,
    directory: PathBuf,
    manifest_path: PathBuf,
    manifest: TopicManifest,
    log: SegmentLog,
    messages: MessageIndex,
    channels: HashMap<String, ChannelRuntime>,
    max_ack_gap: usize,
}

impl TopicHandle {
    pub fn open(
        directory: &Path,
        max_segment_bytes: u64,
        max_ack_gap: usize,
        storage_feature_level: u32,
        index_cache: Arc<MessageIndexCache>,
    ) -> Result<Arc<Self>, BrokerError> {
        let mut topic = Topic::open(
            directory,
            max_segment_bytes,
            max_ack_gap,
            storage_feature_level,
            index_cache,
        )?;
        let (wake, _) = tokio::sync::watch::channel(0);
        topic.recover_channels()?;
        if topic.channels.len() > MAX_CHANNELS_PER_TOPIC {
            return Err(BrokerError::InvalidRecord(format!(
                "topic channel count {} exceeds configured maximum {}",
                topic.channels.len(),
                MAX_CHANNELS_PER_TOPIC
            )));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(topic),
            wake,
        }))
    }

    pub fn create(
        directory: &Path,
        name: &str,
        max_segment_bytes: u64,
        max_ack_gap: usize,
        storage_feature_level: u32,
        index_cache: Arc<MessageIndexCache>,
    ) -> Result<Arc<Self>, BrokerError> {
        std::fs::create_dir_all(directory.join("segments"))?;
        std::fs::create_dir_all(directory.join("channels"))?;
        let manifest = TopicManifest {
            format: 7,
            name: name.into(),
            paused: false,
            deleted: false,
            next_position: 1,
        };
        store_atomic(&directory.join("manifest"), &manifest)?;
        let log = SegmentLog::open_with_feature_level(
            directory.join("segments"),
            max_segment_bytes,
            1,
            storage_feature_level,
        )?;
        let (wake, _) = tokio::sync::watch::channel(0);
        Ok(Arc::new(Self {
            state: Mutex::new(Topic {
                name: name.into(),
                directory: directory.into(),
                manifest_path: directory.join("manifest"),
                manifest,
                log,
                messages: MessageIndex::new(index_cache),
                channels: HashMap::new(),
                max_ack_gap,
            }),
            wake,
        }))
    }

    pub fn signal(&self) {
        self.wake
            .send_modify(|value| *value = value.wrapping_add(1));
    }
}

impl Topic {
    fn open(
        directory: &Path,
        max_segment_bytes: u64,
        max_ack_gap: usize,
        storage_feature_level: u32,
        index_cache: Arc<MessageIndexCache>,
    ) -> Result<Self, BrokerError> {
        let manifest_path = directory.join("manifest");
        let mut manifest: TopicManifest = load_topic_manifest(&manifest_path)?
            .ok_or_else(|| BrokerError::InvalidRecord("topic manifest is missing".into()))?;
        if manifest.format != 7 || manifest.deleted {
            return Err(BrokerError::InvalidRecord(
                "topic manifest is not an active v7 topic".into(),
            ));
        }
        let mut log = SegmentLog::open_with_feature_level(
            directory.join("segments"),
            max_segment_bytes,
            1,
            storage_feature_level,
        )?;
        let mut messages = MessageIndex::new(index_cache);
        for path in log.segment_paths()? {
            let immutable = log.immutable_file(&path).is_some();
            if immutable {
                if let Some(metadata) = log.recovery_metadata_ref(&path) {
                    if messages.recover_sealed(metadata).is_ok() {
                        continue;
                    }
                }
            }
            let mut path_messages = Vec::new();
            for location in log.locations_for_segment(&path)? {
                let record = log.read_location(&location)?;
                if record.kind == RecordKind::PublishBatch {
                    path_messages.extend(batch::metas(&record, &location)?);
                }
            }
            if immutable {
                log.persist_recovery_index(&path, recovery::encode(path_messages.iter()))?;
                let metadata = log.recovery_metadata_ref(&path).ok_or_else(|| {
                    BrokerError::InvalidRecord("sealed topic index was not persisted".into())
                })?;
                messages.recover_sealed(metadata)?;
            } else {
                messages.recover_active(path_messages)?;
            }
        }
        let recovered_next = messages
            .last_position()
            .map_or(1, |position| position.saturating_add(1));
        if manifest.next_position < recovered_next {
            manifest.next_position = recovered_next;
            store_atomic(&manifest_path, &manifest)?;
        }
        Ok(Self {
            name: manifest.name.clone(),
            directory: directory.into(),
            manifest_path,
            manifest,
            log,
            messages,
            channels: HashMap::new(),
            max_ack_gap,
        })
    }

    fn recover_channels(&mut self) -> Result<(), BrokerError> {
        for path in checkpoint_paths(&self.directory.join("channels"))? {
            let (state, store) = ChannelStore::open(&path, self.max_ack_gap)?;
            if state.ephemeral {
                store.remove()?;
                continue;
            }
            self.channels.insert(
                state.name.clone(),
                ChannelRuntime {
                    state,
                    store: Some(store),
                },
            );
        }
        Ok(())
    }

    pub fn append_batch(
        &mut self,
        first_id: u64,
        timestamp_ns: i64,
        available_at_ms: i64,
        batch: EncodedBatch<'_>,
        durable: bool,
        reservation: &mut MetadataReservation,
    ) -> Result<Vec<u64>, BrokerError> {
        if self.manifest.deleted {
            return Err(BrokerError::TopicNotFound);
        }
        let timestamp_ns = self
            .messages
            .last_timestamp_ns()
            .map_or(timestamp_ns, |previous| timestamp_ns.max(previous));
        let record = RecordHeader {
            kind: RecordKind::PublishBatch,
            flags: 0,
            index: self.log.next_index(),
            timestamp_ns,
            message_id: first_id,
            available_at_ms,
        };
        let previous_segment = self.log.current_segment_path().to_path_buf();
        let parts = batch.parts();
        let location = self
            .log
            .append_parts_at_with_location(record, &parts, durable)?;
        let metas = batch::metas_after_append(timestamp_ns, available_at_ms, &location, &batch);
        let ids = metas.iter().map(|message| message.id).collect();
        self.messages.append(metas, reservation)?;
        if previous_segment != self.log.current_segment_path() {
            self.persist_segment_index(&previous_segment)?;
        }
        self.manifest.next_position = self
            .manifest
            .next_position
            .saturating_add(batch.entries.len() as u64);
        Ok(ids)
    }

    pub fn sync_log(&self) -> Result<(), BrokerError> {
        self.log.sync()?;
        Ok(())
    }

    fn seal_log(&mut self) -> Result<(), BrokerError> {
        let previous_segment = self.log.current_segment_path().to_path_buf();
        self.log.seal()?;
        if previous_segment != self.log.current_segment_path() {
            self.persist_segment_index(&previous_segment)?;
        }
        Ok(())
    }

    pub(crate) fn spill_message_metadata(&mut self) -> Result<usize, BrokerError> {
        let count = self.messages.active_count();
        if count > 0 {
            self.seal_log()?;
        }
        Ok(count)
    }

    pub(crate) fn active_metadata_count(&self) -> usize {
        self.messages.active_count()
    }

    fn persist_segment_index(&mut self, path: &Path) -> Result<(), BrokerError> {
        let (_first_index, _last_index) = self
            .log
            .record_index_range(path)
            .ok_or_else(|| BrokerError::InvalidRecord("sealed segment has no records".into()))?;
        let metadata = recovery::encode(self.messages.active_for_path(path));
        self.log.persist_recovery_index(path, metadata)?;
        let reference = self.log.recovery_metadata_ref(path).ok_or_else(|| {
            BrokerError::InvalidRecord("sealed topic index was not persisted".into())
        })?;
        self.messages.seal_path(path, reference)?;
        Ok(())
    }

    pub fn next_position(&self) -> u64 {
        self.manifest.next_position
    }
    pub fn last_position(&self) -> u64 {
        self.manifest.next_position.saturating_sub(1)
    }
    pub fn set_paused(&mut self, paused: bool) -> Result<(), BrokerError> {
        self.manifest.paused = paused;
        store_atomic(&self.manifest_path, &self.manifest)?;
        Ok(())
    }

    pub fn create_channel(
        &mut self,
        name: &str,
        bootstrap_retention: Duration,
    ) -> Result<bool, BrokerError> {
        if self.manifest.deleted {
            return Err(BrokerError::TopicNotFound);
        }
        if self.channels.contains_key(name) {
            return Ok(false);
        }
        if self.channels.len() >= MAX_CHANNELS_PER_TOPIC {
            return Err(BrokerError::ChannelLimit);
        }
        let ephemeral = name.ends_with("#ephemeral");
        let barrier = if ephemeral {
            // NSQ ephemeral channels only observe messages published while at
            // least one consumer keeps the channel alive. Re-creating an
            // ephemeral channel must therefore start at the current tail.
            self.last_position()
        } else {
            let cutoff = now_ns()
                .saturating_sub(bootstrap_retention.as_nanos().min(i64::MAX as u128) as i64);
            self.messages
                .retain_from_timestamp(cutoff, self.manifest.next_position)
                .saturating_sub(1)
        };
        let state = ChannelState::new(name.into(), barrier, ephemeral, self.max_ack_gap);
        let store = if ephemeral {
            None
        } else {
            Some(ChannelStore::create(
                &self.directory.join("channels"),
                &state,
            )?)
        };
        self.channels
            .insert(name.into(), ChannelRuntime { state, store });
        Ok(true)
    }

    pub fn delete_channel(&mut self, name: &str) -> Result<(), BrokerError> {
        let channel = self
            .channels
            .remove(name)
            .ok_or(BrokerError::ChannelNotFound)?;
        if let Some(store) = channel.store {
            store.remove()?;
        }
        Ok(())
    }

    pub fn channel_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.channels.keys().cloned().collect();
        names.sort();
        names
    }

    #[cfg(test)]
    pub(crate) fn index_residency(&self) -> (usize, usize) {
        (self.messages.active_count(), self.messages.sealed_count())
    }

    fn persist_channel(
        &mut self,
        channel: &str,
        command: ChannelCommand,
    ) -> Result<(), BrokerError> {
        let runtime = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if let Some(store) = runtime.store.as_mut() {
            store.append(&command)?;
        }
        runtime.state.apply(&command);
        if let Some(store) = runtime.store.as_mut() {
            store.checkpoint_if_needed(&runtime.state)?;
        }
        Ok(())
    }

    fn persist_channel_buffered(
        &mut self,
        channel: &str,
        command: ChannelCommand,
    ) -> Result<(), BrokerError> {
        let runtime = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if let Some(store) = runtime.store.as_mut() {
            store.append_buffered(&command)?;
        }
        runtime.state.apply(&command);
        Ok(())
    }

    pub fn sync_channel_wals<'a>(
        &mut self,
        channels: impl Iterator<Item = &'a String>,
    ) -> Result<(), BrokerError> {
        for name in channels {
            let runtime = self
                .channels
                .get_mut(name)
                .ok_or(BrokerError::ChannelNotFound)?;
            if let Some(store) = runtime.store.as_mut() {
                store.sync()?;
            }
        }
        Ok(())
    }

    pub fn checkpoint_channels_if_needed<'a>(
        &mut self,
        channels: impl Iterator<Item = &'a String>,
    ) -> Result<(), BrokerError> {
        for name in channels {
            let runtime = self
                .channels
                .get_mut(name)
                .ok_or(BrokerError::ChannelNotFound)?;
            if let Some(store) = runtime.store.as_mut() {
                store.checkpoint_if_needed(&runtime.state)?;
            }
        }
        Ok(())
    }
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
