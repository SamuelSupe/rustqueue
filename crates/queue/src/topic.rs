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
use rustqueue_protocol::validate_name;
use rustqueue_storage::{RecordHeader, RecordKind, SegmentLog};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const MAX_CHANNELS_PER_TOPIC: usize = 1_024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingSync {
    pub messages: u64,
    pub bytes: u64,
    pub since: Instant,
}

pub(crate) struct TopicHandle {
    pub commit_gate: Mutex<()>,
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
    position_gaps: Arc<[(u64, u64)]>,
    deliverable_position: u64,
    durable_position: u64,
    unsynced_messages: u64,
    unsynced_bytes: u64,
    unsynced_since: Option<Instant>,
    channels: HashMap<String, ChannelRuntime>,
    max_ack_gap: usize,
    durable_channel_counters: bool,
    published_count: u64,
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
        topic.reconcile_unrouted_boundary()?;
        Ok(Arc::new(Self {
            commit_gate: Mutex::new(()),
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
            unrouted_from_position: Some(1),
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
            commit_gate: Mutex::new(()),
            state: Mutex::new(Topic {
                name: name.into(),
                directory: directory.into(),
                manifest_path: directory.join("manifest"),
                manifest,
                log,
                messages: MessageIndex::new(index_cache),
                position_gaps: Arc::from(Vec::new()),
                deliverable_position: 0,
                durable_position: 0,
                unsynced_messages: 0,
                unsynced_bytes: 0,
                unsynced_since: None,
                channels: HashMap::new(),
                max_ack_gap,
                durable_channel_counters: storage_feature_level >= 2,
                published_count: 0,
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
        if manifest
            .unrouted_from_position
            .is_some_and(|position| position == 0 || position > manifest.next_position)
        {
            return Err(BrokerError::InvalidRecord(
                "topic manifest has an invalid unrouted position".into(),
            ));
        }
        let expected_directory = hex::encode(manifest.name.as_bytes());
        if validate_name(&manifest.name).is_err()
            || directory.file_name().and_then(|name| name.to_str())
                != Some(expected_directory.as_str())
        {
            return Err(BrokerError::InvalidRecord(
                "topic manifest name does not match its directory".into(),
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
                let messages =
                    log.read_location_with(&location, |header, payload_len, reader| match header
                        .kind
                    {
                        RecordKind::PublishBatch => {
                            batch::metas_from_reader(header, payload_len, reader, &location)
                        }
                        _ => Ok(Vec::new()),
                    })?;
                path_messages.extend(messages);
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
        let deliverable_position = messages.last_position().unwrap_or(0);
        if manifest.next_position < recovered_next {
            manifest.next_position = recovered_next;
            store_atomic(&manifest_path, &manifest)?;
        }
        let published_count = manifest.next_position.saturating_sub(1);
        Ok(Self {
            name: manifest.name.clone(),
            directory: directory.into(),
            manifest_path,
            manifest,
            log,
            messages,
            position_gaps: Arc::from(Vec::new()),
            deliverable_position,
            durable_position: deliverable_position,
            unsynced_messages: 0,
            unsynced_bytes: 0,
            unsynced_since: None,
            channels: HashMap::new(),
            max_ack_gap,
            durable_channel_counters: storage_feature_level >= 2,
            published_count,
        })
    }

    fn recover_channels(&mut self) -> Result<(), BrokerError> {
        for path in checkpoint_paths(&self.directory.join("channels"))? {
            let (state, store) = ChannelStore::open(&path, self.max_ack_gap)?;
            if state.ephemeral {
                store.remove()?;
                continue;
            }
            let name = state.name.clone();
            if self.channels.contains_key(&name) {
                return Err(BrokerError::InvalidRecord(format!(
                    "duplicate stored channel identity {name}"
                )));
            }
            self.channels.insert(
                name,
                ChannelRuntime {
                    state,
                    store: Some(store),
                    durable_counters: self.durable_channel_counters,
                },
            );
        }
        let recovered_next = self
            .messages
            .last_position()
            .map_or(1, |position| position.saturating_add(1));
        let channel_next = self
            .channels
            .values()
            .map(|channel| channel.state.recovered_position_high_watermark())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                BrokerError::InvalidRecord("channel position range is exhausted".into())
            })?;
        if self.manifest.next_position < channel_next {
            self.manifest.next_position = channel_next;
            store_atomic(&self.manifest_path, &self.manifest)?;
            self.published_count = self.manifest.next_position.saturating_sub(1);
        }
        self.position_gaps = Arc::from(
            self.messages
                .position_gaps(self.manifest.next_position.saturating_sub(1)),
        );
        for channel in self.channels.values_mut() {
            channel
                .state
                .set_absent_ranges(Arc::clone(&self.position_gaps));
        }
        if self.manifest.next_position > recovered_next && self.active_metadata_count() > 0 {
            // A relaxed tail can disappear after its position was recorded by
            // Topic metadata or a durable Channel command. Put the surviving
            // prefix in its own segment so later appends preserve that gap.
            self.spill_message_metadata()?;
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
        if !self.has_durable_channels() && self.manifest.unrouted_from_position.is_none() {
            return Err(BrokerError::InvalidRecord(
                "topic without a durable channel is missing its retention boundary".into(),
            ));
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
        let previous_last_position = self.last_position();
        let previous_segment = self.log.current_segment_path().to_path_buf();
        let parts = batch.parts();
        let location = self
            .log
            .append_parts_at_with_location(record, &parts, durable)?;
        let metas = batch::metas_after_append(timestamp_ns, available_at_ms, &location, &batch);
        let ids = metas.iter().map(|message| message.id).collect();
        self.messages.append(metas, reservation)?;
        self.published_count = self
            .published_count
            .saturating_add(batch.entries.len() as u64);
        if previous_segment != self.log.current_segment_path() {
            self.persist_segment_index(&previous_segment)?;
            self.durable_position = self.durable_position.max(previous_last_position);
            self.deliverable_position = self.deliverable_position.max(previous_last_position);
            self.unsynced_messages = 0;
            self.unsynced_bytes = 0;
            self.unsynced_since = None;
        }
        self.manifest.next_position = self
            .manifest
            .next_position
            .saturating_add(batch.entries.len() as u64);
        if durable {
            self.mark_durable_through(self.last_position());
        }
        Ok(ids)
    }

    pub fn clone_log_for_sync(&self) -> Result<File, BrokerError> {
        Ok(self.log.clone_current_for_sync()?)
    }

    pub fn mark_log_sync_failed(&self) {
        self.log.mark_sync_failed();
    }

    pub fn mark_deliverable_through(&mut self, position: u64) {
        debug_assert!(position <= self.last_position());
        self.deliverable_position = self.deliverable_position.max(position);
    }

    pub fn record_unsynced(&mut self, messages: usize, bytes: usize) {
        if messages == 0 || self.durable_position >= self.written_position() {
            return;
        }
        self.unsynced_messages = self.unsynced_messages.saturating_add(messages as u64);
        self.unsynced_bytes = self.unsynced_bytes.saturating_add(bytes as u64);
        self.unsynced_since.get_or_insert_with(Instant::now);
    }

    pub fn pending_sync(&self) -> Option<PendingSync> {
        (self.unsynced_messages > 0).then(|| PendingSync {
            messages: self.unsynced_messages,
            bytes: self.unsynced_bytes,
            since: self.unsynced_since.unwrap_or_else(Instant::now),
        })
    }

    pub fn mark_durable_through(&mut self, position: u64) {
        debug_assert!(position <= self.last_position());
        self.durable_position = self.durable_position.max(position);
        self.deliverable_position = self.deliverable_position.max(position);
        if self.durable_position >= self.written_position() {
            self.unsynced_messages = 0;
            self.unsynced_bytes = 0;
            self.unsynced_since = None;
        }
    }

    fn seal_log(&mut self) -> Result<(), BrokerError> {
        let previous_segment = self.log.current_segment_path().to_path_buf();
        self.log.seal()?;
        self.mark_durable_through(self.last_position());
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
    fn written_position(&self) -> u64 {
        self.messages.last_position().unwrap_or(0)
    }
    pub fn deliverable_position(&self) -> u64 {
        self.deliverable_position
    }

    fn has_durable_channels(&self) -> bool {
        self.channels
            .values()
            .any(|channel| !channel.state.ephemeral)
    }

    fn earliest_retained_position(&self) -> u64 {
        self.messages
            .first_position()
            .unwrap_or(self.manifest.next_position)
    }

    fn unrouted_start_position(&self) -> Result<u64, BrokerError> {
        self.manifest.unrouted_from_position.ok_or_else(|| {
            BrokerError::InvalidRecord(
                "topic without a durable channel is missing its retention boundary".into(),
            )
        })
    }

    fn set_unrouted_from_position(&mut self, position: Option<u64>) -> Result<(), BrokerError> {
        if self.manifest.unrouted_from_position == position {
            return Ok(());
        }
        let mut manifest = self.manifest.clone();
        manifest.unrouted_from_position = position;
        store_atomic(&self.manifest_path, &manifest)?;
        self.manifest = manifest;
        Ok(())
    }

    fn reconcile_unrouted_boundary(&mut self) -> Result<(), BrokerError> {
        let position = if self.has_durable_channels() {
            None
        } else {
            let earliest = self.earliest_retained_position();
            Some(
                self.manifest
                    .unrouted_from_position
                    .unwrap_or(earliest)
                    .max(earliest)
                    .min(self.manifest.next_position),
            )
        };
        self.set_unrouted_from_position(position)
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
        let first_durable = !ephemeral && !self.has_durable_channels();
        let barrier = if ephemeral {
            // NSQ ephemeral channels only observe messages published while at
            // least one consumer keeps the channel alive. Re-creating an
            // ephemeral channel must therefore start at the current tail.
            self.last_position()
        } else if first_durable {
            self.unrouted_start_position()?.saturating_sub(1)
        } else {
            let cutoff = now_ns()
                .saturating_sub(bootstrap_retention.as_nanos().min(i64::MAX as u128) as i64);
            self.messages
                .retain_from_timestamp(cutoff, self.manifest.next_position)
                .saturating_sub(1)
        };
        let mut state = ChannelState::new(name.into(), barrier, ephemeral, self.max_ack_gap);
        state.set_absent_ranges(Arc::clone(&self.position_gaps));
        let store = if ephemeral {
            None
        } else {
            Some(ChannelStore::create(
                &self.directory.join("channels"),
                &state,
            )?)
        };
        if first_durable {
            // The checkpoint reaches disk before the Topic releases this
            // boundary, so recovery always has either the Channel or the hold.
            self.set_unrouted_from_position(None)?;
        }
        self.channels.insert(
            name.into(),
            ChannelRuntime {
                state,
                store,
                durable_counters: self.durable_channel_counters,
            },
        );
        Ok(true)
    }

    pub fn delete_channel(&mut self, name: &str) -> Result<(), BrokerError> {
        let state = &self
            .channels
            .get(name)
            .ok_or(BrokerError::ChannelNotFound)?
            .state;
        let deleting_last_durable = !state.ephemeral
            && self
                .channels
                .values()
                .filter(|channel| !channel.state.ephemeral)
                .count()
                == 1;
        if deleting_last_durable {
            // Establish the new hold before removing the final checkpoint.
            // After a crash, either the Channel recovers or this boundary does.
            self.set_unrouted_from_position(Some(self.manifest.next_position))?;
        }
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

    pub fn channel_counts(&mut self, channel: &str) -> Result<(u64, u64, u64), BrokerError> {
        let now_ms = now_ms();
        let scheduled = self.messages.deferred_positions(now_ms);
        let last_position = self.deliverable_position;
        let channel = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let (depth, in_flight, deferred, _) =
            channel
                .state
                .metric_counts(last_position, &scheduled, now_ms);
        Ok((depth, in_flight, deferred))
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
