#[path = "topic/delivery.rs"]
mod delivery;
#[path = "topic/maintenance.rs"]
mod maintenance;

use crate::batch::{self, EncodedBatch};
use crate::channel::{ChannelCommand, ChannelRuntime, ChannelState};
use crate::channel_store::{checkpoint_paths, ChannelStore};
use crate::metadata::{load_topic_manifest, store_atomic, TopicManifest};
use crate::model::MessageMeta;
use crate::BrokerError;
use parking_lot::Mutex;
use rustqueue_storage::{Record, RecordKind, SegmentLog};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    messages: VecDeque<MessageMeta>,
    channels: HashMap<String, ChannelRuntime>,
    max_backlog_messages: usize,
    max_ack_gap: usize,
}

impl TopicHandle {
    pub fn open(
        directory: &Path,
        max_segment_bytes: u64,
        max_backlog_messages: usize,
        max_ack_gap: usize,
        storage_feature_level: u32,
    ) -> Result<Arc<Self>, BrokerError> {
        let mut topic = Topic::open(
            directory,
            max_segment_bytes,
            max_backlog_messages,
            max_ack_gap,
            storage_feature_level,
        )?;
        let (wake, _) = tokio::sync::watch::channel(0);
        topic.recover_channels()?;
        Ok(Arc::new(Self {
            state: Mutex::new(topic),
            wake,
        }))
    }

    pub fn create(
        directory: &Path,
        name: &str,
        max_segment_bytes: u64,
        max_backlog_messages: usize,
        max_ack_gap: usize,
        storage_feature_level: u32,
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
                messages: VecDeque::new(),
                channels: HashMap::new(),
                max_backlog_messages,
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
        max_backlog_messages: usize,
        max_ack_gap: usize,
        storage_feature_level: u32,
    ) -> Result<Self, BrokerError> {
        let manifest_path = directory.join("manifest");
        let mut manifest: TopicManifest = load_topic_manifest(&manifest_path)?
            .ok_or_else(|| BrokerError::InvalidRecord("topic manifest is missing".into()))?;
        if manifest.format != 7 || manifest.deleted {
            return Err(BrokerError::InvalidRecord(
                "topic manifest is not an active v7 topic".into(),
            ));
        }
        let log = SegmentLog::open_with_feature_level(
            directory.join("segments"),
            max_segment_bytes,
            1,
            storage_feature_level,
        )?;
        let mut messages = VecDeque::new();
        let mut expected_position = None;
        for location in log.locations().to_vec() {
            let record = log.read_location(&location)?;
            if record.kind != RecordKind::PublishBatch {
                continue;
            }
            for message in batch::metas(&record, &location)? {
                if expected_position.is_some_and(|expected| message.position != expected) {
                    return Err(BrokerError::InvalidRecord(
                        "topic message positions are not contiguous".into(),
                    ));
                }
                expected_position = Some(message.position.saturating_add(1));
                messages.push_back(message);
            }
        }
        let recovered_next = messages
            .back()
            .map_or(1, |message| message.position.saturating_add(1));
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
            max_backlog_messages,
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
        batch: EncodedBatch,
        durable: bool,
    ) -> Result<Vec<u64>, BrokerError> {
        if self.manifest.deleted {
            return Err(BrokerError::TopicNotFound);
        }
        if self.messages.len().saturating_add(batch.entries.len()) > self.max_backlog_messages {
            return Err(BrokerError::BacklogLimit);
        }
        let record = Record {
            kind: RecordKind::PublishBatch,
            flags: 0,
            index: self.log.next_index(),
            timestamp_ns,
            message_id: first_id,
            available_at_ms,
            payload: batch.payload.clone(),
        };
        let location = self.log.append_at_with_location(record, durable)?;
        let metas = batch::metas_after_append(timestamp_ns, available_at_ms, &location, &batch);
        let ids = metas.iter().map(|message| message.id).collect();
        self.messages.extend(metas);
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
        let ephemeral = name.ends_with("#ephemeral");
        let barrier = if ephemeral {
            // NSQ ephemeral channels only observe messages published while at
            // least one consumer keeps the channel alive. Re-creating an
            // ephemeral channel must therefore start at the current tail.
            self.last_position()
        } else {
            let cutoff = now_ns()
                .saturating_sub(bootstrap_retention.as_nanos().min(i64::MAX as u128) as i64);
            let initial_barrier = self
                .messages
                .front()
                .map_or(self.last_position(), |message| {
                    message.position.saturating_sub(1)
                });
            self.messages
                .iter()
                .take_while(|message| message.timestamp_ns < cutoff)
                .last()
                .map_or(initial_barrier, |message| message.position)
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
