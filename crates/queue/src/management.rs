use crate::metadata::{load_optional, store_atomic};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

const FENCE_FORMAT: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicManagementAction {
    Create,
    Pause,
    Unpause,
    Empty,
    Delete,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChannelManagementAction {
    Create,
    Pause,
    Unpause,
    Empty,
    Delete,
    Tombstone,
}

#[derive(Clone, Copy, Debug)]
pub struct ChannelManagementCommand<'a> {
    pub operation_id: &'a str,
    pub topic: &'a str,
    pub channel: &'a str,
    pub action: ChannelManagementAction,
    pub expected_revision: u64,
    pub tombstone_until_ms: Option<i64>,
    pub require_idle: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManagementFenceSnapshot {
    #[serde(default)]
    pub revision: String,
    #[serde(default)]
    pub topics: BTreeMap<String, i64>,
    #[serde(default)]
    pub channels: Vec<ChannelFence>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelFence {
    pub topic: String,
    pub channel: String,
    pub until_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManagementResult {
    pub revision: u64,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FenceCatalog {
    format: u8,
    #[serde(default)]
    revision: String,
    #[serde(default)]
    topics: BTreeMap<String, i64>,
    #[serde(default)]
    channels: BTreeMap<String, i64>,
    #[serde(default)]
    local_channels: BTreeMap<String, i64>,
}

impl Default for FenceCatalog {
    fn default() -> Self {
        Self {
            format: FENCE_FORMAT,
            revision: String::new(),
            topics: BTreeMap::new(),
            channels: BTreeMap::new(),
            local_channels: BTreeMap::new(),
        }
    }
}

impl FenceCatalog {
    pub(crate) fn load(data_path: &Path) -> io::Result<(PathBuf, Self)> {
        let path = data_path.join("management-fences.json");
        let catalog = load_optional::<Self>(&path)?.unwrap_or_default();
        if catalog.format != FENCE_FORMAT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported management fence format",
            ));
        }
        Ok((path, catalog))
    }

    pub(crate) fn store(&self, path: &Path) -> io::Result<()> {
        store_atomic(path, self)
    }

    pub(crate) fn topic_blocked(&self, topic: &str, now_ms: i64) -> bool {
        self.topics.get(topic).is_some_and(|until| *until > now_ms)
    }

    pub(crate) fn channel_blocked(&self, topic: &str, channel: &str, now_ms: i64) -> bool {
        let key = channel_key(topic, channel);
        self.channels.get(&key).is_some_and(|until| *until > now_ms)
            || self
                .local_channels
                .get(&key)
                .is_some_and(|until| *until > now_ms)
    }

    pub(crate) fn set_topic(&mut self, topic: &str, until_ms: i64) -> bool {
        self.topics.insert(topic.into(), until_ms) != Some(until_ms)
    }

    pub(crate) fn clear_topic(&mut self, topic: &str) -> bool {
        self.topics.remove(topic).is_some()
    }

    pub(crate) fn set_channel(&mut self, topic: &str, channel: &str, until_ms: i64) -> bool {
        self.channels.insert(channel_key(topic, channel), until_ms) != Some(until_ms)
    }

    pub(crate) fn set_local_channel(&mut self, topic: &str, channel: &str, until_ms: i64) -> bool {
        self.local_channels
            .insert(channel_key(topic, channel), until_ms)
            != Some(until_ms)
    }

    pub(crate) fn clear_channel(&mut self, topic: &str, channel: &str) -> bool {
        let key = channel_key(topic, channel);
        let removed_global = self.channels.remove(&key).is_some();
        let removed_local = self.local_channels.remove(&key).is_some();
        removed_global || removed_local
    }

    pub(crate) fn replace(&mut self, snapshot: ManagementFenceSnapshot) {
        let now_ms = unix_millis();
        self.local_channels.retain(|_, until_ms| *until_ms > now_ms);
        self.revision = snapshot.revision;
        self.topics = snapshot.topics;
        self.channels = snapshot
            .channels
            .into_iter()
            .map(|fence| (channel_key(&fence.topic, &fence.channel), fence.until_ms))
            .collect();
    }
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn channel_key(topic: &str, channel: &str) -> String {
    format!("{}:{}:{}", topic.len(), topic, channel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_keys_cannot_collide_on_separators() {
        assert_ne!(channel_key("a:b", "c"), channel_key("a", "b:c"));
    }

    #[test]
    fn expired_fences_do_not_block() {
        let mut catalog = FenceCatalog::default();
        catalog.set_topic("orders", 9);
        assert!(!catalog.topic_blocked("orders", 9));
    }

    #[test]
    fn snapshot_sync_preserves_only_active_kodo_fences() {
        let mut catalog = FenceCatalog::default();
        let active = unix_millis() + 60_000;
        catalog.set_local_channel("events", "kodo", active);
        catalog.set_local_channel("events", "expired", unix_millis() - 1);
        catalog.set_channel("events", "console", active);
        catalog.replace(ManagementFenceSnapshot {
            revision: "console-2".into(),
            topics: BTreeMap::new(),
            channels: Vec::new(),
        });
        assert!(catalog.channel_blocked("events", "kodo", unix_millis()));
        assert!(!catalog.channel_blocked("events", "expired", unix_millis()));
        assert!(!catalog.channel_blocked("events", "console", unix_millis()));
    }

    #[test]
    fn kodo_fences_survive_catalog_reopen() {
        let root = tempfile::tempdir().unwrap();
        let (path, mut catalog) = FenceCatalog::load(root.path()).unwrap();
        catalog.set_local_channel("events", "workers", unix_millis() + 60_000);
        catalog.store(&path).unwrap();

        let (_, reopened) = FenceCatalog::load(root.path()).unwrap();
        assert!(reopened.channel_blocked("events", "workers", unix_millis()));
    }
}
