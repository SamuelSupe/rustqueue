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
}

impl Default for FenceCatalog {
    fn default() -> Self {
        Self {
            format: FENCE_FORMAT,
            revision: String::new(),
            topics: BTreeMap::new(),
            channels: BTreeMap::new(),
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
        self.channels
            .get(&channel_key(topic, channel))
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

    pub(crate) fn clear_channel(&mut self, topic: &str, channel: &str) -> bool {
        self.channels.remove(&channel_key(topic, channel)).is_some()
    }

    pub(crate) fn replace(&mut self, snapshot: ManagementFenceSnapshot) {
        self.revision = snapshot.revision;
        self.topics = snapshot.topics;
        self.channels = snapshot
            .channels
            .into_iter()
            .map(|fence| (channel_key(&fence.topic, &fence.channel), fence.until_ms))
            .collect();
    }
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
}
