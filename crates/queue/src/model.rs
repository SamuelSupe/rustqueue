use rustqueue_storage::PayloadRef;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: u64,
    pub timestamp_ns: i64,
    pub attempts: u16,
    pub body: Arc<[u8]>,
}

#[derive(Clone, Debug)]
pub(crate) struct MessageMeta {
    pub position: u64,
    pub id: u64,
    pub timestamp_ns: i64,
    pub available_at_ms: i64,
    pub log_index: u64,
    pub payload: PayloadRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BrokerStats {
    pub publish_group_commit: PublishGroupCommitStats,
    pub topics: Vec<TopicStats>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PublishGroupCommitStats {
    pub commits: u64,
    pub requests: u64,
    pub max_batch_requests: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TopicStats {
    pub name: String,
    pub paused: bool,
    pub message_count: u64,
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

pub(crate) struct ReservedDelivery {
    pub position: u64,
    pub id: u64,
    pub timestamp_ns: i64,
    pub attempts: u16,
    pub token: u64,
    pub payload: PayloadRef,
}
