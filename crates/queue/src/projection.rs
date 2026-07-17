use rustqueue_storage::PayloadRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartitionProjection {
    pub topic: String,
    pub partition: u16,
    pub slot: u16,
    pub cell_id: u64,
    pub group_id: u64,
    pub wire_incarnation: u32,
    pub base_sequence: u64,
    pub next_sequence: u64,
    pub messages: Vec<ProjectedMessage>,
    pub channels: BTreeMap<String, ProjectedChannel>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProjectedMessage {
    pub id: u64,
    pub timestamp_ns: i64,
    pub available_at_ms: i64,
    pub log_index: u64,
    pub batch_ordinal: u32,
    pub payload: PayloadRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProjectedChannel {
    pub barrier: usize,
    pub ack_floor: usize,
    pub acknowledged: BTreeSet<u64>,
    pub requeued_until: BTreeMap<u64, i64>,
    pub paused: bool,
    pub ephemeral: bool,
}
