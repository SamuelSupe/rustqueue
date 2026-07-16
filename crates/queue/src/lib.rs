mod batch;
mod broker;
mod channel;
mod channel_store;
mod eviction;
mod metadata;
mod model;
mod outbox;
mod payload_reader;
mod telemetry;
mod topic;

pub use broker::{Broker, BrokerConfig, BrokerError};
pub use eviction::ProtectiveEviction;
pub use model::{
    BrokerLatencyStats, BrokerStats, ChannelStats, Delivery, PublishGroupCommitStats, TopicStats,
};

#[doc(hidden)]
pub fn fuzz_channel_state(checkpoint: &[u8], wal: &[u8]) {
    channel_store::fuzz_channel_state(checkpoint, wal);
}

#[doc(hidden)]
pub fn fuzz_topic_manifest(bytes: &[u8]) {
    metadata::fuzz_topic_manifest(bytes);
}
