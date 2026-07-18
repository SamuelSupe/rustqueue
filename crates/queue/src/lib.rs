mod batch;
mod broker;
mod channel;
mod channel_store;
mod delivery_budget;
mod delivery_guard;
mod eviction;
mod management;
mod management_ops;
mod metadata;
mod model;
mod outbox;
mod payload_reader;
mod telemetry;
mod topic;

pub use broker::{Broker, BrokerConfig, BrokerError};
pub use delivery_budget::DeliveryHold;
pub use delivery_guard::DeliveryGuard;
pub use eviction::ProtectiveEviction;
pub use management::{
    ChannelFence, ChannelManagementAction, ManagementFenceSnapshot, ManagementResult,
    TopicManagementAction,
};
pub use model::{
    BrokerLatencyStats, BrokerStats, ChannelGroupCommitStats, ChannelStats, Delivery,
    DeliveryBatch, DeliveryBudgetStats, PublishGroupCommitStats, QueueAggregateStats, TopicStats,
};

#[doc(hidden)]
pub fn fuzz_channel_state(checkpoint: &[u8], wal: &[u8]) {
    channel_store::fuzz_channel_state(checkpoint, wal);
}

#[doc(hidden)]
pub fn fuzz_topic_manifest(bytes: &[u8]) {
    metadata::fuzz_topic_manifest(bytes);
}
