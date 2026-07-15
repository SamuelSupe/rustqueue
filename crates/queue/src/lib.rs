//! Durable topic/channel state machine.

mod batch;
mod broker;
mod catalog;
mod dedup;
mod model;
mod payload_reader;
mod projection;

pub use broker::{Broker, BrokerConfig, BrokerError, PartitionLayout, ProtectiveEvictionCandidate};
pub use model::{BrokerStats, ChannelStats, Delivery, PartitionStats, TopicStats};
pub use projection::{PartitionProjection, ProjectedChannel, ProjectedMessage};
