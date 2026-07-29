use rustqueue_storage::PayloadRef;
use rustqueue_telemetry::HistogramSnapshot;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: u64,
    pub timestamp_ns: i64,
    pub attempts: u16,
    pub body: Arc<[u8]>,
}

pub struct DeliveryBatch {
    deliveries: Vec<Delivery>,
    guard: crate::delivery_guard::DeliveryGuard,
}

impl DeliveryBatch {
    pub(crate) fn new(
        deliveries: Vec<Delivery>,
        guard: crate::delivery_guard::DeliveryGuard,
    ) -> Self {
        Self { deliveries, guard }
    }

    pub fn into_parts(mut self) -> (Vec<Delivery>, crate::delivery_guard::DeliveryGuard) {
        (
            std::mem::take(&mut self.deliveries),
            std::mem::take(&mut self.guard),
        )
    }

    pub(crate) fn into_deliveries(mut self) -> Vec<Delivery> {
        self.guard.accept_all();
        std::mem::take(&mut self.deliveries)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DeliveryBudgetStats {
    pub in_flight_bytes: u64,
    pub waiters: u64,
    pub waits_total: u64,
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BrokerStats {
    pub publish_group_commit: PublishGroupCommitStats,
    #[serde(default)]
    pub channel_group_commit: ChannelGroupCommitStats,
    #[serde(default)]
    pub latency: BrokerLatencyStats,
    #[serde(default)]
    pub delivery_budget: DeliveryBudgetStats,
    #[serde(default)]
    pub aggregate: QueueAggregateStats,
    pub topics: Vec<TopicStats>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QueueAggregateStats {
    pub topic_count: u64,
    pub message_count: u64,
    pub segment_count: u64,
    pub segment_bytes: u64,
    pub channel_count: u64,
    pub channel_depth: u64,
    pub channel_in_flight: u64,
    pub channel_deferred: u64,
    pub channel_ack_gap: u64,
}

impl QueueAggregateStats {
    pub(crate) fn add_topic(&mut self, topic: &TopicStats) {
        self.topic_count = self.topic_count.saturating_add(1);
        self.message_count = self.message_count.saturating_add(topic.message_count);
        self.segment_count = self.segment_count.saturating_add(topic.segment_count);
        self.segment_bytes = self.segment_bytes.saturating_add(topic.segment_bytes);
        for channel in &topic.channels {
            self.channel_count = self.channel_count.saturating_add(1);
            self.channel_depth = self.channel_depth.saturating_add(channel.depth);
            self.channel_in_flight = self
                .channel_in_flight
                .saturating_add(channel.in_flight_count);
            self.channel_deferred = self.channel_deferred.saturating_add(channel.deferred_count);
            self.channel_ack_gap = self.channel_ack_gap.saturating_add(channel.ack_gap);
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BrokerLatencyStats {
    pub fsync: HistogramSnapshot,
    pub group_commit_wait: HistogramSnapshot,
    pub publish_topic_lock_wait: HistogramSnapshot,
    pub publish_topic_lock_hold: HistogramSnapshot,
    pub publish_ack: HistogramSnapshot,
    pub delivery_topic_lock_wait: HistogramSnapshot,
    pub delivery_topic_lock_hold: HistogramSnapshot,
    pub channel_fsync: HistogramSnapshot,
    pub channel_group_commit_wait: HistogramSnapshot,
    pub channel_ack: HistogramSnapshot,
    pub payload_read: HistogramSnapshot,
    pub scrub: HistogramSnapshot,
    pub gc: HistogramSnapshot,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PublishGroupCommitStats {
    pub commits: u64,
    pub requests: u64,
    pub max_batch_requests: u64,
    #[serde(default)]
    pub active_workers: u64,
    #[serde(default)]
    pub retired_workers: u64,
    #[serde(default)]
    pub rejected_workers: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ChannelGroupCommitStats {
    pub commits: u64,
    pub requests: u64,
    pub max_batch_requests: u64,
    pub active_workers: u64,
    pub retired_workers: u64,
    pub rejected_workers: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TopicStats {
    pub name: String,
    pub paused: bool,
    #[serde(default)]
    pub published_count: u64,
    pub message_count: u64,
    #[serde(default)]
    pub segment_count: u64,
    #[serde(default)]
    pub segment_bytes: u64,
    pub channels: Vec<ChannelStats>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChannelStats {
    pub name: String,
    pub depth: u64,
    #[serde(default)]
    pub message_count: u64,
    pub in_flight_count: u64,
    pub deferred_count: u64,
    #[serde(default)]
    pub requeue_count: u64,
    #[serde(default)]
    pub timeout_count: u64,
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
