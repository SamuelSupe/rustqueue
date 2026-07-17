use crate::model::BrokerLatencyStats;
use rustqueue_telemetry::LatencyHistogram;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct QueueMetrics {
    pub fsync: Arc<LatencyHistogram>,
    pub group_commit_wait: Arc<LatencyHistogram>,
    pub publish_ack: Arc<LatencyHistogram>,
    pub channel_fsync: Arc<LatencyHistogram>,
    pub channel_group_commit_wait: Arc<LatencyHistogram>,
    pub channel_ack: Arc<LatencyHistogram>,
    pub payload_read: Arc<LatencyHistogram>,
    pub scrub: Arc<LatencyHistogram>,
    pub gc: Arc<LatencyHistogram>,
}

impl QueueMetrics {
    pub fn snapshot(&self) -> BrokerLatencyStats {
        BrokerLatencyStats {
            fsync: self.fsync.snapshot(),
            group_commit_wait: self.group_commit_wait.snapshot(),
            publish_ack: self.publish_ack.snapshot(),
            channel_fsync: self.channel_fsync.snapshot(),
            channel_group_commit_wait: self.channel_group_commit_wait.snapshot(),
            channel_ack: self.channel_ack.snapshot(),
            payload_read: self.payload_read.snapshot(),
            scrub: self.scrub.snapshot(),
            gc: self.gc.snapshot(),
        }
    }
}
