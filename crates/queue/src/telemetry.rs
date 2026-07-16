use crate::model::BrokerLatencyStats;
use rustqueue_telemetry::LatencyHistogram;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct QueueMetrics {
    pub fsync: Arc<LatencyHistogram>,
    pub group_commit_wait: Arc<LatencyHistogram>,
    pub publish_ack: Arc<LatencyHistogram>,
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
            payload_read: self.payload_read.snapshot(),
            scrub: self.scrub.snapshot(),
            gc: self.gc.snapshot(),
        }
    }
}
