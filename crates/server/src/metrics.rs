use rustqueue_queue::BrokerStats;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub tcp_connections: AtomicI64,
    pub publish_messages: AtomicU64,
    pub publish_bytes: AtomicU64,
    pub publish_inflight_bytes: AtomicI64,
    pub publish_throttled_requests: AtomicU64,
    pub publish_throttled_bytes: AtomicU64,
    pub delivered_messages: AtomicU64,
    pub fetch_requests: AtomicU64,
    pub fetch_empty: AtomicU64,
    pub fetch_batches: AtomicU64,
    pub fetch_messages: AtomicU64,
    pub fetch_bytes: AtomicU64,
    pub finished_messages: AtomicU64,
    pub requeued_messages: AtomicU64,
    pub dead_letter_messages: AtomicU64,
    pub retention_expired_messages: AtomicU64,
    pub protocol_errors: AtomicU64,
    pub auth_failures: AtomicU64,
    pub storage_errors: AtomicU64,
}

pub fn render_broker(stats: &BrokerStats) -> String {
    let mut output = String::from(
        "# TYPE rustqueue_partition_messages gauge\n\
         # TYPE rustqueue_channel_depth gauge\n\
         # TYPE rustqueue_channel_in_flight gauge\n\
         # TYPE rustqueue_channel_deferred gauge\n\
         # TYPE rustqueue_channel_ack_gap gauge\n",
    );
    for topic in &stats.topics {
        for partition in &topic.partitions {
            let partition_labels = format!(
                "topic=\"{}\",partition=\"{}\",slot=\"{}\"",
                topic.name, partition.partition, partition.slot
            );
            output.push_str(&format!(
                "rustqueue_partition_messages{{{partition_labels}}} {}\n",
                partition.message_count
            ));
            for channel in &partition.channels {
                let labels = format!("{partition_labels},channel=\"{}\"", channel.name);
                output.push_str(&format!(
                    "rustqueue_channel_depth{{{labels}}} {}\n\
                     rustqueue_channel_in_flight{{{labels}}} {}\n\
                     rustqueue_channel_deferred{{{labels}}} {}\n\
                     rustqueue_channel_ack_gap{{{labels}}} {}\n",
                    channel.depth, channel.in_flight_count, channel.deferred_count, channel.ack_gap,
                ));
            }
        }
    }
    output
}

impl Metrics {
    pub fn render(&self) -> String {
        format!(
            concat!(
                "# TYPE rustqueue_tcp_connections gauge\n",
                "rustqueue_tcp_connections {}\n",
                "# TYPE rustqueue_publish_messages_total counter\n",
                "rustqueue_publish_messages_total {}\n",
                "# TYPE rustqueue_publish_bytes_total counter\n",
                "rustqueue_publish_bytes_total {}\n",
                "# TYPE rustqueue_publish_inflight_bytes gauge\n",
                "rustqueue_publish_inflight_bytes {}\n",
                "# TYPE rustqueue_publish_throttled_requests_total counter\n",
                "rustqueue_publish_throttled_requests_total {}\n",
                "# TYPE rustqueue_publish_throttled_bytes_total counter\n",
                "rustqueue_publish_throttled_bytes_total {}\n",
                "# TYPE rustqueue_delivered_messages_total counter\n",
                "rustqueue_delivered_messages_total {}\n",
                "# TYPE rustqueue_consumer_fetch_requests_total counter\n",
                "rustqueue_consumer_fetch_requests_total {}\n",
                "# TYPE rustqueue_consumer_fetch_empty_total counter\n",
                "rustqueue_consumer_fetch_empty_total {}\n",
                "# TYPE rustqueue_consumer_fetch_batches_total counter\n",
                "rustqueue_consumer_fetch_batches_total {}\n",
                "# TYPE rustqueue_consumer_fetch_messages_total counter\n",
                "rustqueue_consumer_fetch_messages_total {}\n",
                "# TYPE rustqueue_consumer_fetch_bytes_total counter\n",
                "rustqueue_consumer_fetch_bytes_total {}\n",
                "# TYPE rustqueue_finished_messages_total counter\n",
                "rustqueue_finished_messages_total {}\n",
                "# TYPE rustqueue_requeued_messages_total counter\n",
                "rustqueue_requeued_messages_total {}\n",
                "# TYPE rustqueue_dead_letter_messages_total counter\n",
                "rustqueue_dead_letter_messages_total {}\n",
                "# TYPE rustqueue_retention_expired_messages_total counter\n",
                "rustqueue_retention_expired_messages_total {}\n",
                "# TYPE rustqueue_protocol_errors_total counter\n",
                "rustqueue_protocol_errors_total {}\n",
                "# TYPE rustqueue_auth_failures_total counter\n",
                "rustqueue_auth_failures_total {}\n",
                "# TYPE rustqueue_storage_errors_total counter\n",
                "rustqueue_storage_errors_total {}\n"
            ),
            self.tcp_connections.load(Ordering::Relaxed),
            self.publish_messages.load(Ordering::Relaxed),
            self.publish_bytes.load(Ordering::Relaxed),
            self.publish_inflight_bytes.load(Ordering::Relaxed),
            self.publish_throttled_requests.load(Ordering::Relaxed),
            self.publish_throttled_bytes.load(Ordering::Relaxed),
            self.delivered_messages.load(Ordering::Relaxed),
            self.fetch_requests.load(Ordering::Relaxed),
            self.fetch_empty.load(Ordering::Relaxed),
            self.fetch_batches.load(Ordering::Relaxed),
            self.fetch_messages.load(Ordering::Relaxed),
            self.fetch_bytes.load(Ordering::Relaxed),
            self.finished_messages.load(Ordering::Relaxed),
            self.requeued_messages.load(Ordering::Relaxed),
            self.dead_letter_messages.load(Ordering::Relaxed),
            self.retention_expired_messages.load(Ordering::Relaxed),
            self.protocol_errors.load(Ordering::Relaxed),
            self.auth_failures.load(Ordering::Relaxed),
            self.storage_errors.load(Ordering::Relaxed),
        )
    }
}
