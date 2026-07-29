use crate::config::MetricsConfig;
use rustqueue_queue::BrokerStats;
use rustqueue_telemetry::render_prometheus;
use serde::Serialize;
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
    pub disk_total_bytes: AtomicU64,
    pub disk_available_bytes: AtomicU64,
    pub disk_used_percent: AtomicU64,
    pub disk_pressure: AtomicI64,
    pub protective_evictions: AtomicU64,
    pub protective_evicted_messages: AtomicU64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub tcp_connections: i64,
    pub publish_messages: u64,
    pub publish_bytes: u64,
    pub publish_inflight_bytes: i64,
    pub publish_throttled_requests: u64,
    pub publish_throttled_bytes: u64,
    pub delivered_messages: u64,
    pub fetch_requests: u64,
    pub fetch_empty: u64,
    pub fetch_batches: u64,
    pub fetch_messages: u64,
    pub fetch_bytes: u64,
    pub finished_messages: u64,
    pub requeued_messages: u64,
    pub dead_letter_messages: u64,
    pub retention_expired_messages: u64,
    pub protocol_errors: u64,
    pub auth_failures: u64,
    pub storage_errors: u64,
    pub disk_total_bytes: u64,
    pub disk_available_bytes: u64,
    pub disk_used_percent: u64,
    pub disk_pressure: i64,
    pub protective_evictions: u64,
    pub protective_evicted_messages: u64,
}

pub fn render_broker(stats: &BrokerStats, config: &MetricsConfig) -> String {
    let topic_count = stats.aggregate.topic_count;
    let message_count = stats.aggregate.message_count;
    let channel_count = stats.aggregate.channel_count;
    let channel_depth = stats.aggregate.channel_depth;
    let channel_in_flight = stats.aggregate.channel_in_flight;
    let channel_deferred = stats.aggregate.channel_deferred;
    let channel_ack_gap = stats.aggregate.channel_ack_gap;
    let sync_lag_seconds = stats.aggregate.sync_lag_ms as f64 / 1_000.0;
    let mut output = format!(
        "# TYPE rustqueue_publish_group_commits_total counter\n\
         rustqueue_publish_group_commits_total {}\n\
         # TYPE rustqueue_publish_group_requests_total counter\n\
         rustqueue_publish_group_requests_total {}\n\
         # TYPE rustqueue_publish_group_max_requests gauge\n\
         rustqueue_publish_group_max_requests {}\n\
         # TYPE rustqueue_publish_workers gauge\n\
         rustqueue_publish_workers {}\n\
         # TYPE rustqueue_publish_workers_retired_total counter\n\
         rustqueue_publish_workers_retired_total {}\n\
         # TYPE rustqueue_publish_workers_rejected_total counter\n\
         rustqueue_publish_workers_rejected_total {}\n\
         # TYPE rustqueue_channel_group_commits_total counter\n\
         rustqueue_channel_group_commits_total {}\n\
         # TYPE rustqueue_channel_group_requests_total counter\n\
         rustqueue_channel_group_requests_total {}\n\
         # TYPE rustqueue_channel_group_max_requests gauge\n\
         rustqueue_channel_group_max_requests {}\n\
         # TYPE rustqueue_channel_commit_workers gauge\n\
         rustqueue_channel_commit_workers {}\n\
         # TYPE rustqueue_channel_commit_workers_retired_total counter\n\
         rustqueue_channel_commit_workers_retired_total {}\n\
         # TYPE rustqueue_channel_commit_workers_rejected_total counter\n\
         rustqueue_channel_commit_workers_rejected_total {}\n\
         # TYPE rustqueue_topics gauge\n\
         rustqueue_topics {topic_count}\n\
         # TYPE rustqueue_topic_messages_total gauge\n\
         rustqueue_topic_messages_total {message_count}\n\
         # TYPE rustqueue_publish_unsynced_messages gauge\n\
         rustqueue_publish_unsynced_messages {}\n\
         # TYPE rustqueue_publish_unsynced_bytes gauge\n\
         rustqueue_publish_unsynced_bytes {}\n\
         # TYPE rustqueue_publish_sync_lag_seconds gauge\n\
         rustqueue_publish_sync_lag_seconds {sync_lag_seconds}\n\
         # TYPE rustqueue_channels gauge\n\
         rustqueue_channels {channel_count}\n\
         # TYPE rustqueue_channel_depth_total gauge\n\
         rustqueue_channel_depth_total {channel_depth}\n\
         # TYPE rustqueue_channel_in_flight_total gauge\n\
         rustqueue_channel_in_flight_total {channel_in_flight}\n\
         # TYPE rustqueue_channel_deferred_total gauge\n\
         rustqueue_channel_deferred_total {channel_deferred}\n\
         # TYPE rustqueue_channel_ack_gap_total gauge\n\
         rustqueue_channel_ack_gap_total {channel_ack_gap}\n\
         # TYPE rustqueue_delivery_inflight_bytes gauge\n\
         rustqueue_delivery_inflight_bytes {}\n\
         # TYPE rustqueue_delivery_budget_waiters gauge\n\
         rustqueue_delivery_budget_waiters {}\n\
         # TYPE rustqueue_delivery_budget_waits_total counter\n\
         rustqueue_delivery_budget_waits_total {}\n",
        stats.publish_group_commit.commits,
        stats.publish_group_commit.requests,
        stats.publish_group_commit.max_batch_requests,
        stats.publish_group_commit.active_workers,
        stats.publish_group_commit.retired_workers,
        stats.publish_group_commit.rejected_workers,
        stats.channel_group_commit.commits,
        stats.channel_group_commit.requests,
        stats.channel_group_commit.max_batch_requests,
        stats.channel_group_commit.active_workers,
        stats.channel_group_commit.retired_workers,
        stats.channel_group_commit.rejected_workers,
        stats.aggregate.unsynced_messages,
        stats.aggregate.unsynced_bytes,
        stats.delivery_budget.in_flight_bytes,
        stats.delivery_budget.waiters,
        stats.delivery_budget.waits_total,
    );
    render_detailed_queue_metrics(&mut output, stats, config);
    for (name, help, snapshot) in [
        (
            "rustqueue_storage_fsync_duration_seconds",
            "Time spent making a publish group durable.",
            &stats.latency.fsync,
        ),
        (
            "rustqueue_group_commit_wait_duration_seconds",
            "Time a publish request waits before group commit processing.",
            &stats.latency.group_commit_wait,
        ),
        (
            "rustqueue_publish_topic_lock_wait_duration_seconds",
            "Time a publish group waits to acquire its Topic state lock.",
            &stats.latency.publish_topic_lock_wait,
        ),
        (
            "rustqueue_publish_topic_lock_hold_duration_seconds",
            "Time a publish group holds its Topic state lock.",
            &stats.latency.publish_topic_lock_hold,
        ),
        (
            "rustqueue_publish_ack_duration_seconds",
            "End-to-end broker publish acknowledgement latency.",
            &stats.latency.publish_ack,
        ),
        (
            "rustqueue_delivery_topic_lock_wait_duration_seconds",
            "Time a delivery reservation waits to acquire its Topic state lock.",
            &stats.latency.delivery_topic_lock_wait,
        ),
        (
            "rustqueue_delivery_topic_lock_hold_duration_seconds",
            "Time a delivery reservation holds its Topic state lock.",
            &stats.latency.delivery_topic_lock_hold,
        ),
        (
            "rustqueue_channel_fsync_duration_seconds",
            "Time spent making a FIN or REQ group durable.",
            &stats.latency.channel_fsync,
        ),
        (
            "rustqueue_channel_group_commit_wait_duration_seconds",
            "Time a FIN or REQ waits before group commit processing.",
            &stats.latency.channel_group_commit_wait,
        ),
        (
            "rustqueue_channel_ack_duration_seconds",
            "End-to-end broker FIN or REQ acknowledgement latency.",
            &stats.latency.channel_ack,
        ),
        (
            "rustqueue_payload_read_duration_seconds",
            "Payload cache and disk read latency.",
            &stats.latency.payload_read,
        ),
        (
            "rustqueue_storage_scrub_duration_seconds",
            "Storage scrub operation latency.",
            &stats.latency.scrub,
        ),
        (
            "rustqueue_storage_gc_duration_seconds",
            "Segment compaction and protective GC latency.",
            &stats.latency.gc,
        ),
    ] {
        output.push_str(&render_prometheus(name, help, snapshot));
    }
    output
}

fn render_detailed_queue_metrics(output: &mut String, stats: &BrokerStats, config: &MetricsConfig) {
    let desired = usize::try_from(stats.aggregate.topic_count)
        .unwrap_or(usize::MAX)
        .saturating_mul(5)
        .saturating_add(
            usize::try_from(stats.aggregate.channel_count)
                .unwrap_or(usize::MAX)
                .saturating_mul(4),
        );
    let mut emitted = 0usize;
    if config.detailed_queue_metrics {
        output.push_str(
            "# TYPE rustqueue_topic_messages gauge\n\
             # TYPE rustqueue_topic_last_durable_position gauge\n\
             # TYPE rustqueue_topic_publish_unsynced_messages gauge\n\
             # TYPE rustqueue_topic_publish_unsynced_bytes gauge\n\
             # TYPE rustqueue_topic_publish_sync_lag_seconds gauge\n\
             # TYPE rustqueue_channel_depth gauge\n\
             # TYPE rustqueue_channel_in_flight gauge\n\
             # TYPE rustqueue_channel_deferred gauge\n\
             # TYPE rustqueue_channel_ack_gap gauge\n",
        );
        for topic in &stats.topics {
            if emitted.saturating_add(5) <= config.max_detailed_series {
                let topic_label = format!("topic=\"{}\"", escape_label(&topic.name));
                output.push_str(&format!(
                    "rustqueue_topic_messages{{{topic_label}}} {}\n\
                     rustqueue_topic_last_durable_position{{{topic_label}}} {}\n\
                     rustqueue_topic_publish_unsynced_messages{{{topic_label}}} {}\n\
                     rustqueue_topic_publish_unsynced_bytes{{{topic_label}}} {}\n\
                     rustqueue_topic_publish_sync_lag_seconds{{{topic_label}}} {}\n",
                    topic.message_count,
                    topic.last_durable_position,
                    topic.unsynced_messages,
                    topic.unsynced_bytes,
                    topic.sync_lag_ms as f64 / 1_000.0,
                ));
                emitted += 5;
            }
            for channel in &topic.channels {
                if emitted.saturating_add(4) > config.max_detailed_series {
                    continue;
                }
                let labels = format!(
                    "topic=\"{}\",channel=\"{}\"",
                    escape_label(&topic.name),
                    escape_label(&channel.name)
                );
                output.push_str(&format!(
                    "rustqueue_channel_depth{{{labels}}} {}\n\
                     rustqueue_channel_in_flight{{{labels}}} {}\n\
                     rustqueue_channel_deferred{{{labels}}} {}\n\
                     rustqueue_channel_ack_gap{{{labels}}} {}\n",
                    channel.depth, channel.in_flight_count, channel.deferred_count, channel.ack_gap,
                ));
                emitted += 4;
            }
        }
    }
    output.push_str(&format!(
        "# TYPE rustqueue_detailed_queue_metric_series gauge\n\
         rustqueue_detailed_queue_metric_series {emitted}\n\
         # TYPE rustqueue_detailed_queue_metric_series_omitted gauge\n\
         rustqueue_detailed_queue_metric_series_omitted {}\n",
        desired.saturating_sub(emitted),
    ));
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

impl Metrics {
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            tcp_connections: self.tcp_connections.load(Ordering::Relaxed),
            publish_messages: self.publish_messages.load(Ordering::Relaxed),
            publish_bytes: self.publish_bytes.load(Ordering::Relaxed),
            publish_inflight_bytes: self.publish_inflight_bytes.load(Ordering::Relaxed),
            publish_throttled_requests: self.publish_throttled_requests.load(Ordering::Relaxed),
            publish_throttled_bytes: self.publish_throttled_bytes.load(Ordering::Relaxed),
            delivered_messages: self.delivered_messages.load(Ordering::Relaxed),
            fetch_requests: self.fetch_requests.load(Ordering::Relaxed),
            fetch_empty: self.fetch_empty.load(Ordering::Relaxed),
            fetch_batches: self.fetch_batches.load(Ordering::Relaxed),
            fetch_messages: self.fetch_messages.load(Ordering::Relaxed),
            fetch_bytes: self.fetch_bytes.load(Ordering::Relaxed),
            finished_messages: self.finished_messages.load(Ordering::Relaxed),
            requeued_messages: self.requeued_messages.load(Ordering::Relaxed),
            dead_letter_messages: self.dead_letter_messages.load(Ordering::Relaxed),
            retention_expired_messages: self.retention_expired_messages.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            storage_errors: self.storage_errors.load(Ordering::Relaxed),
            disk_total_bytes: self.disk_total_bytes.load(Ordering::Relaxed),
            disk_available_bytes: self.disk_available_bytes.load(Ordering::Relaxed),
            disk_used_percent: self.disk_used_percent.load(Ordering::Relaxed),
            disk_pressure: self.disk_pressure.load(Ordering::Relaxed),
            protective_evictions: self.protective_evictions.load(Ordering::Relaxed),
            protective_evicted_messages: self.protective_evicted_messages.load(Ordering::Relaxed),
        }
    }

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
                "rustqueue_storage_errors_total {}\n",
                "# TYPE rustqueue_disk_total_bytes gauge\n",
                "rustqueue_disk_total_bytes {}\n",
                "# TYPE rustqueue_disk_available_bytes gauge\n",
                "rustqueue_disk_available_bytes {}\n",
                "# TYPE rustqueue_disk_used_percent gauge\n",
                "rustqueue_disk_used_percent {}\n",
                "# TYPE rustqueue_disk_pressure gauge\n",
                "rustqueue_disk_pressure {}\n",
                "# TYPE rustqueue_protective_evictions_total counter\n",
                "rustqueue_protective_evictions_total {}\n",
                "# TYPE rustqueue_protective_evicted_messages_total counter\n",
                "rustqueue_protective_evicted_messages_total {}\n"
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
            self.disk_total_bytes.load(Ordering::Relaxed),
            self.disk_available_bytes.load(Ordering::Relaxed),
            self.disk_used_percent.load(Ordering::Relaxed),
            self.disk_pressure.load(Ordering::Relaxed),
            self.protective_evictions.load(Ordering::Relaxed),
            self.protective_evicted_messages.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_queue::{ChannelStats, QueueAggregateStats, TopicStats};

    fn broker_stats() -> BrokerStats {
        BrokerStats {
            aggregate: QueueAggregateStats {
                topic_count: 1,
                message_count: 7,
                segment_count: 1,
                segment_bytes: 100,
                unsynced_messages: 2,
                unsynced_bytes: 32,
                sync_lag_ms: 7,
                channel_count: 1,
                channel_depth: 3,
                channel_in_flight: 2,
                channel_deferred: 1,
                channel_ack_gap: 2,
            },
            topics: vec![TopicStats {
                name: "events".into(),
                paused: false,
                published_count: 7,
                message_count: 7,
                segment_count: 1,
                segment_bytes: 100,
                last_durable_position: 5,
                unsynced_messages: 2,
                unsynced_bytes: 32,
                sync_lag_ms: 7,
                channels: vec![ChannelStats {
                    name: "workers".into(),
                    depth: 3,
                    message_count: 7,
                    in_flight_count: 2,
                    deferred_count: 1,
                    requeue_count: 0,
                    timeout_count: 0,
                    paused: false,
                    ephemeral: false,
                    ack_cursor: 4,
                    ack_gap: 2,
                }],
            }],
            ..BrokerStats::default()
        }
    }

    #[test]
    fn queue_metrics_are_aggregate_only_by_default() {
        let output = render_broker(&broker_stats(), &MetricsConfig::default());
        assert!(output.contains("rustqueue_topic_messages_total 7\n"));
        assert!(output.contains("rustqueue_channel_depth_total 3\n"));
        assert!(output.contains("rustqueue_publish_topic_lock_wait_duration_seconds_count 0\n"));
        assert!(output.contains("rustqueue_delivery_topic_lock_hold_duration_seconds_count 0\n"));
        assert!(output.contains("rustqueue_publish_unsynced_messages 2\n"));
        assert!(output.contains("rustqueue_publish_unsynced_bytes 32\n"));
        assert!(output.contains("rustqueue_publish_sync_lag_seconds 0.007\n"));
        assert!(!output.contains("topic=\"events\""));
        assert!(output.contains("rustqueue_detailed_queue_metric_series 0\n"));
        assert!(output.contains("rustqueue_detailed_queue_metric_series_omitted 9\n"));
    }

    #[test]
    fn detailed_queue_metrics_honor_the_global_series_budget() {
        let output = render_broker(
            &broker_stats(),
            &MetricsConfig {
                detailed_queue_metrics: true,
                max_detailed_series: 5,
            },
        );
        assert!(output.contains("rustqueue_topic_messages{topic=\"events\"} 7\n"));
        assert!(output.contains("rustqueue_topic_last_durable_position{topic=\"events\"} 5\n"));
        assert!(!output.contains("rustqueue_channel_depth{topic="));
        assert!(output.contains("rustqueue_detailed_queue_metric_series 5\n"));
        assert!(output.contains("rustqueue_detailed_queue_metric_series_omitted 4\n"));
    }

    #[test]
    fn prometheus_label_values_are_escaped() {
        assert_eq!(escape_label("a\\b\n\"c"), "a\\\\b\\n\\\"c");
    }
}
