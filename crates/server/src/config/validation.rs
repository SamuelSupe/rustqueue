use super::{Config, MAX_SUPPORTED_BATCH_BYTES, MAX_SUPPORTED_MESSAGE_BYTES};
use crate::admission::{capacity_is_supported, working_set_bytes, PublishShape};
use anyhow::bail;
use std::time::{Duration, Instant};

impl Config {
    pub(super) fn validate_protocol_limits(&self) -> anyhow::Result<()> {
        if self.limits.max_body_bytes == 0 || self.limits.max_body_bytes > MAX_SUPPORTED_BATCH_BYTES
        {
            bail!("limits.max_body_bytes must be in 1..={MAX_SUPPORTED_BATCH_BYTES}");
        }
        let publish_working_set =
            working_set_bytes(self.limits.max_body_bytes, PublishShape::Multi).max(
                working_set_bytes(self.queue.max_message_bytes, PublishShape::Single),
            );
        if !capacity_is_supported(self.limits.connection_publish_inflight_bytes)
            || !capacity_is_supported(self.limits.node_publish_inflight_bytes)
        {
            bail!("publish inflight limits exceed the runtime semaphore or metrics capacity");
        }
        if self.limits.connection_publish_inflight_bytes < publish_working_set
            || self.limits.node_publish_inflight_bytes
                < self.limits.connection_publish_inflight_bytes
        {
            bail!("publish inflight limits must fit the encoded working set and satisfy connection <= node");
        }
        let maximum_record_payload = self.queue.max_message_bytes.saturating_add(24).max(
            self.limits
                .max_body_bytes
                .saturating_add(16 * rustqueue_protocol::MAX_MPUB_MESSAGES),
        );
        if maximum_record_payload > rustqueue_storage::MAX_RECORD_BYTES {
            bail!("configured publish limits can exceed the durable record contract");
        }
        if maximum_record_payload > rustqueue_storage::LEGACY_MAX_RECORD_BYTES
            && self.storage.feature_level < 2
        {
            bail!(
                "publish limits above the v7 legacy record bound require storage.feature_level = 2"
            );
        }
        let retained_message_bound = if self.storage.feature_level >= 2 {
            MAX_SUPPORTED_MESSAGE_BYTES
        } else {
            self.queue.max_message_bytes
        };
        if self.limits.connection_delivery_inflight_bytes < retained_message_bound
            || self
                .limits
                .connection_delivery_inflight_bytes
                .checked_mul(2)
                .is_none_or(|minimum| self.limits.node_delivery_inflight_bytes < minimum)
            || self.limits.node_delivery_inflight_bytes > u32::MAX as usize
        {
            bail!("delivery inflight limits must fit every readable durable message plus the payload-read working set and fit u32");
        }
        if self.limits.client_handshake_timeout_ms == 0
            || self.limits.tcp_command_timeout_ms == 0
            || self.limits.auth_cache_max_entries == 0
            || self.limits.auth_memory_bytes == 0
            || self.limits.max_connections == 0
            || self.limits.max_rdy_count == 0
            || self.limits.auth_response_bytes == 0
            || self.limits.auth_timeout_ms == 0
            || self.limits.auth_max_ttl_seconds == 0
            || self.limits.http_body_timeout_ms == 0
        {
            bail!("limits connection, RDY, auth, TCP and HTTP timeouts/sizes must be greater than zero");
        }
        if !capacity_is_supported(self.limits.auth_memory_bytes)
            || self.limits.auth_response_bytes > self.limits.auth_memory_bytes / 4
        {
            bail!(
                "limits.auth_memory_bytes must fit the runtime semaphore and be at least four times auth_response_bytes"
            );
        }
        if self.limits.heartbeat_interval_ms < 1_000
            || self.limits.heartbeat_interval_ms > self.limits.max_heartbeat_interval_ms
            || self.limits.max_heartbeat_interval_ms > i64::MAX as u64
        {
            bail!("limits heartbeat interval must fit 1000..=max_heartbeat_interval_ms");
        }
        if self.limits.output_buffer_size < 64
            || self.limits.output_buffer_size > self.limits.max_output_buffer_size
            || self.limits.max_output_buffer_size > i64::MAX as usize
        {
            bail!("limits output_buffer_size must fit 64..=max_output_buffer_size");
        }
        if self.limits.min_output_buffer_timeout_ms == 0
            || self.limits.min_output_buffer_timeout_ms > self.limits.output_buffer_timeout_ms
            || self.limits.output_buffer_timeout_ms > self.limits.max_output_buffer_timeout_ms
            || self.limits.max_output_buffer_timeout_ms > i64::MAX as u64
        {
            bail!("limits output buffer timeout must fit min..=default..=max");
        }
        Ok(())
    }

    pub(super) fn validate_runtime_limits(&self) -> anyhow::Result<()> {
        if self.limits.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("limits.max_connections exceeds the runtime semaphore capacity");
        }

        let maximum_visibility = Duration::from_millis(self.queue.max_message_timeout_ms)
            .saturating_add(Duration::from_millis(
                self.limits.max_output_buffer_timeout_ms,
            ));
        let maximum_handoff = Duration::from_millis(self.limits.max_heartbeat_interval_ms)
            .saturating_mul(2)
            .saturating_mul(64)
            .max(maximum_visibility);
        let timers = [
            (
                "storage.maintenance_startup_delay_seconds",
                Duration::from_secs(self.storage.maintenance_startup_delay_seconds),
            ),
            (
                "storage.scrub_interval_seconds",
                Duration::from_secs(self.storage.scrub_interval_seconds),
            ),
            (
                "queue.publish_worker_idle_seconds",
                Duration::from_secs(self.queue.publish_worker_idle_seconds),
            ),
            (
                "limits.client_handshake_timeout_ms",
                Duration::from_millis(self.limits.client_handshake_timeout_ms),
            ),
            (
                "limits.tcp_command_timeout_ms",
                Duration::from_millis(self.limits.tcp_command_timeout_ms),
            ),
            (
                "limits.auth_timeout_ms",
                Duration::from_millis(self.limits.auth_timeout_ms),
            ),
            (
                "limits.auth_max_ttl_seconds",
                Duration::from_secs(self.limits.auth_max_ttl_seconds),
            ),
            (
                "limits.http_body_timeout_ms",
                Duration::from_millis(self.limits.http_body_timeout_ms),
            ),
            (
                "shutdown.grace_seconds",
                Duration::from_secs(self.shutdown.grace_seconds),
            ),
            ("maximum delivery handoff timeout", maximum_handoff),
        ];
        let now = Instant::now();
        for (name, duration) in timers {
            if now.checked_add(duration).is_none() {
                bail!("{name} exceeds the platform timer range");
            }
        }
        Ok(())
    }
}
