use super::{Config, MAX_SUPPORTED_BATCH_BYTES};
use crate::admission::{working_set_bytes, PublishShape};
use anyhow::bail;

impl Config {
    pub(super) fn validate_protocol_limits(&self) -> anyhow::Result<()> {
        if self.limits.max_body_bytes == 0 || self.limits.max_body_bytes > MAX_SUPPORTED_BATCH_BYTES
        {
            bail!("limits.max_body_bytes must be in 1..={MAX_SUPPORTED_BATCH_BYTES}");
        }
        if self.limits.connection_publish_inflight_bytes
            < working_set_bytes(self.limits.max_body_bytes, PublishShape::Multi)
            || self.limits.node_publish_inflight_bytes
                < self.limits.connection_publish_inflight_bytes
        {
            bail!("publish inflight limits must fit the encoded working set and satisfy connection <= node");
        }
        if self.limits.connection_delivery_inflight_bytes < self.queue.max_message_bytes
            || self
                .limits
                .connection_delivery_inflight_bytes
                .checked_mul(2)
                .is_none_or(|minimum| self.limits.node_delivery_inflight_bytes < minimum)
            || self.limits.node_delivery_inflight_bytes > u32::MAX as usize
        {
            bail!("delivery inflight limits must fit one message plus the payload-read working set and fit u32");
        }
        if self.limits.client_handshake_timeout_ms == 0
            || self.limits.tcp_command_timeout_ms == 0
            || self.limits.auth_cache_max_entries == 0
            || self.limits.max_connections == 0
            || self.limits.max_rdy_count == 0
            || self.limits.auth_response_bytes == 0
            || self.limits.auth_timeout_ms == 0
            || self.limits.http_body_timeout_ms == 0
        {
            bail!("limits connection, RDY, auth, TCP and HTTP timeouts/sizes must be greater than zero");
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
}
