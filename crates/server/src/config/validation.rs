use super::{Config, MAX_SUPPORTED_BATCH_BYTES};
use anyhow::bail;

impl Config {
    pub(super) fn validate_protocol_limits(&self) -> anyhow::Result<()> {
        if self.limits.max_body_bytes == 0 || self.limits.max_body_bytes > MAX_SUPPORTED_BATCH_BYTES
        {
            bail!("limits.max_body_bytes must be in 1..={MAX_SUPPORTED_BATCH_BYTES}");
        }
        if self.limits.connection_publish_inflight_bytes < self.limits.max_body_bytes
            || self.limits.node_publish_inflight_bytes
                < self.limits.connection_publish_inflight_bytes
        {
            bail!("publish inflight limits must satisfy max_body_bytes <= connection <= node");
        }
        if self.limits.client_handshake_timeout_ms == 0 || self.limits.auth_cache_max_entries == 0 {
            bail!("limits handshake timeout and auth cache size must be greater than zero");
        }
        if self.limits.heartbeat_interval_ms < 1_000
            || self.limits.heartbeat_interval_ms > self.limits.max_heartbeat_interval_ms
        {
            bail!("limits heartbeat interval must fit 1000..=max_heartbeat_interval_ms");
        }
        if self.limits.output_buffer_size < 64
            || self.limits.output_buffer_size > self.limits.max_output_buffer_size
        {
            bail!("limits output_buffer_size must fit 64..=max_output_buffer_size");
        }
        if self.limits.min_output_buffer_timeout_ms == 0
            || self.limits.min_output_buffer_timeout_ms > self.limits.output_buffer_timeout_ms
            || self.limits.output_buffer_timeout_ms > self.limits.max_output_buffer_timeout_ms
        {
            bail!("limits output buffer timeout must fit min..=default..=max");
        }
        Ok(())
    }
}
