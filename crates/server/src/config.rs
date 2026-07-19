use anyhow::{bail, Context};
mod environment;
mod validation;

use environment::read_optional_secret;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const MAX_SUPPORTED_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SUPPORTED_BATCH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub queue: QueueConfig,
    pub security: SecurityConfig,
    pub limits: LimitsConfig,
    pub metrics: MetricsConfig,
    pub shutdown: ShutdownConfig,
    pub log_format: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub id: u64,
    pub broadcast_address: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    pub tcp_address: SocketAddr,
    pub http_address: SocketAddr,
    pub advertised_tcp_port: u16,
    pub advertised_http_port: u16,
    pub snappy_enabled: bool,
    pub deflate_enabled: bool,
    pub max_deflate_level: i32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub data_path: PathBuf,
    pub feature_level: u32,
    pub max_segment_bytes: u64,
    pub scrub_interval_seconds: u64,
    pub scrub_bytes_per_second: u64,
    pub entry_cache_bytes: usize,
    pub message_index_cache_bytes: usize,
    pub payload_read_workers: usize,
    pub payload_read_queue: usize,
    pub maintenance_startup_delay_seconds: u64,
    pub disk_high_watermark_percent: u8,
    pub disk_low_watermark_percent: u8,
    pub min_free_bytes: u64,
    pub protective_eviction_enabled: bool,
    pub disk_pressure_grace_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    pub max_message_bytes: usize,
    pub message_timeout_ms: u64,
    pub max_message_timeout_ms: u64,
    pub max_defer_ms: u64,
    pub max_ack_gap: usize,
    pub max_topics: usize,
    pub max_publish_workers: usize,
    pub publish_worker_idle_seconds: u64,
    pub bootstrap_retention_seconds: u64,
    pub message_retention_seconds: u64,
    pub max_delivery_attempts: u16,
    pub dead_letter_suffix: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityConfig {
    pub tls: Option<TlsConfig>,
    pub auth_http_addresses: Vec<String>,
    pub admin_token_file: Option<PathBuf>,
    pub publish_token_file: Option<PathBuf>,
    pub registry_token_file: Option<PathBuf>,
    pub console_token_file: Option<PathBuf>,
    pub console_management_enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub client_ca_file: Option<PathBuf>,
    pub require_client_certificate: bool,
    pub required: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_body_bytes: usize,
    pub node_publish_inflight_bytes: usize,
    pub connection_publish_inflight_bytes: usize,
    pub node_delivery_inflight_bytes: usize,
    pub connection_delivery_inflight_bytes: usize,
    pub max_rdy_count: u64,
    pub max_connections: usize,
    pub client_handshake_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_heartbeat_interval_ms: u64,
    pub output_buffer_size: usize,
    pub max_output_buffer_size: usize,
    pub output_buffer_timeout_ms: u64,
    pub min_output_buffer_timeout_ms: u64,
    pub max_output_buffer_timeout_ms: u64,
    pub auth_response_bytes: usize,
    pub auth_timeout_ms: u64,
    pub auth_max_ttl_seconds: u64,
    pub auth_cache_max_entries: usize,
    pub http_body_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub detailed_queue_metrics: bool,
    pub max_detailed_series: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShutdownConfig {
    pub grace_seconds: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            queue: QueueConfig::default(),
            security: SecurityConfig::default(),
            limits: LimitsConfig::default(),
            metrics: MetricsConfig::default(),
            shutdown: ShutdownConfig::default(),
            log_format: "text".into(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: 1,
            broadcast_address: "127.0.0.1".into(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            tcp_address: "0.0.0.0:4150".parse().unwrap(),
            http_address: "0.0.0.0:4151".parse().unwrap(),
            advertised_tcp_port: 4150,
            advertised_http_port: 4151,
            snappy_enabled: true,
            deflate_enabled: true,
            max_deflate_level: 6,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_path: "data".into(),
            feature_level: rustqueue_storage::BASE_STORAGE_FEATURE_LEVEL,
            max_segment_bytes: 100 * 1024 * 1024,
            scrub_interval_seconds: 3600,
            scrub_bytes_per_second: 64 * 1024 * 1024,
            entry_cache_bytes: 64 * 1024 * 1024,
            message_index_cache_bytes: 64 * 1024 * 1024,
            payload_read_workers: 0,
            payload_read_queue: 4096,
            maintenance_startup_delay_seconds: 30,
            disk_high_watermark_percent: 85,
            disk_low_watermark_percent: 75,
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            protective_eviction_enabled: true,
            disk_pressure_grace_seconds: 60,
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 20 * 1024 * 1024,
            message_timeout_ms: 60_000,
            max_message_timeout_ms: 15 * 60_000,
            max_defer_ms: 60 * 60_000,
            max_ack_gap: 65_536,
            max_topics: 10_000,
            max_publish_workers: 1_024,
            publish_worker_idle_seconds: 60,
            bootstrap_retention_seconds: 90,
            message_retention_seconds: 0,
            max_delivery_attempts: 16,
            dead_letter_suffix: ".DLQ".into(),
        }
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            certificate_file: PathBuf::new(),
            private_key_file: PathBuf::new(),
            client_ca_file: None,
            require_client_certificate: false,
            required: false,
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: MAX_SUPPORTED_BATCH_BYTES,
            node_publish_inflight_bytes: 512 * 1024 * 1024,
            connection_publish_inflight_bytes: 80 * 1024 * 1024,
            node_delivery_inflight_bytes: 512 * 1024 * 1024,
            connection_delivery_inflight_bytes: 32 * 1024 * 1024,
            max_rdy_count: 2500,
            max_connections: 10_000,
            client_handshake_timeout_ms: 5_000,
            heartbeat_interval_ms: 30_000,
            max_heartbeat_interval_ms: 60_000,
            output_buffer_size: 16 * 1024,
            max_output_buffer_size: 64 * 1024,
            output_buffer_timeout_ms: 250,
            min_output_buffer_timeout_ms: 25,
            max_output_buffer_timeout_ms: 30_000,
            auth_response_bytes: 1024 * 1024,
            auth_timeout_ms: 5_000,
            auth_max_ttl_seconds: 3600,
            auth_cache_max_entries: 10_000,
            http_body_timeout_ms: 30_000,
        }
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self { grace_seconds: 30 }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            detailed_queue_metrics: false,
            max_detailed_series: 1_000,
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut config = match path {
            Some(path) => toml::from_str(
                &fs::read_to_string(path)
                    .with_context(|| format!("read config {}", path.display()))?,
            )
            .with_context(|| format!("parse config {}", path.display()))?,
            None => Self::default(),
        };
        config.apply_environment()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.node.id == 0 || self.node.id > u16::MAX as u64 {
            bail!("node.id must be in 1..=65535");
        }
        if self.node.broadcast_address.trim().is_empty() {
            bail!("node.broadcast_address cannot be empty");
        }
        if self.queue.max_message_bytes == 0
            || self.queue.max_message_bytes > MAX_SUPPORTED_MESSAGE_BYTES
            || self.queue.max_message_bytes > self.limits.max_body_bytes
        {
            bail!("queue.max_message_bytes exceeds the stable wire contract");
        }
        if self.queue.bootstrap_retention_seconds == 0 {
            bail!("queue.bootstrap_retention_seconds must be greater than zero");
        }
        if self.queue.message_timeout_ms == 0
            || self.queue.max_message_timeout_ms < 1_000
            || self.queue.message_timeout_ms > self.queue.max_message_timeout_ms
            || self.queue.max_message_timeout_ms > i64::MAX as u64
            || self.queue.max_defer_ms > i64::MAX as u64
        {
            bail!("queue message timeouts must satisfy 0 < default <= max, max >= 1000ms, and fit signed wire fields");
        }
        if self.queue.max_ack_gap == 0
            || self.queue.max_topics == 0
            || self.queue.max_publish_workers == 0
            || self.queue.publish_worker_idle_seconds == 0
        {
            bail!("queue limits must be greater than zero");
        }
        if self.storage.entry_cache_bytes == 0 || self.storage.message_index_cache_bytes == 0 {
            bail!("storage caches must be greater than zero");
        }
        if self.queue.max_delivery_attempts == 0 {
            bail!("queue.max_delivery_attempts must be greater than zero");
        }
        if self.queue.dead_letter_suffix.is_empty()
            || self.queue.dead_letter_suffix.len() > 16
            || !self
                .queue
                .dead_letter_suffix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            bail!("queue.dead_letter_suffix must contain 1..=16 NSQ-safe characters");
        }
        if self.storage.max_segment_bytes < self.limits.max_body_bytes as u64 + 64 {
            bail!("storage.max_segment_bytes must fit one maximum command body");
        }
        if self.storage.feature_level < rustqueue_storage::BASE_STORAGE_FEATURE_LEVEL
            || self.storage.feature_level > rustqueue_storage::MAX_WRITER_FEATURE_LEVEL
        {
            bail!(
                "storage.feature_level must fit this binary's writer range {}..={}",
                rustqueue_storage::BASE_STORAGE_FEATURE_LEVEL,
                rustqueue_storage::MAX_WRITER_FEATURE_LEVEL
            );
        }
        if self.storage.scrub_interval_seconds == 0
            || self.storage.scrub_bytes_per_second == 0
            || self.storage.entry_cache_bytes == 0
            || self.storage.payload_read_queue == 0
        {
            bail!("storage limits must be greater than zero");
        }
        if self.storage.disk_low_watermark_percent >= self.storage.disk_high_watermark_percent
            || self.storage.disk_high_watermark_percent > 100
        {
            bail!("storage disk watermarks must satisfy low < high <= 100");
        }
        if !(1..=9).contains(&self.network.max_deflate_level) {
            bail!("network.max_deflate_level must be in 1..=9");
        }
        if self.shutdown.grace_seconds == 0 {
            bail!("shutdown.grace_seconds must be greater than zero");
        }
        if self.metrics.max_detailed_series == 0 {
            bail!("metrics.max_detailed_series must be greater than zero");
        }
        self.validate_protocol_limits()?;
        if !matches!(self.log_format.as_str(), "text" | "json") {
            bail!("log_format must be text or json");
        }
        if let Some(tls) = &self.security.tls {
            if !tls.certificate_file.is_file() || !tls.private_key_file.is_file() {
                bail!("TLS certificate and private key must be readable files");
            }
            if tls.require_client_certificate
                && tls.client_ca_file.as_ref().is_none_or(|p| !p.is_file())
            {
                bail!("TLS client CA is required when client certificates are mandatory");
            }
        }
        for path in [
            self.security.admin_token_file.as_ref(),
            self.security.publish_token_file.as_ref(),
            self.security.registry_token_file.as_ref(),
            self.security.console_token_file.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !path.is_file() {
                bail!("secret file {} is not readable", path.display());
            }
        }
        if self.security.console_management_enabled && self.security.console_token_file.is_none() {
            bail!("security.console_token_file is required when console management is enabled");
        }
        Ok(())
    }

    pub fn message_timeout(&self) -> Duration {
        Duration::from_millis(self.queue.message_timeout_ms)
    }
    pub fn read_admin_token(&self) -> anyhow::Result<Option<String>> {
        read_optional_secret(self.security.admin_token_file.as_deref())
    }
    pub fn read_publish_token(&self) -> anyhow::Result<Option<String>> {
        read_optional_secret(self.security.publish_token_file.as_deref())
    }
    pub fn read_registry_token(&self) -> anyhow::Result<Option<String>> {
        read_optional_secret(self.security.registry_token_file.as_deref())
    }
    pub fn read_console_token(&self) -> anyhow::Result<Option<String>> {
        read_optional_secret(self.security.console_token_file.as_deref())
    }
}

#[cfg(test)]
mod tests;
