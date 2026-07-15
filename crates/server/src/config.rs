use anyhow::{bail, Context};
mod discovery;
mod environment;
mod federation;
mod validation;

pub use discovery::DiscoveryConfig;
use environment::read_optional_secret;
use serde::Deserialize;
use std::collections::BTreeMap;
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
    pub cluster: ClusterConfig,
    pub limits: LimitsConfig,
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
    pub internal_address: SocketAddr,
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
    pub max_segment_bytes: u64,
    pub scrub_interval_seconds: u64,
    pub entry_cache_bytes: usize,
    pub payload_read_workers: usize,
    pub payload_read_queue: usize,
    pub dedup_max_entries: usize,
    pub dedup_ttl_seconds: u64,
    pub disk_high_watermark_percent: u8,
    pub disk_low_watermark_percent: u8,
    pub min_free_bytes: u64,
    pub protective_eviction_enabled: bool,
    pub disk_pressure_grace_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    pub default_partitions: u16,
    pub max_partitions_per_topic: u16,
    pub max_message_bytes: usize,
    pub message_timeout_ms: u64,
    pub max_message_timeout_ms: u64,
    pub max_defer_ms: u64,
    pub max_ack_gap: usize,
    pub max_backlog_messages_per_partition: usize,
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
    pub internal_tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub client_ca_file: Option<PathBuf>,
    pub require_client_certificate: bool,
    pub required: bool,
    pub root_ca_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub bootstrap: bool,
    pub name: String,
    pub nodes: BTreeMap<String, ClusterNodeConfig>,
    pub initial_voters: Vec<u64>,
    pub snapshot_max_bytes: usize,
    pub default_replication_factor: u8,
    pub metadata_replication_factor: u8,
    pub federation: FederationConfig,
    pub discovery: DiscoveryConfig,
    pub automation: AutomationConfig,
    pub shutdown: ShutdownConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FederationConfig {
    pub enabled: bool,
    pub cell_id: u64,
    pub root_voters: Vec<u64>,
    pub max_home_cells_per_topic: usize,
    pub route_cache_ms: u64,
    pub retry_after_ms: u64,
    pub cell_min_nodes: usize,
    pub cell_target_nodes: usize,
    pub cell_max_nodes: usize,
    pub routers_per_cell: usize,
    pub catalog_state_split_bytes: u64,
    pub catalog_topic_split_count: usize,
    pub catalog_ops_split_per_second: u64,
    pub catalog_apply_p99_split_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutomationConfig {
    pub enabled: bool,
    pub poll_interval_seconds: u64,
    pub node_stabilization_seconds: u64,
    pub node_down_grace_seconds: u64,
    pub group_cooldown_seconds: u64,
    pub max_concurrent_migrations: usize,
    pub max_migrations_per_node: usize,
    pub auto_replace_metadata: bool,
    pub operation_history_limit: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShutdownConfig {
    pub grace_seconds: u64,
    pub maintenance_default_ttl_seconds: u64,
    pub maintenance_max_ttl_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterNodeConfig {
    pub raft_address: String,
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub tls_server_name: String,
    pub failure_domain: String,
    #[serde(default)]
    pub cell_id: Option<u64>,
    #[serde(default)]
    pub federation_router: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_body_bytes: usize,
    pub node_publish_inflight_bytes: usize,
    pub connection_publish_inflight_bytes: usize,
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            queue: QueueConfig::default(),
            security: SecurityConfig::default(),
            cluster: ClusterConfig::default(),
            limits: LimitsConfig::default(),
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
            internal_address: "0.0.0.0:4250".parse().unwrap(),
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
            data_path: PathBuf::from("data"),
            max_segment_bytes: 100 * 1024 * 1024,
            scrub_interval_seconds: 3600,
            entry_cache_bytes: 64 * 1024 * 1024,
            payload_read_workers: 0,
            payload_read_queue: 4096,
            dedup_max_entries: 1_000_000,
            dedup_ttl_seconds: 600,
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
            default_partitions: 1,
            max_partitions_per_topic: 1024,
            max_message_bytes: 1024 * 1024,
            message_timeout_ms: 60_000,
            max_message_timeout_ms: 15 * 60_000,
            max_defer_ms: 60 * 60_000,
            max_ack_gap: 65_536,
            max_backlog_messages_per_partition: 10_000_000,
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
            root_ca_file: None,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bootstrap: false,
            name: "rustqueue".into(),
            nodes: BTreeMap::new(),
            initial_voters: Vec::new(),
            snapshot_max_bytes: 512 * 1024 * 1024,
            default_replication_factor: 3,
            metadata_replication_factor: 3,
            federation: FederationConfig::default(),
            discovery: DiscoveryConfig::default(),
            automation: AutomationConfig::default(),
            shutdown: ShutdownConfig::default(),
        }
    }
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cell_id: 1,
            root_voters: Vec::new(),
            max_home_cells_per_topic: 128,
            route_cache_ms: 5_000,
            retry_after_ms: 1_000,
            cell_min_nodes: 3,
            cell_target_nodes: 5,
            cell_max_nodes: 9,
            routers_per_cell: 3,
            catalog_state_split_bytes: 256 * 1024 * 1024,
            catalog_topic_split_count: 100_000,
            catalog_ops_split_per_second: 5_000,
            catalog_apply_p99_split_ms: 50,
        }
    }
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_seconds: 15,
            node_stabilization_seconds: 60,
            node_down_grace_seconds: 600,
            group_cooldown_seconds: 600,
            max_concurrent_migrations: 2,
            max_migrations_per_node: 1,
            auto_replace_metadata: true,
            operation_history_limit: 1000,
        }
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_seconds: 30,
            maintenance_default_ttl_seconds: 1800,
            maintenance_max_ttl_seconds: 86_400,
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: MAX_SUPPORTED_BATCH_BYTES,
            node_publish_inflight_bytes: 512 * 1024 * 1024,
            connection_publish_inflight_bytes: MAX_SUPPORTED_BATCH_BYTES,
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
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut config = match path {
            Some(path) => {
                let contents = fs::read_to_string(path)
                    .with_context(|| format!("read config {}", path.display()))?;
                toml::from_str(&contents)
                    .with_context(|| format!("parse config {}", path.display()))?
            }
            None => Self::default(),
        };
        config.apply_environment()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.node.id == 0 {
            bail!("node.id must be greater than zero");
        }
        if self.node.broadcast_address.trim().is_empty() {
            bail!("node.broadcast_address cannot be empty");
        }
        if self.queue.default_partitions == 0 {
            bail!("queue.default_partitions must be greater than zero");
        }
        if self.queue.max_partitions_per_topic == 0
            || self.queue.default_partitions > self.queue.max_partitions_per_topic
        {
            bail!("queue.default_partitions must fit max_partitions_per_topic");
        }
        if self.queue.max_message_bytes == 0
            || self.queue.max_message_bytes > MAX_SUPPORTED_MESSAGE_BYTES
            || self.queue.max_message_bytes > self.limits.max_body_bytes
        {
            bail!(
                "queue.max_message_bytes must be in 1..=min(limits.max_body_bytes, {MAX_SUPPORTED_MESSAGE_BYTES})"
            );
        }
        if self.queue.max_ack_gap == 0 || self.queue.max_ack_gap > 1_048_576 {
            bail!("queue.max_ack_gap must be in 1..=1048576");
        }
        if self.queue.max_backlog_messages_per_partition == 0 {
            bail!("queue.max_backlog_messages_per_partition must be greater than zero");
        }
        if self.queue.max_delivery_attempts == 0 {
            bail!("queue.max_delivery_attempts must be greater than zero");
        }
        if self.queue.message_retention_seconds > 0
            && self.queue.message_retention_seconds.saturating_mul(1_000) <= self.queue.max_defer_ms
        {
            bail!("queue.message_retention_seconds must exceed max_defer_ms when enabled");
        }
        if self.queue.dead_letter_suffix.is_empty()
            || self.queue.dead_letter_suffix.len() > 16
            || !self
                .queue
                .dead_letter_suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            bail!("queue.dead_letter_suffix must be 1..=16 NSQ-safe characters");
        }
        if self.storage.max_segment_bytes < self.limits.max_body_bytes as u64 + 64 {
            bail!("storage.max_segment_bytes must fit one maximum command body");
        }
        if self.storage.scrub_interval_seconds == 0 {
            bail!("storage.scrub_interval_seconds must be greater than zero");
        }
        if self.storage.entry_cache_bytes == 0
            || self.storage.payload_read_queue == 0
            || self.storage.dedup_max_entries == 0
            || self.storage.dedup_ttl_seconds == 0
        {
            bail!("storage cache, read queue, and dedup limits must be greater than zero");
        }
        if self.storage.disk_low_watermark_percent >= self.storage.disk_high_watermark_percent
            || self.storage.disk_high_watermark_percent > 100
        {
            bail!("storage disk watermarks must satisfy low < high <= 100");
        }
        if self.storage.protective_eviction_enabled && self.storage.disk_pressure_grace_seconds == 0
        {
            bail!("storage.disk_pressure_grace_seconds must be greater than zero");
        }
        if !(1..=9).contains(&self.network.max_deflate_level) {
            bail!("network.max_deflate_level must be in 1..=9");
        }
        self.validate_protocol_limits()?;
        if !matches!(self.log_format.as_str(), "text" | "json") {
            bail!("log_format must be text or json");
        }
        if let Some(tls) = &self.security.tls {
            if !tls.certificate_file.is_file() || !tls.private_key_file.is_file() {
                bail!("TLS certificate and private key must be readable files");
            }
            if tls.require_client_certificate && tls.client_ca_file.is_none() {
                bail!("TLS client CA is required when client certificates are mandatory");
            }
        }
        self.cluster.discovery.validate(self.cluster.enabled)?;
        if self.cluster.enabled {
            let automation = &self.cluster.automation;
            if automation.poll_interval_seconds == 0
                || automation.node_stabilization_seconds == 0
                || automation.node_down_grace_seconds == 0
                || automation.group_cooldown_seconds == 0
                || automation.max_concurrent_migrations == 0
                || automation.max_migrations_per_node == 0
                || automation.operation_history_limit == 0
            {
                bail!("cluster.automation limits must be greater than zero");
            }
            let shutdown = &self.cluster.shutdown;
            if shutdown.grace_seconds == 0
                || shutdown.maintenance_default_ttl_seconds == 0
                || shutdown.maintenance_default_ttl_seconds > shutdown.maintenance_max_ttl_seconds
            {
                bail!("cluster.shutdown TTL and grace values are invalid");
            }
            if !self.cluster.nodes.contains_key(&self.node.id.to_string()) {
                bail!("cluster.nodes must contain node.id");
            }
            self.validate_cluster_topology()?;
            let placement_node_count = self.placement_nodes().len();
            if !matches!(self.cluster.default_replication_factor, 3 | 5)
                || self.cluster.default_replication_factor as usize > placement_node_count
            {
                bail!("cluster.default_replication_factor must be 3 or 5 and fit the local Cell");
            }
            if !matches!(self.cluster.metadata_replication_factor, 3 | 5)
                || self.cluster.metadata_replication_factor as usize > placement_node_count
            {
                bail!("cluster.metadata_replication_factor must be 3 or 5 and fit the local Cell");
            }
            if !self.cluster.initial_voters.is_empty() {
                if self.cluster.initial_voters.len()
                    != self.cluster.metadata_replication_factor as usize
                {
                    bail!("cluster.initial_voters must match metadata_replication_factor");
                }
                for node_id in &self.cluster.initial_voters {
                    if !self.placement_nodes().contains_key(&node_id.to_string()) {
                        bail!("initial voter {node_id} is not present in the local Cell");
                    }
                }
            }
            let internal_tls = self
                .security
                .internal_tls
                .as_ref()
                .context("security.internal_tls is required in cluster mode")?;
            if !internal_tls.require_client_certificate {
                bail!("cluster internal TLS must require verified client certificates");
            }
            for path in [
                Some(&internal_tls.certificate_file),
                Some(&internal_tls.private_key_file),
                internal_tls.client_ca_file.as_ref(),
                internal_tls.root_ca_file.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if !path.is_file() {
                    bail!("internal TLS file {} is not readable", path.display());
                }
            }
            for (id, node) in &self.cluster.nodes {
                let parsed_id: u64 = id
                    .parse()
                    .with_context(|| format!("cluster node ID {id} must be an unsigned integer"))?;
                if parsed_id == 0 {
                    bail!("cluster node IDs must be greater than zero");
                }
                if !node.raft_address.starts_with("https://") {
                    bail!("cluster node addresses must use https://");
                }
                if node.broadcast_address.trim().is_empty() {
                    bail!("cluster node broadcast_address cannot be empty");
                }
                if node.tls_server_name.trim().is_empty() || node.failure_domain.trim().is_empty() {
                    bail!("cluster node tls_server_name and failure_domain are required");
                }
            }
        }
        for path in [
            self.security.admin_token_file.as_ref(),
            self.security.publish_token_file.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !path.is_file() {
                bail!("secret file {} is not readable", path.display());
            }
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
}

#[cfg(test)]
mod tests;
