use super::Config;
use anyhow::{bail, Context};
use std::{env, fs, path::Path};

impl Config {
    pub(super) fn apply_environment(&mut self) -> anyhow::Result<()> {
        set_from_env("RUSTQUEUE_NODE_ID", &mut self.node.id)?;
        set_string_env(
            "RUSTQUEUE_BROADCAST_ADDRESS",
            &mut self.node.broadcast_address,
        );
        set_from_env("RUSTQUEUE_TCP_ADDRESS", &mut self.network.tcp_address)?;
        set_from_env("RUSTQUEUE_HTTP_ADDRESS", &mut self.network.http_address)?;
        if let Ok(value) = env::var("RUSTQUEUE_DATA_PATH") {
            self.storage.data_path = value.into();
        }
        set_from_env(
            "RUSTQUEUE_DEFAULT_PARTITIONS",
            &mut self.queue.default_partitions,
        )?;
        set_from_env(
            "RUSTQUEUE_MAX_PARTITIONS_PER_TOPIC",
            &mut self.queue.max_partitions_per_topic,
        )?;
        set_from_env(
            "RUSTQUEUE_MAX_MESSAGE_BYTES",
            &mut self.queue.max_message_bytes,
        )?;
        set_from_env("RUSTQUEUE_MAX_BODY_BYTES", &mut self.limits.max_body_bytes)?;
        set_from_env(
            "RUSTQUEUE_NODE_PUBLISH_INFLIGHT_BYTES",
            &mut self.limits.node_publish_inflight_bytes,
        )?;
        set_from_env(
            "RUSTQUEUE_CONNECTION_PUBLISH_INFLIGHT_BYTES",
            &mut self.limits.connection_publish_inflight_bytes,
        )?;
        set_from_env(
            "RUSTQUEUE_PROTECTIVE_EVICTION_ENABLED",
            &mut self.storage.protective_eviction_enabled,
        )?;
        set_from_env(
            "RUSTQUEUE_DISK_PRESSURE_GRACE_SECONDS",
            &mut self.storage.disk_pressure_grace_seconds,
        )?;
        set_from_env(
            "RUSTQUEUE_MESSAGE_RETENTION_SECONDS",
            &mut self.queue.message_retention_seconds,
        )?;
        set_from_env(
            "RUSTQUEUE_MAX_DELIVERY_ATTEMPTS",
            &mut self.queue.max_delivery_attempts,
        )?;
        set_string_env(
            "RUSTQUEUE_DEAD_LETTER_SUFFIX",
            &mut self.queue.dead_letter_suffix,
        );
        set_from_env(
            "RUSTQUEUE_DISCOVERY_ENABLED",
            &mut self.cluster.discovery.enabled,
        )?;
        set_from_env(
            "RUSTQUEUE_FEDERATION_ENABLED",
            &mut self.cluster.federation.enabled,
        )?;
        set_from_env("RUSTQUEUE_CELL_ID", &mut self.cluster.federation.cell_id)?;
        set_string_env("RUSTQUEUE_LOG_FORMAT", &mut self.log_format);
        Ok(())
    }
}

fn set_string_env(name: &str, target: &mut String) {
    if let Ok(value) = env::var(name) {
        *target = value;
    }
}

fn set_from_env<T>(name: &str, target: &mut T) -> anyhow::Result<()>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    if let Ok(value) = env::var(name) {
        *target = value
            .parse()
            .with_context(|| format!("parse environment variable {name}"))?;
    }
    Ok(())
}

pub(super) fn read_optional_secret(path: Option<&Path>) -> anyhow::Result<Option<String>> {
    path.map(|path| {
        let value = fs::read_to_string(path)
            .with_context(|| format!("read secret file {}", path.display()))?;
        let value = value.trim().to_owned();
        if value.is_empty() {
            bail!("secret file {} is empty", path.display());
        }
        Ok(value)
    })
    .transpose()
}
