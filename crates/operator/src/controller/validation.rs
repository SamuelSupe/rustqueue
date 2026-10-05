use super::{storage, KODO_BOOTSTRAP_RETENTION_SECONDS};
use crate::RustQueue;
use anyhow::{bail, Context as _};
use std::sync::Arc;

pub(super) fn with_storage_feature_floor(
    cluster: Arc<RustQueue>,
    active_feature_floor: u32,
) -> Arc<RustQueue> {
    let effective =
        effective_storage_feature_level(cluster.spec.storage_feature_level, active_feature_floor);
    let (connection_delivery, node_delivery) = effective_delivery_limits(
        effective,
        cluster.spec.max_message_bytes,
        cluster.spec.connection_delivery_inflight_bytes,
        cluster.spec.node_delivery_inflight_bytes,
    );
    if effective == cluster.spec.storage_feature_level
        && connection_delivery == cluster.spec.connection_delivery_inflight_bytes
        && node_delivery == cluster.spec.node_delivery_inflight_bytes
    {
        return cluster;
    }
    let mut adjusted = cluster.as_ref().clone();
    adjusted.spec.storage_feature_level = effective;
    adjusted.spec.connection_delivery_inflight_bytes = connection_delivery;
    adjusted.spec.node_delivery_inflight_bytes = node_delivery;
    Arc::new(adjusted)
}

fn effective_storage_feature_level(requested: u32, active: u32) -> u32 {
    requested.max(active).max(1)
}

fn effective_delivery_limits(
    storage_feature_level: u32,
    max_message_bytes: usize,
    connection: usize,
    node: usize,
) -> (usize, usize) {
    let retained_message_bound = if storage_feature_level >= 2 {
        100 * 1024 * 1024
    } else {
        max_message_bytes
    };
    let connection = connection.max(retained_message_bound);
    (connection, node.max(connection.saturating_mul(2)))
}

pub(super) fn validate(cluster: &RustQueue, active_feature_floor: u32) -> anyhow::Result<()> {
    if cluster.spec.image.trim().is_empty() {
        bail!("spec.image is required");
    }
    if cluster.spec.min_brokers < 1 || cluster.spec.max_brokers < cluster.spec.min_brokers {
        bail!("broker limits must satisfy 1 <= minBrokers <= maxBrokers");
    }
    if cluster.spec.storage_class_name.trim().is_empty()
        || cluster.spec.storage_size.trim().is_empty()
    {
        bail!("storageClassName and storageSize are required");
    }
    if cluster.spec.storage_feature_level == 0 {
        bail!("storageFeatureLevel must be greater than zero");
    }
    if cluster.spec.disk_low_watermark_percent >= cluster.spec.disk_high_watermark_percent
        || cluster.spec.disk_high_watermark_percent > 100
    {
        bail!("disk watermarks must satisfy low < high <= 100");
    }
    if cluster.spec.bootstrap_retention_seconds == 0
        || cluster.spec.max_message_bytes == 0
        || cluster.spec.max_message_bytes > 100 * 1024 * 1024
        || cluster.spec.message_index_cache_bytes == 0
        || cluster.spec.connection_delivery_inflight_bytes < cluster.spec.max_message_bytes
        || cluster
            .spec
            .connection_delivery_inflight_bytes
            .checked_mul(2)
            .is_none_or(|minimum| cluster.spec.node_delivery_inflight_bytes < minimum)
        || cluster.spec.node_delivery_inflight_bytes > u32::MAX as usize
        || cluster.spec.max_topics == 0
        || cluster.spec.max_publish_workers == 0
        || cluster.spec.publish_worker_idle_seconds == 0
        || (cluster.spec.publish_ack_mode != "durable"
            && (cluster.spec.relaxed_sync_messages == 0
                || cluster.spec.relaxed_sync_bytes < 4096
                || cluster.spec.relaxed_sync_interval_ms == 0))
        || cluster.spec.max_detailed_metric_series == 0
    {
        bail!("queue limits are outside the stable v7 contract");
    }
    if !matches!(
        cluster.spec.publish_ack_mode.as_str(),
        "durable" | "write_ack" | "nsq_relaxed"
    ) {
        bail!("publishAckMode must be durable, write_ack, or nsq_relaxed");
    }
    if cluster.spec.websocket.max_connections == 0
        || cluster.spec.websocket.max_connections > tokio::sync::Semaphore::MAX_PERMITS
        || cluster.spec.websocket.max_connections_per_topic == 0
        || cluster.spec.websocket.max_connections_per_topic > cluster.spec.websocket.max_connections
        || cluster.spec.websocket.frame_inflight_bytes
            < cluster.spec.max_message_bytes.saturating_add(24)
        || cluster.spec.websocket.frame_inflight_bytes > u32::MAX as usize
    {
        bail!("WebSocket limits must fit one maximum message and satisfy per-Topic <= global connections");
    }
    if cluster.spec.websocket.allowed_origins.iter().any(|origin| {
        let authority = origin
            .strip_prefix("http://")
            .or_else(|| origin.strip_prefix("https://"));
        origin == "*"
            || authority.is_none_or(|authority| {
                authority.is_empty()
                    || authority.contains('/')
                    || authority.contains('?')
                    || authority.contains('#')
                    || authority.contains(',')
                    || authority.trim() != authority
            })
    }) {
        bail!("WebSocket allowedOrigins must be exact HTTP origins without paths or wildcards");
    }
    validate_message_storage_contract(
        cluster.spec.max_message_bytes,
        effective_storage_feature_level(cluster.spec.storage_feature_level, active_feature_floor),
    )?;
    if cluster.spec.kodo_compatibility.enabled
        && (cluster.spec.min_brokers != 3
            || cluster.spec.max_brokers != 3
            || cluster.spec.storage_feature_level != 2
            || cluster.spec.bootstrap_retention_seconds < KODO_BOOTSTRAP_RETENTION_SECONDS
            || cluster.spec.max_message_bytes != 100 * 1024 * 1024
            || cluster.spec.connection_delivery_inflight_bytes < 128 * 1024 * 1024
            || cluster.spec.node_delivery_inflight_bytes < 512 * 1024 * 1024
            || cluster.spec.publish_ack_mode != "durable")
    {
        bail!(
            "Kodo compatibility requires exactly 3 brokers, storageFeatureLevel 2, \
             bootstrapRetentionSeconds >= 180, \
             maxMessageBytes 104857600, connectionDeliveryInflightBytes >= 134217728, \
             nodeDeliveryInflightBytes >= 536870912, and publishAckMode durable"
        );
    }
    if !(630..=86_400).contains(&cluster.spec.kodo_compatibility.cutover_grace_seconds) {
        bail!("Kodo compatibility cutoverGraceSeconds must be between 630 and 86400");
    }
    if cluster.spec.kodo_compatibility.enabled {
        if cluster.spec.kodo_compatibility.decommission_confirmed {
            bail!("decommissionConfirmed must be false while Kodo compatibility is enabled");
        }
        if cluster.spec.kodo_compatibility.cleanup_enabled {
            bail!(
                "Kodo automatic cleanup is disabled until cluster-wide atomic deletion is available"
            );
        }
        let target_image = cluster
            .spec
            .rollout
            .rollback_to_image
            .as_deref()
            .unwrap_or(&cluster.spec.image);
        if cluster.spec.image_pull_policy != "Never" && !has_sha256_digest(target_image) {
            bail!(
                "Kodo compatibility requires an immutable @sha256 image or imagePullPolicy Never"
            );
        }
        if cluster
            .spec
            .kodo_compatibility
            .allowed_pod_selector
            .is_empty()
            || cluster
                .spec
                .kodo_compatibility
                .allowed_pod_selector
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
            || cluster
                .spec
                .kodo_compatibility
                .allowed_namespace_selector
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
        {
            bail!("Kodo compatibility requires a non-empty allowedPodSelector and valid selector labels");
        }
        let memory_request = storage::parse_quantity(&cluster.spec.broker_resources.memory_request)
            .context("parse broker memory request")?;
        if memory_request < 2_u128 << 30 {
            bail!("Kodo compatibility requires brokerResources.memoryRequest >= 2Gi");
        }
        let cpu_request = parse_cpu_millis(&cluster.spec.broker_resources.cpu_request)
            .context("parse broker CPU request")?;
        if cpu_request < 1_000.0 {
            bail!("Kodo compatibility requires brokerResources.cpuRequest >= 1 CPU");
        }
        if let Some(cpu_limit) = cluster.spec.broker_resources.cpu_limit.as_deref() {
            let cpu_limit = parse_cpu_millis(cpu_limit).context("parse broker CPU limit")?;
            if cpu_limit < cpu_request {
                bail!("brokerResources.cpuLimit must be greater than or equal to cpuRequest");
            }
        }
        if let Some(memory_limit) = cluster.spec.broker_resources.memory_limit.as_deref() {
            let memory_limit =
                storage::parse_quantity(memory_limit).context("parse broker memory limit")?;
            if memory_limit < memory_request {
                bail!("brokerResources.memoryLimit must be greater than or equal to memoryRequest");
            }
        }
    }
    if cluster.spec.rollout.timeout_seconds == 0 || cluster.spec.rollout.timeout_seconds > 86_400 {
        bail!("rollout timeoutSeconds must be between 1 and 86400");
    }
    if cluster
        .spec
        .rollout
        .rollback_to_image
        .as_ref()
        .is_some_and(|image| image.trim().is_empty())
    {
        bail!("rollout rollbackToImage cannot be empty");
    }
    if cluster
        .spec
        .broker_scheduling
        .topology_key
        .trim()
        .is_empty()
        || cluster.spec.broker_resources.cpu_request.trim().is_empty()
        || cluster
            .spec
            .broker_resources
            .memory_request
            .trim()
            .is_empty()
    {
        bail!("broker scheduling and resource requests cannot be empty");
    }
    Ok(())
}

fn parse_cpu_millis(value: &str) -> anyhow::Result<f64> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix('m') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('u') {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('n') {
        (number, 0.000_001)
    } else {
        (value, 1_000.0)
    };
    let number: f64 = number.parse()?;
    anyhow::ensure!(
        number.is_finite() && number >= 0.0,
        "invalid CPU quantity {value}"
    );
    Ok(number * multiplier)
}

fn validate_message_storage_contract(
    max_message_bytes: usize,
    storage_feature_level: u32,
) -> anyhow::Result<()> {
    const LEGACY_MAX_RECORD_BYTES: usize = 72 * 1024 * 1024;
    const SINGLE_MESSAGE_ENVELOPE_BYTES: usize = 24;
    const MPUB_ENTRY_BYTES: usize = 16;
    const MAX_MPUB_MESSAGES: usize = 65_536;
    let max_body_bytes = (64 * 1024 * 1024).max(max_message_bytes);
    let maximum_record = max_message_bytes
        .saturating_add(SINGLE_MESSAGE_ENVELOPE_BYTES)
        .max(max_body_bytes.saturating_add(MPUB_ENTRY_BYTES.saturating_mul(MAX_MPUB_MESSAGES)));
    if maximum_record > LEGACY_MAX_RECORD_BYTES && storage_feature_level < 2 {
        bail!("messages above the v7 legacy record bound require storageFeatureLevel 2");
    }
    Ok(())
}

fn has_sha256_digest(image: &str) -> bool {
    image.rsplit_once("@sha256:").is_some_and(|(_, digest)| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_storage_feature_level_is_a_monotonic_floor() {
        assert_eq!(effective_storage_feature_level(1, 2), 2);
        assert_eq!(effective_storage_feature_level(2, 1), 2);
    }

    #[test]
    fn cpu_quantities_compare_in_millicores() {
        assert_eq!(parse_cpu_millis("1").unwrap(), 1_000.0);
        assert_eq!(parse_cpu_millis("1000m").unwrap(), 1_000.0);
        assert_eq!(parse_cpu_millis("500m").unwrap(), 500.0);
        assert_eq!(parse_cpu_millis("500000u").unwrap(), 500.0);
        assert!(parse_cpu_millis("-1").is_err());
    }

    #[test]
    fn feature_two_retains_large_message_delivery_capacity() {
        assert_eq!(
            effective_delivery_limits(2, 20 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024),
            (100 * 1024 * 1024, 200 * 1024 * 1024)
        );
    }

    #[test]
    fn legacy_feature_rejects_messages_that_cross_the_record_bound() {
        let maximum = 71 * 1024 * 1024;
        assert!(validate_message_storage_contract(maximum, 1).is_ok());
        assert!(validate_message_storage_contract(maximum + 1, 1).is_err());
        assert!(validate_message_storage_contract(100 * 1024 * 1024, 2).is_ok());
    }

    #[test]
    fn immutable_image_detection_requires_a_complete_sha256_digest() {
        let digest = "a".repeat(64);
        assert!(has_sha256_digest(&format!(
            "registry/rustqueue@sha256:{digest}"
        )));
        assert!(!has_sha256_digest(&format!(
            "registry/rustqueue@sha256:{}",
            "A".repeat(64)
        )));
        assert!(!has_sha256_digest("registry/rustqueue:latest"));
        assert!(!has_sha256_digest("registry/rustqueue@sha256:abcd"));
    }

    #[test]
    fn kodo_contract_requires_a_second_lookup_poll_retention_window() {
        let mut cluster: RustQueue = serde_json::from_value(serde_json::json!({
            "apiVersion": "rustqueue.io/v1alpha1",
            "kind": "RustQueue",
            "metadata": {"name": "queue", "namespace": "test"},
            "spec": {
                "image": "rustqueue:test",
                "imagePullPolicy": "Never",
                "minBrokers": 3,
                "maxBrokers": 3,
                "storageFeatureLevel": 2,
                "bootstrapRetentionSeconds": 180,
                "maxMessageBytes": 104857600,
                "connectionDeliveryInflightBytes": 134217728,
                "nodeDeliveryInflightBytes": 536870912,
                "brokerResources": {"cpuRequest": "1", "memoryRequest": "2Gi"},
                "kodoCompatibility": {"enabled": true}
            }
        }))
        .unwrap();

        assert!(validate(&cluster, 2).is_ok());
        cluster.spec.broker_resources.cpu_request = "999m".into();
        assert!(validate(&cluster, 2)
            .unwrap_err()
            .to_string()
            .contains("cpuRequest >= 1 CPU"));
        cluster.spec.broker_resources.cpu_request = "1".into();
        cluster.spec.bootstrap_retention_seconds = KODO_BOOTSTRAP_RETENTION_SECONDS - 1;
        assert!(validate(&cluster, 2)
            .unwrap_err()
            .to_string()
            .contains("bootstrapRetentionSeconds >= 180"));
        cluster.spec.bootstrap_retention_seconds = KODO_BOOTSTRAP_RETENTION_SECONDS;
        cluster.spec.publish_ack_mode = "nsq_relaxed".into();
        assert!(validate(&cluster, 2)
            .unwrap_err()
            .to_string()
            .contains("publishAckMode durable"));
    }
}
