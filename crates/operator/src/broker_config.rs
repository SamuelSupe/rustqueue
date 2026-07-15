use crate::crd::RustQueueClusterSpec;
use crate::layout::{BrokerPlan, ClusterLayout};
use anyhow::{bail, Context};
use std::collections::{BTreeMap, BTreeSet};

pub struct ConfigInput<'a> {
    pub cluster_name: &'a str,
    pub namespace: &'a str,
    pub spec: &'a RustQueueClusterSpec,
    pub layout: &'a ClusterLayout,
    pub broker: &'a BrokerPlan,
    pub failure_domains: &'a BTreeMap<u64, String>,
}

pub fn render(input: &ConfigInput<'_>) -> anyhow::Result<String> {
    let cell = input
        .layout
        .cells
        .iter()
        .find(|cell| cell.id == input.broker.cell_id)
        .context("Broker Cell is absent from layout")?;
    let metadata_rf = usize::from(input.spec.replication.metadata);
    if cell.brokers.len() < metadata_rf
        || cell.brokers.len() < usize::from(input.spec.replication.partitions)
    {
        bail!(
            "Cell {} does not satisfy configured replication factors",
            cell.id
        );
    }

    let root_voters = input.layout.brokers().take(3).collect::<Vec<_>>();
    if root_voters.len() != 3 {
        bail!("federation requires at least three root voters");
    }
    let initial_voters = cell
        .brokers
        .iter()
        .take(metadata_rf)
        .map(|broker| broker.node_id)
        .collect::<Vec<_>>();

    let mut visible = BTreeSet::new();
    visible.extend(cell.brokers.iter().map(|broker| broker.node_id));
    visible.extend(root_voters.iter().map(|broker| broker.node_id));
    let local_seed = cell.brokers.first().context("Cell has no seed Broker")?;
    let root_seed = root_voters[0];
    let mut seeds = BTreeSet::new();
    seeds.insert(p2p_address(input, local_seed));
    seeds.insert(p2p_address(input, root_seed));

    let mut output = String::new();
    output.push_str("log_format = \"json\"\n");
    output.push_str("[node]\n");
    line(&mut output, "id", input.broker.node_id);
    string_line(
        &mut output,
        "broadcast_address",
        &pod_fqdn(input, input.broker),
    );
    output.push_str("[network]\n");
    string_line(&mut output, "tcp_address", "0.0.0.0:4150");
    string_line(&mut output, "http_address", "0.0.0.0:4151");
    string_line(&mut output, "internal_address", "0.0.0.0:4250");
    line(&mut output, "advertised_tcp_port", 4150);
    line(&mut output, "advertised_http_port", 4151);
    output.push_str("[storage]\n");
    string_line(&mut output, "data_path", "/data");
    line(&mut output, "max_segment_bytes", 100 * 1024 * 1024_u64);
    line(&mut output, "entry_cache_bytes", 64 * 1024 * 1024_u64);
    line(&mut output, "payload_read_workers", 0);
    line(&mut output, "payload_read_queue", 4096);
    line(&mut output, "dedup_max_entries", 1_000_000);
    line(&mut output, "dedup_ttl_seconds", 600);
    line(
        &mut output,
        "disk_high_watermark_percent",
        input.spec.storage.disk_high_watermark_percent,
    );
    line(
        &mut output,
        "disk_low_watermark_percent",
        input.spec.storage.disk_low_watermark_percent,
    );
    line(
        &mut output,
        "min_free_bytes",
        input.spec.storage.min_free_bytes,
    );
    bool_line(
        &mut output,
        "protective_eviction_enabled",
        input.spec.storage.protective_eviction_enabled,
    );
    line(&mut output, "disk_pressure_grace_seconds", 60);
    output.push_str("[queue]\n");
    line(
        &mut output,
        "default_partitions",
        input.spec.queue.default_partitions,
    );
    line(
        &mut output,
        "max_partitions_per_topic",
        input.spec.queue.max_partitions_per_topic,
    );
    line(
        &mut output,
        "max_message_bytes",
        input.spec.queue.max_message_bytes,
    );
    line(&mut output, "max_ack_gap", 65_536);
    line(
        &mut output,
        "max_backlog_messages_per_partition",
        input.spec.queue.max_backlog_messages_per_partition,
    );
    line(
        &mut output,
        "message_retention_seconds",
        input.spec.queue.message_retention_seconds,
    );
    line(
        &mut output,
        "max_delivery_attempts",
        input.spec.queue.max_delivery_attempts,
    );
    output.push_str("[limits]\n");
    line(
        &mut output,
        "max_body_bytes",
        input.spec.queue.max_body_bytes,
    );
    line(
        &mut output,
        "connection_publish_inflight_bytes",
        input.spec.queue.max_body_bytes,
    );
    line(
        &mut output,
        "node_publish_inflight_bytes",
        512 * 1024 * 1024_u64,
    );
    output.push_str("[security]\n");
    string_line(
        &mut output,
        "admin_token_file",
        "/etc/rustqueue/shared/admin.token",
    );
    output.push_str("[security.internal_tls]\n");
    string_line(
        &mut output,
        "certificate_file",
        "/etc/rustqueue/tls/tls.crt",
    );
    string_line(
        &mut output,
        "private_key_file",
        "/etc/rustqueue/tls/tls.key",
    );
    string_line(
        &mut output,
        "client_ca_file",
        "/etc/rustqueue/shared/ca.crt",
    );
    string_line(&mut output, "root_ca_file", "/etc/rustqueue/shared/ca.crt");
    bool_line(&mut output, "require_client_certificate", true);
    bool_line(&mut output, "required", true);
    output.push_str("[cluster]\n");
    bool_line(&mut output, "enabled", true);
    bool_line(&mut output, "bootstrap", input.broker.ordinal == 0);
    string_line(&mut output, "name", input.cluster_name);
    list_line(&mut output, "initial_voters", &initial_voters);
    line(
        &mut output,
        "default_replication_factor",
        input.spec.replication.partitions,
    );
    line(
        &mut output,
        "metadata_replication_factor",
        input.spec.replication.metadata,
    );
    output.push_str("[cluster.federation]\n");
    bool_line(&mut output, "enabled", true);
    line(&mut output, "cell_id", input.broker.cell_id);
    list_line(
        &mut output,
        "root_voters",
        &root_voters
            .iter()
            .map(|broker| broker.node_id)
            .collect::<Vec<_>>(),
    );
    line(
        &mut output,
        "max_home_cells_per_topic",
        input.spec.cells.max_home_cells_per_topic,
    );
    line(&mut output, "route_cache_ms", 1000);
    line(&mut output, "retry_after_ms", 1000);
    line(&mut output, "cell_min_nodes", input.spec.cells.min_nodes);
    line(
        &mut output,
        "cell_target_nodes",
        input.spec.cells.target_nodes,
    );
    line(&mut output, "cell_max_nodes", input.spec.cells.max_nodes);
    line(
        &mut output,
        "routers_per_cell",
        input.spec.cells.routers_per_cell,
    );
    output.push_str("[cluster.discovery]\n");
    bool_line(&mut output, "enabled", true);
    string_line(&mut output, "listen_address", "/ip4/0.0.0.0/tcp/4350");
    string_list_line(
        &mut output,
        "seed_addresses",
        seeds.iter().map(String::as_str),
    );
    bool_line(&mut output, "mdns", false);
    string_line(
        &mut output,
        "join_token_file",
        "/etc/rustqueue/shared/discovery.token",
    );
    line(&mut output, "announce_interval_seconds", 5);
    line(&mut output, "max_known_peers", 4096);
    output.push_str("[cluster.automation]\n");
    bool_line(&mut output, "enabled", true);
    line(&mut output, "poll_interval_seconds", 15);
    line(&mut output, "node_stabilization_seconds", 60);
    line(&mut output, "node_down_grace_seconds", 600);
    line(&mut output, "group_cooldown_seconds", 600);
    line(&mut output, "max_concurrent_migrations", 2);
    line(&mut output, "max_migrations_per_node", 1);
    bool_line(&mut output, "auto_replace_metadata", true);
    line(&mut output, "operation_history_limit", 1000);
    output.push_str("[cluster.shutdown]\n");
    line(&mut output, "grace_seconds", 60);
    line(&mut output, "maintenance_default_ttl_seconds", 1800);
    line(&mut output, "maintenance_max_ttl_seconds", 86_400);

    for node_id in visible {
        let broker = input
            .layout
            .broker(node_id)
            .context("visible Broker is absent")?;
        output.push_str(&format!("[cluster.nodes.{node_id}]\n"));
        string_line(
            &mut output,
            "raft_address",
            &format!("https://{}:4250", pod_fqdn(input, broker)),
        );
        string_line(&mut output, "broadcast_address", &pod_fqdn(input, broker));
        line(&mut output, "tcp_port", 4150);
        line(&mut output, "http_port", 4151);
        string_line(&mut output, "tls_server_name", &pod_fqdn(input, broker));
        string_line(
            &mut output,
            "failure_domain",
            input
                .failure_domains
                .get(&node_id)
                .map(String::as_str)
                .unwrap_or("pending"),
        );
        line(&mut output, "cell_id", broker.cell_id);
        bool_line(
            &mut output,
            "federation_router",
            broker.ordinal < input.spec.cells.routers_per_cell,
        );
    }
    toml::from_str::<toml::Value>(&output).context("generated Broker config is invalid TOML")?;
    Ok(output)
}

pub fn pod_fqdn(input: &ConfigInput<'_>, broker: &BrokerPlan) -> String {
    format!(
        "{}.{}.{}.svc",
        broker.pod_name, broker.headless_service, input.namespace
    )
}

fn p2p_address(input: &ConfigInput<'_>, broker: &BrokerPlan) -> String {
    format!("/dns4/{}/tcp/4350", pod_fqdn(input, broker))
}

fn quoted(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

fn line(output: &mut String, name: &str, value: impl std::fmt::Display) {
    output.push_str(&format!("{name} = {value}\n"));
}

fn bool_line(output: &mut String, name: &str, value: bool) {
    line(output, name, value);
}

fn string_line(output: &mut String, name: &str, value: &str) {
    output.push_str(&format!("{name} = {}\n", quoted(value)));
}

fn list_line(output: &mut String, name: &str, values: &[u64]) {
    let values = values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    output.push_str(&format!("{name} = [{values}]\n"));
}

fn string_list_line<'a>(output: &mut String, name: &str, values: impl Iterator<Item = &'a str>) {
    let values = values.map(quoted).collect::<Vec<_>>().join(", ");
    output.push_str(&format!("{name} = [{values}]\n"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::RustQueueClusterSpec;
    use crate::layout;

    #[test]
    fn generated_config_is_cell_scoped_and_uses_pod_identity() {
        let mut spec = RustQueueClusterSpec::default();
        spec.storage.class_name = "ssd".into();
        let layout = layout::plan("queue", 12, &spec.cells);
        let domains = layout
            .brokers()
            .map(|broker| (broker.node_id, format!("zone-{}", broker.node_id)))
            .collect();
        let config = render(&ConfigInput {
            cluster_name: "queue",
            namespace: "messaging",
            spec: &spec,
            layout: &layout,
            broker: layout.broker(10).unwrap(),
            failure_domains: &domains,
        })
        .unwrap();
        assert!(config.contains("id = 10"));
        assert!(config.contains("cell_id = 2"));
        assert!(config.contains("[cluster.nodes.1]"));
        assert!(config.contains("[cluster.nodes.10]"));
        assert!(!config.contains("[cluster.nodes.4]"));
        assert!(config.contains("queue-c2-n1-0.queue-cell-2.messaging.svc"));
    }
}
