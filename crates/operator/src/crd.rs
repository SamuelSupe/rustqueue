use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "rustqueue.io",
    version = "v1alpha1",
    kind = "RustQueue",
    plural = "rustqueues",
    namespaced,
    status = "RustQueueStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueSpec {
    pub image: String,
    #[serde(default = "default_image_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default = "default_min_brokers")]
    pub min_brokers: i32,
    #[serde(default = "default_max_brokers")]
    pub max_brokers: i32,
    #[serde(default = "default_eligible_selector")]
    pub eligible_node_selector: String,
    #[serde(default = "default_storage_class")]
    pub storage_class_name: String,
    #[serde(default = "default_storage_size")]
    pub storage_size: String,
    #[serde(default = "default_storage_feature_level")]
    pub storage_feature_level: u32,
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    #[serde(default = "default_disk_high_watermark")]
    pub disk_high_watermark_percent: u8,
    #[serde(default = "default_disk_low_watermark")]
    pub disk_low_watermark_percent: u8,
    #[serde(default = "enabled")]
    pub protective_eviction_enabled: bool,
    #[serde(default = "default_disk_pressure_grace")]
    pub disk_pressure_grace_seconds: u64,
    #[serde(default = "default_bootstrap_retention")]
    pub bootstrap_retention_seconds: u64,
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    #[serde(default = "default_max_backlog_messages")]
    pub max_backlog_messages: usize,
    #[serde(default)]
    pub registry_secret_name: Option<String>,
    #[serde(default)]
    pub client_tls_secret_name: Option<String>,
    #[serde(default)]
    pub proxy_node_selector: BTreeMap<String, String>,
    #[serde(default = "default_discovery_replicas")]
    pub discovery_replicas: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueStatus {
    pub observed_generation: Option<i64>,
    pub desired_brokers: i32,
    pub ready_brokers: i32,
    pub phase: String,
    pub message: String,
    pub active_storage_feature_level: u32,
}

fn default_min_brokers() -> i32 {
    1
}
fn default_max_brokers() -> i32 {
    500
}
fn default_image_pull_policy() -> String {
    "IfNotPresent".into()
}
fn default_eligible_selector() -> String {
    "rustqueue.io/eligible=true".into()
}
fn default_storage_class() -> String {
    "local-path".into()
}
fn default_storage_size() -> String {
    "100Gi".into()
}
fn default_storage_feature_level() -> u32 {
    1
}
fn default_min_free_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}
fn default_disk_high_watermark() -> u8 {
    85
}
fn default_disk_low_watermark() -> u8 {
    75
}
fn enabled() -> bool {
    true
}
fn default_disk_pressure_grace() -> u64 {
    60
}
fn default_bootstrap_retention() -> u64 {
    30
}
fn default_max_message_bytes() -> usize {
    20 * 1024 * 1024
}
fn default_max_backlog_messages() -> usize {
    10_000_000
}
fn default_discovery_replicas() -> i32 {
    2
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn crd_exposes_the_share_nothing_scale_contract() {
        let crd = serde_json::to_value(RustQueue::crd()).unwrap();
        let schema = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"];
        assert!(schema.to_string().contains("eligibleNodeSelector"));
        assert!(!schema.to_string().contains("replicationFactor"));
        assert!(!schema.to_string().contains("cell"));
    }
}
