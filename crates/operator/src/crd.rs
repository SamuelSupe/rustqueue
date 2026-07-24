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
    #[serde(default = "default_message_index_cache_bytes")]
    pub message_index_cache_bytes: usize,
    #[serde(default = "default_maintenance_startup_delay")]
    pub maintenance_startup_delay_seconds: u64,
    #[serde(default = "default_node_delivery_inflight_bytes")]
    pub node_delivery_inflight_bytes: usize,
    #[serde(default = "default_connection_delivery_inflight_bytes")]
    pub connection_delivery_inflight_bytes: usize,
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    #[serde(default = "default_disk_high_watermark")]
    pub disk_high_watermark_percent: u8,
    #[serde(default = "default_disk_low_watermark")]
    pub disk_low_watermark_percent: u8,
    #[serde(default)]
    pub protective_eviction_enabled: bool,
    #[serde(default = "default_disk_pressure_grace")]
    pub disk_pressure_grace_seconds: u64,
    #[serde(default = "default_bootstrap_retention")]
    pub bootstrap_retention_seconds: u64,
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    #[serde(default = "default_max_topics")]
    pub max_topics: usize,
    #[serde(default = "default_max_publish_workers")]
    pub max_publish_workers: usize,
    #[serde(default = "default_publish_worker_idle_seconds")]
    pub publish_worker_idle_seconds: u64,
    #[serde(default)]
    pub detailed_queue_metrics: bool,
    #[serde(default = "default_max_detailed_metric_series")]
    pub max_detailed_metric_series: usize,
    #[serde(default)]
    pub registry_secret_name: Option<String>,
    #[serde(default)]
    pub console_management_enabled: bool,
    #[serde(default)]
    pub client_tls_secret_name: Option<String>,
    #[serde(default)]
    pub proxy_node_selector: BTreeMap<String, String>,
    #[serde(default = "default_proxy_tcp_connection_age")]
    pub proxy_tcp_max_connection_age_seconds: u64,
    #[serde(default = "default_discovery_replicas")]
    pub discovery_replicas: i32,
    #[serde(default)]
    pub kodo_compatibility: KodoCompatibility,
    #[serde(default)]
    pub maintenance: Option<BrokerMaintenance>,
    #[serde(default)]
    pub rollout: RolloutPolicy,
    #[serde(default)]
    pub broker_scheduling: BrokerScheduling,
    #[serde(default)]
    pub broker_resources: WorkloadResources,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KodoCompatibility {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub decommission_confirmed: bool,
    #[serde(default)]
    pub producer_restart_nonce: String,
    #[serde(default)]
    pub cleanup_enabled: bool,
    #[serde(default = "default_kodo_cutover_grace")]
    pub cutover_grace_seconds: u64,
    #[serde(default = "default_kodo_allowed_pod_selector")]
    pub allowed_pod_selector: BTreeMap<String, String>,
    #[serde(default)]
    pub allowed_namespace_selector: BTreeMap<String, String>,
}

impl Default for KodoCompatibility {
    fn default() -> Self {
        Self {
            enabled: false,
            decommission_confirmed: false,
            producer_restart_nonce: String::new(),
            cleanup_enabled: false,
            cutover_grace_seconds: default_kodo_cutover_grace(),
            allowed_pod_selector: default_kodo_allowed_pod_selector(),
            allowed_namespace_selector: BTreeMap::new(),
        }
    }
}

impl KodoCompatibility {
    pub(crate) fn effective_cleanup_enabled(&self) -> bool {
        false
    }

    pub(crate) fn effective_allowed_pod_selector(&self) -> BTreeMap<String, String> {
        if self.allowed_pod_selector.is_empty() {
            default_kodo_allowed_pod_selector()
        } else {
            self.allowed_pod_selector.clone()
        }
    }
}

fn default_kodo_allowed_pod_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".into(), "kodo".into())])
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMaintenance {
    pub broker: String,
    #[serde(default = "enabled")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RolloutPolicy {
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub require_canary_approval: bool,
    #[serde(default)]
    pub approved_revision: Option<String>,
    #[serde(default = "default_rollout_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub rollback_to_image: Option<String>,
    #[serde(default)]
    pub retry_nonce: String,
}

impl Default for RolloutPolicy {
    fn default() -> Self {
        Self {
            paused: false,
            require_canary_approval: false,
            approved_revision: None,
            timeout_seconds: default_rollout_timeout(),
            rollback_to_image: None,
            retry_nonce: String::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerScheduling {
    #[serde(default = "default_topology_key")]
    pub topology_key: String,
    #[serde(default)]
    pub priority_class_name: Option<String>,
    #[serde(default)]
    pub tolerations: Vec<BrokerToleration>,
}

impl Default for BrokerScheduling {
    fn default() -> Self {
        Self {
            topology_key: default_topology_key(),
            priority_class_name: None,
            tolerations: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrokerToleration {
    pub key: Option<String>,
    pub operator: Option<String>,
    pub value: Option<String>,
    pub effect: Option<String>,
    pub toleration_seconds: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadResources {
    #[serde(default = "default_broker_cpu_request")]
    pub cpu_request: String,
    #[serde(default = "default_broker_memory_request")]
    pub memory_request: String,
    #[serde(default)]
    pub cpu_limit: Option<String>,
    #[serde(default)]
    pub memory_limit: Option<String>,
}

impl Default for WorkloadResources {
    fn default() -> Self {
        Self {
            cpu_request: default_broker_cpu_request(),
            memory_request: default_broker_memory_request(),
            cpu_limit: None,
            memory_limit: None,
        }
    }
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
    #[serde(default)]
    pub conditions: Vec<RustQueueCondition>,
    #[serde(default)]
    pub current_operation: Option<OperationStatus>,
    #[serde(default)]
    pub operation_history: Vec<OperationStatus>,
    #[serde(default)]
    pub orphaned_pvcs: Vec<String>,
    #[serde(default)]
    pub desired_storage_size: String,
    #[serde(default)]
    pub kodo_producer_restart_baseline_nonce: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub observed_generation: Option<i64>,
    pub last_transition_time: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OperationStatus {
    pub id: String,
    pub kind: String,
    pub phase: String,
    pub target: String,
    pub revision: String,
    pub message: String,
    pub started_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub previous_image: Option<String>,
    pub current_broker: Option<String>,
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
    90
}
fn default_proxy_tcp_connection_age() -> u64 {
    300
}
fn default_kodo_cutover_grace() -> u64 {
    630
}
fn default_max_message_bytes() -> usize {
    20 * 1024 * 1024
}
fn default_message_index_cache_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_maintenance_startup_delay() -> u64 {
    30
}
fn default_node_delivery_inflight_bytes() -> usize {
    512 * 1024 * 1024
}
fn default_connection_delivery_inflight_bytes() -> usize {
    32 * 1024 * 1024
}
fn default_max_topics() -> usize {
    10_000
}
fn default_max_publish_workers() -> usize {
    1_024
}
fn default_publish_worker_idle_seconds() -> u64 {
    60
}
fn default_max_detailed_metric_series() -> usize {
    1_000
}
fn default_discovery_replicas() -> i32 {
    2
}
fn default_rollout_timeout() -> u64 {
    600
}
fn default_topology_key() -> String {
    "kubernetes.io/hostname".into()
}
fn default_broker_cpu_request() -> String {
    "100m".into()
}
fn default_broker_memory_request() -> String {
    "256Mi".into()
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
        assert!(schema
            .to_string()
            .contains("proxyTcpMaxConnectionAgeSeconds"));
        assert!(schema.to_string().contains("kodoCompatibility"));
        assert!(schema.to_string().contains("decommissionConfirmed"));
        assert!(schema.to_string().contains("producerRestartNonce"));
        assert!(!schema.to_string().contains("replicationFactor"));
        assert!(!schema.to_string().contains("cell"));
    }

    #[test]
    fn disabling_kodo_also_disables_a_stale_cleanup_request() {
        let mut compatibility = KodoCompatibility {
            cleanup_enabled: true,
            ..KodoCompatibility::default()
        };
        assert!(!compatibility.effective_cleanup_enabled());
        compatibility.enabled = true;
        assert!(!compatibility.effective_cleanup_enabled());
    }

    #[test]
    fn empty_kodo_selector_falls_back_to_the_fail_closed_default() {
        let mut compatibility = KodoCompatibility::default();
        compatibility.allowed_pod_selector.clear();
        assert_eq!(
            compatibility.effective_allowed_pod_selector(),
            BTreeMap::from([("app.kubernetes.io/name".into(), "kodo".into())])
        );
    }
}
