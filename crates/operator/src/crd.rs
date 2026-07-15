use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "rustqueue.io",
    version = "v1alpha1",
    kind = "RustQueueCluster",
    plural = "rustqueueclusters",
    singular = "rustqueuecluster",
    shortname = "rq",
    namespaced,
    status = "RustQueueClusterStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueClusterSpec {
    #[serde(default = "default_image")]
    pub image: String,
    #[serde(default = "default_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default)]
    pub storage: StorageSpec,
    #[serde(default)]
    pub nodes: NodePoolSpec,
    #[serde(default)]
    pub cells: CellPolicy,
    #[serde(default)]
    pub replication: ReplicationPolicy,
    #[serde(default)]
    pub queue: QueuePolicy,
    #[serde(default)]
    pub security: SecurityPolicy,
    #[serde(default)]
    pub upgrade: UpgradePolicy,
    #[serde(default)]
    pub resources: ResourcePolicy,
    #[serde(default)]
    pub development: DevelopmentPolicy,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    #[serde(default)]
    pub class_name: String,
    #[serde(default = "default_storage_size")]
    pub size: String,
    #[serde(default = "default_true")]
    pub retain_on_delete: bool,
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    #[serde(default = "default_disk_high")]
    pub disk_high_watermark_percent: u8,
    #[serde(default = "default_disk_low")]
    pub disk_low_watermark_percent: u8,
    #[serde(default = "default_true")]
    pub protective_eviction_enabled: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodePoolSpec {
    #[serde(default = "default_node_selector")]
    pub selector: BTreeMap<String, String>,
    #[serde(default = "default_true")]
    pub dedicated: bool,
    #[serde(default = "default_taint_key")]
    pub taint_key: String,
    #[serde(default = "default_failure_domain_label")]
    pub failure_domain_label: String,
    #[serde(default = "default_true")]
    pub auto_scale_from_nodes: bool,
    #[serde(default)]
    pub replicas: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellPolicy {
    #[serde(default = "default_cell_min")]
    pub min_nodes: u8,
    #[serde(default = "default_cell_target")]
    pub target_nodes: u8,
    #[serde(default = "default_cell_max")]
    pub max_nodes: u8,
    #[serde(default = "default_routers")]
    pub routers_per_cell: u8,
    #[serde(default = "default_home_cells")]
    pub max_home_cells_per_topic: u16,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationPolicy {
    #[serde(default = "default_rf")]
    pub metadata: u8,
    #[serde(default = "default_rf")]
    pub partitions: u8,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuePolicy {
    #[serde(default = "default_partitions")]
    pub default_partitions: u16,
    #[serde(default = "default_max_partitions")]
    pub max_partitions_per_topic: u16,
    #[serde(default = "default_max_message")]
    pub max_message_bytes: u64,
    #[serde(default = "default_max_body")]
    pub max_body_bytes: u64,
    #[serde(default = "default_backlog")]
    pub max_backlog_messages_per_partition: u64,
    #[serde(default)]
    pub message_retention_seconds: u64,
    #[serde(default = "default_delivery_attempts")]
    pub max_delivery_attempts: u16,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicy {
    #[serde(default = "default_ca_days")]
    pub ca_validity_days: u32,
    #[serde(default = "default_leaf_days")]
    pub certificate_validity_days: u32,
    #[serde(default = "default_renew_days")]
    pub renew_before_days: u32,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradePolicy {
    #[serde(default = "default_true")]
    pub automatic: bool,
    #[serde(default = "default_one")]
    pub max_unavailable_per_cell: u8,
    #[serde(default = "default_upgrade_deadline")]
    pub progress_deadline_seconds: u64,
    #[serde(default)]
    pub retry_generation: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcePolicy {
    #[serde(default = "default_cpu_request")]
    pub cpu_request: String,
    #[serde(default = "default_memory_request")]
    pub memory_request: String,
    #[serde(default = "default_cpu_limit")]
    pub cpu_limit: String,
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DevelopmentPolicy {
    #[serde(default)]
    pub allow_single_node: bool,
    #[serde(default = "default_virtual_replicas")]
    pub virtual_replicas: u16,
}

impl Default for DevelopmentPolicy {
    fn default() -> Self {
        Self {
            allow_single_node: false,
            virtual_replicas: default_virtual_replicas(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RustQueueClusterStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub allocated_replicas: u16,
    #[serde(default)]
    pub desired_replicas: u16,
    #[serde(default)]
    pub ready_replicas: u16,
    #[serde(default)]
    pub pending_nodes: u16,
    #[serde(default)]
    pub ca_revision: String,
    #[serde(default)]
    pub cells: Vec<CellStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade: Option<UpgradeStatus>,
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellStatus {
    pub id: u64,
    pub desired_replicas: u16,
    pub ready_replicas: u16,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeStatus {
    pub target_image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_node_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub observed_retry_generation: u64,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

impl RustQueueClusterSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.image.trim().is_empty() {
            return Err("spec.image cannot be empty".into());
        }
        if self.storage.class_name.trim().is_empty() {
            return Err("spec.storage.className is required".into());
        }
        if self.storage.size.trim().is_empty() {
            return Err("spec.storage.size cannot be empty".into());
        }
        if self.cells.min_nodes < 3
            || self.cells.min_nodes > self.cells.target_nodes
            || self.cells.target_nodes > self.cells.max_nodes
            || self.cells.max_nodes > 9
        {
            return Err("Cell sizing must satisfy 3 <= min <= target <= max <= 9".into());
        }
        if self.cells.routers_per_cell == 0 || self.cells.routers_per_cell > self.cells.min_nodes {
            return Err("routersPerCell must fit 1..=minNodes".into());
        }
        if !matches!(self.replication.metadata, 3 | 5)
            || !matches!(self.replication.partitions, 3 | 5)
            || self.replication.metadata > self.cells.min_nodes
            || self.replication.partitions > self.cells.min_nodes
        {
            return Err(
                "metadata and partition replication factors must be 3 or 5 and fit a Cell".into(),
            );
        }
        if self.storage.disk_low_watermark_percent >= self.storage.disk_high_watermark_percent
            || self.storage.disk_high_watermark_percent > 100
        {
            return Err("storage watermarks must satisfy low < high <= 100".into());
        }
        if self.queue.default_partitions == 0
            || self.queue.default_partitions > self.queue.max_partitions_per_topic
        {
            return Err("defaultPartitions must fit maxPartitionsPerTopic".into());
        }
        if self.queue.max_message_bytes == 0
            || self.queue.max_message_bytes > 32 * 1024 * 1024
            || self.queue.max_body_bytes < self.queue.max_message_bytes
            || self.queue.max_body_bytes > 64 * 1024 * 1024
        {
            return Err("message/body limits exceed RustQueue format v6 bounds".into());
        }
        if self.security.renew_before_days == 0
            || self.security.renew_before_days >= self.security.certificate_validity_days
            || self.security.certificate_validity_days >= self.security.ca_validity_days
        {
            return Err("certificate validity must satisfy 0 < renewBefore < leaf < CA".into());
        }
        if self.upgrade.max_unavailable_per_cell != 1 {
            return Err(
                "0.6 permits exactly one unavailable Broker per Cell during upgrades".into(),
            );
        }
        if self.development.allow_single_node && self.development.virtual_replicas < 3 {
            return Err("development.virtualReplicas must be at least 3".into());
        }
        Ok(())
    }
}

impl Default for RustQueueClusterSpec {
    fn default() -> Self {
        Self {
            image: default_image(),
            image_pull_policy: default_pull_policy(),
            storage: StorageSpec::default(),
            nodes: NodePoolSpec::default(),
            cells: CellPolicy::default(),
            replication: ReplicationPolicy::default(),
            queue: QueuePolicy::default(),
            security: SecurityPolicy::default(),
            upgrade: UpgradePolicy::default(),
            resources: ResourcePolicy::default(),
            development: DevelopmentPolicy::default(),
        }
    }
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            class_name: String::new(),
            size: default_storage_size(),
            retain_on_delete: true,
            min_free_bytes: default_min_free_bytes(),
            disk_high_watermark_percent: default_disk_high(),
            disk_low_watermark_percent: default_disk_low(),
            protective_eviction_enabled: true,
        }
    }
}

impl Default for NodePoolSpec {
    fn default() -> Self {
        Self {
            selector: default_node_selector(),
            dedicated: true,
            taint_key: default_taint_key(),
            failure_domain_label: default_failure_domain_label(),
            auto_scale_from_nodes: true,
            replicas: None,
        }
    }
}

impl Default for CellPolicy {
    fn default() -> Self {
        Self {
            min_nodes: default_cell_min(),
            target_nodes: default_cell_target(),
            max_nodes: default_cell_max(),
            routers_per_cell: default_routers(),
            max_home_cells_per_topic: default_home_cells(),
        }
    }
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            metadata: default_rf(),
            partitions: default_rf(),
        }
    }
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self {
            default_partitions: default_partitions(),
            max_partitions_per_topic: default_max_partitions(),
            max_message_bytes: default_max_message(),
            max_body_bytes: default_max_body(),
            max_backlog_messages_per_partition: default_backlog(),
            message_retention_seconds: 0,
            max_delivery_attempts: default_delivery_attempts(),
        }
    }
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            ca_validity_days: default_ca_days(),
            certificate_validity_days: default_leaf_days(),
            renew_before_days: default_renew_days(),
        }
    }
}

impl Default for UpgradePolicy {
    fn default() -> Self {
        Self {
            automatic: true,
            max_unavailable_per_cell: 1,
            progress_deadline_seconds: default_upgrade_deadline(),
            retry_generation: 0,
        }
    }
}

impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            cpu_request: default_cpu_request(),
            memory_request: default_memory_request(),
            cpu_limit: default_cpu_limit(),
            memory_limit: default_memory_limit(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_one() -> u8 {
    1
}
fn default_image() -> String {
    "rustqueue:0.6.0".into()
}
fn default_pull_policy() -> String {
    "IfNotPresent".into()
}
fn default_storage_size() -> String {
    "100Gi".into()
}
fn default_min_free_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}
fn default_disk_high() -> u8 {
    85
}
fn default_disk_low() -> u8 {
    75
}
fn default_node_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("rustqueue.io/dedicated".into(), "true".into())])
}
fn default_taint_key() -> String {
    "rustqueue.io/dedicated".into()
}
fn default_failure_domain_label() -> String {
    "topology.kubernetes.io/zone".into()
}
fn default_cell_min() -> u8 {
    3
}
fn default_cell_target() -> u8 {
    9
}
fn default_cell_max() -> u8 {
    9
}
fn default_routers() -> u8 {
    3
}
fn default_home_cells() -> u16 {
    128
}
fn default_rf() -> u8 {
    3
}
fn default_partitions() -> u16 {
    4
}
fn default_max_partitions() -> u16 {
    1024
}
fn default_max_message() -> u64 {
    32 * 1024 * 1024
}
fn default_max_body() -> u64 {
    64 * 1024 * 1024
}
fn default_backlog() -> u64 {
    10_000_000
}
fn default_delivery_attempts() -> u16 {
    16
}
fn default_ca_days() -> u32 {
    3650
}
fn default_leaf_days() -> u32 {
    365
}
fn default_renew_days() -> u32 {
    30
}
fn default_upgrade_deadline() -> u64 {
    600
}
fn default_virtual_replicas() -> u16 {
    3
}
fn default_cpu_request() -> String {
    "500m".into()
}
fn default_memory_request() -> String {
    "1Gi".into()
}
fn default_cpu_limit() -> String {
    "4".into()
}
fn default_memory_limit() -> String {
    "8Gi".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn defaults_are_production_safe_but_require_storage() {
        let spec = RustQueueClusterSpec::default();
        assert!(spec.validate().unwrap_err().contains("storage.className"));
        assert!(!spec.development.allow_single_node);
        assert_eq!(spec.cells.max_nodes, 9);
    }

    #[test]
    fn crd_contains_status_and_short_name() {
        let value = serde_json::to_value(RustQueueCluster::crd()).unwrap();
        assert_eq!(value["spec"]["names"]["shortNames"][0], "rq");
        assert!(value["spec"]["versions"][0]["subresources"]["status"].is_object());
    }
}
