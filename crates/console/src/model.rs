use rustqueue_operator::{OperationStatus, RustQueueCondition};
use rustqueue_queue::BrokerStats;
use rustqueue_telemetry::HistogramSnapshot;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BrokerObservation {
    pub schema_version: u32,
    pub collected_at_ms: u64,
    #[serde(default)]
    pub registry_revision: u64,
    pub node: ObserverNode,
    pub readiness: ObserverReadiness,
    pub disk: ObserverDisk,
    pub storage: ObserverStorage,
    pub runtime: RuntimeCounters,
    pub queue: BrokerStats,
    pub limits: ObserverLimits,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObserverNode {
    pub id: u64,
    pub address: String,
    pub version: String,
    pub data_format: u32,
    pub compatibility: Value,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObserverReadiness {
    pub process_ready: bool,
    pub storage_healthy: bool,
    pub disk_ready: bool,
    pub publish_ready: bool,
    pub consume_ready: bool,
    pub draining: bool,
    #[serde(default)]
    pub management_fences_ready: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObserverDisk {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: u64,
    pub pressure: bool,
    pub high_watermark_percent: u8,
    pub low_watermark_percent: u8,
    pub min_free_bytes: u64,
    pub protective_eviction_enabled: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObserverStorage {
    pub segment_count: u64,
    pub segment_bytes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObserverLimits {
    pub max_message_bytes: usize,
    pub max_backlog_messages: usize,
    pub max_connections: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RuntimeCounters {
    pub tcp_connections: i64,
    pub publish_messages: u64,
    pub publish_bytes: u64,
    pub publish_inflight_bytes: i64,
    pub publish_throttled_requests: u64,
    pub publish_throttled_bytes: u64,
    pub delivered_messages: u64,
    pub fetch_requests: u64,
    pub fetch_empty: u64,
    pub fetch_batches: u64,
    pub fetch_messages: u64,
    pub fetch_bytes: u64,
    pub finished_messages: u64,
    pub requeued_messages: u64,
    pub dead_letter_messages: u64,
    pub retention_expired_messages: u64,
    pub protocol_errors: u64,
    pub auth_failures: u64,
    pub storage_errors: u64,
    pub disk_total_bytes: u64,
    pub disk_available_bytes: u64,
    pub disk_used_percent: u64,
    pub disk_pressure: i64,
    pub protective_evictions: u64,
    pub protective_evicted_messages: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub collected_at_ms: u64,
    pub complete: bool,
    pub errors: Vec<String>,
    pub cluster: ClusterView,
    pub summary: SummaryView,
    pub brokers: Vec<BrokerView>,
    pub topics: Vec<TopicView>,
    pub storage: StorageView,
    pub conditions: Vec<RustQueueCondition>,
    pub current_operation: Option<OperationStatus>,
    pub operation_history: Vec<OperationStatus>,
    pub events: Vec<EventView>,
    pub anomalies: Vec<AnomalyView>,
    pub history: Vec<TrendSample>,
    pub management: ManagementView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterView {
    pub name: String,
    pub namespace: String,
    pub phase: String,
    pub message: String,
    pub desired_brokers: i32,
    pub ready_brokers: i32,
    pub active_storage_feature_level: u32,
    pub observed_generation: Option<i64>,
    pub generation: Option<i64>,
    pub spec: Value,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SummaryView {
    pub stored_messages: u64,
    pub depth: u64,
    pub in_flight: u64,
    pub deferred: u64,
    pub connections: i64,
    pub publish_per_second: f64,
    pub deliver_per_second: f64,
    pub finish_per_second: f64,
    pub publish_bytes_per_second: f64,
    pub retry_total: u64,
    pub dead_letter_total: u64,
    pub throttled_total: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BrokerView {
    pub name: String,
    pub node_name: String,
    pub pod_ip: String,
    pub phase: String,
    pub ready: bool,
    pub restarts: i32,
    pub image: String,
    pub image_id: String,
    pub started_at: Option<String>,
    pub pvc: Option<PvcView>,
    pub observation: Option<BrokerObservation>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PvcView {
    pub name: String,
    pub phase: String,
    pub requested: String,
    pub capacity: String,
    pub storage_class: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TopicView {
    pub name: String,
    pub owners: Vec<String>,
    pub paused: bool,
    pub stored_messages: u64,
    pub segment_count: u64,
    pub segment_bytes: u64,
    pub channels: Vec<ChannelView>,
    pub managed_phase: String,
    pub management_revision: u64,
    pub tombstone_until_ms: Option<i64>,
    pub management_error: Option<String>,
    pub resource_uid: String,
    pub resource_version: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ChannelView {
    pub name: String,
    pub owners: Vec<String>,
    pub depth: u64,
    pub in_flight: u64,
    pub deferred: u64,
    pub ack_gap: u64,
    pub paused: bool,
    pub ephemeral: bool,
    pub managed_phase: String,
    pub management_revision: u64,
    pub tombstone_until_ms: Option<i64>,
    pub management_error: Option<String>,
    pub resource_uid: String,
    pub resource_version: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ManagementView {
    pub enabled: bool,
    pub registry_available: bool,
    pub crd_fresh: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageView {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: f64,
    pub segment_count: u64,
    pub segment_bytes: u64,
    pub pressure_brokers: Vec<String>,
    pub fsync: HistogramSnapshot,
    pub group_commit_wait: HistogramSnapshot,
    pub payload_read: HistogramSnapshot,
    pub scrub: HistogramSnapshot,
    pub gc: HistogramSnapshot,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EventView {
    pub at: String,
    pub type_: String,
    pub reason: String,
    pub message: String,
    pub object: String,
    pub count: i32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AnomalyView {
    pub severity: String,
    pub code: String,
    pub subject: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TrendSample {
    pub at_ms: u64,
    pub publish_per_second: f64,
    pub deliver_per_second: f64,
    pub finish_per_second: f64,
    pub publish_bytes_per_second: f64,
    pub depth: u64,
    pub in_flight: u64,
    pub disk_used_percent: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RawCounters {
    pub at_ms: u64,
    pub membership: u64,
    pub publish_messages: u64,
    pub delivered_messages: u64,
    pub finished_messages: u64,
    pub publish_bytes: u64,
}
