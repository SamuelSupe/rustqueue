mod ack;
mod automation;
mod automation_api;
mod automation_plan;
mod catalog_route;
mod channel_lifecycle;
mod control_plane;
mod delivery;
mod discovery;
mod disk;
mod disk_pressure;
mod drain_operation;
mod expansion;
mod federation_channel;
mod federation_data;
mod federation_metrics;
mod federation_migration;
mod fetch_scheduler;
mod group_api;
mod groups;
mod health;
mod leader_route;
mod membership_operation;
mod metrics;
mod migration_executor;
mod migration_transport;
mod operation_error;
mod operations;
mod queue;
mod retention;
mod root_route;
mod routing;
mod scrub;
mod stats;
mod topic_deletion;

pub use ack::AckWriteResult;
pub use automation::RebalancePlanItem;
pub use control_plane::ControlPlaneOptions;
pub use federation_channel::{FederationChannelAction, FederationChannelForward};
pub use federation_data::{
    FederationFetchForward, FederationForwardError, FederationReadyForward,
    FederationReleaseForward, FederationTouchForward, FederationWriteForward,
};
pub use federation_migration::{
    FederationMigrationAction, FederationMigrationForward, FederationMigrationResponse,
    MigrationReplicaStatus, MigrationReplicaStatusResponse,
};
pub use retention::{dead_letter_topic, RetentionOptions};
pub use stats::{ClusterStats, GroupStatsResponse};

use leader_route::LeaderRoutes;
use routing::*;

use crate::clock::{wall_time_ms, ClockGuard};
use crate::latency::LatencyHistogram;
use crate::ChannelLifecycle;
use crate::ClockStatus;
use crate::{
    BasicNode, ConsensusNode, FetchRequest, FetchResponse, GroupKey, MetadataCatalog, Network,
    NodeId, OperationResponse, PartitionDescriptor, QueueCommand, QueueResponse, ReleaseRequest,
    RoutedResponse, StateMachineRole, TouchRequest, INTERNAL_CATALOG_FRAME_BYTES,
    INTERNAL_FETCH_RESPONSE_BYTES, INTERNAL_SMALL_FRAME_BYTES, INTERNAL_WRITE_FRAME_BYTES,
    INTERNAL_WRITE_RESPONSE_BYTES,
};
use rustqueue_queue::Broker;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnsureGroupRequest {
    pub topic: String,
    pub partition: PartitionDescriptor,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InitializeGroupRequest {
    pub voters: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RebalanceGroupRequest {
    pub voters: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RebalanceStepRequest {
    pub voters: BTreeSet<NodeId>,
    pub phase: crate::OperationPhase,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RepairReplicaRequest {
    pub node_id: NodeId,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ScrubResult {
    pub records_checked: usize,
    pub replicas_repaired: usize,
}

#[derive(Clone, Debug)]
pub struct AutomationOptions {
    pub enabled: bool,
    pub node_stabilization_seconds: u64,
    pub node_down_grace_seconds: u64,
    pub group_cooldown_seconds: u64,
    pub max_concurrent_migrations: usize,
    pub max_migrations_per_node: usize,
    pub operation_history_limit: usize,
    pub auto_replace_metadata: bool,
    pub disk_high_watermark_percent: u8,
    pub disk_low_watermark_percent: u8,
    pub min_free_bytes: u64,
    pub protective_eviction_enabled: bool,
    pub disk_pressure_grace_seconds: u64,
}

impl Default for AutomationOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            node_stabilization_seconds: 60,
            node_down_grace_seconds: 600,
            group_cooldown_seconds: 600,
            max_concurrent_migrations: 2,
            max_migrations_per_node: 1,
            operation_history_limit: 1000,
            auto_replace_metadata: true,
            disk_high_watermark_percent: 85,
            disk_low_watermark_percent: 75,
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            protective_eviction_enabled: true,
            disk_pressure_grace_seconds: 60,
        }
    }
}

pub struct ClusterRuntime {
    node_id: NodeId,
    cluster_name: String,
    directory: PathBuf,
    broker: Arc<Broker>,
    metadata: Arc<MetadataCatalog>,
    nodes: BTreeMap<NodeId, BasicNode>,
    client: reqwest::Client,
    snapshot_client: reqwest::Client,
    metadata_group: Arc<ConsensusNode>,
    control: Option<control_plane::ControlPlane>,
    groups: RwLock<BTreeMap<GroupKey, Arc<ConsensusNode>>>,
    leader_routes: LeaderRoutes,
    healthy_nodes: RwLock<BTreeSet<NodeId>>,
    ensure_lock: Mutex<()>,
    topic_delete_lock: Mutex<()>,
    fetch_scheduler: fetch_scheduler::FetchScheduler,
    health_cache: Mutex<health::HealthCache>,
    stats_cache: Mutex<stats::StatsCache>,
    retention_cursor: AtomicU64,
    repair_lock: Mutex<()>,
    clock: ClockGuard,
    scrub_records: AtomicU64,
    replica_repairs: AtomicU64,
    retention_moved: AtomicU64,
    retention_failures: AtomicU64,
    forward_latency: LatencyHistogram,
    repair_latency: LatencyHistogram,
    federation_metrics: federation_metrics::FederationMetrics,
    automation: AutomationOptions,
    retention: RetentionOptions,
    storage_eligible: AtomicBool,
    accepting: AtomicBool,
    observed_feature_floor: AtomicU64,
    disk_pressure_since_ms: AtomicU64,
    disk_gc_cursor: AtomicU64,
    protective_evicted_messages: AtomicU64,
    protective_evicted_bytes: AtomicU64,
}

impl ClusterRuntime {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        node_id: NodeId,
        cluster_name: &str,
        nodes: BTreeMap<NodeId, BasicNode>,
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        client: reqwest::Client,
        snapshot_client: reqwest::Client,
        control_options: ControlPlaneOptions,
        automation: AutomationOptions,
        retention: RetentionOptions,
    ) -> anyhow::Result<Arc<Self>> {
        let directory = directory.as_ref().to_path_buf();
        let metadata_key = GroupKey::cell_metadata(metadata.snapshot().cell_id);
        let metadata_group = ConsensusNode::open_group(
            metadata_key,
            node_id,
            &format!("{cluster_name}-metadata"),
            nodes.clone(),
            directory
                .join("groups")
                .join(metadata_key.storage_component()),
            Arc::clone(&broker),
            Arc::clone(&metadata),
            Network::for_group_with_snapshot(client.clone(), snapshot_client.clone(), metadata_key),
            StateMachineRole::CellMetadata,
        )
        .await?;
        let control = if control_options.enabled {
            Some(
                control_plane::ControlPlane::open(
                    node_id,
                    cluster_name,
                    &directory,
                    Arc::clone(&broker),
                    client.clone(),
                    snapshot_client.clone(),
                    control_options,
                )
                .await?,
            )
        } else {
            None
        };
        let mut groups = BTreeMap::from([(metadata_key, Arc::clone(&metadata_group))]);
        if let Some(control) = &control {
            groups.extend(control.hosted_groups());
        }
        let storage_eligible = disk::probe(
            &directory,
            automation.disk_high_watermark_percent,
            automation.disk_low_watermark_percent,
            automation.min_free_bytes,
            true,
        )
        .map(|status| status.eligible)
        .unwrap_or(false);
        Ok(Arc::new(Self {
            node_id,
            cluster_name: cluster_name.to_owned(),
            directory,
            broker,
            metadata,
            nodes,
            client,
            snapshot_client,
            metadata_group: Arc::clone(&metadata_group),
            control,
            groups: RwLock::new(groups),
            leader_routes: LeaderRoutes::default(),
            healthy_nodes: RwLock::new(BTreeSet::from([node_id])),
            ensure_lock: Mutex::new(()),
            topic_delete_lock: Mutex::new(()),
            repair_lock: Mutex::new(()),
            fetch_scheduler: fetch_scheduler::FetchScheduler::default(),
            health_cache: Mutex::new(health::HealthCache::default()),
            stats_cache: Mutex::new(stats::StatsCache::default()),
            retention_cursor: AtomicU64::new(0),
            clock: ClockGuard::default(),
            scrub_records: AtomicU64::new(0),
            replica_repairs: AtomicU64::new(0),
            retention_moved: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
            forward_latency: LatencyHistogram::default(),
            repair_latency: LatencyHistogram::default(),
            federation_metrics: federation_metrics::FederationMetrics::default(),
            automation,
            retention,
            storage_eligible: AtomicBool::new(storage_eligible),
            accepting: AtomicBool::new(true),
            observed_feature_floor: AtomicU64::new(crate::FEATURE_LEVEL_BASELINE),
            disk_pressure_since_ms: AtomicU64::new(0),
            disk_gc_cursor: AtomicU64::new(0),
            protective_evicted_messages: AtomicU64::new(0),
            protective_evicted_bytes: AtomicU64::new(0),
        }))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn metadata(&self) -> &Arc<MetadataCatalog> {
        &self.metadata
    }

    pub fn clock_status(&self) -> ClockStatus {
        self.clock.status()
    }

    pub fn disk_status(&self) -> anyhow::Result<disk::DiskStatus> {
        let current = self.storage_eligible.load(Ordering::Acquire);
        let status = disk::probe(
            &self.directory,
            self.automation.disk_high_watermark_percent,
            self.automation.disk_low_watermark_percent,
            self.automation.min_free_bytes,
            current,
        )?;
        self.storage_eligible
            .store(status.eligible, Ordering::Release);
        Ok(status)
    }

    pub fn ensure_clock_safe(&self) -> Result<(), String> {
        self.clock.ensure_safe()
    }

    pub fn ensure_write_safe(&self) -> Result<(), String> {
        self.ensure_clock_safe()?;
        self.storage_eligible
            .load(Ordering::Acquire)
            .then_some(())
            .ok_or_else(|| "storage is above the configured disk watermark".to_owned())
    }

    pub fn storage_eligible(&self) -> bool {
        self.storage_eligible.load(Ordering::Acquire)
    }

    pub fn disk_pressure_since_ms(&self) -> Option<u64> {
        match self.disk_pressure_since_ms.load(Ordering::Acquire) {
            0 => None,
            since => Some(since),
        }
    }

    pub fn protective_eviction_enabled(&self) -> bool {
        self.automation.protective_eviction_enabled
    }

    pub fn active_feature_level(&self) -> u64 {
        self.metadata.snapshot().active_feature_level
    }

    pub fn observed_feature_floor(&self) -> u64 {
        self.observed_feature_floor.load(Ordering::Acquire)
    }

    pub fn ensure_feature_level(&self, required: u64) -> Result<(), String> {
        let active = self.active_feature_level();
        (active >= required).then_some(()).ok_or_else(|| {
            format!("cluster feature level {active} does not satisfy required level {required}")
        })
    }

    pub async fn check_clock_once(&self) -> anyhow::Result<ClockStatus> {
        self.clock.observe_local();
        let mut healthy_nodes = BTreeSet::from([self.node_id]);
        let mut feature_floor = crate::CURRENT_FEATURE_LEVEL;
        let mut disk_statuses = BTreeMap::new();
        let local_disk = self.disk_status().unwrap_or(disk::DiskStatus {
            used_percent: 100,
            free_bytes: 0,
            eligible: false,
        });
        disk_statuses.insert(self.node_id, local_disk);
        for (node_id, node) in self.nodes_snapshot() {
            if node_id == self.node_id {
                continue;
            }
            let before = wall_time_ms();
            let response = self
                .client
                .get(format!("{}/raft/time", node.addr.trim_end_matches('/')))
                .send()
                .await;
            let after = wall_time_ms();
            let Ok(response) = response else {
                feature_floor = feature_floor.min(crate::FEATURE_LEVEL_BASELINE);
                continue;
            };
            if !response.status().is_success() {
                feature_floor = feature_floor.min(crate::FEATURE_LEVEL_BASELINE);
                continue;
            }
            let value: serde_json::Value = response.json().await?;
            let compatible = value["data_format"].as_u64()
                == Some(rustqueue_storage::DATA_FORMAT_VERSION as u64)
                && value["command_schema"].as_u64() == Some(crate::COMMAND_SCHEMA_VERSION as u64)
                && value["rpc_format"].as_u64() == Some(crate::INTERNAL_RPC_FORMAT as u64)
                && value["rpc_version"].as_u64() == Some(crate::INTERNAL_RPC_VERSION as u64);
            if !compatible {
                feature_floor = feature_floor.min(crate::FEATURE_LEVEL_BASELINE);
                tracing::warn!(
                    peer = node_id,
                    "peer is incompatible with the stable rolling-upgrade contract"
                );
                continue;
            }
            feature_floor = feature_floor.min(crate::feature::advertised_feature_level(&value));
            if let Some(peer) = value["wall_time_ms"].as_u64() {
                self.clock.observe_peer(peer, before, after);
            }
            if value["clock_healthy"].as_bool().unwrap_or(false)
                && value["gateway_ready"].as_bool().unwrap_or(false)
            {
                healthy_nodes.insert(node_id);
            }
            disk_statuses.insert(
                node_id,
                disk::DiskStatus {
                    used_percent: value["disk"]["used_percent"].as_u64().unwrap_or(100) as u8,
                    free_bytes: value["disk"]["free_bytes"].as_u64().unwrap_or_default(),
                    eligible: value["disk"]["eligible"].as_bool().unwrap_or(false),
                },
            );
        }
        let status = self.clock.status();
        if !status.healthy || !self.gateway_ready() {
            healthy_nodes.remove(&self.node_id);
        }
        if !status.healthy {
            self.evacuate_local_leaders().await;
        }
        if let Err(error) = self
            .persist_health_observations(
                &healthy_nodes,
                &disk_statuses,
                wall_time_ms().min(i64::MAX as u64) as i64,
            )
            .await
        {
            tracing::debug!(%error, "node health observation was not persisted");
        }
        if self.control.is_some() {
            if let Err(error) = self.root_snapshot_fresh().await {
                tracing::debug!(%error, "Root route cache refresh will retry");
            }
        }
        *self.healthy_nodes.write().await = healthy_nodes;
        self.observed_feature_floor
            .store(feature_floor, Ordering::Release);
        self.try_activate_feature_level(feature_floor).await;
        Ok(status)
    }

    pub async fn healthy_node_ids(&self) -> BTreeSet<NodeId> {
        self.healthy_nodes.read().await.clone()
    }

    pub async fn set_node_drained(&self, node_id: NodeId, drained: bool) -> anyhow::Result<()> {
        let response = self
            .metadata_group()
            .write(QueueCommand::SetNodeDrained { node_id, drained })
            .await?;
        ensure_response(&response)
    }

    pub fn gateway_ready(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
            && crate::CURRENT_FEATURE_LEVEL >= self.active_feature_level()
            && self.observed_feature_floor() >= self.active_feature_level()
            && !self.metadata_group.is_isolated()
            && self
                .metadata_group
                .raft()
                .metrics()
                .borrow()
                .current_leader
                .is_some()
    }

    async fn try_activate_feature_level(&self, feature_level: u64) {
        let active = self.active_feature_level();
        if feature_level <= active
            || self.metadata_group.raft().metrics().borrow().current_leader != Some(self.node_id)
        {
            return;
        }
        if let Err(error) = self
            .metadata_group
            .write(QueueCommand::ActivateFeatureLevel { feature_level })
            .await
            .and_then(|response| ensure_response(&response))
        {
            tracing::debug!(%error, feature_level, "feature-level activation will retry");
        }
    }

    pub fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.begin_shutdown();
        self.evacuate_local_leaders().await;
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        for group in groups {
            group.raft().shutdown().await?;
        }
        Ok(())
    }

    pub(crate) fn scrub_record_count(&self) -> u64 {
        self.scrub_records.load(Ordering::Relaxed)
    }

    pub(crate) fn replica_repair_count(&self) -> u64 {
        self.replica_repairs.load(Ordering::Relaxed)
    }
}
