use crate::{CatalogState, CellId, FederationRoot, NodeId, ScopedFeatureLevels};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

pub const METADATA_GROUP_ID: u64 = 0;
const FIRST_GROUP_ID: u64 = 1;
const FIRST_SLOT: u32 = 1;
const MAX_SLOT: u32 = u16::MAX as u32;

fn default_feature_level() -> u64 {
    crate::FEATURE_LEVEL_BASELINE
}

fn default_cell_id() -> CellId {
    CellId::BOOTSTRAP
}

fn default_wire_incarnation() -> u32 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub raft_address: String,
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub tls_server_name: String,
    pub failure_domain: String,
    #[serde(default)]
    pub peer_id: Option<String>,
    #[serde(default = "default_cell_id")]
    pub cell_id: CellId,
    #[serde(default)]
    pub federation_router: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicState {
    Preparing,
    Active,
    Deleting,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionLifecycle {
    Preparing,
    #[default]
    Active,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PartitionDescriptor {
    pub group_id: u64,
    /// Immutable namespace of `group_id`; unlike `home_cell`, this never
    /// changes when the partition is migrated.
    pub origin_cell: CellId,
    pub number: u16,
    pub slot: u16,
    pub replication_factor: u8,
    pub replicas: BTreeSet<NodeId>,
    pub leader_hint: Option<NodeId>,
    #[serde(default)]
    pub lifecycle: PartitionLifecycle,
    #[serde(default)]
    pub operation_id: Option<u64>,
    #[serde(default = "default_cell_id")]
    pub home_cell: CellId,
    #[serde(default = "default_wire_incarnation")]
    pub wire_incarnation: u32,
}

impl PartitionDescriptor {
    pub fn global_id(&self) -> crate::GlobalGroupId {
        crate::GlobalGroupId {
            cell: self.origin_cell,
            local: self.group_id,
        }
    }

    pub fn group_key(&self) -> crate::GroupKey {
        crate::GroupKey::Partition(self.global_id())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TopicDescriptor {
    pub name: String,
    pub state: TopicState,
    pub replication_factor: u8,
    pub partitions: Vec<PartitionDescriptor>,
    pub channels: BTreeMap<String, ChannelDescriptor>,
    pub next_channel_generation: u64,
    pub paused: bool,
    pub topology_generation: u64,
    pub key_routing_slots: Vec<u16>,
    pub channel_catalog_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelDescriptor {
    pub name: String,
    pub generation: u64,
    pub state: ChannelLifecycle,
    pub ephemeral: bool,
    #[serde(default)]
    pub leases: BTreeMap<u64, i64>,
    #[serde(default)]
    pub lease_started: bool,
    #[serde(default)]
    pub paused: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChannelLifecycle {
    Preparing,
    Active,
    Deleting,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    ExpandPartitions {
        topic: String,
        source_partitions: u16,
        target_partitions: u16,
        partition_groups: Vec<crate::GlobalGroupId>,
    },
    RebalanceGroup {
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
    },
    DrainNode {
        node_id: NodeId,
    },
    RepairReplica {
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    },
    TransferLeader {
        group: crate::GroupKey,
        node_id: NodeId,
    },
    ReplaceOfflineReplica {
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
        replacement: Option<NodeId>,
    },
    ReplaceMetadataVoter {
        node_id: NodeId,
        replacement: Option<NodeId>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Running,
    Paused,
    NeedsOperator,
    Completed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    Reserved,
    CreateGroups,
    InitMembership,
    ChannelBarriers,
    ArmGroups,
    ActivateRouting,
    Planned,
    TransferLeader,
    AddLearner,
    CatchUp,
    JointConsensus,
    RemoveOld,
    Retire,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DrainGroupPlan {
    pub group_id: crate::GlobalGroupId,
    pub voters: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DrainProgress {
    pub groups: Vec<DrainGroupPlan>,
    pub current: usize,
    pub metadata_replacement: Option<NodeId>,
    pub metadata_completed: bool,
}

impl DrainProgress {
    pub fn current_group_id(&self) -> Option<crate::GlobalGroupId> {
        self.groups.get(self.current).map(|group| group.group_id)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationProgress {
    #[default]
    None,
    Drain(DrainProgress),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MaintenanceOperation {
    pub id: u64,
    pub kind: OperationKind,
    pub state: OperationState,
    pub phase: OperationPhase,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub error: Option<String>,
    #[serde(default)]
    pub progress: OperationProgress,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MaintenanceLease {
    pub expires_at_ms: i64,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct NodeHealthRecord {
    pub available: bool,
    pub consecutive_failures: u16,
    pub unavailable_since_ms: Option<i64>,
    pub stable_since_ms: Option<i64>,
    pub last_observed_ms: i64,
    pub disk_used_percent: u8,
    pub disk_free_bytes: u64,
    pub storage_eligible: bool,
}

impl Default for NodeHealthRecord {
    fn default() -> Self {
        Self {
            available: false,
            consecutive_failures: 0,
            unavailable_since_ms: None,
            stable_since_ms: None,
            last_observed_ms: 0,
            disk_used_percent: 0,
            disk_free_bytes: u64::MAX,
            storage_eligible: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ClusterMetadata {
    #[serde(default = "default_cell_id")]
    pub cell_id: CellId,
    pub nodes: BTreeMap<NodeId, NodeDescriptor>,
    #[serde(default)]
    pub drained_nodes: BTreeSet<NodeId>,
    pub topics: BTreeMap<String, TopicDescriptor>,
    pub next_group_id: u64,
    pub next_slot: u32,
    pub epoch: u64,
    #[serde(default)]
    pub routing_epoch: u64,
    pub next_operation_id: u64,
    pub operations: BTreeMap<u64, MaintenanceOperation>,
    pub automation_enabled: bool,
    pub maintenance_nodes: BTreeMap<NodeId, MaintenanceLease>,
    pub node_health: BTreeMap<NodeId, NodeHealthRecord>,
    #[serde(default = "default_feature_level")]
    pub active_feature_level: u64,
    #[serde(default)]
    pub federation_root: FederationRoot,
    #[serde(default)]
    pub catalog: CatalogState,
    #[serde(default)]
    pub scoped_feature_levels: ScopedFeatureLevels,
}

pub struct MetadataCatalog {
    state: RwLock<ClusterMetadata>,
    routes: RwLock<MetadataRoutes>,
    default_partitions: u16,
    default_replication_factor: u8,
    max_home_cells_per_topic: usize,
}

mod catalog;
mod channel;
mod expansion;
mod federated_partition;
mod federation;
mod operations;
mod placement;
mod routes;

use routes::MetadataRoutes;
pub(crate) use routes::TopicRoute;

#[cfg(test)]
mod tests;
