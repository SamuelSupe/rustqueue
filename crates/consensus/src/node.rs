mod batcher;
mod data_plane;
mod leadership;
mod membership;
mod read_barrier;

use crate::latency::GroupLatencyMetrics;
use crate::{
    GroupKey, LogStore, MetadataCatalog, Network, NodeId, QueueCommand, QueueResponse, Raft,
    StateMachineRole, StateMachineStore,
};
use batcher::WriteBatcher;
use bytes::Bytes;
use openraft::BasicNode;
use read_barrier::ReadBarrier;
use rustqueue_queue::Broker;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct ConsensusNode {
    raft: Raft,
    client: reqwest::Client,
    broker: Arc<Broker>,
    node_id: NodeId,
    nodes: BTreeMap<NodeId, BasicNode>,
    write_batcher: WriteBatcher,
    metadata: Arc<MetadataCatalog>,
    group_key: GroupKey,
    log_store: LogStore,
    state_machine: Arc<StateMachineStore>,
    isolated: AtomicBool,
    leadership_gate: tokio::sync::RwLock<()>,
    latency: Arc<GroupLatencyMetrics>,
    read_barrier: Arc<ReadBarrier>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FetchRequest {
    pub topic: String,
    pub channel: String,
    pub partition_cursor: usize,
    pub timeout_ms: u64,
    pub max_messages: u16,
    pub max_bytes: u32,
    pub wait_ms: u32,
    #[serde(default)]
    pub partition: Option<u16>,
    #[serde(default)]
    pub expired_before_ns: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FetchResponse {
    pub deliveries: Vec<RemoteDelivery>,
    pub partition_cursor: usize,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RemoteDelivery {
    pub id: u64,
    pub timestamp_ns: i64,
    pub attempts: u16,
    pub body: Bytes,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TouchRequest {
    pub topic: String,
    pub channel: String,
    pub message_id: u64,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReleaseRequest {
    pub topic: String,
    pub channel: String,
    pub message_ids: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChangeMembershipRequest {
    pub voters: BTreeSet<NodeId>,
    pub retain_removed_as_learners: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperationResponse {
    pub error: Option<String>,
}

impl ConsensusNode {
    pub async fn open(
        node_id: NodeId,
        cluster_name: &str,
        nodes: BTreeMap<NodeId, BasicNode>,
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        network: Network,
    ) -> anyhow::Result<Arc<Self>> {
        Self::open_group(
            GroupKey::cell_metadata(metadata.snapshot().cell_id),
            node_id,
            cluster_name,
            nodes,
            directory,
            broker,
            metadata,
            network,
            StateMachineRole::All,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_group(
        group_key: GroupKey,
        node_id: NodeId,
        cluster_name: &str,
        nodes: BTreeMap<NodeId, BasicNode>,
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        network: Network,
        role: StateMachineRole,
    ) -> anyhow::Result<Arc<Self>> {
        let client = network.client().clone();
        let configuration = openraft::Config {
            cluster_name: cluster_name.into(),
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100_000),
            max_in_snapshot_log_to_keep: 10_000,
            // The network returns OpenRaft PartialSuccess at the byte boundary,
            // so small entries catch up in wide batches while a large publish
            // entry can still travel alone within the 80 MiB endpoint limit.
            max_payload_entries: 64,
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            ..Default::default()
        }
        .validate()?;
        let directory = directory.as_ref();
        let latency = Arc::new(GroupLatencyMetrics::default());
        let log_store = LogStore::open_with_metrics(
            directory.join("raft-log"),
            100 * 1024 * 1024,
            Arc::clone(&latency),
        )?;
        let recovered_entries = log_store.recovered_entries_with_payloads().await?;
        let retained_log_store = log_store.clone();
        let state_machine = StateMachineStore::open_for_group_with_log_and_metrics(
            directory.join("raft-state"),
            Arc::clone(&broker),
            Arc::clone(&metadata),
            role,
            recovered_entries,
            retained_log_store.clone(),
            Arc::clone(&latency),
        )?;
        let raft = Raft::new(
            node_id,
            Arc::new(configuration),
            network,
            log_store,
            Arc::clone(&state_machine),
        )
        .await?;
        let write_batcher = WriteBatcher::new(raft.clone(), Arc::clone(&latency));
        Ok(Arc::new(Self {
            raft,
            client,
            broker,
            node_id,
            nodes,
            write_batcher,
            metadata,
            group_key,
            log_store: retained_log_store,
            state_machine,
            isolated: AtomicBool::new(false),
            leadership_gate: tokio::sync::RwLock::new(()),
            latency,
            read_barrier: ReadBarrier::new(),
        }))
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn group_key(&self) -> GroupKey {
        self.group_key
    }

    pub fn is_isolated(&self) -> bool {
        self.isolated.load(Ordering::Acquire)
    }

    pub async fn isolate(&self) {
        if !self.isolated.swap(true, Ordering::AcqRel) {
            let _ = self.raft.shutdown().await;
        }
    }

    pub fn metadata(&self) -> &Arc<MetadataCatalog> {
        &self.metadata
    }

    pub(crate) fn latency_metrics(&self) -> &GroupLatencyMetrics {
        &self.latency
    }

    fn node(&self, node_id: NodeId) -> Option<BasicNode> {
        self.nodes.get(&node_id).cloned().or_else(|| {
            self.metadata
                .node(node_id)
                .map(|descriptor| BasicNode::new(descriptor.raft_address))
        })
    }

    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<
        (),
        openraft::error::RaftError<NodeId, openraft::error::InitializeError<NodeId, BasicNode>>,
    > {
        self.raft.initialize(members).await
    }
}
