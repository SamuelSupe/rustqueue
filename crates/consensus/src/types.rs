use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub type NodeId = u64;

/// Frozen baseline for Raft command persistence and replication.
///
/// The envelope is intentionally made only of fixed-width scalar fields plus
/// the command. Compatible releases may append `QueueCommand` variants, but
/// must not reorder existing variants or change their stable tags.
pub const COMMAND_SCHEMA_VERSION: u16 = 1;

pub const COMMAND_SCOPE_ANY: u8 = 0;
pub const COMMAND_SCOPE_ROOT: u8 = 1;
pub const COMMAND_SCOPE_CATALOG: u8 = 2;
pub const COMMAND_SCOPE_CELL_METADATA: u8 = 3;
pub const COMMAND_SCOPE_PARTITION: u8 = 4;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandEnvelope {
    pub schema_version: u16,
    pub scope: u8,
    pub kind: u16,
    pub required_feature_level: u64,
    pub command: QueueCommand,
}

impl CommandEnvelope {
    pub fn new(command: QueueCommand) -> Self {
        Self {
            schema_version: COMMAND_SCHEMA_VERSION,
            scope: command.scope(),
            kind: command.kind(),
            required_feature_level: command.required_feature_level(),
            command,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != COMMAND_SCHEMA_VERSION {
            return Err(format!(
                "unsupported command schema version {}",
                self.schema_version
            ));
        }
        if self.scope != self.command.scope() {
            return Err("command envelope scope does not match its payload".into());
        }
        if self.kind != self.command.kind() {
            return Err("command envelope kind does not match its payload".into());
        }
        if self.required_feature_level != self.command.required_feature_level() {
            return Err("command envelope feature level does not match its payload".into());
        }
        if self.required_feature_level > crate::CURRENT_FEATURE_LEVEL {
            return Err(format!(
                "command requires unsupported feature level {}",
                self.required_feature_level
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueCommand {
    Batch {
        commands: Vec<QueueCommand>,
    },
    Publish {
        operation_id: u64,
        topic: String,
        bodies: Vec<Bytes>,
        timestamp_ns: i64,
        available_at_ms: i64,
        partition: Option<u16>,
        routing_key: Option<Vec<u8>>,
    },
    CreateTopic {
        topic: String,
        partitions: Option<u16>,
        replication_factor: Option<u8>,
    },
    DeleteTopic {
        topic: String,
    },
    PrepareDeleteTopic {
        topic: String,
    },
    CompleteDeleteTopic {
        topic: String,
    },
    CreateChannel {
        topic: String,
        channel: String,
    },
    DeleteChannel {
        topic: String,
        channel: String,
    },
    EmptyTopic {
        topic: String,
    },
    EmptyChannel {
        topic: String,
        channel: String,
    },
    PauseChannel {
        topic: String,
        channel: String,
        paused: bool,
    },
    SetChannelMetadataPaused {
        topic: String,
        channel: String,
        paused: bool,
    },
    PauseTopic {
        topic: String,
        paused: bool,
    },
    PrepareChannel {
        topic: String,
        channel: String,
    },
    ActivateChannel {
        topic: String,
        channel: String,
        generation: u64,
    },
    PrepareDeleteChannel {
        topic: String,
        channel: String,
    },
    CompleteDeleteChannel {
        topic: String,
        channel: String,
        generation: u64,
    },
    UpdatePartitionReplicas {
        group_id: crate::GlobalGroupId,
        replicas: BTreeSet<NodeId>,
    },
    RegisterNode {
        descriptor: crate::NodeDescriptor,
    },
    SetNodeDrained {
        node_id: NodeId,
        drained: bool,
    },
    ReservePartitionExpansion {
        topic: String,
        target_partitions: u16,
        max_partitions: u16,
        now_ms: i64,
    },
    AdvancePartitionExpansion {
        operation_id: u64,
        phase: crate::OperationPhase,
        state: crate::OperationState,
        now_ms: i64,
        error: Option<String>,
    },
    ActivatePartitionExpansion {
        operation_id: u64,
        expected_channel_revision: u64,
        now_ms: i64,
    },
    CancelPartitionExpansion {
        operation_id: u64,
        now_ms: i64,
    },
    SetOperationPaused {
        operation_id: u64,
        paused: bool,
    },
    SetAutomationEnabled {
        enabled: bool,
    },
    SetMaintenance {
        node_id: NodeId,
        lease: Option<crate::MaintenanceLease>,
    },
    CreateOperation {
        kind: crate::OperationKind,
        now_ms: i64,
        history_limit: usize,
    },
    UpdateOperation {
        operation_id: u64,
        phase: crate::OperationPhase,
        state: crate::OperationState,
        now_ms: i64,
        error: Option<String>,
        progress: Option<crate::OperationProgress>,
    },
    ObserveNodeHealth {
        node_id: NodeId,
        healthy: bool,
        disk_used_percent: u8,
        disk_free_bytes: u64,
        storage_eligible: bool,
        now_ms: i64,
    },
    RenewEphemeralLease {
        topic: String,
        channel: String,
        lease_id: u64,
        expires_at_ms: i64,
    },
    ReleaseEphemeralLease {
        topic: String,
        channel: String,
        lease_id: u64,
    },
    Finish {
        topic: String,
        channel: String,
        message_id: u64,
    },
    Requeue {
        topic: String,
        channel: String,
        message_id: u64,
        available_at_ms: i64,
    },
    ActivateFeatureLevel {
        feature_level: u64,
    },
    RegisterFederationNode {
        node: crate::FederationNode,
    },
    ApplyRootAction {
        action: crate::RootAction,
        now_ms: i64,
        policy: crate::CellFormationPolicy,
    },
    BeginPartitionMigration {
        topic: String,
        partition: crate::GlobalGroupId,
        target: crate::CellId,
        now_ms: i64,
        max_home_cells: usize,
    },
    AdvancePartitionMigration {
        operation_id: u64,
        expected: crate::PartitionMigrationPhase,
        next: crate::PartitionMigrationPhase,
        observed_lag_entries: u64,
        now_ms: i64,
        max_home_cells: usize,
    },
    ActivateBucketMove {
        topic: String,
        start: u16,
        end: u16,
        target: crate::GlobalGroupId,
        expected_epoch: u64,
    },
    ActivateScopedFeature {
        activation: crate::FeatureActivation,
        observed_protocol_floor: u32,
    },
    ProtectiveEvict {
        operation_id: u64,
        topic: String,
        partition: u16,
        through_message_id: u64,
    },
    SyncCatalogTopic {
        descriptor: crate::TopicDescriptor,
    },
    RemoveCatalogTopic {
        topic: String,
    },
    PrepareCatalogChannel {
        topic: String,
        channel: String,
    },
    UpdateCatalogChannel {
        topic: String,
        channel: String,
        generation: u64,
        state: crate::ChannelLifecycle,
        paused: bool,
    },
    RemoveCatalogChannel {
        topic: String,
        channel: String,
        generation: u64,
    },
    InstallChannelMetadata {
        topic: String,
        descriptor: crate::ChannelDescriptor,
    },
    UpsertFederatedPartition {
        template: crate::TopicDescriptor,
        partition: crate::PartitionDescriptor,
    },
    MarkPartitionMigrationNeedsOperator {
        operation_id: u64,
        error: String,
        now_ms: i64,
    },
    RenewCatalogEphemeralLease {
        topic: String,
        channel: String,
        lease_id: u64,
        expires_at_ms: i64,
    },
    ReleaseCatalogEphemeralLease {
        topic: String,
        channel: String,
        lease_id: u64,
        now_ms: i64,
    },
    ExpireCatalogEphemeralLeases {
        now_ms: i64,
    },
    BeginCatalogTopicDeletion {
        topic: String,
    },
}

impl QueueCommand {
    /// Stable, append-only command tags. These values are persisted and must
    /// never be reassigned to another command.
    pub fn kind(&self) -> u16 {
        match self {
            Self::Batch { .. } => 1,
            Self::Publish { .. } => 2,
            Self::CreateTopic { .. } => 3,
            Self::DeleteTopic { .. } => 4,
            Self::PrepareDeleteTopic { .. } => 5,
            Self::CompleteDeleteTopic { .. } => 6,
            Self::CreateChannel { .. } => 7,
            Self::DeleteChannel { .. } => 8,
            Self::EmptyTopic { .. } => 9,
            Self::EmptyChannel { .. } => 10,
            Self::PauseChannel { .. } => 11,
            Self::SetChannelMetadataPaused { .. } => 12,
            Self::PauseTopic { .. } => 13,
            Self::PrepareChannel { .. } => 14,
            Self::ActivateChannel { .. } => 15,
            Self::PrepareDeleteChannel { .. } => 16,
            Self::CompleteDeleteChannel { .. } => 17,
            Self::UpdatePartitionReplicas { .. } => 18,
            Self::RegisterNode { .. } => 19,
            Self::SetNodeDrained { .. } => 20,
            Self::ReservePartitionExpansion { .. } => 21,
            Self::AdvancePartitionExpansion { .. } => 22,
            Self::ActivatePartitionExpansion { .. } => 23,
            Self::CancelPartitionExpansion { .. } => 24,
            Self::SetOperationPaused { .. } => 25,
            Self::SetAutomationEnabled { .. } => 26,
            Self::SetMaintenance { .. } => 27,
            Self::CreateOperation { .. } => 28,
            Self::UpdateOperation { .. } => 29,
            Self::ObserveNodeHealth { .. } => 30,
            Self::RenewEphemeralLease { .. } => 31,
            Self::ReleaseEphemeralLease { .. } => 32,
            Self::Finish { .. } => 33,
            Self::Requeue { .. } => 34,
            Self::ActivateFeatureLevel { .. } => 35,
            Self::RegisterFederationNode { .. } => 36,
            Self::ApplyRootAction { .. } => 37,
            Self::BeginPartitionMigration { .. } => 38,
            Self::AdvancePartitionMigration { .. } => 39,
            Self::ActivateBucketMove { .. } => 40,
            Self::ActivateScopedFeature { .. } => 41,
            Self::ProtectiveEvict { .. } => 42,
            Self::SyncCatalogTopic { .. } => 43,
            Self::RemoveCatalogTopic { .. } => 44,
            Self::PrepareCatalogChannel { .. } => 45,
            Self::UpdateCatalogChannel { .. } => 46,
            Self::RemoveCatalogChannel { .. } => 47,
            Self::InstallChannelMetadata { .. } => 48,
            Self::UpsertFederatedPartition { .. } => 49,
            Self::MarkPartitionMigrationNeedsOperator { .. } => 50,
            Self::RenewCatalogEphemeralLease { .. } => 51,
            Self::ReleaseCatalogEphemeralLease { .. } => 52,
            Self::ExpireCatalogEphemeralLeases { .. } => 53,
            Self::BeginCatalogTopicDeletion { .. } => 54,
        }
    }

    pub fn scope(&self) -> u8 {
        match self {
            Self::Batch { commands } => commands
                .first()
                .map(QueueCommand::scope)
                .filter(|scope| commands.iter().all(|command| command.scope() == *scope))
                .unwrap_or(COMMAND_SCOPE_ANY),
            Self::RegisterFederationNode { .. } | Self::ApplyRootAction { .. } => {
                COMMAND_SCOPE_ROOT
            }
            Self::BeginPartitionMigration { .. }
            | Self::AdvancePartitionMigration { .. }
            | Self::ActivateBucketMove { .. }
            | Self::ActivateScopedFeature { .. }
            | Self::SyncCatalogTopic { .. }
            | Self::RemoveCatalogTopic { .. }
            | Self::PrepareCatalogChannel { .. }
            | Self::UpdateCatalogChannel { .. }
            | Self::RemoveCatalogChannel { .. } => COMMAND_SCOPE_CATALOG,
            Self::MarkPartitionMigrationNeedsOperator { .. }
            | Self::RenewCatalogEphemeralLease { .. }
            | Self::ReleaseCatalogEphemeralLease { .. }
            | Self::ExpireCatalogEphemeralLeases { .. } => COMMAND_SCOPE_CATALOG,
            Self::BeginCatalogTopicDeletion { .. } => COMMAND_SCOPE_CATALOG,
            Self::InstallChannelMetadata { .. } | Self::UpsertFederatedPartition { .. } => {
                COMMAND_SCOPE_CELL_METADATA
            }
            Self::Publish { .. }
            | Self::CreateChannel { .. }
            | Self::DeleteChannel { .. }
            | Self::EmptyTopic { .. }
            | Self::EmptyChannel { .. }
            | Self::PauseChannel { .. }
            | Self::Finish { .. }
            | Self::Requeue { .. }
            | Self::ProtectiveEvict { .. } => COMMAND_SCOPE_PARTITION,
            _ => COMMAND_SCOPE_CELL_METADATA,
        }
    }

    pub fn is_scoped_to(&self, scope: u8) -> bool {
        match self {
            Self::Batch { commands } => commands.iter().all(|command| command.is_scoped_to(scope)),
            command => command.scope() == scope,
        }
    }

    pub fn required_feature_level(&self) -> u64 {
        match self {
            Self::Batch { commands } => commands
                .iter()
                .map(QueueCommand::required_feature_level)
                .max()
                .unwrap_or(crate::FEATURE_LEVEL_BASELINE),
            Self::Publish { bodies, .. } => crate::feature::required_publish_feature(bodies),
            Self::ProtectiveEvict { .. } => crate::FEATURE_LEVEL_PROTECTIVE_EVICTION,
            Self::RegisterFederationNode { .. }
            | Self::ApplyRootAction { .. }
            | Self::ActivateBucketMove { .. }
            | Self::ActivateScopedFeature { .. }
            | Self::SyncCatalogTopic { .. }
            | Self::RemoveCatalogTopic { .. } => crate::FEATURE_LEVEL_FEDERATED_SCHEMA,
            Self::BeginPartitionMigration { .. }
            | Self::AdvancePartitionMigration { .. }
            | Self::PrepareCatalogChannel { .. }
            | Self::UpdateCatalogChannel { .. }
            | Self::RemoveCatalogChannel { .. }
            | Self::MarkPartitionMigrationNeedsOperator { .. } => {
                crate::FEATURE_LEVEL_HOME_CELL_ROUTING
            }
            Self::RenewCatalogEphemeralLease { .. }
            | Self::ReleaseCatalogEphemeralLease { .. }
            | Self::ExpireCatalogEphemeralLeases { .. } => crate::FEATURE_LEVEL_HOME_CELL_ROUTING,
            Self::BeginCatalogTopicDeletion { .. } => crate::FEATURE_LEVEL_HOME_CELL_ROUTING,
            Self::InstallChannelMetadata { .. } | Self::UpsertFederatedPartition { .. } => {
                crate::FEATURE_LEVEL_HOME_CELL_ROUTING
            }
            _ => crate::FEATURE_LEVEL_BASELINE,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QueueResponse {
    pub message_ids: Vec<u64>,
    pub error: Option<String>,
    #[serde(default)]
    pub results: Vec<QueueResponse>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = CommandEnvelope,
        R = QueueResponse,
        Node = openraft::BasicNode,
        SnapshotData = crate::SnapshotData,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_envelope_has_a_frozen_json_contract() {
        let envelope =
            CommandEnvelope::new(QueueCommand::ActivateFeatureLevel { feature_level: 4 });
        assert_eq!(
            serde_json::to_string(&envelope).unwrap(),
            r#"{"schema_version":1,"scope":3,"kind":35,"required_feature_level":1,"command":{"activate_feature_level":{"feature_level":4}}}"#
        );
        envelope.validate().unwrap();
    }

    #[test]
    fn command_envelope_rejects_tampered_identity() {
        let mut envelope = CommandEnvelope::new(QueueCommand::EmptyTopic {
            topic: "events".into(),
        });
        assert_eq!(envelope.kind, 9);
        assert_eq!(envelope.scope, COMMAND_SCOPE_PARTITION);
        envelope.kind = 10;
        assert!(envelope.validate().is_err());
    }

    #[test]
    fn batch_scope_and_feature_are_derived_from_children() {
        let envelope = CommandEnvelope::new(QueueCommand::Batch {
            commands: vec![
                QueueCommand::EmptyTopic {
                    topic: "events".into(),
                },
                QueueCommand::ActivateFeatureLevel { feature_level: 4 },
            ],
        });
        assert_eq!(envelope.scope, COMMAND_SCOPE_ANY);
        assert_eq!(
            envelope.required_feature_level,
            crate::FEATURE_LEVEL_BASELINE
        );
    }

    #[test]
    fn catalog_p0_command_tags_are_append_only() {
        assert_eq!(
            QueueCommand::RenewCatalogEphemeralLease {
                topic: "events".into(),
                channel: "tail#ephemeral".into(),
                lease_id: 1,
                expires_at_ms: 2,
            }
            .kind(),
            51
        );
        assert_eq!(
            QueueCommand::ReleaseCatalogEphemeralLease {
                topic: "events".into(),
                channel: "tail#ephemeral".into(),
                lease_id: 1,
                now_ms: 2,
            }
            .kind(),
            52
        );
        assert_eq!(
            QueueCommand::ExpireCatalogEphemeralLeases { now_ms: 2 }.kind(),
            53
        );
        assert_eq!(
            QueueCommand::BeginCatalogTopicDeletion {
                topic: "events".into(),
            }
            .kind(),
            54
        );
    }
}
