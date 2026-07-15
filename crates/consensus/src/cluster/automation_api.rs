use super::automation::RebalancePlanItem;
use super::automation_plan::*;
use super::*;
use crate::{MaintenanceOperation, OperationKind, OperationPhase, OperationState};

impl ClusterRuntime {
    pub(super) async fn create_operation(
        &self,
        kind: OperationKind,
        now_ms: i64,
    ) -> anyhow::Result<MaintenanceOperation> {
        let response = self
            .metadata_group()
            .write(QueueCommand::CreateOperation {
                kind,
                now_ms,
                history_limit: self.automation.operation_history_limit,
            })
            .await?;
        ensure_response(&response)?;
        let operation_id = *response
            .message_ids
            .first()
            .ok_or_else(|| anyhow::anyhow!("operation ID was not returned"))?;
        let operation = self
            .metadata
            .operation(operation_id)
            .ok_or_else(|| anyhow::anyhow!("created operation is unavailable"))?;
        tracing::info!(
            audit_event = "operation_created",
            operation_id,
            kind = ?operation.kind,
            "maintenance operation persisted"
        );
        Ok(operation)
    }

    pub async fn enqueue_operation(
        &self,
        kind: OperationKind,
    ) -> anyhow::Result<MaintenanceOperation> {
        let snapshot = self.metadata.snapshot();
        validate_operation(&snapshot, &kind)?;
        if let OperationKind::TransferLeader { group, node_id } = &kind {
            if *group == self.metadata_group().group_key() {
                let voters: BTreeSet<_> = self
                    .metadata_group()
                    .raft()
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .collect();
                if !voters.contains(node_id) {
                    anyhow::bail!("metadata leadership target is not a voter");
                }
            }
        }
        if operation_conflicts(&snapshot, &kind) {
            anyhow::bail!("a non-terminal operation already owns this group or node");
        }
        self.create_operation(kind, now_i64()).await
    }

    pub fn rebalance_plan(&self) -> Vec<RebalancePlanItem> {
        build_rebalance_plan(
            &self.metadata.snapshot(),
            now_i64(),
            self.automation.node_stabilization_seconds,
            self.automation.group_cooldown_seconds,
        )
    }

    pub async fn run_rebalance_plan(&self) -> anyhow::Result<Vec<u64>> {
        let active = self
            .metadata
            .operations()
            .into_iter()
            .filter(|operation| !terminal(operation.state))
            .count();
        let available = self
            .automation
            .max_concurrent_migrations
            .saturating_sub(active);
        let mut operation_ids = Vec::new();
        for item in self.rebalance_plan().into_iter().take(available) {
            let operation = self
                .enqueue_operation(OperationKind::RebalanceGroup {
                    group_id: item.group_id,
                    voters: item.voters,
                })
                .await?;
            operation_ids.push(operation.id);
        }
        Ok(operation_ids)
    }

    pub(super) async fn update_operation(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        state: OperationState,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let has_error = error.is_some();
        let response = self
            .metadata_group()
            .write(QueueCommand::UpdateOperation {
                operation_id,
                phase,
                state,
                now_ms: now_i64(),
                error,
                progress: None,
            })
            .await?;
        ensure_response(&response)?;
        tracing::info!(
            audit_event = "operation_updated",
            operation_id,
            ?phase,
            ?state,
            has_error,
            "maintenance operation state persisted"
        );
        Ok(())
    }

    pub(super) async fn update_operation_progress(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        state: OperationState,
        error: Option<String>,
        progress: crate::OperationProgress,
    ) -> anyhow::Result<()> {
        let response = self
            .metadata_group()
            .write(QueueCommand::UpdateOperation {
                operation_id,
                phase,
                state,
                now_ms: now_i64(),
                error,
                progress: Some(progress),
            })
            .await?;
        ensure_response(&response)
    }

    pub(super) async fn complete_operation(&self, operation_id: u64) -> anyhow::Result<()> {
        self.update_operation(
            operation_id,
            OperationPhase::Completed,
            OperationState::Completed,
            None,
        )
        .await
    }
}
