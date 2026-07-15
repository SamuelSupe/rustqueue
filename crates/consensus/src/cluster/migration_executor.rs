use super::migration_transport::{unavailable, TargetSelectionError};
use super::*;
use crate::{PartitionMigration, PartitionMigrationPhase};

const MAX_PHASE_ADVANCES_PER_CYCLE: usize = 16;

impl ClusterRuntime {
    pub(super) async fn reconcile_partition_migrations(&self) -> anyhow::Result<usize> {
        let Some(control) = &self.control else {
            return Ok(0);
        };
        let Some(catalog_group) = &control.catalog else {
            return Ok(0);
        };
        if catalog_group.leader_state().0 != Some(self.node_id) {
            return Ok(0);
        }
        catalog_group.ensure_quorum_local().await?;
        let operations = control
            .metadata
            .catalog_snapshot()
            .migrations
            .into_values()
            .filter(|operation| {
                !matches!(
                    operation.phase,
                    PartitionMigrationPhase::Completed | PartitionMigrationPhase::NeedsOperator
                )
            })
            .take(self.automation.max_concurrent_migrations)
            .collect::<Vec<_>>();
        let mut progressed = 0;
        for mut operation in operations {
            for _ in 0..MAX_PHASE_ADVANCES_PER_CYCLE {
                match self.reconcile_partition_migration(&operation).await {
                    Ok(true) => {
                        progressed += 1;
                        let Some(updated) = control
                            .metadata
                            .catalog_snapshot()
                            .migrations
                            .get(&operation.operation_id)
                            .cloned()
                        else {
                            break;
                        };
                        operation = updated;
                        if matches!(
                            operation.phase,
                            PartitionMigrationPhase::Completed
                                | PartitionMigrationPhase::NeedsOperator
                        ) {
                            break;
                        }
                    }
                    Ok(false) => break,
                    Err(TargetSelectionError::Unsafe(error)) => {
                        let response = self
                            .write_control(QueueCommand::MarkPartitionMigrationNeedsOperator {
                                operation_id: operation.operation_id,
                                error: error.clone(),
                                now_ms: now_i64(),
                            })
                            .await?;
                        ensure_response(&response)?;
                        tracing::error!(
                            operation_id = operation.operation_id,
                            %error,
                            "partition migration requires operator action"
                        );
                        self.federation_metrics.migration_needs_operator();
                        progressed += 1;
                        break;
                    }
                    Err(TargetSelectionError::Unavailable(error)) => {
                        tracing::debug!(
                            operation_id = operation.operation_id,
                            %error,
                            "partition migration will retry"
                        );
                        break;
                    }
                }
            }
        }
        Ok(progressed)
    }

    async fn reconcile_partition_migration(
        &self,
        operation: &PartitionMigration,
    ) -> Result<bool, TargetSelectionError> {
        use PartitionMigrationPhase::*;
        match operation.phase {
            Planned => {
                self.advance_migration(operation, PrepareTarget, u64::MAX)
                    .await?;
                Ok(true)
            }
            PrepareTarget => {
                self.prepare_migration_target(operation).await?;
                self.advance_migration(operation, SnapshotCopy, u64::MAX)
                    .await?;
                Ok(true)
            }
            SnapshotCopy => {
                let (source, target) = self.migration_descriptors(operation).await?;
                self.add_migration_learners(&source.1, &target.1).await?;
                self.advance_migration(operation, CatchUp, u64::MAX).await?;
                Ok(true)
            }
            CatchUp => {
                let (source, target) = self.migration_descriptors(operation).await?;
                let view = self.migration_view(&source.1, &target.1).await?;
                if view.target_lag != 0 {
                    return Ok(false);
                }
                let mut fenced = source.1;
                fenced.lifecycle = crate::PartitionLifecycle::Preparing;
                self.upsert_migration_partition(operation.source, source.0, fenced)
                    .await?;
                self.advance_migration(operation, SourceFence, view.target_lag)
                    .await?;
                Ok(true)
            }
            SourceFence => {
                self.advance_migration(operation, FinalCatchUp, operation.observed_lag_entries)
                    .await?;
                Ok(true)
            }
            FinalCatchUp => {
                let (source, target) = self.migration_descriptors(operation).await?;
                let view = self
                    .migration_view_with_target_election(&source.1, &target.1)
                    .await?;
                if view.target_lag != 0 {
                    return Ok(false);
                }
                self.move_migration_membership(&source.1, &target.1, &view)
                    .await?;
                self.advance_migration(operation, RemoveSourceLearners, 0)
                    .await?;
                Ok(true)
            }
            RemoveSourceLearners => {
                let (source, target) = self.migration_descriptors(operation).await?;
                let view = self
                    .migration_view_with_target_election(&source.1, &target.1)
                    .await?;
                self.finalize_migration_membership(&source.1, &target.1, &view)
                    .await?;
                let mut active = target.1;
                active.lifecycle = crate::PartitionLifecycle::Active;
                self.upsert_migration_partition(operation.target, target.0, active)
                    .await?;
                self.advance_migration(operation, Cutover, 0).await?;
                self.invalidate_catalog_topic(&operation.topic).await;
                Ok(true)
            }
            Cutover => {
                self.advance_migration(operation, DrainSource, 0).await?;
                Ok(true)
            }
            DrainSource => {
                let source = self
                    .describe_migration_cell(operation.source, operation)
                    .await?;
                let mut retired = source.1.clone();
                retired.lifecycle = crate::PartitionLifecycle::Retired;
                self.upsert_migration_partition(operation.source, source.0, retired)
                    .await?;
                self.retire_migration_sources(&source.1).await?;
                self.advance_migration(operation, Completed, 0).await?;
                Ok(true)
            }
            Completed | NeedsOperator => Ok(false),
        }
    }

    async fn prepare_migration_target(
        &self,
        operation: &PartitionMigration,
    ) -> Result<(), TargetSelectionError> {
        let source = self
            .describe_migration_cell(operation.source, operation)
            .await?;
        let target_existing = self
            .migration_cell(
                operation.target,
                FederationMigrationAction::Describe {
                    topic: operation.topic.clone(),
                    partition: operation.partition,
                },
            )
            .await;
        let target_partition = match target_existing {
            Ok(response) => response.partition.ok_or_else(|| {
                TargetSelectionError::Unavailable("target descriptor is incomplete".into())
            })?,
            Err(FederationForwardError::StaleRoute(_)) => {
                let replicas = self
                    .select_migration_targets(
                        operation.target,
                        source.1.replication_factor as usize,
                    )
                    .await?;
                let mut partition = source.1.clone();
                partition.home_cell = operation.target;
                partition.replicas = replicas;
                partition.leader_hint = None;
                partition.lifecycle = crate::PartitionLifecycle::Preparing;
                partition
            }
            Err(error) => return Err(unavailable(error)),
        };
        self.upsert_migration_partition(operation.target, source.0, target_partition)
            .await
    }

    async fn migration_descriptors(
        &self,
        operation: &PartitionMigration,
    ) -> Result<
        (
            (crate::TopicDescriptor, PartitionDescriptor),
            (crate::TopicDescriptor, PartitionDescriptor),
        ),
        TargetSelectionError,
    > {
        let source = self
            .describe_migration_cell(operation.source, operation)
            .await?;
        let target = self
            .describe_migration_cell(operation.target, operation)
            .await?;
        Ok((source, target))
    }

    async fn describe_migration_cell(
        &self,
        cell: crate::CellId,
        operation: &PartitionMigration,
    ) -> Result<(crate::TopicDescriptor, PartitionDescriptor), TargetSelectionError> {
        let response = self
            .migration_cell(
                cell,
                FederationMigrationAction::Describe {
                    topic: operation.topic.clone(),
                    partition: operation.partition,
                },
            )
            .await
            .map_err(unavailable)?;
        match (response.topic, response.partition) {
            (Some(topic), Some(partition)) => Ok((topic, partition)),
            _ => Err(TargetSelectionError::Unavailable(
                "Home Cell returned an incomplete migration descriptor".into(),
            )),
        }
    }

    async fn upsert_migration_partition(
        &self,
        cell: crate::CellId,
        template: crate::TopicDescriptor,
        partition: PartitionDescriptor,
    ) -> Result<(), TargetSelectionError> {
        self.migration_cell(
            cell,
            FederationMigrationAction::Upsert {
                template,
                partition,
            },
        )
        .await
        .map(|_| ())
        .map_err(unavailable)
    }

    async fn advance_migration(
        &self,
        operation: &PartitionMigration,
        next: PartitionMigrationPhase,
        lag: u64,
    ) -> Result<(), TargetSelectionError> {
        let response = self
            .write_control(QueueCommand::AdvancePartitionMigration {
                operation_id: operation.operation_id,
                expected: operation.phase,
                next,
                observed_lag_entries: lag,
                now_ms: now_i64(),
                max_home_cells: self
                    .control
                    .as_ref()
                    .map(|control| control.metadata.max_home_cells_per_topic())
                    .unwrap_or(128),
            })
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        ensure_response(&response)
            .map_err(|error| unavailable(error.to_string()))
            .inspect(|_| self.federation_metrics.migration_advanced())
    }
}

fn now_i64() -> i64 {
    wall_time_ms().min(i64::MAX as u64) as i64
}
