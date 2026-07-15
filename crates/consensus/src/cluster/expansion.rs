use super::operation_error::{OperationAttempt, OperationAttemptError};
use super::*;
use crate::{
    MaintenanceOperation, OperationKind, OperationPhase, OperationState, PartitionLifecycle,
};

impl ClusterRuntime {
    pub async fn expand_partitions(
        &self,
        topic: &str,
        target_partitions: u16,
        max_partitions: u16,
    ) -> anyhow::Result<MaintenanceOperation> {
        if self.control_plane_enabled() {
            self.validate_federated_expansion_home(topic).await?;
        } else {
            self.ensure_topic(topic, None, None).await?;
        }
        let response = self
            .metadata_group()
            .write(QueueCommand::ReservePartitionExpansion {
                topic: topic.to_owned(),
                target_partitions,
                max_partitions,
                now_ms: now_i64(),
            })
            .await?;
        ensure_response(&response)?;
        let operation_id = *response
            .message_ids
            .first()
            .ok_or_else(|| anyhow::anyhow!("metadata did not return an operation ID"))?;
        let operation = self
            .metadata
            .operation(operation_id)
            .ok_or_else(|| anyhow::anyhow!("partition expansion metadata is unavailable"))?;
        tracing::info!(
            audit_event = "partition_expansion_reserved",
            operation_id,
            topic,
            target_partitions,
            "partition slots and groups reserved"
        );
        Ok(operation)
    }

    async fn validate_federated_expansion_home(&self, topic: &str) -> anyhow::Result<()> {
        let catalog = match self.catalog_topic_descriptor(topic).await? {
            Some(catalog) => catalog,
            None => {
                self.ensure_topic(topic, None, None).await?;
                self.catalog_topic_descriptor(topic)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("topic is not present in Catalog"))?
            }
        };
        if catalog.deleting {
            anyhow::bail!("topic deletion is in progress");
        }
        let homes = catalog
            .partitions
            .values()
            .filter(|partition| partition.lifecycle != crate::PartitionHomeLifecycle::Retired)
            .map(|partition| partition.home_cell)
            .collect::<std::collections::BTreeSet<_>>();
        if homes.len() != 1 {
            anyhow::bail!("online expansion is paused while a topic spans multiple Home Cells");
        }
        let home = *homes.iter().next().expect("one Home Cell was required");
        let local_cell = self.metadata.snapshot().cell_id;
        if home != local_cell {
            anyhow::bail!("partition expansion must be submitted to Home Cell {home}");
        }
        let local = self
            .metadata
            .topic(topic)
            .ok_or_else(|| anyhow::anyhow!("topic metadata is missing from Home Cell {home}"))?;
        let catalog_ids = catalog
            .partitions
            .values()
            .filter(|partition| partition.lifecycle == crate::PartitionHomeLifecycle::Active)
            .map(|partition| partition.id)
            .collect::<std::collections::BTreeSet<_>>();
        let local_ids = local
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
            .map(PartitionDescriptor::global_id)
            .collect::<std::collections::BTreeSet<_>>();
        if catalog_ids != local_ids {
            anyhow::bail!("Home Cell metadata has not converged with Catalog");
        }
        Ok(())
    }

    pub async fn reconcile_partition_expansions(&self) -> anyhow::Result<usize> {
        let operations = self.metadata.pending_partition_expansions();
        let mut reconciled = 0;
        for operation in operations {
            match self.reconcile_partition_expansion(&operation).await {
                Ok(()) => reconciled += 1,
                Err(error) => {
                    let state = if error.is_retryable() {
                        OperationState::Running
                    } else {
                        OperationState::NeedsOperator
                    };
                    let _ = self
                        .record_expansion_phase(
                            operation.id,
                            operation.phase,
                            state,
                            Some(error.to_string()),
                        )
                        .await;
                }
            }
        }
        Ok(reconciled)
    }

    async fn reconcile_partition_expansion(
        &self,
        operation: &MaintenanceOperation,
    ) -> OperationAttempt<()> {
        let (topic_name, groups) = expansion_details(operation)?;
        self.record_expansion_phase(
            operation.id,
            OperationPhase::CreateGroups,
            OperationState::Running,
            None,
        )
        .await
        .map_err(OperationAttemptError::retryable)?;
        let topic = self.metadata.topic(topic_name).ok_or_else(|| {
            OperationAttemptError::needs_operator(anyhow::anyhow!(
                "topic disappeared during partition expansion"
            ))
        })?;
        for group_id in groups {
            let partition = topic
                .partitions
                .iter()
                .find(|partition| partition.global_id() == *group_id)
                .ok_or_else(|| {
                    OperationAttemptError::needs_operator(anyhow::anyhow!(
                        "reserved partition is missing"
                    ))
                })?;
            if partition.lifecycle != PartitionLifecycle::Preparing {
                return Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "reserved partition is not preparing"
                )));
            }
            // Group creation is deliberately serial. This is below the global
            // limit of two concurrent group creations and avoids snapshot
            // fan-out starving ACTIVE partitions.
            self.ensure_partition_group(topic_name, partition)
                .await
                .map_err(OperationAttemptError::retryable)?;
        }
        self.record_expansion_phase(
            operation.id,
            OperationPhase::InitMembership,
            OperationState::Running,
            None,
        )
        .await
        .map_err(OperationAttemptError::retryable)?;

        for _ in 0..32 {
            self.record_expansion_phase(
                operation.id,
                OperationPhase::ChannelBarriers,
                OperationState::Running,
                None,
            )
            .await
            .map_err(OperationAttemptError::retryable)?;
            let topic = self.metadata.topic(topic_name).ok_or_else(|| {
                OperationAttemptError::needs_operator(anyhow::anyhow!(
                    "topic metadata is unavailable"
                ))
            })?;
            let revision = topic.channel_catalog_revision;
            for group_id in groups {
                let partition = topic
                    .partitions
                    .iter()
                    .find(|partition| partition.global_id() == *group_id)
                    .ok_or_else(|| {
                        OperationAttemptError::needs_operator(anyhow::anyhow!(
                            "reserved partition is missing"
                        ))
                    })?;
                for channel in topic.channels.values() {
                    let command = if channel.state == ChannelLifecycle::Deleting {
                        QueueCommand::DeleteChannel {
                            topic: topic_name.to_owned(),
                            channel: channel.name.clone(),
                        }
                    } else {
                        QueueCommand::CreateChannel {
                            topic: topic_name.to_owned(),
                            channel: channel.name.clone(),
                        }
                    };
                    let response = self
                        .write_partition(partition, command)
                        .await
                        .map_err(OperationAttemptError::retryable)?;
                    ensure_response(&response).map_err(OperationAttemptError::retryable)?;
                    if channel.state != ChannelLifecycle::Deleting && channel.paused {
                        let response = self
                            .write_partition(
                                partition,
                                QueueCommand::PauseChannel {
                                    topic: topic_name.to_owned(),
                                    channel: channel.name.clone(),
                                    paused: true,
                                },
                            )
                            .await
                            .map_err(OperationAttemptError::retryable)?;
                        ensure_response(&response).map_err(OperationAttemptError::retryable)?;
                    }
                }
            }
            self.record_expansion_phase(
                operation.id,
                OperationPhase::ArmGroups,
                OperationState::Running,
                None,
            )
            .await
            .map_err(OperationAttemptError::retryable)?;
            self.record_expansion_phase(
                operation.id,
                OperationPhase::ActivateRouting,
                OperationState::Running,
                None,
            )
            .await
            .map_err(OperationAttemptError::retryable)?;
            let activated = self
                .metadata_group()
                .write(QueueCommand::ActivatePartitionExpansion {
                    operation_id: operation.id,
                    expected_channel_revision: revision,
                    now_ms: now_i64(),
                })
                .await
                .map_err(OperationAttemptError::retryable)?;
            if activated.error.as_deref().is_some_and(|error| {
                error == "channel catalog changed while expansion barriers were applied"
            }) {
                continue;
            }
            ensure_response(&activated).map_err(OperationAttemptError::needs_operator)?;
            self.sync_catalog_topic(topic_name)
                .await
                .map_err(OperationAttemptError::retryable)?;
            return Ok(());
        }
        Err(OperationAttemptError::retryable(anyhow::anyhow!(
            "channel catalog did not stabilize during partition expansion"
        )))
    }

    pub async fn cancel_expansion(&self, operation_id: u64) -> anyhow::Result<()> {
        let operation = self
            .metadata
            .operation(operation_id)
            .ok_or_else(|| anyhow::anyhow!("operation not found"))?;
        let (topic, groups) = expansion_details(&operation)?;
        let descriptor = self
            .metadata
            .topic(topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found"))?;
        // Commit the lifecycle transition first. If this node crashes while
        // retiring replicas, recovery will never route to or recreate them.
        let response = self
            .metadata_group()
            .write(QueueCommand::CancelPartitionExpansion {
                operation_id,
                now_ms: now_i64(),
            })
            .await?;
        ensure_response(&response)?;
        for group_id in groups {
            if let Some(partition) = descriptor
                .partitions
                .iter()
                .find(|partition| partition.global_id() == *group_id)
            {
                for node_id in &partition.replicas {
                    let _ = self.retire_replica(*group_id, *node_id).await;
                }
            }
        }
        tracing::info!(operation_id, topic, "partition expansion cancelled");
        Ok(())
    }

    async fn record_expansion_phase(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        state: OperationState,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let has_error = error.is_some();
        let response = self
            .metadata_group()
            .write(QueueCommand::AdvancePartitionExpansion {
                operation_id,
                phase,
                state,
                now_ms: now_i64(),
                error,
            })
            .await?;
        ensure_response(&response)?;
        tracing::info!(
            audit_event = "partition_expansion_phase",
            operation_id,
            ?phase,
            ?state,
            has_error,
            "partition expansion phase persisted"
        );
        Ok(())
    }
}

fn expansion_details(
    operation: &MaintenanceOperation,
) -> OperationAttempt<(&str, &[crate::GlobalGroupId])> {
    match &operation.kind {
        OperationKind::ExpandPartitions {
            topic,
            partition_groups,
            ..
        } => Ok((topic, partition_groups)),
        _ => Err(OperationAttemptError::needs_operator(anyhow::anyhow!(
            "operation is not a partition expansion"
        ))),
    }
}

fn now_i64() -> i64 {
    wall_time_ms().min(i64::MAX as u64) as i64
}
