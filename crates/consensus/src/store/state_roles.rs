use super::*;

impl StateMachineStore {
    pub(super) fn apply_command_with_payloads(
        &self,
        command: &QueueCommand,
        payloads: &[PayloadRef],
        log_index: u64,
    ) -> Result<QueueResponse, BrokerError> {
        let mut payloads = payloads.iter().cloned();
        let response = self.apply_command_with_payload_iter(command, &mut payloads, log_index)?;
        if payloads.next().is_some() {
            return Err(BrokerError::InvalidRecord(
                "Raft entry contains unused payload references".into(),
            ));
        }
        Ok(response)
    }

    fn apply_command_with_payload_iter(
        &self,
        command: &QueueCommand,
        payloads: &mut impl Iterator<Item = PayloadRef>,
        log_index: u64,
    ) -> Result<QueueResponse, BrokerError> {
        match command {
            QueueCommand::Batch { commands } => {
                self.broker.begin_replicated_batch();
                let mut results = Vec::with_capacity(commands.len());
                for command in commands {
                    match self.apply_command_with_payload_iter(command, payloads, log_index) {
                        Ok(response) => results.push(response),
                        Err(error) if is_fatal_queue_error(&error) => return Err(error),
                        Err(error) => results.push(QueueResponse {
                            message_ids: Vec::new(),
                            error: Some(error.to_string()),
                            results: Vec::new(),
                        }),
                    }
                }
                Ok(QueueResponse {
                    message_ids: Vec::new(),
                    error: None,
                    results,
                })
            }
            QueueCommand::Publish {
                operation_id,
                topic,
                bodies,
                timestamp_ns,
                available_at_ms,
                partition,
                ..
            } => {
                let StateMachineRole::Partition {
                    topic: group_topic,
                    partition: group_partition,
                } = &self.role
                else {
                    return self.apply_command(command);
                };
                if topic != group_topic || partition.is_some_and(|value| value != *group_partition)
                {
                    return Err(BrokerError::InvalidRecord(
                        "publish does not belong to this partition group".into(),
                    ));
                }
                let references = (0..bodies.len())
                    .map(|_| {
                        payloads.next().ok_or_else(|| {
                            BrokerError::InvalidRecord(
                                "Raft entry payload reference table is incomplete".into(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.broker
                    .publish_replicated_refs(
                        *operation_id,
                        topic,
                        references,
                        *timestamp_ns,
                        *available_at_ms,
                        *group_partition,
                        log_index,
                    )
                    .map(|message_ids| QueueResponse {
                        message_ids,
                        error: None,
                        results: Vec::new(),
                    })
            }
            _ => self.apply_command(command),
        }
    }

    pub(super) fn apply_metadata_command(
        &self,
        command: &QueueCommand,
    ) -> Result<QueueResponse, BrokerError> {
        match command {
            QueueCommand::Batch { commands } => self.apply_batch(commands, true),
            QueueCommand::CreateTopic {
                topic,
                partitions,
                replication_factor,
            } => {
                let descriptor = self
                    .metadata
                    .ensure_topic(topic, *partitions, *replication_factor)
                    .map_err(BrokerError::InvalidRecord)?;
                ensure_broker_topic(&self.broker, &descriptor)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::DeleteTopic { topic } => {
                self.broker.delete_topic(topic)?;
                self.metadata.delete_topic(topic);
                Ok(QueueResponse::default())
            }
            QueueCommand::PrepareDeleteTopic { topic } => {
                self.metadata.prepare_delete_topic(topic);
                Ok(QueueResponse::default())
            }
            QueueCommand::CompleteDeleteTopic { topic } => {
                self.broker.delete_topic(topic)?;
                self.metadata.delete_topic(topic);
                Ok(QueueResponse::default())
            }
            QueueCommand::PauseTopic { topic, paused } => {
                self.broker.set_topic_paused(topic, *paused)?;
                self.metadata
                    .set_topic_paused(topic, *paused)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::SetChannelMetadataPaused {
                topic,
                channel,
                paused,
            } => self
                .metadata
                .set_channel_metadata_paused(topic, channel, *paused)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::PrepareChannel { topic, channel } => match self
                .metadata
                .prepare_channel(topic, channel)
                .map_err(BrokerError::InvalidRecord)?
            {
                Some(_) => Ok(QueueResponse::default()),
                None => Ok(command_error("channel is being deleted")),
            },
            QueueCommand::ActivateChannel {
                topic,
                channel,
                generation,
            } => {
                self.metadata
                    .activate_channel(topic, channel, *generation)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::PrepareDeleteChannel { topic, channel } => match self
                .metadata
                .prepare_delete_channel(topic, channel)
                .map_err(BrokerError::InvalidRecord)?
            {
                Some(_) => Ok(QueueResponse::default()),
                None => Ok(command_error("channel not found")),
            },
            QueueCommand::CompleteDeleteChannel {
                topic,
                channel,
                generation,
            } => {
                self.metadata
                    .complete_delete_channel(topic, channel, *generation)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::UpdatePartitionReplicas { group_id, replicas } => {
                self.metadata
                    .update_partition_replicas(*group_id, replicas.clone())
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::RegisterNode { descriptor } => {
                self.metadata
                    .register_node(descriptor.clone())
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::SetNodeDrained { node_id, drained } => {
                self.metadata
                    .set_node_drained(*node_id, *drained)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::ReservePartitionExpansion {
                topic,
                target_partitions,
                max_partitions,
                now_ms,
            } => self
                .metadata
                .reserve_partition_expansion(topic, *target_partitions, *max_partitions, *now_ms)
                .map(|operation| QueueResponse {
                    message_ids: vec![operation.id],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::AdvancePartitionExpansion {
                operation_id,
                phase,
                state,
                now_ms,
                error,
            } => self
                .metadata
                .advance_partition_expansion(*operation_id, *phase, *state, *now_ms, error.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ActivatePartitionExpansion {
                operation_id,
                expected_channel_revision,
                now_ms,
            } => self
                .metadata
                .activate_partition_expansion(*operation_id, *expected_channel_revision, *now_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::CancelPartitionExpansion {
                operation_id,
                now_ms,
            } => self
                .metadata
                .cancel_partition_expansion(*operation_id, *now_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SetOperationPaused {
                operation_id,
                paused,
            } => self
                .metadata
                .set_operation_paused(*operation_id, *paused)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SetAutomationEnabled { enabled } => {
                self.metadata.set_automation_enabled(*enabled);
                Ok(QueueResponse::default())
            }
            QueueCommand::SetMaintenance { node_id, lease } => self
                .metadata
                .set_maintenance(*node_id, lease.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::CreateOperation {
                kind,
                now_ms,
                history_limit,
            } => self
                .metadata
                .create_operation(kind.clone(), *now_ms, *history_limit)
                .map(|operation| QueueResponse {
                    message_ids: vec![operation.id],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::UpdateOperation {
                operation_id,
                phase,
                state,
                now_ms,
                error,
                progress,
            } => self
                .metadata
                .update_operation(
                    *operation_id,
                    *phase,
                    *state,
                    *now_ms,
                    error.clone(),
                    progress.clone(),
                )
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ObserveNodeHealth {
                node_id,
                healthy,
                disk_used_percent,
                disk_free_bytes,
                storage_eligible,
                now_ms,
            } => self
                .metadata
                .observe_node_health(
                    *node_id,
                    *healthy,
                    *disk_used_percent,
                    *disk_free_bytes,
                    *storage_eligible,
                    *now_ms,
                )
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RenewEphemeralLease {
                topic,
                channel,
                lease_id,
                expires_at_ms,
            } => {
                self.metadata
                    .renew_ephemeral_lease(topic, channel, *lease_id, *expires_at_ms)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::ReleaseEphemeralLease {
                topic,
                channel,
                lease_id,
            } => {
                self.metadata
                    .release_ephemeral_lease(topic, channel, *lease_id)
                    .map_err(BrokerError::InvalidRecord)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::InstallChannelMetadata { topic, descriptor } => self
                .metadata
                .install_channel_metadata(topic, descriptor.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::UpsertFederatedPartition {
                template,
                partition,
            } => {
                let descriptor = self
                    .metadata
                    .upsert_federated_partition(template, partition.clone())
                    .map_err(BrokerError::InvalidRecord)?;
                ensure_broker_topic(&self.broker, &descriptor)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::ActivateFeatureLevel { feature_level } => self
                .metadata
                .activate_feature_level(*feature_level)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RegisterFederationNode { node } => self
                .metadata
                .register_federation_node(node.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ApplyRootAction {
                action,
                now_ms,
                policy,
            } => self
                .metadata
                .apply_root_action(action.clone(), *now_ms, *policy)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::BeginPartitionMigration {
                topic,
                partition,
                target,
                now_ms,
                max_home_cells,
            } => self
                .metadata
                .begin_partition_migration(topic, *partition, *target, *now_ms, *max_home_cells)
                .map(|operation| QueueResponse {
                    message_ids: vec![operation.operation_id],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::AdvancePartitionMigration {
                operation_id,
                expected,
                next,
                observed_lag_entries,
                now_ms,
                max_home_cells,
            } => self
                .metadata
                .advance_partition_migration(
                    *operation_id,
                    *expected,
                    *next,
                    *observed_lag_entries,
                    *now_ms,
                    *max_home_cells,
                )
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::MarkPartitionMigrationNeedsOperator {
                operation_id,
                error,
                now_ms,
            } => self
                .metadata
                .mark_partition_migration_needs_operator(*operation_id, error.clone(), *now_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ActivateBucketMove {
                topic,
                start,
                end,
                target,
                expected_epoch,
            } => self
                .metadata
                .activate_bucket_move(topic, *start, *end, *target, *expected_epoch)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ActivateScopedFeature {
                activation,
                observed_protocol_floor,
            } => self
                .metadata
                .activate_scoped_feature(activation.clone(), *observed_protocol_floor)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SyncCatalogTopic { descriptor } => self
                .metadata
                .sync_catalog_topic(descriptor.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RemoveCatalogTopic { topic } => {
                self.metadata.remove_catalog_topic(topic);
                Ok(QueueResponse::default())
            }
            QueueCommand::BeginCatalogTopicDeletion { topic } => self
                .metadata
                .begin_catalog_topic_deletion(topic)
                .map(|changed| QueueResponse {
                    message_ids: vec![u64::from(changed)],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::PrepareCatalogChannel { topic, channel } => self
                .metadata
                .prepare_catalog_channel(topic, channel)
                .map(|generation| QueueResponse {
                    message_ids: vec![generation],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::UpdateCatalogChannel {
                topic,
                channel,
                generation,
                state,
                paused,
            } => self
                .metadata
                .update_catalog_channel(topic, channel, *generation, state.clone(), *paused)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RemoveCatalogChannel {
                topic,
                channel,
                generation,
            } => self
                .metadata
                .remove_catalog_channel(topic, channel, *generation)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RenewCatalogEphemeralLease {
                topic,
                channel,
                lease_id,
                expires_at_ms,
            } => self
                .metadata
                .renew_catalog_ephemeral_lease(topic, channel, *lease_id, *expires_at_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ReleaseCatalogEphemeralLease {
                topic,
                channel,
                lease_id,
                now_ms,
            } => self
                .metadata
                .release_catalog_ephemeral_lease(topic, channel, *lease_id, *now_ms)
                .map(|deleting| QueueResponse {
                    message_ids: vec![u64::from(deleting)],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ExpireCatalogEphemeralLeases { now_ms } => Ok(QueueResponse {
                message_ids: vec![self.metadata.expire_catalog_ephemeral_leases(*now_ms) as u64],
                error: None,
                results: Vec::new(),
            }),
            _ => Err(BrokerError::InvalidRecord(
                "partition command submitted to metadata group".into(),
            )),
        }
    }

    pub(super) fn apply_partition_command(
        &self,
        command: &QueueCommand,
        group_topic: &str,
        group_partition: u16,
    ) -> Result<QueueResponse, BrokerError> {
        let command_topic = match command {
            QueueCommand::Publish { topic, .. }
            | QueueCommand::CreateChannel { topic, .. }
            | QueueCommand::DeleteChannel { topic, .. }
            | QueueCommand::EmptyTopic { topic }
            | QueueCommand::EmptyChannel { topic, .. }
            | QueueCommand::PauseChannel { topic, .. }
            | QueueCommand::Finish { topic, .. }
            | QueueCommand::Requeue { topic, .. }
            | QueueCommand::ProtectiveEvict { topic, .. } => Some(topic.as_str()),
            QueueCommand::Batch { commands } => return self.apply_batch(commands, true),
            _ => None,
        };
        if command_topic != Some(group_topic) {
            return Err(BrokerError::InvalidRecord(
                "command does not belong to this partition group".into(),
            ));
        }
        match command {
            QueueCommand::Publish {
                operation_id,
                topic,
                bodies,
                timestamp_ns,
                available_at_ms,
                partition,
                routing_key: _,
            } => {
                if partition.is_some_and(|value| value != group_partition) {
                    return Err(BrokerError::PartitionNotFound);
                }
                self.broker
                    .publish_replicated(
                        *operation_id,
                        topic,
                        bodies.clone(),
                        *timestamp_ns,
                        *available_at_ms,
                        Some(group_partition),
                        None,
                    )
                    .map(|message_ids| QueueResponse {
                        message_ids,
                        error: None,
                        results: Vec::new(),
                    })
            }
            QueueCommand::CreateChannel { topic, channel } => self
                .broker
                .create_channel_partition(topic, channel, group_partition)
                .map(|_| QueueResponse::default()),
            QueueCommand::DeleteChannel { topic, channel } => self
                .broker
                .delete_channel_partition(topic, channel, group_partition)
                .map(|_| QueueResponse::default()),
            QueueCommand::EmptyTopic { topic } => self
                .broker
                .empty_topic_partition(topic, group_partition)
                .map(|_| QueueResponse::default()),
            QueueCommand::EmptyChannel { topic, channel } => self
                .broker
                .empty_channel_partition(topic, channel, group_partition)
                .map(|_| QueueResponse::default()),
            QueueCommand::PauseChannel {
                topic,
                channel,
                paused,
            } => self
                .broker
                .set_channel_paused_partition(topic, channel, group_partition, *paused)
                .map(|_| QueueResponse::default()),
            QueueCommand::Finish {
                topic,
                channel,
                message_id,
            } => self
                .broker
                .commit_finish(topic, channel, *message_id)
                .map(|_| QueueResponse::default()),
            QueueCommand::Requeue {
                topic,
                channel,
                message_id,
                available_at_ms,
            } => self
                .broker
                .commit_requeue_at(topic, channel, *message_id, *available_at_ms)
                .map(|_| QueueResponse::default()),
            QueueCommand::ProtectiveEvict {
                topic,
                partition,
                through_message_id,
                ..
            } => {
                if *partition != group_partition {
                    return Err(BrokerError::PartitionNotFound);
                }
                self.broker
                    .protective_evict_through(topic, *partition, *through_message_id)
                    .map(|_| QueueResponse::default())
            }
            _ => Err(BrokerError::InvalidRecord(
                "metadata command submitted to partition group".into(),
            )),
        }
    }
}
