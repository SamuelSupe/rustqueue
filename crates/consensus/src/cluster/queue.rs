use super::*;

impl ClusterRuntime {
    pub async fn reconcile_once(&self) -> anyhow::Result<usize> {
        let restored = self.restore_assigned_groups().await?;
        let migrations = self.reconcile_partition_migrations().await?;
        let expired_catalog_leases = self.expire_federated_ephemeral_leases().await?;
        let catalog_deletions = self.reconcile_federated_topic_deletions().await?;
        if self.metadata_group().leader_state().0 != Some(self.node_id) {
            return Ok(restored
                + migrations
                + expired_catalog_leases
                + catalog_deletions
                + self.reconcile_disk_pressure().await?);
        }
        let automated = self.reconcile_automation().await?;
        let expansions = self.reconcile_partition_expansions().await?;
        let deletions = self.reconcile_topic_deletions().await?;
        let retained = self.reconcile_retention().await?;
        let disk_pressure = self.reconcile_disk_pressure().await?;
        let federated_channels = self.reconcile_federated_channels().await?;
        let expired = self
            .metadata
            .expired_ephemeral_channels(wall_time_ms().min(i64::MAX as u64) as i64);
        for (topic, channel) in &expired {
            self.write(QueueCommand::DeleteChannel {
                topic: topic.clone(),
                channel: channel.clone(),
            })
            .await?;
        }
        let pending: Vec<_> = self
            .metadata
            .snapshot()
            .topics
            .into_values()
            .filter(|topic| topic.state == crate::TopicState::Active)
            .flat_map(|topic| {
                let topic_name = topic.name;
                topic.channels.into_values().filter_map(move |channel| {
                    matches!(
                        channel.state,
                        ChannelLifecycle::Preparing | ChannelLifecycle::Deleting
                    )
                    .then_some((topic_name.clone(), channel))
                })
            })
            .collect();
        for (topic, channel) in &pending {
            let command = match channel.state {
                ChannelLifecycle::Preparing => QueueCommand::CreateChannel {
                    topic: topic.clone(),
                    channel: channel.name.clone(),
                },
                ChannelLifecycle::Deleting => QueueCommand::DeleteChannel {
                    topic: topic.clone(),
                    channel: channel.name.clone(),
                },
                ChannelLifecycle::Active => continue,
            };
            self.write(command).await?;
        }
        Ok(restored
            + migrations
            + expired_catalog_leases
            + catalog_deletions
            + automated
            + expansions
            + deletions
            + retained
            + disk_pressure
            + federated_channels
            + pending.len()
            + expired.len())
    }

    /// Reopen every partition assigned to this node from the durable metadata
    /// catalog. This also closes the crash window between committing topic
    /// metadata and initializing its partition groups.
    pub async fn restore_assigned_groups(&self) -> anyhow::Result<usize> {
        let topics = self.metadata.snapshot().topics;
        let mut restored = 0;
        for topic in topics.into_values() {
            if topic.state == crate::TopicState::Deleting {
                continue;
            }
            for partition in topic
                .partitions
                .into_iter()
                .filter(|partition| partition.lifecycle != crate::PartitionLifecycle::Retired)
            {
                if !partition.replicas.contains(&self.node_id) {
                    continue;
                }
                let was_open = self.group(partition.group_key()).await.is_some();
                self.ensure_partition_local(EnsureGroupRequest {
                    topic: topic.name.clone(),
                    partition: partition.clone(),
                })
                .await?;
                if !was_open {
                    restored += 1;
                }

                if partition_initializer(&partition) == self.node_id {
                    self.initialize_group_local(partition.global_id(), partition.replicas)
                        .await?;
                }
            }
        }
        Ok(restored)
    }

    pub async fn write(&self, command: QueueCommand) -> anyhow::Result<QueueResponse> {
        if matches!(&command, QueueCommand::Publish { .. }) {
            self.ensure_write_safe().map_err(anyhow::Error::msg)?;
        }
        match command {
            QueueCommand::Batch { commands } => {
                let mut results = Vec::with_capacity(commands.len());
                for command in commands {
                    results.push(Box::pin(self.write(command)).await?);
                }
                Ok(QueueResponse {
                    message_ids: Vec::new(),
                    error: None,
                    results,
                })
            }
            command @ QueueCommand::CreateTopic { .. } => {
                let topic = match &command {
                    QueueCommand::CreateTopic { topic, .. } => topic.clone(),
                    _ => unreachable!(),
                };
                if self.control_plane_enabled() {
                    if let Some(catalog) = self.catalog_topic_descriptor(&topic).await? {
                        if catalog.deleting {
                            anyhow::bail!("topic deletion is in progress");
                        }
                        return Ok(QueueResponse::default());
                    }
                }
                let response = self.metadata_group().write(command).await?;
                ensure_response(&response)?;
                self.ensure_topic_groups(&topic).await?;
                self.sync_catalog_topic(&topic).await?;
                Ok(response)
            }
            command @ (QueueCommand::RegisterNode { .. }
            | QueueCommand::UpdatePartitionReplicas { .. }
            | QueueCommand::SetNodeDrained { .. }
            | QueueCommand::ReservePartitionExpansion { .. }
            | QueueCommand::AdvancePartitionExpansion { .. }
            | QueueCommand::ActivatePartitionExpansion { .. }
            | QueueCommand::CancelPartitionExpansion { .. }
            | QueueCommand::SetOperationPaused { .. }
            | QueueCommand::SetAutomationEnabled { .. }
            | QueueCommand::SetMaintenance { .. }
            | QueueCommand::CreateOperation { .. }
            | QueueCommand::UpdateOperation { .. }
            | QueueCommand::ObserveNodeHealth { .. }
            | QueueCommand::SetChannelMetadataPaused { .. }
            | QueueCommand::InstallChannelMetadata { .. }
            | QueueCommand::UpsertFederatedPartition { .. }
            | QueueCommand::ActivateFeatureLevel { .. }) => {
                self.metadata_group().write(command).await
            }
            command @ (QueueCommand::RegisterFederationNode { .. }
            | QueueCommand::ApplyRootAction { .. }
            | QueueCommand::BeginPartitionMigration { .. }
            | QueueCommand::AdvancePartitionMigration { .. }
            | QueueCommand::MarkPartitionMigrationNeedsOperator { .. }
            | QueueCommand::ActivateBucketMove { .. }
            | QueueCommand::ActivateScopedFeature { .. }
            | QueueCommand::SyncCatalogTopic { .. }
            | QueueCommand::RemoveCatalogTopic { .. }
            | QueueCommand::PrepareCatalogChannel { .. }
            | QueueCommand::UpdateCatalogChannel { .. }
            | QueueCommand::RemoveCatalogChannel { .. }) => self.write_control(command).await,
            command @ (QueueCommand::RenewCatalogEphemeralLease { .. }
            | QueueCommand::ReleaseCatalogEphemeralLease { .. }
            | QueueCommand::ExpireCatalogEphemeralLeases { .. }
            | QueueCommand::BeginCatalogTopicDeletion { .. }) => self.write_control(command).await,
            QueueCommand::RenewEphemeralLease {
                topic,
                channel,
                lease_id,
                expires_at_ms,
            } => {
                if self.control_plane_enabled() {
                    self.write_control(QueueCommand::RenewCatalogEphemeralLease {
                        topic,
                        channel,
                        lease_id,
                        expires_at_ms,
                    })
                    .await
                } else {
                    self.metadata_group()
                        .write(QueueCommand::RenewEphemeralLease {
                            topic,
                            channel,
                            lease_id,
                            expires_at_ms,
                        })
                        .await
                }
            }
            QueueCommand::ReleaseEphemeralLease {
                topic,
                channel,
                lease_id,
            } => {
                if self.control_plane_enabled() {
                    let response = self
                        .write_control(QueueCommand::ReleaseCatalogEphemeralLease {
                            topic: topic.clone(),
                            channel: channel.clone(),
                            lease_id,
                            now_ms: wall_time_ms().min(i64::MAX as u64) as i64,
                        })
                        .await?;
                    ensure_response(&response)?;
                    if response.message_ids.first() == Some(&1) {
                        self.delete_channel_federated(&topic, &channel).await?;
                    }
                    Ok(response)
                } else {
                    self.metadata_group()
                        .write(QueueCommand::ReleaseEphemeralLease {
                            topic,
                            channel,
                            lease_id,
                        })
                        .await
                }
            }
            QueueCommand::Publish {
                operation_id,
                topic,
                bodies,
                timestamp_ns,
                available_at_ms,
                partition,
                routing_key,
            } => {
                self.ensure_feature_level(crate::feature::required_publish_feature(&bodies))
                    .map_err(anyhow::Error::msg)?;
                if self.control_plane_enabled() {
                    match self
                        .catalog_route(&topic, operation_id, partition, routing_key.as_deref())
                        .await
                    {
                        Ok(_) => {}
                        Err(crate::RouteError::TopicNotFound) => {
                            self.ensure_topic(&topic, None, None).await?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                    let command = QueueCommand::Publish {
                        operation_id,
                        topic: topic.clone(),
                        bodies,
                        timestamp_ns,
                        available_at_ms,
                        partition,
                        routing_key: None,
                    };
                    let (response, target) = self
                        .write_catalog_partition(
                            &topic,
                            operation_id,
                            partition,
                            routing_key.as_deref(),
                            command,
                        )
                        .await?;
                    let target_number = u16::try_from(target.partition.number)
                        .map_err(|_| anyhow::anyhow!("partition number exceeds wire range"))?;
                    if response.error.is_none() {
                        self.broker.cache_publish_result(
                            operation_id,
                            &topic,
                            target_number,
                            response.message_ids.clone(),
                        );
                    }
                    return Ok(response);
                }
                self.ensure_topic(&topic, None, None).await?;
                let descriptor = self
                    .metadata
                    .topic_route(&topic)
                    .ok_or_else(|| anyhow::anyhow!("topic metadata is unavailable"))?;
                let target = descriptor
                    .select_partition(operation_id, partition, routing_key.as_deref())
                    .map_err(anyhow::Error::msg)?;
                if let Some(message_ids) =
                    self.broker
                        .cached_publish_result(operation_id, &topic, target.number)
                {
                    return Ok(QueueResponse {
                        message_ids,
                        error: None,
                        results: Vec::new(),
                    });
                }
                let response = self
                    .write_partition(
                        target.as_ref(),
                        QueueCommand::Publish {
                            operation_id,
                            topic: topic.clone(),
                            bodies,
                            timestamp_ns,
                            available_at_ms,
                            partition: Some(target.number),
                            routing_key: None,
                        },
                    )
                    .await?;
                if response.error.is_none() {
                    self.broker.cache_publish_result(
                        operation_id,
                        &topic,
                        target.number,
                        response.message_ids.clone(),
                    );
                }
                Ok(response)
            }
            QueueCommand::CreateChannel { topic, channel } => {
                if self.control_plane_enabled() {
                    return self.create_channel_federated(&topic, &channel).await;
                }
                self.ensure_topic(&topic, None, None).await?;
                if self.metadata.channel_is_active(&topic, &channel) {
                    return Ok(QueueResponse::default());
                }
                let generation = self.prepare_channel_generation(&topic, &channel).await?;
                let response = self
                    .broadcast_topic(
                        &topic,
                        QueueCommand::CreateChannel {
                            topic: topic.clone(),
                            channel: channel.clone(),
                        },
                    )
                    .await?;
                let activated = self
                    .metadata_group()
                    .write(QueueCommand::ActivateChannel {
                        topic: topic.clone(),
                        channel,
                        generation,
                    })
                    .await?;
                ensure_response(&activated)?;
                self.sync_catalog_topic(&topic).await?;
                Ok(response)
            }
            QueueCommand::DeleteChannel { topic, channel } => {
                if self.control_plane_enabled() {
                    return self.delete_channel_federated(&topic, &channel).await;
                }
                let Some(existing) = self.metadata.channel(&topic, &channel) else {
                    return Ok(QueueResponse::default());
                };
                let prepared = self
                    .metadata_group()
                    .write(QueueCommand::PrepareDeleteChannel {
                        topic: topic.clone(),
                        channel: channel.clone(),
                    })
                    .await?;
                ensure_response(&prepared)?;
                let response = self
                    .broadcast_topic(
                        &topic,
                        QueueCommand::DeleteChannel {
                            topic: topic.clone(),
                            channel: channel.clone(),
                        },
                    )
                    .await?;
                let completed = self
                    .metadata_group()
                    .write(QueueCommand::CompleteDeleteChannel {
                        topic: topic.clone(),
                        channel,
                        generation: existing.generation,
                    })
                    .await?;
                ensure_response(&completed)?;
                self.sync_catalog_topic(&topic).await?;
                Ok(response)
            }
            command @ (QueueCommand::EmptyTopic { .. } | QueueCommand::EmptyChannel { .. }) => {
                let topic = command_topic(&command)
                    .expect("matched command has topic")
                    .to_owned();
                if self.control_plane_enabled() {
                    return self.broadcast_federated_command(&topic, command).await;
                }
                self.ensure_topic(&topic, None, None).await?;
                self.broadcast_topic(&topic, command).await
            }
            QueueCommand::PauseChannel {
                topic,
                channel,
                paused,
            } => {
                if self.control_plane_enabled() {
                    return self.pause_channel_federated(&topic, &channel, paused).await;
                }
                self.ensure_topic(&topic, None, None).await?;
                let metadata = self
                    .metadata_group()
                    .write(QueueCommand::SetChannelMetadataPaused {
                        topic: topic.clone(),
                        channel: channel.clone(),
                        paused,
                    })
                    .await?;
                ensure_response(&metadata)?;
                let response = self
                    .broadcast_topic(
                        &topic,
                        QueueCommand::PauseChannel {
                            topic: topic.clone(),
                            channel,
                            paused,
                        },
                    )
                    .await?;
                self.sync_catalog_topic(&topic).await?;
                Ok(response)
            }
            command @ QueueCommand::PauseTopic { .. } => {
                let (topic, paused) = match &command {
                    QueueCommand::PauseTopic { topic, paused } => (topic.clone(), *paused),
                    _ => unreachable!(),
                };
                if self.control_plane_enabled() {
                    return self.pause_topic_federated(&topic, paused).await;
                }
                let response = self.metadata_group().write(command).await?;
                ensure_response(&response)?;
                self.sync_catalog_topic(&topic).await?;
                Ok(response)
            }
            command @ (QueueCommand::Finish { .. } | QueueCommand::Requeue { .. }) => {
                let (topic, message_id) =
                    command_message(&command).expect("matched message command");
                if self.control_plane_enabled() {
                    return self
                        .write_catalog_message(topic, message_id, command.clone())
                        .await;
                }
                let partition = self.partition_for_message(topic, message_id)?;
                self.write_partition(&partition, command).await
            }
            QueueCommand::ProtectiveEvict {
                operation_id,
                topic,
                partition,
                through_message_id,
            } => {
                let descriptor = self
                    .metadata
                    .topic_route(&topic)
                    .and_then(|route| route.partition_by_number(partition))
                    .ok_or_else(|| anyhow::anyhow!("protective eviction partition not found"))?;
                self.write_partition(
                    descriptor.as_ref(),
                    QueueCommand::ProtectiveEvict {
                        operation_id,
                        topic,
                        partition,
                        through_message_id,
                    },
                )
                .await
            }
            QueueCommand::DeleteTopic { topic } => self.delete_topic_durable(&topic).await,
            command @ (QueueCommand::PrepareChannel { .. }
            | QueueCommand::PrepareDeleteTopic { .. }
            | QueueCommand::CompleteDeleteTopic { .. }
            | QueueCommand::ActivateChannel { .. }
            | QueueCommand::PrepareDeleteChannel { .. }
            | QueueCommand::CompleteDeleteChannel { .. }) => {
                self.metadata_group().write(command).await
            }
        }
    }

    pub async fn renew_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        expires_at_ms: i64,
    ) -> anyhow::Result<()> {
        let response = self
            .write(QueueCommand::RenewEphemeralLease {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                lease_id,
                expires_at_ms,
            })
            .await?;
        ensure_response(&response)
    }

    pub async fn create_ephemeral_channel(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        expires_at_ms: i64,
    ) -> anyhow::Result<QueueResponse> {
        if self.control_plane_enabled() {
            if self.catalog_topic_descriptor(topic).await?.is_none() {
                self.ensure_topic(topic, None, None).await?;
            }
            let leased = self
                .write_control(QueueCommand::RenewCatalogEphemeralLease {
                    topic: topic.to_owned(),
                    channel: channel.to_owned(),
                    lease_id,
                    expires_at_ms,
                })
                .await?;
            ensure_response(&leased)?;
            self.invalidate_catalog_topic(topic).await;
            return self.create_channel_federated(topic, channel).await;
        }
        self.ensure_topic(topic, None, None).await?;
        let generation = self.prepare_channel_generation(topic, channel).await?;
        let leased = self
            .metadata_group()
            .write(QueueCommand::RenewEphemeralLease {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                lease_id,
                expires_at_ms,
            })
            .await?;
        ensure_response(&leased)?;
        let response = self
            .broadcast_topic(
                topic,
                QueueCommand::CreateChannel {
                    topic: topic.to_owned(),
                    channel: channel.to_owned(),
                },
            )
            .await?;
        let activated = self
            .metadata_group()
            .write(QueueCommand::ActivateChannel {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                generation,
            })
            .await?;
        ensure_response(&activated)?;
        self.sync_catalog_topic(topic).await?;
        Ok(response)
    }

    pub async fn release_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
    ) -> anyhow::Result<()> {
        let response = self
            .write(QueueCommand::ReleaseEphemeralLease {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                lease_id,
            })
            .await?;
        ensure_response(&response)
    }
}
