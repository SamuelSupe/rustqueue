use super::*;
use futures::{stream, StreamExt, TryStreamExt};

const MAX_CONCURRENT_HOME_CELLS: usize = 16;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FederationChannelAction {
    Apply(crate::ChannelDescriptor),
    Remove { channel: String, generation: u64 },
    Broadcast(QueueCommand),
    PauseTopic { paused: bool },
    DeleteTopic,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationChannelForward {
    pub topic: String,
    pub action: FederationChannelAction,
}

impl ClusterRuntime {
    pub(super) async fn expire_federated_ephemeral_leases(&self) -> anyhow::Result<usize> {
        let Some(control) = &self.control else {
            return Ok(0);
        };
        let Some(catalog_group) = &control.catalog else {
            return Ok(0);
        };
        if catalog_group.leader_state().0 != Some(self.node_id) {
            return Ok(0);
        }
        let now_ms = wall_time_ms().min(i64::MAX as u64) as i64;
        let has_expired = control
            .metadata
            .catalog_snapshot()
            .topics
            .values()
            .any(|topic| {
                topic.channels.values().any(|channel| {
                    channel.ephemeral
                        && channel.lease_started
                        && channel.state == ChannelLifecycle::Active
                        && channel
                            .leases
                            .values()
                            .all(|expires_at| *expires_at <= now_ms)
                })
            });
        if !has_expired {
            return Ok(0);
        }
        let response = self
            .write_control(QueueCommand::ExpireCatalogEphemeralLeases { now_ms })
            .await?;
        ensure_response(&response)?;
        Ok(response.message_ids.first().copied().unwrap_or_default() as usize)
    }

    pub async fn forwarded_channel_local(
        &self,
        forward: FederationChannelForward,
    ) -> Result<QueueResponse, FederationForwardError> {
        let topic_exists = self.metadata.topic(&forward.topic).is_some();
        if !topic_exists
            && !matches!(
                &forward.action,
                FederationChannelAction::Remove { .. } | FederationChannelAction::DeleteTopic
            )
        {
            return Err(FederationForwardError::StaleRoute(
                "topic is not installed in this Home Cell".into(),
            ));
        }
        match forward.action {
            FederationChannelAction::Apply(descriptor) => {
                self.apply_channel_descriptor_local(&forward.topic, descriptor)
                    .await
            }
            FederationChannelAction::Remove {
                channel,
                generation,
            } => {
                let Some(existing) = self.metadata.channel(&forward.topic, &channel) else {
                    return Ok(QueueResponse::default());
                };
                if existing.generation > generation {
                    return Ok(QueueResponse::default());
                }
                self.delete_channel_generation_local(&forward.topic, existing)
                    .await
            }
            FederationChannelAction::Broadcast(command) => {
                if !matches!(
                    &command,
                    QueueCommand::EmptyTopic { .. } | QueueCommand::EmptyChannel { .. }
                ) || command_topic(&command) != Some(forward.topic.as_str())
                {
                    return Err(FederationForwardError::Invalid(
                        "invalid federated partition broadcast".into(),
                    ));
                }
                self.broadcast_topic(&forward.topic, command).await
            }
            FederationChannelAction::PauseTopic { paused } => {
                self.pause_topic_cell_local(&forward.topic, paused).await
            }
            FederationChannelAction::DeleteTopic => {
                if !topic_exists {
                    Ok(QueueResponse::default())
                } else {
                    self.delete_topic_cell_local(&forward.topic).await
                }
            }
        }
        .map_err(|error| FederationForwardError::Unavailable(error.to_string()))
    }

    pub(super) async fn create_channel_federated(
        &self,
        topic: &str,
        channel: &str,
    ) -> anyhow::Result<QueueResponse> {
        if self.catalog_topic_descriptor(topic).await?.is_none() {
            self.ensure_topic(topic, None, None).await?;
        }
        let prepared = self
            .write_control(QueueCommand::PrepareCatalogChannel {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
            })
            .await?;
        ensure_response(&prepared)?;
        let generation = *prepared
            .message_ids
            .first()
            .ok_or_else(|| anyhow::anyhow!("Catalog did not return a channel generation"))?;
        self.invalidate_catalog_topic(topic).await;
        let mut descriptor = self
            .catalog_topic_descriptor(topic)
            .await?
            .and_then(|topic| topic.channels.get(channel).cloned())
            .ok_or_else(|| anyhow::anyhow!("prepared Catalog channel disappeared"))?;
        if descriptor.generation != generation {
            anyhow::bail!("prepared Catalog channel generation changed");
        }
        descriptor.state = ChannelLifecycle::Active;
        let response = self
            .apply_channel_all_homes(topic, FederationChannelAction::Apply(descriptor.clone()))
            .await?;
        self.update_catalog_channel(topic, &descriptor).await?;
        Ok(response)
    }

    pub(super) async fn delete_channel_federated(
        &self,
        topic: &str,
        channel: &str,
    ) -> anyhow::Result<QueueResponse> {
        let Some(catalog) = self.catalog_topic_descriptor(topic).await? else {
            return Ok(QueueResponse::default());
        };
        let Some(mut descriptor) = catalog.channels.get(channel).cloned() else {
            return Ok(QueueResponse::default());
        };
        descriptor.state = ChannelLifecycle::Deleting;
        self.update_catalog_channel(topic, &descriptor).await?;
        let response = self
            .apply_channel_all_homes(topic, FederationChannelAction::Apply(descriptor.clone()))
            .await?;
        let removed = self
            .write_control(QueueCommand::RemoveCatalogChannel {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                generation: descriptor.generation,
            })
            .await?;
        ensure_response(&removed)?;
        self.invalidate_catalog_topic(topic).await;
        Ok(response)
    }

    pub(super) async fn pause_channel_federated(
        &self,
        topic: &str,
        channel: &str,
        paused: bool,
    ) -> anyhow::Result<QueueResponse> {
        let catalog = self
            .catalog_topic_descriptor(topic)
            .await?
            .ok_or_else(|| anyhow::anyhow!("topic not found"))?;
        let mut descriptor = catalog
            .channels
            .get(channel)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("channel not found"))?;
        descriptor.paused = paused;
        self.update_catalog_channel(topic, &descriptor).await?;
        self.apply_channel_all_homes(topic, FederationChannelAction::Apply(descriptor))
            .await
    }

    pub(super) async fn broadcast_federated_command(
        &self,
        topic: &str,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        self.apply_channel_all_homes(topic, FederationChannelAction::Broadcast(command))
            .await
    }

    pub(super) async fn pause_topic_federated(
        &self,
        topic: &str,
        paused: bool,
    ) -> anyhow::Result<QueueResponse> {
        self.apply_channel_all_homes(topic, FederationChannelAction::PauseTopic { paused })
            .await
    }

    pub(super) async fn delete_topic_federated(
        &self,
        topic: &str,
    ) -> anyhow::Result<QueueResponse> {
        let Some(catalog) = self.catalog_topic_descriptor(topic).await? else {
            return if self.metadata.topic(topic).is_some() {
                self.delete_topic_cell_local(topic).await
            } else {
                Ok(QueueResponse::default())
            };
        };
        if !catalog.deleting {
            let prepared = self
                .write_control(QueueCommand::BeginCatalogTopicDeletion {
                    topic: topic.to_owned(),
                })
                .await?;
            ensure_response(&prepared)?;
            self.invalidate_catalog_topic(topic).await;
        }
        let response = self
            .apply_channel_all_homes(topic, FederationChannelAction::DeleteTopic)
            .await?;
        self.remove_control_topic(topic).await?;
        Ok(response)
    }

    pub(super) async fn reconcile_federated_topic_deletions(&self) -> anyhow::Result<usize> {
        let Some(control) = &self.control else {
            return Ok(0);
        };
        let Some(catalog_group) = &control.catalog else {
            return Ok(0);
        };
        if catalog_group.leader_state().0 != Some(self.node_id) {
            return Ok(0);
        }
        let topics = control
            .metadata
            .catalog_snapshot()
            .topics
            .into_values()
            .filter(|topic| topic.deleting)
            .map(|topic| topic.name)
            .collect::<Vec<_>>();
        for topic in &topics {
            self.delete_topic_federated(topic).await?;
        }
        Ok(topics.len())
    }

    pub(super) async fn reconcile_federated_channels(&self) -> anyhow::Result<usize> {
        if !self.control_plane_enabled() {
            return Ok(0);
        }
        let topics = self.metadata.snapshot().topics;
        let mut reconciled = 0;
        for local_topic in topics.into_values() {
            if local_topic.state != crate::TopicState::Active {
                continue;
            }
            let Some(catalog) = self.catalog_topic_descriptor(&local_topic.name).await? else {
                continue;
            };
            if local_topic.paused != catalog.paused {
                self.pause_topic_cell_local(&local_topic.name, catalog.paused)
                    .await?;
                reconciled += 1;
            }
            for descriptor in catalog.channels.values() {
                match descriptor.state {
                    ChannelLifecycle::Preparing => {
                        let mut active = descriptor.clone();
                        active.state = ChannelLifecycle::Active;
                        self.apply_channel_all_homes(
                            &local_topic.name,
                            FederationChannelAction::Apply(active.clone()),
                        )
                        .await?;
                        self.update_catalog_channel(&local_topic.name, &active)
                            .await?;
                        reconciled += 1;
                    }
                    ChannelLifecycle::Deleting => {
                        self.apply_channel_all_homes(
                            &local_topic.name,
                            FederationChannelAction::Apply(descriptor.clone()),
                        )
                        .await?;
                        let response = self
                            .write_control(QueueCommand::RemoveCatalogChannel {
                                topic: local_topic.name.clone(),
                                channel: descriptor.name.clone(),
                                generation: descriptor.generation,
                            })
                            .await?;
                        ensure_response(&response)?;
                        self.invalidate_catalog_topic(&local_topic.name).await;
                        reconciled += 1;
                    }
                    ChannelLifecycle::Active => {
                        if self.metadata.channel(&local_topic.name, &descriptor.name)
                            != Some(descriptor.clone())
                        {
                            self.forwarded_channel_local(FederationChannelForward {
                                topic: local_topic.name.clone(),
                                action: FederationChannelAction::Apply(descriptor.clone()),
                            })
                            .await
                            .map_err(anyhow::Error::msg)?;
                            reconciled += 1;
                        }
                    }
                }
            }
            for (channel, generation) in &catalog.channel_tombstones {
                if self
                    .metadata
                    .channel(&local_topic.name, channel)
                    .is_some_and(|local| local.generation <= *generation)
                {
                    self.forwarded_channel_local(FederationChannelForward {
                        topic: local_topic.name.clone(),
                        action: FederationChannelAction::Remove {
                            channel: channel.clone(),
                            generation: *generation,
                        },
                    })
                    .await
                    .map_err(anyhow::Error::msg)?;
                    reconciled += 1;
                }
            }
        }
        self.federation_metrics.reconciled_channels(reconciled);
        Ok(reconciled)
    }

    async fn update_catalog_channel(
        &self,
        topic: &str,
        descriptor: &crate::ChannelDescriptor,
    ) -> anyhow::Result<()> {
        let response = self
            .write_control(QueueCommand::UpdateCatalogChannel {
                topic: topic.to_owned(),
                channel: descriptor.name.clone(),
                generation: descriptor.generation,
                state: descriptor.state.clone(),
                paused: descriptor.paused,
            })
            .await?;
        ensure_response(&response)?;
        self.invalidate_catalog_topic(topic).await;
        Ok(())
    }

    async fn apply_channel_all_homes(
        &self,
        topic: &str,
        action: FederationChannelAction,
    ) -> anyhow::Result<QueueResponse> {
        let catalog = self
            .catalog_topic_descriptor(topic)
            .await?
            .ok_or_else(|| anyhow::anyhow!("topic not found"))?;
        let responses = stream::iter(catalog.home_cells)
            .map(|cell| {
                let forward = FederationChannelForward {
                    topic: topic.to_owned(),
                    action: action.clone(),
                };
                async move {
                    if cell == self.metadata.snapshot().cell_id {
                        self.forwarded_channel_local(forward)
                            .await
                            .map_err(anyhow::Error::msg)
                    } else {
                        self.post_home(
                            cell,
                            "channel",
                            &forward,
                            INTERNAL_SMALL_FRAME_BYTES,
                            INTERNAL_WRITE_RESPONSE_BYTES,
                        )
                        .await
                        .map_err(anyhow::Error::msg)
                    }
                }
            })
            .buffer_unordered(MAX_CONCURRENT_HOME_CELLS)
            .try_collect()
            .await?;
        Ok(QueueResponse {
            message_ids: Vec::new(),
            error: None,
            results: responses,
        })
    }

    async fn apply_channel_descriptor_local(
        &self,
        topic: &str,
        descriptor: crate::ChannelDescriptor,
    ) -> anyhow::Result<QueueResponse> {
        if descriptor.state == ChannelLifecycle::Deleting {
            let installed = self
                .metadata_group()
                .write(QueueCommand::InstallChannelMetadata {
                    topic: topic.to_owned(),
                    descriptor: descriptor.clone(),
                })
                .await?;
            ensure_response(&installed)?;
            return self
                .delete_channel_generation_local(topic, descriptor)
                .await;
        }
        let mut preparing = descriptor.clone();
        preparing.state = ChannelLifecycle::Preparing;
        let installed = self
            .metadata_group()
            .write(QueueCommand::InstallChannelMetadata {
                topic: topic.to_owned(),
                descriptor: preparing,
            })
            .await?;
        ensure_response(&installed)?;
        let created = self
            .broadcast_channel_barrier(
                topic,
                QueueCommand::CreateChannel {
                    topic: topic.to_owned(),
                    channel: descriptor.name.clone(),
                },
            )
            .await?;
        let installed = self
            .metadata_group()
            .write(QueueCommand::InstallChannelMetadata {
                topic: topic.to_owned(),
                descriptor: descriptor.clone(),
            })
            .await?;
        ensure_response(&installed)?;
        self.broadcast_channel_barrier(
            topic,
            QueueCommand::PauseChannel {
                topic: topic.to_owned(),
                channel: descriptor.name,
                paused: descriptor.paused,
            },
        )
        .await?;
        Ok(created)
    }

    async fn delete_channel_generation_local(
        &self,
        topic: &str,
        descriptor: crate::ChannelDescriptor,
    ) -> anyhow::Result<QueueResponse> {
        let deleted = self
            .broadcast_channel_barrier(
                topic,
                QueueCommand::DeleteChannel {
                    topic: topic.to_owned(),
                    channel: descriptor.name.clone(),
                },
            )
            .await?;
        let completed = self
            .metadata_group()
            .write(QueueCommand::CompleteDeleteChannel {
                topic: topic.to_owned(),
                channel: descriptor.name,
                generation: descriptor.generation,
            })
            .await?;
        ensure_response(&completed)?;
        Ok(deleted)
    }

    async fn pause_topic_cell_local(
        &self,
        topic: &str,
        paused: bool,
    ) -> anyhow::Result<QueueResponse> {
        let response = self
            .metadata_group()
            .write(QueueCommand::PauseTopic {
                topic: topic.to_owned(),
                paused,
            })
            .await?;
        ensure_response(&response)?;
        self.sync_catalog_topic(topic).await?;
        Ok(response)
    }
}
