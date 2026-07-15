use super::*;

pub(super) fn is_fatal_queue_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::Storage(_) | BrokerError::Io(_) | BrokerError::InvalidRecord(_)
    )
}

pub(super) fn recovery_tail(
    entries: &[Entry<TypeConfig>],
    snapshot_index: Option<u64>,
    boundary_index: Option<u64>,
) -> io::Result<Vec<Entry<TypeConfig>>> {
    let Some(boundary_index) = boundary_index else {
        return Ok(Vec::new());
    };
    if snapshot_index.is_some_and(|index| index >= boundary_index) {
        return Ok(Vec::new());
    }

    let mut expected = snapshot_index.map_or(0, |index| index + 1);
    let mut tail = Vec::new();
    for entry in entries {
        let index = entry.log_id.index;
        if snapshot_index.is_some_and(|snapshot| index <= snapshot) || index > boundary_index {
            continue;
        }
        if index != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Raft recovery gap: expected index {expected}, found {index}"),
            ));
        }
        tail.push(entry.clone());
        expected += 1;
    }
    if expected <= boundary_index {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Raft recovery stops at index {}, below applied boundary {boundary_index}",
                expected.saturating_sub(1)
            ),
        ));
    }
    Ok(tail)
}

pub(super) fn replay_metadata(
    metadata: &MetadataCatalog,
    command: &QueueCommand,
) -> Result<(), String> {
    match command {
        QueueCommand::Batch { commands } => {
            for command in commands {
                replay_metadata(metadata, command)?;
            }
        }
        QueueCommand::Publish { topic, .. } => {
            metadata.ensure_topic(topic, None, None)?;
        }
        QueueCommand::CreateTopic {
            topic,
            partitions,
            replication_factor,
        } => {
            metadata.ensure_topic(topic, *partitions, *replication_factor)?;
        }
        QueueCommand::DeleteTopic { topic } => metadata.delete_topic(topic),
        QueueCommand::PrepareDeleteTopic { topic } => {
            metadata.prepare_delete_topic(topic);
        }
        QueueCommand::CompleteDeleteTopic { topic } => metadata.delete_topic(topic),
        QueueCommand::PauseTopic { topic, paused } => {
            metadata.set_topic_paused(topic, *paused)?;
        }
        QueueCommand::SetChannelMetadataPaused {
            topic,
            channel,
            paused,
        } => metadata.set_channel_metadata_paused(topic, channel, *paused)?,
        QueueCommand::PrepareChannel { topic, channel } => {
            metadata.prepare_channel(topic, channel)?;
        }
        QueueCommand::ActivateChannel {
            topic,
            channel,
            generation,
        } => metadata.activate_channel(topic, channel, *generation)?,
        QueueCommand::PrepareDeleteChannel { topic, channel } => {
            metadata.prepare_delete_channel(topic, channel)?;
        }
        QueueCommand::CompleteDeleteChannel {
            topic,
            channel,
            generation,
        } => metadata.complete_delete_channel(topic, channel, *generation)?,
        QueueCommand::UpdatePartitionReplicas { group_id, replicas } => {
            metadata.update_partition_replicas(*group_id, replicas.clone())?;
        }
        QueueCommand::RegisterNode { descriptor } => {
            metadata.register_node(descriptor.clone())?;
        }
        QueueCommand::SetNodeDrained { node_id, drained } => {
            metadata.set_node_drained(*node_id, *drained)?;
        }
        QueueCommand::ReservePartitionExpansion {
            topic,
            target_partitions,
            max_partitions,
            now_ms,
        } => {
            metadata.reserve_partition_expansion(
                topic,
                *target_partitions,
                *max_partitions,
                *now_ms,
            )?;
        }
        QueueCommand::AdvancePartitionExpansion {
            operation_id,
            phase,
            state,
            now_ms,
            error,
        } => metadata.advance_partition_expansion(
            *operation_id,
            *phase,
            *state,
            *now_ms,
            error.clone(),
        )?,
        QueueCommand::ActivatePartitionExpansion {
            operation_id,
            expected_channel_revision,
            now_ms,
        } => metadata.activate_partition_expansion(
            *operation_id,
            *expected_channel_revision,
            *now_ms,
        )?,
        QueueCommand::CancelPartitionExpansion {
            operation_id,
            now_ms,
        } => {
            metadata.cancel_partition_expansion(*operation_id, *now_ms)?;
        }
        QueueCommand::SetOperationPaused {
            operation_id,
            paused,
        } => metadata.set_operation_paused(*operation_id, *paused)?,
        QueueCommand::SetAutomationEnabled { enabled } => {
            metadata.set_automation_enabled(*enabled);
        }
        QueueCommand::SetMaintenance { node_id, lease } => {
            metadata.set_maintenance(*node_id, lease.clone())?;
        }
        QueueCommand::CreateOperation {
            kind,
            now_ms,
            history_limit,
        } => {
            metadata.create_operation(kind.clone(), *now_ms, *history_limit)?;
        }
        QueueCommand::UpdateOperation {
            operation_id,
            phase,
            state,
            now_ms,
            error,
            progress,
        } => metadata.update_operation(
            *operation_id,
            *phase,
            *state,
            *now_ms,
            error.clone(),
            progress.clone(),
        )?,
        QueueCommand::ObserveNodeHealth {
            node_id,
            healthy,
            disk_used_percent,
            disk_free_bytes,
            storage_eligible,
            now_ms,
        } => metadata.observe_node_health(
            *node_id,
            *healthy,
            *disk_used_percent,
            *disk_free_bytes,
            *storage_eligible,
            *now_ms,
        )?,
        QueueCommand::RenewEphemeralLease {
            topic,
            channel,
            lease_id,
            expires_at_ms,
        } => metadata.renew_ephemeral_lease(topic, channel, *lease_id, *expires_at_ms)?,
        QueueCommand::ReleaseEphemeralLease {
            topic,
            channel,
            lease_id,
        } => metadata.release_ephemeral_lease(topic, channel, *lease_id)?,
        QueueCommand::ActivateFeatureLevel { feature_level } => {
            metadata.activate_feature_level(*feature_level)?;
        }
        QueueCommand::RegisterFederationNode { node } => {
            metadata.register_federation_node(node.clone())?;
        }
        QueueCommand::ApplyRootAction {
            action,
            now_ms,
            policy,
        } => metadata.apply_root_action(action.clone(), *now_ms, *policy)?,
        QueueCommand::BeginPartitionMigration {
            topic,
            partition,
            target,
            now_ms,
            max_home_cells,
        } => {
            metadata.begin_partition_migration(
                topic,
                *partition,
                *target,
                *now_ms,
                *max_home_cells,
            )?;
        }
        QueueCommand::AdvancePartitionMigration {
            operation_id,
            expected,
            next,
            observed_lag_entries,
            now_ms,
            max_home_cells,
        } => metadata.advance_partition_migration(
            *operation_id,
            *expected,
            *next,
            *observed_lag_entries,
            *now_ms,
            *max_home_cells,
        )?,
        QueueCommand::MarkPartitionMigrationNeedsOperator {
            operation_id,
            error,
            now_ms,
        } => metadata.mark_partition_migration_needs_operator(
            *operation_id,
            error.clone(),
            *now_ms,
        )?,
        QueueCommand::ActivateBucketMove {
            topic,
            start,
            end,
            target,
            expected_epoch,
        } => {
            metadata.activate_bucket_move(topic, *start, *end, *target, *expected_epoch)?;
        }
        QueueCommand::ActivateScopedFeature {
            activation,
            observed_protocol_floor,
        } => {
            metadata.activate_scoped_feature(activation.clone(), *observed_protocol_floor)?;
        }
        QueueCommand::SyncCatalogTopic { descriptor } => {
            metadata.sync_catalog_topic(descriptor.clone())?;
        }
        QueueCommand::RemoveCatalogTopic { topic } => metadata.remove_catalog_topic(topic),
        QueueCommand::BeginCatalogTopicDeletion { topic } => {
            metadata.begin_catalog_topic_deletion(topic)?;
        }
        QueueCommand::PrepareCatalogChannel { topic, channel } => {
            metadata.prepare_catalog_channel(topic, channel)?;
        }
        QueueCommand::UpdateCatalogChannel {
            topic,
            channel,
            generation,
            state,
            paused,
        } => {
            metadata.update_catalog_channel(topic, channel, *generation, state.clone(), *paused)?
        }
        QueueCommand::RemoveCatalogChannel {
            topic,
            channel,
            generation,
        } => metadata.remove_catalog_channel(topic, channel, *generation)?,
        QueueCommand::RenewCatalogEphemeralLease {
            topic,
            channel,
            lease_id,
            expires_at_ms,
        } => metadata.renew_catalog_ephemeral_lease(topic, channel, *lease_id, *expires_at_ms)?,
        QueueCommand::ReleaseCatalogEphemeralLease {
            topic,
            channel,
            lease_id,
            now_ms,
        } => {
            metadata.release_catalog_ephemeral_lease(topic, channel, *lease_id, *now_ms)?;
        }
        QueueCommand::ExpireCatalogEphemeralLeases { now_ms } => {
            metadata.expire_catalog_ephemeral_leases(*now_ms);
        }
        QueueCommand::InstallChannelMetadata { topic, descriptor } => {
            metadata.install_channel_metadata(topic, descriptor.clone())?;
        }
        QueueCommand::UpsertFederatedPartition {
            template,
            partition,
        } => {
            metadata.upsert_federated_partition(template, partition.clone())?;
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn rebuild_broker_projection(
    broker: &Broker,
    role: &StateMachineRole,
    commands: &[QueueCommand],
) -> Result<(), BrokerError> {
    for command in commands {
        match command {
            QueueCommand::Batch { commands } => {
                rebuild_broker_projection(broker, role, commands)?;
            }
            QueueCommand::CreateTopic {
                topic, partitions, ..
            } if role.carries_cell_metadata() => {
                broker.create_topic(topic, *partitions)?;
            }
            QueueCommand::DeleteTopic { topic } if role.carries_cell_metadata() => {
                broker.delete_topic(topic)?;
            }
            QueueCommand::PrepareDeleteTopic { .. } if role.carries_cell_metadata() => {}
            QueueCommand::CompleteDeleteTopic { topic } if role.carries_cell_metadata() => {
                broker.delete_topic(topic)?;
            }
            QueueCommand::PauseTopic { topic, paused } if role.carries_cell_metadata() => {
                broker.set_topic_paused(topic, *paused)?;
            }
            command => rebuild_partition_command(broker, role, command)?,
        }
    }
    Ok(())
}

pub(super) fn rebuild_broker_projection_refs(
    broker: &Broker,
    role: &StateMachineRole,
    command: &QueueCommand,
    payloads: &[PayloadRef],
    log_index: u64,
) -> Result<(), BrokerError> {
    let mut payloads = payloads.iter().cloned();
    rebuild_command_refs(broker, role, command, &mut payloads, log_index)?;
    if payloads.next().is_some() {
        return Err(BrokerError::InvalidRecord(
            "Raft entry contains unused payload references".into(),
        ));
    }
    Ok(())
}

fn rebuild_command_refs(
    broker: &Broker,
    role: &StateMachineRole,
    command: &QueueCommand,
    payloads: &mut impl Iterator<Item = PayloadRef>,
    log_index: u64,
) -> Result<(), BrokerError> {
    match command {
        QueueCommand::Batch { commands } => {
            for command in commands {
                rebuild_command_refs(broker, role, command, payloads, log_index)?;
            }
            Ok(())
        }
        QueueCommand::Publish {
            operation_id,
            topic: command_topic,
            bodies,
            timestamp_ns,
            available_at_ms,
            ..
        } => {
            let StateMachineRole::Partition { topic, partition } = role else {
                return rebuild_partition_command(broker, role, command);
            };
            if command_topic != topic {
                return Ok(());
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
            broker.publish_replicated_refs(
                *operation_id,
                topic,
                references,
                *timestamp_ns,
                *available_at_ms,
                *partition,
                log_index,
            )?;
            Ok(())
        }
        _ => rebuild_partition_command(broker, role, command),
    }
}

pub(super) fn reconcile_broker_topics(
    broker: &Broker,
    metadata: &ClusterMetadata,
) -> Result<(), BrokerError> {
    for topic in metadata.topics.values() {
        let layouts: Vec<_> = topic
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle != crate::PartitionLifecycle::Retired)
            .map(|partition| rustqueue_queue::PartitionLayout {
                number: partition.number,
                slot: partition.slot,
                cell_id: partition.origin_cell.0,
                group_id: partition.group_id,
                wire_incarnation: partition.wire_incarnation,
            })
            .collect();
        if layouts.is_empty() {
            continue;
        }
        broker.ensure_topic_layout_v4(&topic.name, &layouts, &topic.key_routing_slots)?;
        broker.set_topic_paused(&topic.name, topic.paused)?;
    }
    Ok(())
}

pub(super) fn ensure_broker_topic(
    broker: &Broker,
    topic: &crate::TopicDescriptor,
) -> Result<(), BrokerError> {
    let layouts: Vec<_> = topic
        .partitions
        .iter()
        .map(|partition| rustqueue_queue::PartitionLayout {
            number: partition.number,
            slot: partition.slot,
            cell_id: partition.origin_cell.0,
            group_id: partition.group_id,
            wire_incarnation: partition.wire_incarnation,
        })
        .collect();
    if layouts.is_empty() {
        return Err(BrokerError::InvalidRecord("topic has no partitions".into()));
    }
    broker.ensure_topic_layout_v4(&topic.name, &layouts, &topic.key_routing_slots)
}

fn rebuild_partition_command(
    broker: &Broker,
    role: &StateMachineRole,
    command: &QueueCommand,
) -> Result<(), BrokerError> {
    let StateMachineRole::Partition { topic, partition } = role else {
        return Ok(());
    };
    match command {
        QueueCommand::Publish {
            operation_id,
            topic: command_topic,
            bodies,
            timestamp_ns,
            available_at_ms,
            ..
        } if command_topic == topic => {
            broker.publish_replicated(
                *operation_id,
                topic,
                bodies.clone(),
                *timestamp_ns,
                *available_at_ms,
                Some(*partition),
                None,
            )?;
        }
        QueueCommand::CreateChannel {
            topic: command_topic,
            channel,
        } if command_topic == topic => {
            broker.create_channel_partition(topic, channel, *partition)?;
        }
        QueueCommand::DeleteChannel {
            topic: command_topic,
            channel,
        } if command_topic == topic => {
            broker.delete_channel_partition(topic, channel, *partition)?;
        }
        QueueCommand::EmptyTopic {
            topic: command_topic,
        } if command_topic == topic => broker.empty_topic_partition(topic, *partition)?,
        QueueCommand::EmptyChannel {
            topic: command_topic,
            channel,
        } if command_topic == topic => {
            broker.empty_channel_partition(topic, channel, *partition)?;
        }
        QueueCommand::PauseChannel {
            topic: command_topic,
            channel,
            paused,
        } if command_topic == topic => {
            broker.set_channel_paused_partition(topic, channel, *partition, *paused)?;
        }
        QueueCommand::Finish {
            topic: command_topic,
            channel,
            message_id,
        } if command_topic == topic => broker.commit_finish(topic, channel, *message_id)?,
        QueueCommand::Requeue {
            topic: command_topic,
            channel,
            message_id,
            available_at_ms,
        } if command_topic == topic => {
            broker.commit_requeue_at(topic, channel, *message_id, *available_at_ms)?;
        }
        QueueCommand::ProtectiveEvict {
            topic: command_topic,
            partition: command_partition,
            through_message_id,
            ..
        } if command_topic == topic && command_partition == partition => {
            broker.protective_evict_through(topic, *partition, *through_message_id)?;
        }
        _ => {}
    }
    Ok(())
}
