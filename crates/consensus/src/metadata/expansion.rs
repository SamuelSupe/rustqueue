use super::placement::{
    choose_replicas, replica_loads, validate_failure_domains, validate_replication_factor,
};
use super::*;

impl MetadataCatalog {
    pub fn reserve_partition_expansion(
        &self,
        topic_name: &str,
        target_partitions: u16,
        max_partitions: u16,
        now_ms: i64,
    ) -> Result<MaintenanceOperation, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if let Some(existing) = active_expansion(&state, topic_name) {
            let same_target = matches!(
                existing.kind,
                OperationKind::ExpandPartitions {
                    target_partitions: target,
                    ..
                } if target == target_partitions
            );
            return if same_target {
                Ok(existing.clone())
            } else {
                Err("topic already has an active partition expansion".into())
            };
        }

        let topic = state
            .topics
            .get(topic_name)
            .cloned()
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic.state != TopicState::Active {
            return Err("topic is not active".into());
        }
        let source_partitions = topic
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
            .count() as u16;
        if target_partitions <= source_partitions {
            return Err("partition count can only be increased".into());
        }
        if target_partitions > max_partitions {
            return Err(format!(
                "target partition count exceeds configured maximum {max_partitions}"
            ));
        }
        let additional = target_partitions - source_partitions;
        let next_number = topic
            .partitions
            .iter()
            .map(|partition| partition.number)
            .max()
            .map_or(0u32, |number| number as u32 + 1);
        if next_number + additional as u32 > u16::MAX as u32 + 1 {
            return Err("partition number space exhausted".into());
        }

        let available_nodes: BTreeMap<_, _> = state
            .nodes
            .iter()
            .filter(|(id, _)| {
                !state.drained_nodes.contains(id)
                    && state
                        .node_health
                        .get(id)
                        .is_some_and(|health| health.available && health.storage_eligible)
                    && state
                        .maintenance_nodes
                        .get(id)
                        .is_none_or(|lease| lease.expires_at_ms <= now_ms)
            })
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        validate_replication_factor(topic.replication_factor, available_nodes.len())?;
        validate_failure_domains(&available_nodes, topic.replication_factor)?;

        let operation_id = state.next_operation_id;
        state.next_operation_id = state.next_operation_id.saturating_add(1);
        let mut loads = replica_loads(&state);
        let mut partitions = Vec::with_capacity(additional as usize);
        let mut catalog_partitions = Vec::with_capacity(additional as usize);
        let cell_id = state.cell_id;
        for offset in 0..additional as u32 {
            let group_id = state.next_group_id;
            let number = (next_number + offset) as u16;
            let slot = number
                .checked_add(1)
                .ok_or_else(|| "topic partition wire slot space exhausted".to_owned())?;
            let replicas = choose_replicas(
                &available_nodes,
                &loads,
                topic.replication_factor as usize,
                slot as usize,
            );
            for node in &replicas {
                *loads.entry(*node).or_default() += 1;
            }
            partitions.push(PartitionDescriptor {
                group_id,
                origin_cell: cell_id,
                number,
                slot,
                replication_factor: topic.replication_factor,
                replicas,
                leader_hint: None,
                lifecycle: PartitionLifecycle::Preparing,
                operation_id: Some(operation_id),
                home_cell: cell_id,
                wire_incarnation: 1,
            });
            catalog_partitions.push(crate::PartitionHome {
                id: crate::GlobalGroupId {
                    cell: cell_id,
                    local: group_id,
                },
                number: u32::from(number),
                wire_slot: slot,
                wire_incarnation: 1,
                home_cell: cell_id,
                lifecycle: crate::PartitionHomeLifecycle::Preparing,
                routing_epoch: state.routing_epoch,
            });
            state.next_group_id = state.next_group_id.saturating_add(1);
            state.next_slot = state.next_slot.max(u32::from(slot).saturating_add(1));
        }
        let groups = partitions
            .iter()
            .map(PartitionDescriptor::global_id)
            .collect();
        state
            .topics
            .get_mut(topic_name)
            .expect("topic was found above")
            .partitions
            .extend(partitions);
        state.catalog.reserve_partitions(
            topic_name,
            catalog_partitions,
            self.max_home_cells_per_topic,
        )?;
        let operation = MaintenanceOperation {
            id: operation_id,
            kind: OperationKind::ExpandPartitions {
                topic: topic_name.to_owned(),
                source_partitions,
                target_partitions,
                partition_groups: groups,
            },
            state: OperationState::Running,
            phase: OperationPhase::Reserved,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error: None,
            progress: OperationProgress::None,
        };
        state.operations.insert(operation_id, operation.clone());
        super::routes::bump_routing_epoch(&mut state);
        state.epoch = state.epoch.saturating_add(1);
        Ok(operation)
    }

    pub fn advance_partition_expansion(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        operation_state: OperationState,
        now_ms: i64,
        error: Option<String>,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state
            .operations
            .get_mut(&operation_id)
            .ok_or_else(|| "operation not found".to_owned())?;
        if !matches!(operation.kind, OperationKind::ExpandPartitions { .. }) {
            return Err("operation is not a partition expansion".into());
        }
        if matches!(
            operation.state,
            OperationState::Completed | OperationState::Cancelled
        ) {
            return Ok(());
        }
        operation.phase = phase;
        operation.updated_at_ms = now_ms;
        operation.error = error;
        operation.state = operation_state;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn activate_partition_expansion(
        &self,
        operation_id: u64,
        expected_channel_revision: u64,
        now_ms: i64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state
            .operations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| "operation not found".to_owned())?;
        let (topic_name, target_partitions, groups) = match operation.kind {
            OperationKind::ExpandPartitions {
                topic,
                target_partitions,
                partition_groups,
                ..
            } => (topic, target_partitions, partition_groups),
            _ => return Err("operation is not a partition expansion".into()),
        };
        if operation.state == OperationState::Completed {
            return Ok(());
        }
        if operation.state == OperationState::Cancelled {
            return Err("partition expansion was cancelled".into());
        }
        let global_groups = {
            let topic = state
                .topics
                .get_mut(&topic_name)
                .ok_or_else(|| "topic not found".to_owned())?;
            if topic.channel_catalog_revision != expected_channel_revision {
                return Err("channel catalog changed while expansion barriers were applied".into());
            }
            for group_id in &groups {
                let partition = topic
                    .partitions
                    .iter_mut()
                    .find(|partition| partition.global_id() == *group_id)
                    .ok_or_else(|| "reserved partition is missing".to_owned())?;
                if partition.lifecycle != PartitionLifecycle::Preparing
                    || partition.operation_id != Some(operation_id)
                {
                    return Err("reserved partition lifecycle mismatch".into());
                }
                partition.lifecycle = PartitionLifecycle::Active;
                partition.operation_id = None;
            }
            let active = topic
                .partitions
                .iter()
                .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
                .count() as u16;
            if active != target_partitions {
                return Err("activated partition count does not match expansion target".into());
            }
            topic.topology_generation = topic.topology_generation.saturating_add(1);
            groups.iter().copied().collect::<BTreeSet<_>>()
        };
        state
            .catalog
            .activate_partitions(&topic_name, &global_groups)?;
        let operation = state.operations.get_mut(&operation_id).unwrap();
        operation.phase = OperationPhase::Completed;
        operation.state = OperationState::Completed;
        operation.updated_at_ms = now_ms;
        operation.error = None;
        state.epoch = state.epoch.saturating_add(1);
        super::routes::bump_routing_epoch(&mut state);
        Ok(())
    }

    pub fn cancel_partition_expansion(
        &self,
        operation_id: u64,
        now_ms: i64,
    ) -> Result<Vec<crate::GlobalGroupId>, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state
            .operations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| "operation not found".to_owned())?;
        let (topic_name, groups) = match operation.kind {
            OperationKind::ExpandPartitions {
                topic,
                partition_groups,
                ..
            } => (topic, partition_groups),
            _ => return Err("operation is not a partition expansion".into()),
        };
        if operation.state == OperationState::Completed {
            return Err("activated partition expansion cannot be cancelled".into());
        }
        if operation.state == OperationState::Cancelled {
            return Ok(groups);
        }
        let global_groups = {
            let topic = state
                .topics
                .get_mut(&topic_name)
                .ok_or_else(|| "topic not found".to_owned())?;
            for group_id in &groups {
                if let Some(partition) = topic
                    .partitions
                    .iter_mut()
                    .find(|partition| partition.global_id() == *group_id)
                {
                    partition.lifecycle = PartitionLifecycle::Retired;
                    partition.operation_id = None;
                }
            }
            groups.iter().copied().collect::<BTreeSet<_>>()
        };
        state
            .catalog
            .retire_partitions(&topic_name, &global_groups)?;
        let operation = state.operations.get_mut(&operation_id).unwrap();
        operation.state = OperationState::Cancelled;
        operation.updated_at_ms = now_ms;
        operation.error = None;
        state.epoch = state.epoch.saturating_add(1);
        super::routes::bump_routing_epoch(&mut state);
        Ok(groups)
    }

    pub fn pending_partition_expansions(&self) -> Vec<MaintenanceOperation> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .operations
            .values()
            .filter(|operation| {
                matches!(operation.kind, OperationKind::ExpandPartitions { .. })
                    && operation.state == OperationState::Running
            })
            .cloned()
            .collect()
    }
}

fn active_expansion<'a>(
    state: &'a ClusterMetadata,
    topic: &str,
) -> Option<&'a MaintenanceOperation> {
    state.operations.values().find(|operation| {
        matches!(
            &operation.kind,
            OperationKind::ExpandPartitions { topic: name, .. } if name == topic
        ) && !matches!(
            operation.state,
            OperationState::Completed | OperationState::Cancelled
        )
    })
}
