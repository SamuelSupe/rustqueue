use super::catalog::build_bucket_ranges;
use super::{CatalogState, CatalogTopic, PartitionHome, PartitionHomeLifecycle, RoutingMode};
use crate::{PartitionLifecycle, TopicDescriptor};
use std::collections::{BTreeMap, BTreeSet};

impl CatalogState {
    pub fn sync_topic(
        &mut self,
        descriptor: &TopicDescriptor,
        feature_level: u64,
        max_home_cells: usize,
    ) -> Result<(), String> {
        if self
            .topics
            .get(&descriptor.name)
            .is_some_and(|topic| topic.deleting)
        {
            return Err("Catalog topic deletion is in progress".into());
        }
        let incoming = descriptor
            .partitions
            .iter()
            .map(|partition| PartitionHome {
                id: partition.global_id(),
                number: u32::from(partition.number),
                wire_slot: partition.slot,
                wire_incarnation: partition.wire_incarnation,
                home_cell: partition.home_cell,
                lifecycle: match partition.lifecycle {
                    PartitionLifecycle::Preparing => PartitionHomeLifecycle::Preparing,
                    PartitionLifecycle::Active => PartitionHomeLifecycle::Active,
                    PartitionLifecycle::Retired => PartitionHomeLifecycle::Retired,
                },
                routing_epoch: descriptor.topology_generation,
            })
            .collect::<Vec<_>>();
        if incoming.is_empty() {
            return Err("Catalog topic requires at least one partition".into());
        }
        let topic = self
            .topics
            .entry(descriptor.name.clone())
            .or_insert_with(|| CatalogTopic {
                name: descriptor.name.clone(),
                routing_mode: RoutingMode::Elastic,
                topology_generation: descriptor.topology_generation,
                routing_epoch: 1,
                catalog_revision: 0,
                feature_level,
                paused: descriptor.paused,
                channels: BTreeMap::new(),
                channel_tombstones: BTreeMap::new(),
                partitions: BTreeMap::new(),
                partition_numbers: BTreeMap::new(),
                bucket_ranges: Vec::new(),
                home_cells: BTreeSet::new(),
                deleting: false,
            });
        let incoming_can_update_topic = incoming.iter().any(|partition| {
            topic
                .partitions
                .get(&partition.id)
                .is_none_or(|existing| existing.home_cell == partition.home_cell)
        });
        for partition in &incoming {
            if let Some(existing_id) = topic.partition_numbers.get(&partition.number) {
                if *existing_id != partition.id {
                    return Err(format!(
                        "partition number {} is already assigned to another global group",
                        partition.number
                    ));
                }
            }
            if let Some(existing) = topic.partitions.get(&partition.id) {
                if existing.number != partition.number
                    || existing.wire_slot != partition.wire_slot
                    || existing.wire_incarnation != partition.wire_incarnation
                {
                    return Err("partition identity changed during Catalog sync".into());
                }
            }
        }
        for partition in incoming {
            if let Some(existing) = topic.partitions.get_mut(&partition.id) {
                // Home ownership and migration fencing are changed only by a
                // Catalog migration command. A stale source Cell must never
                // be able to undo cutover by replaying its local descriptor.
                if existing.home_cell == partition.home_cell
                    && !matches!(
                        existing.lifecycle,
                        PartitionHomeLifecycle::Migrating | PartitionHomeLifecycle::Retired
                    )
                {
                    existing.lifecycle = partition.lifecycle;
                    existing.routing_epoch = existing.routing_epoch.max(partition.routing_epoch);
                }
                continue;
            }
            topic
                .partition_numbers
                .insert(partition.number, partition.id);
            topic.partitions.insert(partition.id, partition);
        }
        topic.home_cells = topic
            .partitions
            .values()
            .filter(|partition| partition.lifecycle != PartitionHomeLifecycle::Retired)
            .map(|partition| partition.home_cell)
            .collect();
        if topic.home_cells.len() > max_home_cells {
            return Err("topic exceeds max_home_cells_per_topic".into());
        }
        if topic.bucket_ranges.is_empty() {
            let active = topic
                .partition_numbers
                .values()
                .filter(|id| {
                    topic.partitions.get(id).is_some_and(|partition| {
                        partition.lifecycle == PartitionHomeLifecycle::Active
                    })
                })
                .copied()
                .collect::<Vec<_>>();
            if active.is_empty() {
                return Err("Catalog topic has no active partition".into());
            }
            topic.bucket_ranges = build_bucket_ranges(&active);
        }
        topic.topology_generation = topic
            .topology_generation
            .max(descriptor.topology_generation);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        topic.feature_level = topic.feature_level.max(feature_level);
        if incoming_can_update_topic {
            topic.paused = descriptor.paused;
        }
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn begin_topic_deletion(&mut self, topic: &str) -> Result<bool, String> {
        if self.migrations.values().any(|operation| {
            operation.topic == topic
                && !matches!(
                    operation.phase,
                    super::PartitionMigrationPhase::Completed
                        | super::PartitionMigrationPhase::NeedsOperator
                )
        }) {
            return Err("topic has an active partition migration".into());
        }
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "Catalog topic not found".to_owned())?;
        if topic.deleting {
            return Ok(false);
        }
        topic.deleting = true;
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(true)
    }

    pub fn remove_topic(&mut self, topic: &str) {
        if self.topics.remove(topic).is_some() {
            self.epoch = self.epoch.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellId, PartitionDescriptor, TopicState};

    fn descriptor(cell: u64, local_group: u64) -> TopicDescriptor {
        TopicDescriptor {
            name: "events".into(),
            state: TopicState::Active,
            replication_factor: 3,
            partitions: vec![PartitionDescriptor {
                group_id: local_group,
                origin_cell: CellId(cell),
                number: 0,
                slot: 1,
                replication_factor: 3,
                replicas: BTreeSet::from([cell * 10 + 1, cell * 10 + 2, cell * 10 + 3]),
                leader_hint: None,
                lifecycle: PartitionLifecycle::Active,
                operation_id: None,
                home_cell: CellId(cell),
                wire_incarnation: 1,
            }],
            channels: BTreeMap::new(),
            next_channel_generation: 1,
            paused: false,
            topology_generation: 1,
            key_routing_slots: vec![1],
            channel_catalog_revision: 0,
        }
    }

    #[test]
    fn concurrent_home_creation_cannot_alias_a_partition_number() {
        let mut catalog = CatalogState::default();
        catalog.sync_topic(&descriptor(1, 7), 1, 128).unwrap();
        assert!(catalog.sync_topic(&descriptor(2, 8), 1, 128).is_err());
        let topic = &catalog.topics["events"];
        assert_eq!(topic.partitions.len(), 1);
        assert_eq!(
            topic.partitions.values().next().unwrap().home_cell,
            CellId(1)
        );
    }

    #[test]
    fn stale_source_sync_cannot_undo_catalog_cutover() {
        let mut catalog = CatalogState::default();
        let source = descriptor(1, 7);
        let group = source.partitions[0].global_id();
        catalog.sync_topic(&source, 1, 128).unwrap();
        catalog
            .move_partition_home("events", group, CellId(1), CellId(2), 1, 128)
            .unwrap();
        catalog.sync_topic(&source, 1, 128).unwrap();
        let route = &catalog.topics["events"].partitions[&group];
        assert_eq!(route.home_cell, CellId(2));
        assert_eq!(route.lifecycle, PartitionHomeLifecycle::Active);
    }

    #[test]
    fn deletion_intent_is_idempotent_and_blocks_stale_sync() {
        let mut catalog = CatalogState::default();
        let source = descriptor(1, 7);
        catalog.sync_topic(&source, 1, 128).unwrap();

        assert!(catalog.begin_topic_deletion("events").unwrap());
        assert!(!catalog.begin_topic_deletion("events").unwrap());
        assert!(catalog.topics["events"].deleting);
        assert!(catalog.sync_topic(&source, 1, 128).is_err());
    }
}
