use super::{CatalogState, CellId, GlobalGroupId, PartitionHome, PartitionHomeLifecycle};
use std::collections::BTreeSet;

impl CatalogState {
    pub fn reserve_partitions(
        &mut self,
        topic: &str,
        partitions: Vec<PartitionHome>,
        max_home_cells: usize,
    ) -> Result<(), String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        let mut cells = topic.home_cells.clone();
        for partition in &partitions {
            if partition.lifecycle != PartitionHomeLifecycle::Preparing
                || partition.wire_slot == 0
                || partition.wire_incarnation == 0
                || topic.partitions.contains_key(&partition.id)
                || topic.partition_numbers.contains_key(&partition.number)
            {
                return Err("reserved Catalog partition is invalid or duplicate".into());
            }
            cells.insert(partition.home_cell);
        }
        if cells.len() > max_home_cells {
            return Err("topic exceeds max_home_cells_per_topic".into());
        }
        for partition in partitions {
            topic
                .partition_numbers
                .insert(partition.number, partition.id);
            topic.partitions.insert(partition.id, partition);
        }
        topic.home_cells = cells;
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn activate_partitions(
        &mut self,
        topic: &str,
        groups: &BTreeSet<GlobalGroupId>,
    ) -> Result<u64, String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        for group in groups {
            let partition = topic
                .partitions
                .get_mut(group)
                .ok_or_else(|| "reserved Catalog partition is missing".to_owned())?;
            if partition.lifecycle != PartitionHomeLifecycle::Preparing {
                return Err("Catalog partition lifecycle changed".into());
            }
            partition.lifecycle = PartitionHomeLifecycle::Active;
        }
        topic.topology_generation = topic.topology_generation.saturating_add(1);
        topic.routing_epoch = topic.routing_epoch.saturating_add(1);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(topic.routing_epoch)
    }

    pub fn retire_partitions(
        &mut self,
        topic: &str,
        groups: &BTreeSet<GlobalGroupId>,
    ) -> Result<(), String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        for group in groups {
            let partition = topic
                .partitions
                .get_mut(group)
                .ok_or_else(|| "Catalog partition is missing".to_owned())?;
            if partition.lifecycle == PartitionHomeLifecycle::Active {
                return Err("active Catalog partition cannot be retired by cancellation".into());
            }
            partition.lifecycle = PartitionHomeLifecycle::Retired;
        }
        topic.home_cells = topic
            .partitions
            .values()
            .filter(|partition| partition.lifecycle != PartitionHomeLifecycle::Retired)
            .map(|partition| partition.home_cell)
            .collect::<BTreeSet<CellId>>();
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }
}
