use super::{CatalogState, CellId, GlobalGroupId, PartitionHomeLifecycle};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionMigrationPhase {
    Planned,
    PrepareTarget,
    SnapshotCopy,
    CatchUp,
    SourceFence,
    FinalCatchUp,
    Cutover,
    DrainSource,
    Completed,
    NeedsOperator,
    /// Appended to preserve the frozen binary ordinal of existing phases.
    RemoveSourceLearners,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PartitionMigration {
    pub operation_id: u64,
    pub topic: String,
    pub partition: GlobalGroupId,
    pub source: CellId,
    pub target: CellId,
    pub expected_routing_epoch: u64,
    pub phase: PartitionMigrationPhase,
    pub observed_lag_entries: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub error: Option<String>,
}

impl CatalogState {
    pub fn begin_partition_migration(
        &mut self,
        topic: &str,
        partition: GlobalGroupId,
        target: CellId,
        now_ms: i64,
        max_home_cells: usize,
    ) -> Result<PartitionMigration, String> {
        if self.migrations.values().any(|operation| {
            operation.partition == partition
                && !matches!(
                    operation.phase,
                    PartitionMigrationPhase::Completed | PartitionMigrationPhase::NeedsOperator
                )
        }) {
            return Err("partition already has an active migration".into());
        }
        let topic_state = self
            .topics
            .get(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic_state.deleting {
            return Err("topic deletion is in progress".into());
        }
        let route = topic_state
            .partitions
            .get(&partition)
            .ok_or_else(|| "partition not found".to_owned())?;
        if route.lifecycle != PartitionHomeLifecycle::Active || route.home_cell == target {
            return Err("partition is not active or already belongs to the target Cell".into());
        }
        let mut cells = topic_state.home_cells.clone();
        cells.insert(target);
        if cells.len() > max_home_cells {
            return Err("topic exceeds max_home_cells_per_topic".into());
        }
        let operation_id = self.next_operation_id.max(1);
        self.next_operation_id = operation_id.saturating_add(1);
        let operation = PartitionMigration {
            operation_id,
            topic: topic.to_owned(),
            partition,
            source: route.home_cell,
            target,
            expected_routing_epoch: topic_state.routing_epoch,
            phase: PartitionMigrationPhase::Planned,
            observed_lag_entries: u64::MAX,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error: None,
        };
        self.migrations.insert(operation_id, operation.clone());
        self.epoch = self.epoch.saturating_add(1);
        Ok(operation)
    }

    pub fn advance_partition_migration(
        &mut self,
        operation_id: u64,
        expected: PartitionMigrationPhase,
        next: PartitionMigrationPhase,
        observed_lag_entries: u64,
        now_ms: i64,
        max_home_cells: usize,
    ) -> Result<(), String> {
        let operation = self
            .migrations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| "migration operation not found".to_owned())?;
        if operation.phase != expected || !valid_transition(expected, next) {
            return Err("partition migration phase changed or transition is invalid".into());
        }
        if next == PartitionMigrationPhase::SourceFence {
            let route = self
                .topics
                .get_mut(&operation.topic)
                .and_then(|topic| topic.partitions.get_mut(&operation.partition))
                .ok_or_else(|| "partition route disappeared".to_owned())?;
            route.lifecycle = PartitionHomeLifecycle::Migrating;
        }
        if next == PartitionMigrationPhase::Cutover {
            if observed_lag_entries != 0 {
                return Err("target Cell has not caught up to the source fence".into());
            }
            self.move_partition_home(
                &operation.topic,
                operation.partition,
                operation.source,
                operation.target,
                operation.expected_routing_epoch,
                max_home_cells,
            )?;
        }
        let operation = self
            .migrations
            .get_mut(&operation_id)
            .expect("migration still exists");
        operation.phase = next;
        operation.observed_lag_entries = observed_lag_entries;
        operation.updated_at_ms = now_ms;
        operation.error = None;
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    pub fn mark_partition_migration_needs_operator(
        &mut self,
        operation_id: u64,
        error: String,
        now_ms: i64,
    ) -> Result<(), String> {
        let operation = self
            .migrations
            .get_mut(&operation_id)
            .ok_or_else(|| "migration operation not found".to_owned())?;
        operation.phase = PartitionMigrationPhase::NeedsOperator;
        operation.error = Some(error);
        operation.updated_at_ms = now_ms;
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }
}

fn valid_transition(from: PartitionMigrationPhase, to: PartitionMigrationPhase) -> bool {
    use PartitionMigrationPhase::*;
    matches!(
        (from, to),
        (Planned, PrepareTarget)
            | (PrepareTarget, SnapshotCopy)
            | (SnapshotCopy, CatchUp)
            | (CatchUp, SourceFence)
            | (SourceFence, FinalCatchUp)
            | (FinalCatchUp, RemoveSourceLearners)
            | (RemoveSourceLearners, Cutover)
            | (Cutover, DrainSource)
            | (DrainSource, Completed)
            | (NeedsOperator, Planned)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PartitionHome, RoutingMode};

    #[test]
    fn cutover_requires_zero_lag_and_changes_home_once() {
        let mut catalog = CatalogState::default();
        let group = GlobalGroupId::new(CellId(1), 9).unwrap();
        catalog
            .create_topic(
                "events",
                vec![PartitionHome {
                    id: group,
                    number: 0,
                    wire_slot: 1,
                    wire_incarnation: 1,
                    home_cell: CellId(1),
                    lifecycle: PartitionHomeLifecycle::Active,
                    routing_epoch: 1,
                }],
                RoutingMode::Elastic,
                1,
                128,
            )
            .unwrap();
        let operation = catalog
            .begin_partition_migration("events", group, CellId(2), 0, 128)
            .unwrap();
        for (from, to) in [
            (
                PartitionMigrationPhase::Planned,
                PartitionMigrationPhase::PrepareTarget,
            ),
            (
                PartitionMigrationPhase::PrepareTarget,
                PartitionMigrationPhase::SnapshotCopy,
            ),
            (
                PartitionMigrationPhase::SnapshotCopy,
                PartitionMigrationPhase::CatchUp,
            ),
            (
                PartitionMigrationPhase::CatchUp,
                PartitionMigrationPhase::SourceFence,
            ),
            (
                PartitionMigrationPhase::SourceFence,
                PartitionMigrationPhase::FinalCatchUp,
            ),
            (
                PartitionMigrationPhase::FinalCatchUp,
                PartitionMigrationPhase::RemoveSourceLearners,
            ),
        ] {
            catalog
                .advance_partition_migration(operation.operation_id, from, to, 1, 1, 128)
                .unwrap();
        }
        assert!(catalog
            .advance_partition_migration(
                operation.operation_id,
                PartitionMigrationPhase::RemoveSourceLearners,
                PartitionMigrationPhase::Cutover,
                1,
                2,
                128,
            )
            .is_err());
        catalog
            .advance_partition_migration(
                operation.operation_id,
                PartitionMigrationPhase::RemoveSourceLearners,
                PartitionMigrationPhase::Cutover,
                0,
                3,
                128,
            )
            .unwrap();
        assert_eq!(
            catalog.topics["events"].partitions[&group].home_cell,
            CellId(2)
        );
    }

    #[test]
    fn source_route_is_fenced_only_after_live_catch_up() {
        let mut catalog = CatalogState::default();
        let group = GlobalGroupId::new(CellId(1), 9).unwrap();
        catalog
            .create_topic(
                "events",
                vec![PartitionHome {
                    id: group,
                    number: 0,
                    wire_slot: 1,
                    wire_incarnation: 1,
                    home_cell: CellId(1),
                    lifecycle: PartitionHomeLifecycle::Active,
                    routing_epoch: 1,
                }],
                RoutingMode::Elastic,
                1,
                128,
            )
            .unwrap();
        let operation = catalog
            .begin_partition_migration("events", group, CellId(2), 0, 128)
            .unwrap();
        for (from, to) in [
            (
                PartitionMigrationPhase::Planned,
                PartitionMigrationPhase::PrepareTarget,
            ),
            (
                PartitionMigrationPhase::PrepareTarget,
                PartitionMigrationPhase::SnapshotCopy,
            ),
            (
                PartitionMigrationPhase::SnapshotCopy,
                PartitionMigrationPhase::CatchUp,
            ),
        ] {
            catalog
                .advance_partition_migration(operation.operation_id, from, to, 100, 1, 128)
                .unwrap();
            assert_eq!(
                catalog.topics["events"].partitions[&group].lifecycle,
                PartitionHomeLifecycle::Active
            );
        }
        catalog
            .advance_partition_migration(
                operation.operation_id,
                PartitionMigrationPhase::CatchUp,
                PartitionMigrationPhase::SourceFence,
                10,
                2,
                128,
            )
            .unwrap();
        assert_eq!(
            catalog.topics["events"].partitions[&group].lifecycle,
            PartitionHomeLifecycle::Migrating
        );
    }

    #[test]
    fn migration_phase_binary_ordinals_are_append_only() {
        assert_eq!(
            bincode::serialize(&PartitionMigrationPhase::Planned).unwrap(),
            0_u32.to_le_bytes()
        );
        assert_eq!(
            bincode::serialize(&PartitionMigrationPhase::NeedsOperator).unwrap(),
            9_u32.to_le_bytes()
        );
        assert_eq!(
            bincode::serialize(&PartitionMigrationPhase::RemoveSourceLearners).unwrap(),
            10_u32.to_le_bytes()
        );
    }
}
