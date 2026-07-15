use super::{CellId, GlobalGroupId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[path = "catalog_serde.rs"]
mod partition_map;

pub const VIRTUAL_BUCKET_COUNT: u32 = 65_536;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingMode {
    #[default]
    Elastic,
    Pinned,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionHomeLifecycle {
    Preparing,
    Active,
    Migrating,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PartitionHome {
    pub id: GlobalGroupId,
    pub number: u32,
    pub wire_slot: u16,
    pub wire_incarnation: u32,
    pub home_cell: CellId,
    pub lifecycle: PartitionHomeLifecycle,
    pub routing_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketRange {
    pub start: u16,
    pub end: u16,
    pub partition: GlobalGroupId,
}

impl BucketRange {
    fn contains(&self, bucket: u16) -> bool {
        bucket >= self.start && bucket <= self.end
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogTopic {
    pub name: String,
    pub routing_mode: RoutingMode,
    pub topology_generation: u64,
    pub routing_epoch: u64,
    pub catalog_revision: u64,
    pub feature_level: u64,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub channels: BTreeMap<String, crate::ChannelDescriptor>,
    #[serde(default)]
    pub channel_tombstones: BTreeMap<String, u64>,
    #[serde(with = "partition_map")]
    pub partitions: BTreeMap<GlobalGroupId, PartitionHome>,
    pub partition_numbers: BTreeMap<u32, GlobalGroupId>,
    pub bucket_ranges: Vec<BucketRange>,
    pub home_cells: BTreeSet<CellId>,
    #[serde(default)]
    pub deleting: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogState {
    pub shard_id: u64,
    pub epoch: u64,
    pub topics: BTreeMap<String, CatalogTopic>,
    #[serde(default)]
    pub migrations: BTreeMap<u64, super::PartitionMigration>,
    #[serde(default = "default_next_operation_id")]
    pub next_operation_id: u64,
}

fn default_next_operation_id() -> u64 {
    1
}

#[derive(Clone, Copy, Debug)]
pub enum RouteRequest<'a> {
    Ordinary {
        operation_id: u64,
        preferred_cell: Option<CellId>,
    },
    Keyed(&'a [u8]),
    Explicit(u32),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RouteDecision {
    pub partition: PartitionHome,
    pub topology_generation: u64,
    pub routing_epoch: u64,
    pub direct: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("topic is not present in this Catalog shard")]
    TopicNotFound,
    #[error("Catalog is unavailable; retry after {retry_after_ms} ms")]
    CatalogUnavailable { retry_after_ms: u64 },
    #[error("topic has no active partition")]
    NoActivePartition,
    #[error("partition is unknown or not active")]
    PartitionNotActive,
    #[error("partition migration is fenced for cutover")]
    MigrationFenced { retry_after_ms: u64 },
    #[error("Home Cell {cell_id} is unavailable")]
    HomeCellUnavailable {
        cell_id: CellId,
        retry_after_ms: u64,
    },
    #[error("topic deletion is in progress")]
    TopicDeleting,
}

impl CatalogState {
    pub fn create_topic(
        &mut self,
        name: &str,
        partitions: Vec<PartitionHome>,
        routing_mode: RoutingMode,
        feature_level: u64,
        max_home_cells: usize,
    ) -> Result<&CatalogTopic, String> {
        if self.topics.contains_key(name) {
            return Ok(self.topics.get(name).expect("topic existence was checked"));
        }
        if name.is_empty() || partitions.is_empty() {
            return Err("topic name and at least one partition are required".into());
        }
        let mut by_id = BTreeMap::new();
        let mut by_number = BTreeMap::new();
        let mut home_cells = BTreeSet::new();
        for partition in partitions {
            if partition.lifecycle != PartitionHomeLifecycle::Active
                || partition.wire_slot == 0
                || partition.wire_incarnation == 0
                || by_number.insert(partition.number, partition.id).is_some()
                || by_id.insert(partition.id, partition.clone()).is_some()
            {
                return Err("topic contains an invalid or duplicate partition".into());
            }
            home_cells.insert(partition.home_cell);
        }
        if home_cells.len() > max_home_cells {
            return Err("topic exceeds max_home_cells_per_topic".into());
        }
        let ordered: Vec<_> = by_number.values().copied().collect();
        let bucket_ranges = build_bucket_ranges(&ordered);
        let topic = CatalogTopic {
            name: name.to_owned(),
            deleting: false,
            routing_mode,
            topology_generation: 1,
            routing_epoch: 1,
            catalog_revision: 1,
            feature_level,
            paused: false,
            channels: BTreeMap::new(),
            channel_tombstones: BTreeMap::new(),
            partitions: by_id,
            partition_numbers: by_number,
            bucket_ranges,
            home_cells,
        };
        self.topics.insert(name.to_owned(), topic);
        self.epoch = self.epoch.saturating_add(1);
        Ok(self.topics.get(name).expect("topic was inserted"))
    }

    pub fn route(
        &self,
        topic: &str,
        request: RouteRequest<'_>,
        available_cells: &BTreeSet<CellId>,
        retry_after_ms: u64,
    ) -> Result<RouteDecision, RouteError> {
        let topic = self.topics.get(topic).ok_or(RouteError::TopicNotFound)?;
        if topic.deleting {
            return Err(RouteError::TopicDeleting);
        }
        let partition = match request {
            RouteRequest::Explicit(number) => {
                let partition = topic
                    .partition_numbers
                    .get(&number)
                    .and_then(|id| topic.partitions.get(id))
                    .ok_or(RouteError::PartitionNotActive)?;
                if partition.lifecycle == PartitionHomeLifecycle::Migrating {
                    return Err(RouteError::MigrationFenced { retry_after_ms });
                }
                if partition.lifecycle != PartitionHomeLifecycle::Active {
                    return Err(RouteError::PartitionNotActive);
                }
                partition
            }
            RouteRequest::Keyed(key) => {
                let bucket = (crc32c::crc32c(key) % VIRTUAL_BUCKET_COUNT) as u16;
                let id = topic
                    .bucket_ranges
                    .iter()
                    .find(|range| range.contains(bucket))
                    .map(|range| range.partition)
                    .ok_or(RouteError::NoActivePartition)?;
                let partition = topic
                    .partitions
                    .get(&id)
                    .ok_or(RouteError::PartitionNotActive)?;
                if partition.lifecycle == PartitionHomeLifecycle::Migrating {
                    return Err(RouteError::MigrationFenced { retry_after_ms });
                }
                if partition.lifecycle != PartitionHomeLifecycle::Active {
                    return Err(RouteError::PartitionNotActive);
                }
                partition
            }
            RouteRequest::Ordinary {
                operation_id,
                preferred_cell,
            } => {
                let mut active: Vec<_> = topic
                    .partitions
                    .values()
                    .filter(|partition| {
                        partition.lifecycle == PartitionHomeLifecycle::Active
                            && available_cells.contains(&partition.home_cell)
                    })
                    .collect();
                if active.is_empty() {
                    return Err(RouteError::NoActivePartition);
                }
                active.sort_by_key(|partition| {
                    (
                        preferred_cell.is_none_or(|cell| cell != partition.home_cell),
                        partition.number,
                    )
                });
                active[operation_id as usize % active.len()]
            }
        };
        if !available_cells.contains(&partition.home_cell) {
            return Err(RouteError::HomeCellUnavailable {
                cell_id: partition.home_cell,
                retry_after_ms,
            });
        }
        Ok(RouteDecision {
            partition: partition.clone(),
            topology_generation: topic.topology_generation,
            routing_epoch: topic.routing_epoch,
            direct: true,
        })
    }

    pub fn move_partition_home(
        &mut self,
        topic: &str,
        partition: GlobalGroupId,
        source: CellId,
        target: CellId,
        expected_epoch: u64,
        max_home_cells: usize,
    ) -> Result<u64, String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic.routing_epoch != expected_epoch {
            return Err("routing epoch changed; refresh and retry".into());
        }
        let route = topic
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| "partition not found".to_owned())?;
        if route.home_cell != source || route.lifecycle == PartitionHomeLifecycle::Retired {
            return Err("partition source Cell or lifecycle changed".into());
        }
        let mut cells = topic.home_cells.clone();
        cells.insert(target);
        if cells.len() > max_home_cells {
            return Err("topic exceeds max_home_cells_per_topic".into());
        }
        route.home_cell = target;
        route.lifecycle = PartitionHomeLifecycle::Active;
        topic.routing_epoch = topic.routing_epoch.saturating_add(1);
        route.routing_epoch = topic.routing_epoch;
        topic.home_cells = topic
            .partitions
            .values()
            .filter(|partition| partition.lifecycle != PartitionHomeLifecycle::Retired)
            .map(|partition| partition.home_cell)
            .collect();
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(topic.routing_epoch)
    }

    pub fn activate_bucket_move(
        &mut self,
        topic: &str,
        start: u16,
        end: u16,
        target: GlobalGroupId,
        expected_epoch: u64,
    ) -> Result<u64, String> {
        let topic = self
            .topics
            .get_mut(topic)
            .ok_or_else(|| "topic not found".to_owned())?;
        if topic.routing_mode != RoutingMode::Elastic {
            return Err("pinned topics do not permit bucket migration".into());
        }
        if start > end || topic.routing_epoch != expected_epoch {
            return Err("bucket range or routing epoch is invalid".into());
        }
        if !topic
            .partitions
            .get(&target)
            .is_some_and(|partition| partition.lifecycle == PartitionHomeLifecycle::Active)
        {
            return Err("target partition is not active".into());
        }
        topic.bucket_ranges = replace_bucket_range(&topic.bucket_ranges, start, end, target);
        topic.routing_epoch = topic.routing_epoch.saturating_add(1);
        topic.catalog_revision = topic.catalog_revision.saturating_add(1);
        self.epoch = self.epoch.saturating_add(1);
        Ok(topic.routing_epoch)
    }
}

pub(super) fn build_bucket_ranges(partitions: &[GlobalGroupId]) -> Vec<BucketRange> {
    let mut ranges = Vec::with_capacity(partitions.len());
    for (index, partition) in partitions.iter().enumerate() {
        let start = (index as u32 * VIRTUAL_BUCKET_COUNT / partitions.len() as u32) as u16;
        let end = if index + 1 == partitions.len() {
            u16::MAX
        } else {
            (((index + 1) as u32 * VIRTUAL_BUCKET_COUNT / partitions.len() as u32) - 1) as u16
        };
        ranges.push(BucketRange {
            start,
            end,
            partition: *partition,
        });
    }
    ranges
}

fn replace_bucket_range(
    ranges: &[BucketRange],
    start: u16,
    end: u16,
    target: GlobalGroupId,
) -> Vec<BucketRange> {
    let mut result = Vec::new();
    for range in ranges {
        if range.end < start || range.start > end {
            result.push(range.clone());
            continue;
        }
        if range.start < start {
            result.push(BucketRange {
                start: range.start,
                end: start - 1,
                partition: range.partition,
            });
        }
        result.push(BucketRange {
            start: range.start.max(start),
            end: range.end.min(end),
            partition: target,
        });
        if range.end > end {
            result.push(BucketRange {
                start: end + 1,
                end: range.end,
                partition: range.partition,
            });
        }
    }
    normalize_ranges(result)
}

fn normalize_ranges(ranges: Vec<BucketRange>) -> Vec<BucketRange> {
    let mut result: Vec<BucketRange> = Vec::new();
    for range in ranges {
        if let Some(last) = result.last_mut() {
            if last.partition == range.partition && last.end.checked_add(1) == Some(range.start) {
                last.end = range.end;
                continue;
            }
        }
        result.push(range);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(cell: u64, local: u64, number: u32, slot: u16) -> PartitionHome {
        PartitionHome {
            id: GlobalGroupId::new(CellId(cell), local).unwrap(),
            number,
            wire_slot: slot,
            wire_incarnation: 1,
            home_cell: CellId(cell),
            lifecycle: PartitionHomeLifecycle::Active,
            routing_epoch: 1,
        }
    }

    #[test]
    fn ordinary_routes_skip_unavailable_cells_but_keyed_routes_fail_closed() {
        let mut catalog = CatalogState::default();
        catalog
            .create_topic(
                "events",
                vec![partition(1, 1, 0, 1), partition(2, 1, 1, 2)],
                RoutingMode::Elastic,
                1,
                128,
            )
            .unwrap();
        let available = BTreeSet::from([CellId(1)]);
        for operation in 0..8 {
            let route = catalog
                .route(
                    "events",
                    RouteRequest::Ordinary {
                        operation_id: operation,
                        preferred_cell: None,
                    },
                    &available,
                    1_000,
                )
                .unwrap();
            assert_eq!(route.partition.home_cell, CellId(1));
        }

        let key_for_second = (0_u64..)
            .map(|value| value.to_be_bytes())
            .find(|key| crc32c::crc32c(key) as u16 >= 32_768)
            .unwrap();
        assert!(matches!(
            catalog.route(
                "events",
                RouteRequest::Keyed(&key_for_second),
                &available,
                1_000
            ),
            Err(RouteError::HomeCellUnavailable {
                cell_id: CellId(2),
                ..
            })
        ));
    }

    #[test]
    fn elastic_bucket_cutover_is_atomic_at_one_epoch() {
        let mut catalog = CatalogState::default();
        let first = partition(1, 1, 0, 1);
        let second = partition(2, 1, 1, 2);
        catalog
            .create_topic(
                "events",
                vec![first.clone(), second.clone()],
                RoutingMode::Elastic,
                1,
                128,
            )
            .unwrap();
        let next = catalog
            .activate_bucket_move("events", 100, 199, second.id, 1)
            .unwrap();
        assert_eq!(next, 2);
        let topic = &catalog.topics["events"];
        assert_eq!(topic.bucket_ranges.first().unwrap().end, 99);
        assert!(topic
            .bucket_ranges
            .iter()
            .any(|range| range.start == 100 && range.end == 199 && range.partition == second.id));
    }

    #[test]
    fn topic_deleting_route_error_is_append_only() {
        assert_eq!(
            bincode::serialize(&RouteError::TopicDeleting).unwrap(),
            6_u32.to_le_bytes()
        );
    }
}
