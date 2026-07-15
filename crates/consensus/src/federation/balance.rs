use super::{CellId, GlobalGroupId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CellLoad {
    pub cell_id: CellId,
    pub healthy: bool,
    pub catalog_healthy: bool,
    pub disk_used_percent: u8,
    pub disk_free_bytes: u64,
    pub publish_bytes_per_sec: u64,
    pub consume_bytes_per_sec: u64,
    pub commit_p99_micros: u64,
    pub fetch_p99_micros: u64,
    pub backlog_growth_bytes_per_sec: i64,
    pub sustained_since_ms: i64,
}

impl CellLoad {
    fn pressure_score(&self) -> u128 {
        u128::from(self.disk_used_percent) * 1_000_000_000
            + u128::from(self.publish_bytes_per_sec)
            + u128::from(self.consume_bytes_per_sec)
            + u128::from(self.commit_p99_micros) * 1_000
            + u128::from(self.fetch_p99_micros) * 1_000
            + self.backlog_growth_bytes_per_sec.max(0) as u128
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartitionLoad {
    pub group_id: GlobalGroupId,
    pub topic: String,
    pub home_cell: CellId,
    pub bytes_on_disk: u64,
    pub publish_bytes_per_sec: u64,
    pub consume_bytes_per_sec: u64,
    pub backlog_growth_bytes_per_sec: i64,
    pub last_moved_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct BalancePolicy {
    pub sustained_ms: i64,
    pub cooldown_ms: i64,
    pub disk_high_watermark_percent: u8,
    pub disk_low_watermark_percent: u8,
    pub min_free_bytes: u64,
    pub min_pressure_delta_percent: u8,
}

impl Default for BalancePolicy {
    fn default() -> Self {
        Self {
            sustained_ms: 10 * 60 * 1_000,
            cooldown_ms: 30 * 60 * 1_000,
            disk_high_watermark_percent: 85,
            disk_low_watermark_percent: 75,
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            min_pressure_delta_percent: 20,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BalanceDecision {
    pub group_id: GlobalGroupId,
    pub topic: String,
    pub source: CellId,
    pub target: CellId,
    pub reason: String,
}

pub struct FederationBalancePlanner;

impl FederationBalancePlanner {
    pub fn plan(
        now_ms: i64,
        cells: &[CellLoad],
        partitions: &[PartitionLoad],
        active_pairs: &BTreeSet<(CellId, CellId)>,
        policy: BalancePolicy,
    ) -> Vec<BalanceDecision> {
        let eligible: BTreeMap<_, _> = cells
            .iter()
            .filter(|cell| {
                cell.healthy
                    && cell.catalog_healthy
                    && cell.disk_used_percent < policy.disk_high_watermark_percent
                    && cell.disk_free_bytes >= policy.min_free_bytes
                    && now_ms.saturating_sub(cell.sustained_since_ms) >= policy.sustained_ms
            })
            .map(|cell| (cell.cell_id, cell))
            .collect();
        let mut sources: Vec<_> = cells
            .iter()
            .filter(|cell| {
                cell.healthy
                    && cell.catalog_healthy
                    && now_ms.saturating_sub(cell.sustained_since_ms) >= policy.sustained_ms
                    && (cell.disk_used_percent >= policy.disk_high_watermark_percent
                        || cell.backlog_growth_bytes_per_sec > 0)
            })
            .collect();
        sources.sort_by_key(|cell| std::cmp::Reverse(cell.pressure_score()));
        let mut targets: Vec<_> = eligible.values().copied().collect();
        targets.sort_by_key(|cell| cell.pressure_score());
        let mut inbound = BTreeSet::new();
        let mut outbound = BTreeSet::new();
        for (source, target) in active_pairs {
            outbound.insert(*source);
            inbound.insert(*target);
        }

        let mut decisions = Vec::new();
        for source in sources {
            if outbound.contains(&source.cell_id) {
                continue;
            }
            let Some(target) = targets.iter().find(|target| {
                target.cell_id != source.cell_id
                    && !inbound.contains(&target.cell_id)
                    && sufficiently_lower(source, target, policy.min_pressure_delta_percent)
            }) else {
                continue;
            };
            let candidate = partitions
                .iter()
                .filter(|partition| {
                    partition.home_cell == source.cell_id
                        && now_ms.saturating_sub(partition.last_moved_at_ms) >= policy.cooldown_ms
                })
                .max_by_key(|partition| {
                    (
                        partition.backlog_growth_bytes_per_sec,
                        partition.publish_bytes_per_sec,
                        partition.bytes_on_disk,
                    )
                });
            let Some(partition) = candidate else { continue };
            decisions.push(BalanceDecision {
                group_id: partition.group_id,
                topic: partition.topic.clone(),
                source: source.cell_id,
                target: target.cell_id,
                reason: "sustained Cell pressure with hysteresis".into(),
            });
            outbound.insert(source.cell_id);
            inbound.insert(target.cell_id);
        }
        decisions
    }
}

fn sufficiently_lower(source: &CellLoad, target: &CellLoad, delta_percent: u8) -> bool {
    let source_score = source.pressure_score();
    let target_score = target.pressure_score();
    target.disk_used_percent <= source.disk_used_percent.saturating_sub(delta_percent)
        || target_score.saturating_mul(100)
            < source_score.saturating_mul(u128::from(100_u8.saturating_sub(delta_percent)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(id: u64, disk: u8, growth: i64) -> CellLoad {
        CellLoad {
            cell_id: CellId(id),
            healthy: true,
            catalog_healthy: true,
            disk_used_percent: disk,
            disk_free_bytes: 100 * 1024 * 1024 * 1024,
            publish_bytes_per_sec: disk as u64 * 1_000,
            consume_bytes_per_sec: 0,
            commit_p99_micros: 1_000,
            fetch_p99_micros: 1_000,
            backlog_growth_bytes_per_sec: growth,
            sustained_since_ms: 0,
        }
    }

    #[test]
    fn allows_disjoint_pairs_but_only_one_inbound_and_outbound_per_cell() {
        let now = 20 * 60 * 1_000;
        let cells = vec![
            cell(1, 90, 10),
            cell(2, 20, 0),
            cell(3, 91, 10),
            cell(4, 21, 0),
        ];
        let partitions = vec![
            PartitionLoad {
                group_id: GlobalGroupId::new(CellId(1), 1).unwrap(),
                topic: "a".into(),
                home_cell: CellId(1),
                bytes_on_disk: 100,
                publish_bytes_per_sec: 100,
                consume_bytes_per_sec: 0,
                backlog_growth_bytes_per_sec: 10,
                last_moved_at_ms: -2_000_000,
            },
            PartitionLoad {
                group_id: GlobalGroupId::new(CellId(3), 1).unwrap(),
                topic: "b".into(),
                home_cell: CellId(3),
                bytes_on_disk: 100,
                publish_bytes_per_sec: 100,
                consume_bytes_per_sec: 0,
                backlog_growth_bytes_per_sec: 10,
                last_moved_at_ms: -2_000_000,
            },
        ];
        let plan = FederationBalancePlanner::plan(
            now,
            &cells,
            &partitions,
            &BTreeSet::new(),
            BalancePolicy::default(),
        );
        assert_eq!(plan.len(), 2);
        assert_ne!(plan[0].source, plan[1].source);
        assert_ne!(plan[0].target, plan[1].target);
    }

    #[test]
    fn transient_pressure_is_ignored() {
        let cells = vec![cell(1, 90, 10), cell(2, 20, 0)];
        assert!(FederationBalancePlanner::plan(
            60_000,
            &cells,
            &[],
            &BTreeSet::new(),
            BalancePolicy::default(),
        )
        .is_empty());
    }
}
