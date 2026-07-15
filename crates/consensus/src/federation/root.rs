use super::{CatalogSplit, CellId, GlobalGroupId};
use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const ROOT_GROUP_ID: u64 = u64::MAX;
pub type CatalogShardId = u64;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CellLifecycle {
    Preparing,
    Active,
    Degraded,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CellDescriptor {
    pub id: CellId,
    pub nodes: BTreeSet<NodeId>,
    pub routers: BTreeSet<NodeId>,
    pub lifecycle: CellLifecycle,
    pub feature_level: u64,
    pub created_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodePlacement {
    Unassigned,
    Spare,
    Member(CellId),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FederationNode {
    pub id: NodeId,
    pub failure_domain: String,
    pub placement: NodePlacement,
    pub stable_since_ms: i64,
    pub available: bool,
    pub protocol_version: u32,
    pub feature_level: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogShardDescriptor {
    pub id: CatalogShardId,
    pub hash_start: u64,
    pub hash_end: u64,
    pub voters: BTreeSet<NodeId>,
    pub epoch: u64,
}

impl CatalogShardDescriptor {
    pub fn contains(&self, hash: u64) -> bool {
        hash >= self.hash_start && hash <= self.hash_end
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorLeaseState {
    Active,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GeneratorLease {
    pub slot: u16,
    pub holder: GlobalGroupId,
    pub incarnation: u32,
    pub state: GeneratorLeaseState,
    pub quarantine_until_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GeneratorSlotRange {
    pub start: u16,
    pub end: u16,
}

impl GeneratorSlotRange {
    pub fn new(start: u16, end: u16) -> Result<Self, &'static str> {
        if start == 0 || start > end {
            return Err("generator slot range must satisfy 1 <= start <= end");
        }
        Ok(Self { start, end })
    }

    pub fn contains(self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct GeneratorReleaseProof {
    pub live_segments: u64,
    pub snapshot_references: u64,
    pub ack_references: u64,
    pub in_flight: u64,
}

impl GeneratorReleaseProof {
    pub(super) fn is_clear(self) -> bool {
        self.live_segments == 0
            && self.snapshot_references == 0
            && self.ack_references == 0
            && self.in_flight == 0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct CellFormationPolicy {
    pub min_nodes: usize,
    pub target_nodes: usize,
    pub max_nodes: usize,
    pub stabilization_ms: i64,
    pub routers_per_cell: usize,
}

impl Default for CellFormationPolicy {
    fn default() -> Self {
        Self {
            min_nodes: 3,
            target_nodes: 5,
            max_nodes: 9,
            stabilization_ms: 60_000,
            routers_per_cell: 3,
        }
    }
}

impl CellFormationPolicy {
    pub fn validate(self) -> Result<(), &'static str> {
        if self.min_nodes < 3
            || self.min_nodes > self.target_nodes
            || self.target_nodes > self.max_nodes
            || self.max_nodes > 9
            || self.routers_per_cell == 0
            || self.routers_per_cell > self.min_nodes
        {
            return Err("cell policy must satisfy 3 <= min <= target <= max <= 9");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RootAction {
    AssignNode {
        node_id: NodeId,
        cell_id: CellId,
    },
    CreateCell {
        cell_id: CellId,
        nodes: BTreeSet<NodeId>,
    },
    MarkSpare {
        node_id: NodeId,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FederationRoot {
    pub epoch: u64,
    pub next_cell_id: u64,
    pub cells: BTreeMap<CellId, CellDescriptor>,
    pub nodes: BTreeMap<NodeId, FederationNode>,
    pub catalog_shards: BTreeMap<CatalogShardId, CatalogShardDescriptor>,
    pub catalog_splits: BTreeMap<u64, CatalogSplit>,
    pub root_voters: BTreeSet<NodeId>,
    pub generator_leases: BTreeMap<u16, GeneratorLease>,
    pub generator_ranges: BTreeMap<CellId, GeneratorSlotRange>,
    pub next_generator_incarnation: u32,
    pub min_protocol_version: u32,
    pub max_protocol_version: u32,
}

impl Default for FederationRoot {
    fn default() -> Self {
        Self {
            epoch: 0,
            next_cell_id: CellId::BOOTSTRAP.0,
            cells: BTreeMap::new(),
            nodes: BTreeMap::new(),
            catalog_shards: BTreeMap::new(),
            catalog_splits: BTreeMap::new(),
            root_voters: BTreeSet::new(),
            generator_leases: BTreeMap::new(),
            generator_ranges: BTreeMap::new(),
            next_generator_incarnation: 1,
            min_protocol_version: 1,
            max_protocol_version: 1,
        }
    }
}

impl FederationRoot {
    pub fn register_node(&mut self, node: FederationNode) -> Result<bool, String> {
        if node.id == 0 || node.failure_domain.trim().is_empty() {
            return Err("federation node ID and failure domain are required".into());
        }
        if let Some(existing) = self.nodes.get(&node.id) {
            if existing == &node {
                return Ok(false);
            }
            return Err("federation node ID is already registered".into());
        }
        self.nodes.insert(node.id, node);
        self.bump_epoch();
        Ok(true)
    }

    pub fn plan_cells(
        &self,
        now_ms: i64,
        policy: CellFormationPolicy,
    ) -> Result<Vec<RootAction>, String> {
        policy.validate().map_err(str::to_owned)?;
        let mut candidates: Vec<_> = self
            .nodes
            .values()
            .filter(|node| {
                node.available
                    && matches!(
                        node.placement,
                        NodePlacement::Unassigned | NodePlacement::Spare
                    )
                    && now_ms.saturating_sub(node.stable_since_ms) >= policy.stabilization_ms
            })
            .cloned()
            .collect();
        candidates.sort_by_key(|node| node.id);
        let mut actions = Vec::new();

        for cell in self
            .cells
            .values()
            .filter(|cell| cell.lifecycle == CellLifecycle::Active)
        {
            let missing = policy.target_nodes.saturating_sub(cell.nodes.len());
            for _ in 0..missing.min(policy.max_nodes.saturating_sub(cell.nodes.len())) {
                let domains: BTreeSet<_> = cell
                    .nodes
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .map(|node| node.failure_domain.as_str())
                    .collect();
                let index = candidates
                    .iter()
                    .position(|node| !domains.contains(node.failure_domain.as_str()))
                    .or_else(|| (!candidates.is_empty()).then_some(0));
                let Some(index) = index else { break };
                let node = candidates.remove(index);
                actions.push(RootAction::AssignNode {
                    node_id: node.id,
                    cell_id: cell.id,
                });
            }
        }

        let mut next_cell = self.next_cell_id.max(CellId::BOOTSTRAP.0);
        loop {
            let Some(nodes) = take_distinct_domains(&mut candidates, policy.min_nodes) else {
                break;
            };
            next_cell = next_cell.saturating_add(1);
            actions.push(RootAction::CreateCell {
                cell_id: CellId(next_cell),
                nodes,
            });
        }
        actions.extend(
            candidates
                .into_iter()
                .filter(|node| node.placement != NodePlacement::Spare)
                .map(|node| RootAction::MarkSpare { node_id: node.id }),
        );
        Ok(actions)
    }

    pub fn apply_cell_action(
        &mut self,
        action: RootAction,
        now_ms: i64,
        policy: CellFormationPolicy,
    ) -> Result<(), String> {
        policy.validate().map_err(str::to_owned)?;
        match action {
            RootAction::AssignNode { node_id, cell_id } => {
                let cell = self
                    .cells
                    .get_mut(&cell_id)
                    .ok_or_else(|| "target Cell does not exist".to_owned())?;
                if cell.nodes.len() >= policy.max_nodes {
                    return Err("target Cell is at its node limit".into());
                }
                let node = self
                    .nodes
                    .get_mut(&node_id)
                    .ok_or_else(|| "node is not registered".to_owned())?;
                if matches!(node.placement, NodePlacement::Member(_)) {
                    return Err("healthy nodes are not moved automatically between Cells".into());
                }
                node.placement = NodePlacement::Member(cell_id);
                cell.nodes.insert(node_id);
                elect_routers(cell, &self.nodes, policy.routers_per_cell);
            }
            RootAction::CreateCell { cell_id, nodes } => {
                if self.cells.contains_key(&cell_id) || nodes.len() < policy.min_nodes {
                    return Err("new Cell ID is used or has too few nodes".into());
                }
                let domains = nodes
                    .iter()
                    .map(|node_id| {
                        self.nodes
                            .get(node_id)
                            .ok_or_else(|| "Cell contains an unknown node".to_owned())
                            .map(|node| node.failure_domain.as_str())
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                if domains.len() < policy.min_nodes {
                    return Err("new Cell requires distinct failure domains".into());
                }
                for node_id in &nodes {
                    let node = self.nodes.get_mut(node_id).expect("validated node");
                    if matches!(node.placement, NodePlacement::Member(_)) {
                        return Err("node already belongs to a Cell".into());
                    }
                    node.placement = NodePlacement::Member(cell_id);
                }
                let mut cell = CellDescriptor {
                    id: cell_id,
                    nodes,
                    routers: BTreeSet::new(),
                    lifecycle: CellLifecycle::Active,
                    feature_level: 1,
                    created_at_ms: now_ms,
                };
                elect_routers(&mut cell, &self.nodes, policy.routers_per_cell);
                self.cells.insert(cell_id, cell);
                self.next_cell_id = self.next_cell_id.max(cell_id.0);
            }
            RootAction::MarkSpare { node_id } => {
                let node = self
                    .nodes
                    .get_mut(&node_id)
                    .ok_or_else(|| "node is not registered".to_owned())?;
                if matches!(node.placement, NodePlacement::Member(_)) {
                    return Err("Cell member cannot be marked spare".into());
                }
                node.placement = NodePlacement::Spare;
            }
        }
        self.bump_epoch();
        Ok(())
    }

    pub fn catalog_shard_for_hash(&self, hash: u64) -> Option<&CatalogShardDescriptor> {
        self.catalog_shards
            .values()
            .find(|shard| shard.contains(hash))
    }

    pub(super) fn bump_epoch(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
    }
}

fn take_distinct_domains(
    candidates: &mut Vec<FederationNode>,
    count: usize,
) -> Option<BTreeSet<NodeId>> {
    let mut domains = BTreeSet::new();
    let mut indexes = Vec::new();
    for (index, node) in candidates.iter().enumerate() {
        if domains.insert(node.failure_domain.clone()) {
            indexes.push(index);
            if indexes.len() == count {
                break;
            }
        }
    }
    if indexes.len() != count {
        return None;
    }
    let mut nodes = BTreeSet::new();
    for index in indexes.into_iter().rev() {
        nodes.insert(candidates.remove(index).id);
    }
    Some(nodes)
}

fn elect_routers(
    cell: &mut CellDescriptor,
    nodes: &BTreeMap<NodeId, FederationNode>,
    count: usize,
) {
    let mut selected = Vec::new();
    let mut domains = BTreeSet::new();
    let mut candidates: Vec<_> = cell.nodes.iter().copied().collect();
    candidates.sort_unstable();
    for node_id in candidates.iter().copied() {
        let Some(node) = nodes.get(&node_id) else {
            continue;
        };
        if domains.insert(node.failure_domain.clone()) {
            selected.push(node_id);
        }
        if selected.len() == count {
            break;
        }
    }
    for node_id in candidates {
        if selected.len() == count {
            break;
        }
        if !selected.contains(&node_id) {
            selected.push(node_id);
        }
    }
    cell.routers = selected.into_iter().collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, domain: &str) -> FederationNode {
        FederationNode {
            id,
            failure_domain: domain.into(),
            placement: NodePlacement::Unassigned,
            stable_since_ms: 0,
            available: true,
            protocol_version: 1,
            feature_level: 1,
        }
    }

    #[test]
    fn forms_cells_from_stable_distinct_domains_and_keeps_spares() {
        let mut root = FederationRoot::default();
        for id in 1..=8 {
            root.register_node(node(id, &format!("zone-{id}"))).unwrap();
        }
        let policy = CellFormationPolicy::default();
        let actions = root.plan_cells(60_000, policy).unwrap();
        assert!(matches!(actions[0], RootAction::CreateCell { .. }));
        for action in actions {
            root.apply_cell_action(action, 60_000, policy).unwrap();
        }
        assert_eq!(root.cells.len(), 2);
        assert_eq!(
            root.cells
                .values()
                .map(|cell| cell.nodes.len())
                .sum::<usize>(),
            6
        );
        assert_eq!(
            root.nodes
                .values()
                .filter(|node| node.placement == NodePlacement::Spare)
                .count(),
            2
        );
    }
}
