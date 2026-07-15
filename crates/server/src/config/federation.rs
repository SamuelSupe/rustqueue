use super::{ClusterNodeConfig, Config};
use anyhow::bail;
use std::collections::{BTreeMap, BTreeSet};

impl Config {
    pub(crate) fn placement_nodes(&self) -> BTreeMap<String, &ClusterNodeConfig> {
        if !self.cluster.federation.enabled {
            return self
                .cluster
                .nodes
                .iter()
                .map(|(id, node)| (id.clone(), node))
                .collect();
        }
        self.cluster
            .nodes
            .iter()
            .filter(|(_, node)| node.cell_id == Some(self.cluster.federation.cell_id))
            .map(|(id, node)| (id.clone(), node))
            .collect()
    }

    pub(crate) fn local_cell_id(&self) -> rustqueue_consensus::CellId {
        rustqueue_consensus::CellId(if self.cluster.federation.enabled {
            self.cluster.federation.cell_id
        } else {
            1
        })
    }

    pub(crate) fn is_federation_router(&self, node_id: u64) -> bool {
        if !self.cluster.federation.enabled {
            return false;
        }
        let local = self.placement_nodes();
        let mut routers: BTreeSet<_> = local
            .iter()
            .filter(|(_, node)| node.federation_router)
            .filter_map(|(id, _)| id.parse::<u64>().ok())
            .collect();
        if routers.len() < self.cluster.federation.routers_per_cell {
            routers.extend(
                local
                    .keys()
                    .filter_map(|id| id.parse::<u64>().ok())
                    .take(self.cluster.federation.routers_per_cell),
            );
        }
        routers
            .into_iter()
            .take(self.cluster.federation.routers_per_cell)
            .any(|id| id == node_id)
    }

    pub(crate) fn control_voters(&self) -> BTreeSet<u64> {
        if !self.cluster.federation.root_voters.is_empty() {
            return self
                .cluster
                .federation
                .root_voters
                .iter()
                .copied()
                .collect();
        }
        let mut first_by_cell = BTreeMap::new();
        for (id, node) in &self.cluster.nodes {
            if let (Ok(id), Some(cell)) = (id.parse::<u64>(), node.cell_id) {
                first_by_cell.entry(cell).or_insert(id);
            }
        }
        let spread: BTreeSet<_> = first_by_cell.into_values().take(3).collect();
        if spread.len() == 3 {
            spread
        } else {
            self.cluster
                .nodes
                .keys()
                .filter_map(|id| id.parse::<u64>().ok())
                .take(3)
                .collect()
        }
    }

    pub(super) fn validate_cluster_topology(&self) -> anyhow::Result<()> {
        let federation = &self.cluster.federation;
        if !federation.enabled {
            if !(3..=9).contains(&self.cluster.nodes.len()) {
                bail!("cluster mode requires 3 to 9 configured nodes unless federation is enabled");
            }
            return Ok(());
        }
        if federation.cell_id == 0
            || federation.max_home_cells_per_topic == 0
            || federation.route_cache_ms == 0
            || !(1_000..=5_000).contains(&federation.retry_after_ms)
        {
            bail!("cluster.federation Cell, Home Cell, cache, or retry settings are invalid");
        }
        if federation.cell_min_nodes < 3
            || federation.cell_min_nodes > federation.cell_target_nodes
            || federation.cell_target_nodes > federation.cell_max_nodes
            || federation.cell_max_nodes > 9
            || federation.routers_per_cell == 0
            || federation.routers_per_cell > federation.cell_min_nodes
        {
            bail!("federation Cell sizing must satisfy 3 <= min <= target <= max <= 9");
        }
        if federation.catalog_state_split_bytes == 0
            || federation.catalog_topic_split_count == 0
            || federation.catalog_ops_split_per_second == 0
            || federation.catalog_apply_p99_split_ms == 0
        {
            bail!("federation Catalog split thresholds must be greater than zero");
        }
        let local = self.placement_nodes();
        if !(federation.cell_min_nodes..=federation.cell_max_nodes).contains(&local.len()) {
            bail!(
                "local Cell {} requires {} to {} configured members",
                federation.cell_id,
                federation.cell_min_nodes,
                federation.cell_max_nodes
            );
        }
        if self
            .cluster
            .nodes
            .get(&self.node.id.to_string())
            .and_then(|node| node.cell_id)
            != Some(federation.cell_id)
        {
            bail!("local node must be assigned to cluster.federation.cell_id");
        }
        let local_domains = local
            .values()
            .map(|node| node.failure_domain.as_str())
            .collect::<BTreeSet<_>>();
        if local_domains.len() < self.cluster.default_replication_factor as usize {
            bail!("local Cell does not have enough distinct failure domains for its RF");
        }
        if !matches!(federation.root_voters.len(), 0 | 3 | 5) {
            bail!("federation root_voters must contain 3 or 5 nodes");
        }
        for node_id in &federation.root_voters {
            if !self.cluster.nodes.contains_key(&node_id.to_string()) {
                bail!("federation root voter {node_id} is not in cluster.nodes");
            }
        }
        let active_cells = self
            .cluster
            .nodes
            .values()
            .filter_map(|node| node.cell_id)
            .collect::<BTreeSet<_>>();
        if active_cells.len() >= 5 && federation.root_voters.len() == 5 {
            let voter_cells = federation
                .root_voters
                .iter()
                .filter_map(|id| self.cluster.nodes.get(&id.to_string()))
                .filter_map(|node| node.cell_id)
                .collect::<BTreeSet<_>>();
            if voter_cells.len() != 5 {
                bail!("five federation root voters must span five Cells after bootstrap");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(cell_id: Option<u64>, domain: &str) -> ClusterNodeConfig {
        ClusterNodeConfig {
            raft_address: "https://node:4250".into(),
            broadcast_address: "node".into(),
            tcp_port: 4150,
            http_port: 4151,
            tls_server_name: "node".into(),
            failure_domain: domain.into(),
            cell_id,
            federation_router: false,
        }
    }

    #[test]
    fn five_hundred_nodes_stay_out_of_the_local_cell_complexity() {
        let mut config = Config::default();
        config.cluster.enabled = true;
        config.cluster.federation.enabled = true;
        config.cluster.federation.cell_id = 1;
        for id in 1..=500_u64 {
            let cell = (id - 1) / 5 + 1;
            config
                .cluster
                .nodes
                .insert(id.to_string(), node(Some(cell), &format!("zone-{id}")));
        }
        config.node.id = 1;
        assert!(config.validate_cluster_topology().is_ok());
        assert_eq!(config.placement_nodes().len(), 5);
    }

    #[test]
    fn oversized_local_cell_is_rejected() {
        let mut config = Config::default();
        config.cluster.enabled = true;
        config.cluster.federation.enabled = true;
        for id in 1..=10_u64 {
            config
                .cluster
                .nodes
                .insert(id.to_string(), node(Some(1), &format!("zone-{id}")));
        }
        assert!(config.validate_cluster_topology().is_err());
    }
}
