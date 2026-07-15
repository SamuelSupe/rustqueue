use crate::crd::CellPolicy;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterLayout {
    pub cells: Vec<CellPlan>,
    pub pending_nodes: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellPlan {
    pub id: u64,
    pub brokers: Vec<BrokerPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerPlan {
    pub node_id: u64,
    pub cell_id: u64,
    pub ordinal: u8,
    pub stateful_set: String,
    pub pod_name: String,
    pub config_map: String,
    pub tls_secret: String,
    pub headless_service: String,
}

pub fn plan(cluster_name: &str, requested: u16, policy: &CellPolicy) -> ClusterLayout {
    let minimum = u16::from(policy.min_nodes);
    let maximum = u16::from(policy.max_nodes);
    if requested < minimum {
        return ClusterLayout {
            cells: Vec::new(),
            pending_nodes: requested,
        };
    }

    let full_cells = requested / maximum;
    let remainder = requested % maximum;
    let active_remainder = remainder >= minimum;
    let mut sizes = vec![maximum; usize::from(full_cells)];
    if active_remainder {
        sizes.push(remainder);
    }
    if sizes.is_empty() {
        sizes.push(requested);
    }

    let mut next_node_id = 1_u64;
    let cells = sizes
        .into_iter()
        .enumerate()
        .map(|(index, size)| {
            let cell_id = index as u64 + 1;
            let headless_service = format!("{cluster_name}-cell-{cell_id}");
            let brokers = (0..size)
                .map(|ordinal| {
                    let node_id = next_node_id;
                    next_node_id += 1;
                    let identity = format!("{cluster_name}-c{cell_id}-n{}", ordinal + 1);
                    BrokerPlan {
                        node_id,
                        cell_id,
                        ordinal: ordinal as u8,
                        pod_name: format!("{identity}-0"),
                        stateful_set: identity.clone(),
                        config_map: format!("{identity}-config"),
                        tls_secret: format!("{identity}-tls"),
                        headless_service: headless_service.clone(),
                    }
                })
                .collect();
            CellPlan {
                id: cell_id,
                brokers,
            }
        })
        .collect();

    ClusterLayout {
        cells,
        pending_nodes: if active_remainder { 0 } else { remainder },
    }
}

impl ClusterLayout {
    pub fn brokers(&self) -> impl Iterator<Item = &BrokerPlan> {
        self.cells.iter().flat_map(|cell| &cell.brokers)
    }

    pub fn active_replicas(&self) -> u16 {
        self.cells
            .iter()
            .map(|cell| cell.brokers.len() as u16)
            .sum()
    }

    pub fn broker(&self, node_id: u64) -> Option<&BrokerPlan> {
        self.brokers().find(|broker| broker.node_id == node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_cells_remain_spare_until_quorum_exists() {
        let policy = CellPolicy::default();
        let layout = plan("queue", 11, &policy);
        assert_eq!(layout.cells.len(), 1);
        assert_eq!(layout.active_replicas(), 9);
        assert_eq!(layout.pending_nodes, 2);

        let layout = plan("queue", 12, &policy);
        assert_eq!(
            layout
                .cells
                .iter()
                .map(|cell| cell.brokers.len())
                .collect::<Vec<_>>(),
            vec![9, 3]
        );
        assert_eq!(layout.pending_nodes, 0);
    }

    #[test]
    fn identities_are_stable_and_globally_unique() {
        let layout = plan("queue", 18, &CellPolicy::default());
        let ids = layout
            .brokers()
            .map(|broker| broker.node_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, (1..=18).collect::<Vec<_>>());
        assert_eq!(layout.broker(10).unwrap().cell_id, 2);
        assert_eq!(layout.broker(10).unwrap().pod_name, "queue-c2-n1-0");
    }
}
