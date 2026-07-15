use crate::layout::ClusterLayout;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EligibleNode {
    pub name: String,
    pub failure_domain: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerPlacement {
    pub node_name: String,
    pub failure_domain: String,
}

pub fn assign(
    layout: &ClusterLayout,
    nodes: &[EligibleNode],
    current: &BTreeMap<u64, String>,
    allow_single_node: bool,
) -> BTreeMap<u64, BrokerPlacement> {
    let by_name = nodes
        .iter()
        .map(|node| (node.name.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let mut result = BTreeMap::new();
    let mut used = BTreeSet::new();
    let mut domain_load = BTreeMap::<String, usize>::new();

    for broker in layout.brokers() {
        let Some(node_name) = current.get(&broker.node_id) else {
            continue;
        };
        let Some(node) = by_name.get(node_name.as_str()) else {
            continue;
        };
        if !allow_single_node && !used.insert(node.name.clone()) {
            continue;
        }
        let failure_domain = if allow_single_node {
            format!("virtual-node-{}", broker.node_id)
        } else {
            node.failure_domain.clone()
        };
        *domain_load.entry(failure_domain.clone()).or_default() += 1;
        result.insert(
            broker.node_id,
            BrokerPlacement {
                node_name: node.name.clone(),
                failure_domain,
            },
        );
    }

    for broker in layout.brokers() {
        if result.contains_key(&broker.node_id) {
            continue;
        }
        let candidate = nodes
            .iter()
            .filter(|node| allow_single_node || !used.contains(&node.name))
            .min_by_key(|node| {
                (
                    if allow_single_node {
                        0
                    } else {
                        domain_load
                            .get(node.failure_domain.as_str())
                            .copied()
                            .unwrap_or_default()
                    },
                    node.name.as_str(),
                )
            });
        let Some(node) = candidate else { break };
        used.insert(node.name.clone());
        let failure_domain = if allow_single_node {
            format!("virtual-node-{}", broker.node_id)
        } else {
            node.failure_domain.clone()
        };
        *domain_load.entry(failure_domain.clone()).or_default() += 1;
        result.insert(
            broker.node_id,
            BrokerPlacement {
                node_name: node.name.clone(),
                failure_domain,
            },
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::CellPolicy;
    use crate::layout;

    #[test]
    fn keeps_healthy_assignments_and_spreads_new_brokers() {
        let layout = layout::plan("queue", 3, &CellPolicy::default());
        let nodes = vec![
            EligibleNode {
                name: "a".into(),
                failure_domain: "z1".into(),
            },
            EligibleNode {
                name: "b".into(),
                failure_domain: "z2".into(),
            },
            EligibleNode {
                name: "c".into(),
                failure_domain: "z1".into(),
            },
        ];
        let placements = assign(&layout, &nodes, &BTreeMap::from([(1, "b".into())]), false);
        assert_eq!(placements[&1].node_name, "b");
        assert_eq!(placements.len(), 3);
        assert_ne!(placements[&1].node_name, placements[&2].node_name);
    }

    #[test]
    fn development_mode_can_place_a_quorum_on_one_node() {
        let layout = layout::plan("queue", 3, &CellPolicy::default());
        let nodes = vec![EligibleNode {
            name: "orbstack".into(),
            failure_domain: "local".into(),
        }];
        let placements = assign(&layout, &nodes, &BTreeMap::new(), true);
        assert_eq!(placements.len(), 3);
        assert!(placements
            .values()
            .all(|value| value.node_name == "orbstack"));
        assert_eq!(placements[&3].failure_domain, "virtual-node-3");
    }
}
