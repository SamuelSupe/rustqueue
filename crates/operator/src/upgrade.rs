use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRolloutState {
    pub node_id: u64,
    pub cell_id: u64,
    pub pod_name: String,
    pub image: String,
    pub tls_revision: u64,
    pub config_revision: u64,
    pub target_node: String,
    pub rollout_revision: u64,
    pub ready: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutTarget<'a> {
    pub image: &'a str,
    pub tls_revisions: &'a BTreeMap<u64, u64>,
    pub config_revisions: &'a BTreeMap<u64, u64>,
    pub target_nodes: &'a BTreeMap<u64, String>,
    pub rollout_revision: u64,
}

pub fn next_candidate<'a>(
    pods: &'a [PodRolloutState],
    target: &RolloutTarget<'_>,
    max_unavailable_per_cell: u8,
) -> Option<&'a PodRolloutState> {
    let mut unavailable = BTreeMap::<u64, usize>::new();
    for pod in pods.iter().filter(|pod| !pod.ready) {
        *unavailable.entry(pod.cell_id).or_default() += 1;
    }
    let mut candidates = pods
        .iter()
        .filter(|pod| {
            pod.image != target.image
                || target
                    .tls_revisions
                    .get(&pod.node_id)
                    .is_some_and(|revision| *revision != pod.tls_revision)
                || target
                    .config_revisions
                    .get(&pod.node_id)
                    .is_some_and(|revision| *revision != pod.config_revision)
                || target
                    .target_nodes
                    .get(&pod.node_id)
                    .is_some_and(|node| *node != pod.target_node)
                || target.rollout_revision != pod.rollout_revision
        })
        .filter(|pod| {
            let unavailable = unavailable.get(&pod.cell_id).copied().unwrap_or_default();
            unavailable < usize::from(max_unavailable_per_cell)
                || (!pod.ready && unavailable <= usize::from(max_unavailable_per_cell))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|pod| (pod.cell_id, pod.node_id));
    candidates.into_iter().next()
}

pub fn complete(pods: &[PodRolloutState], target: &RolloutTarget<'_>) -> bool {
    !pods.is_empty()
        && pods.len() == target.tls_revisions.len()
        && pods.iter().all(|pod| {
            pod.ready
                && pod.image == target.image
                && target
                    .tls_revisions
                    .get(&pod.node_id)
                    .is_some_and(|revision| *revision == pod.tls_revision)
                && target
                    .config_revisions
                    .get(&pod.node_id)
                    .is_some_and(|revision| *revision == pod.config_revision)
                && target
                    .target_nodes
                    .get(&pod.node_id)
                    .is_some_and(|node| *node == pod.target_node)
                && target.rollout_revision == pod.rollout_revision
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(node_id: u64, cell_id: u64, image: &str, ready: bool) -> PodRolloutState {
        PodRolloutState {
            node_id,
            cell_id,
            pod_name: format!("pod-{node_id}"),
            image: image.into(),
            tls_revision: 1,
            config_revision: 1,
            target_node: format!("node-{node_id}"),
            rollout_revision: 0,
            ready,
        }
    }

    #[test]
    fn rollout_never_takes_a_second_member_from_an_unhealthy_cell() {
        let pods = vec![
            pod(1, 1, "new", false),
            pod(2, 1, "old", true),
            pod(3, 2, "old", true),
        ];
        let revisions = BTreeMap::from([(1, 1), (2, 1), (3, 1)]);
        let target = RolloutTarget {
            image: "new",
            tls_revisions: &revisions,
            config_revisions: &revisions,
            target_nodes: &BTreeMap::from([
                (1, "node-1".into()),
                (2, "node-2".into()),
                (3, "node-3".into()),
            ]),
            rollout_revision: 0,
        };
        assert_eq!(next_candidate(&pods, &target, 1).unwrap().node_id, 3);
    }

    #[test]
    fn certificate_revision_alone_triggers_a_rollout() {
        let pods = vec![pod(1, 1, "same", true)];
        let revisions = BTreeMap::from([(1, 2)]);
        let target = RolloutTarget {
            image: "same",
            tls_revisions: &revisions,
            config_revisions: &BTreeMap::from([(1, 1)]),
            target_nodes: &BTreeMap::from([(1, "node-1".into())]),
            rollout_revision: 0,
        };
        assert_eq!(next_candidate(&pods, &target, 1).unwrap().node_id, 1);
    }

    #[test]
    fn an_already_unavailable_broker_can_move_without_consuming_another_budget() {
        let mut moving = pod(1, 1, "same", false);
        moving.target_node = "old-node".into();
        let pods = vec![moving, pod(2, 1, "same", true), pod(3, 1, "same", true)];
        let revisions = BTreeMap::from([(1, 1), (2, 1), (3, 1)]);
        let target_nodes = BTreeMap::from([
            (1, "replacement-node".into()),
            (2, "node-2".into()),
            (3, "node-3".into()),
        ]);
        let target = RolloutTarget {
            image: "same",
            tls_revisions: &revisions,
            config_revisions: &revisions,
            target_nodes: &target_nodes,
            rollout_revision: 0,
        };
        assert_eq!(next_candidate(&pods, &target, 1).unwrap().node_id, 1);
    }
}
