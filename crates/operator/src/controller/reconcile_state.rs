use crate::crd::{CellStatus, RustQueueCluster, RustQueueClusterStatus, UpgradeStatus};
use crate::layout::ClusterLayout;
use crate::placement::BrokerPlacement;
use crate::resources::{
    self, ANNOTATION_CONFIG_REVISION, ANNOTATION_ROLLOUT_REVISION, ANNOTATION_TARGET_NODE,
    ANNOTATION_TLS_REVISION, LABEL_NODE_ID,
};
use crate::status::condition;
use crate::upgrade::{PodRolloutState, RolloutTarget};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use std::collections::BTreeMap;

pub fn current_placements(stateful_sets: &[StatefulSet], pods: &[Pod]) -> BTreeMap<u64, String> {
    let mut result = BTreeMap::new();
    for set in stateful_sets {
        if let (Some(node_id), Some(node)) = (
            object_node_id(&set.metadata.labels),
            set.spec
                .as_ref()
                .and_then(|spec| spec.template.metadata.as_ref())
                .and_then(|metadata| metadata.annotations.as_ref())
                .and_then(|annotations| annotations.get(ANNOTATION_TARGET_NODE)),
        ) {
            result.insert(node_id, node.clone());
        }
    }
    for pod in pods {
        if let (Some(node_id), Some(node)) = (
            object_node_id(&pod.metadata.labels),
            pod.spec.as_ref().and_then(|spec| spec.node_name.as_ref()),
        ) {
            result.insert(node_id, node.clone());
        }
    }
    result
}

pub fn pod_states(pods: &[Pod]) -> Vec<PodRolloutState> {
    let mut result = pods
        .iter()
        .filter_map(|pod| {
            let node_id = object_node_id(&pod.metadata.labels)?;
            let annotations = pod.metadata.annotations.as_ref();
            let container = pod
                .spec
                .as_ref()?
                .containers
                .iter()
                .find(|container| container.name == "rustqueue")?;
            Some(PodRolloutState {
                node_id,
                cell_id: pod
                    .metadata
                    .labels
                    .as_ref()?
                    .get(resources::LABEL_CELL)?
                    .parse()
                    .ok()?,
                pod_name: pod.name_any(),
                image: container.image.clone().unwrap_or_default(),
                tls_revision: annotation_number(annotations, ANNOTATION_TLS_REVISION),
                config_revision: annotation_number(annotations, ANNOTATION_CONFIG_REVISION),
                target_node: annotation_value(annotations, ANNOTATION_TARGET_NODE),
                rollout_revision: annotation_number(annotations, ANNOTATION_ROLLOUT_REVISION),
                ready: pod_ready(pod),
            })
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|pod| pod.node_id);
    result
}

#[allow(clippy::too_many_arguments)]
pub fn build_status(
    cluster: &RustQueueCluster,
    generation: Option<i64>,
    allocated: u16,
    layout: &ClusterLayout,
    placements: &BTreeMap<u64, BrokerPlacement>,
    configured: u16,
    ca_revision: u64,
    pods: &[PodRolloutState],
    upgrade: Option<UpgradeStatus>,
    target: &RolloutTarget<'_>,
) -> RustQueueClusterStatus {
    let ready = pods.iter().filter(|pod| pod.ready).count() as u16;
    let desired = layout.active_replicas();
    let placement_ready = placements.len() == usize::from(desired);
    let rollout_ready = crate::upgrade::complete(pods, target);
    let available = desired >= u16::from(cluster.spec.cells.min_nodes)
        && ready == desired
        && configured == desired
        && rollout_ready;
    let phase = if available {
        "Ready"
    } else if desired == 0 {
        "PendingNodes"
    } else if !placement_ready {
        "Degraded"
    } else {
        "Reconciling"
    };
    let cells = layout
        .cells
        .iter()
        .map(|cell| CellStatus {
            id: cell.id,
            desired_replicas: cell.brokers.len() as u16,
            ready_replicas: pods
                .iter()
                .filter(|pod| pod.cell_id == cell.id && pod.ready)
                .count() as u16,
        })
        .collect();
    let mut status = RustQueueClusterStatus {
        observed_generation: generation,
        phase: phase.into(),
        allocated_replicas: allocated,
        desired_replicas: desired,
        ready_replicas: ready,
        pending_nodes: layout.pending_nodes,
        ca_revision: ca_revision.to_string(),
        cells,
        upgrade,
        conditions: vec![
            condition(
                generation,
                "SpecValid",
                true,
                "Validated",
                "configuration is valid",
            ),
            condition(
                generation,
                "StorageReady",
                true,
                "StorageClassResolved",
                format!("PVCs use StorageClass {}", cluster.spec.storage.class_name),
            ),
            condition(
                generation,
                "CertificatesReady",
                true,
                "ManagedPKIReady",
                format!("managed CA revision {ca_revision} is active"),
            ),
            condition(
                generation,
                "UpgradeReady",
                rollout_ready,
                if rollout_ready {
                    "RolloutComplete"
                } else {
                    "RolloutProgressing"
                },
                if rollout_ready {
                    "every Broker runs the desired workload revision"
                } else {
                    "one-at-a-time rollout is still converging"
                },
            ),
            condition(
                generation,
                "PlacementReady",
                placement_ready,
                if placement_ready {
                    "Assigned"
                } else {
                    "InsufficientEligibleNodes"
                },
                format!(
                    "{} of {desired} Brokers have node assignments",
                    placements.len()
                ),
            ),
            condition(
                generation,
                "Available",
                available,
                if available {
                    "AllBrokersReady"
                } else {
                    "Progressing"
                },
                format!("{ready} of {desired} Brokers are Ready"),
            ),
        ],
    };
    preserve_condition_times(cluster, &mut status);
    status
}

pub fn allocated_capacity(cluster: &RustQueueCluster, eligible: u16) -> anyhow::Result<u16> {
    let discovered = if cluster.spec.development.allow_single_node {
        if eligible == 0 {
            0
        } else {
            cluster.spec.development.virtual_replicas
        }
    } else if let Some(replicas) = cluster.spec.nodes.replicas {
        replicas
    } else if cluster.spec.nodes.auto_scale_from_nodes {
        eligible
    } else {
        anyhow::bail!("nodes.replicas is required when autoScaleFromNodes is false")
    };
    Ok(discovered.max(
        cluster
            .status
            .as_ref()
            .map(|status| status.allocated_replicas)
            .unwrap_or_default(),
    ))
}

pub fn blocked_status(
    cluster: &RustQueueCluster,
    generation: Option<i64>,
    type_: &str,
    reason: &str,
    message: &str,
) -> RustQueueClusterStatus {
    let mut status = RustQueueClusterStatus {
        observed_generation: generation,
        phase: "Invalid".into(),
        conditions: vec![condition(generation, type_, false, reason, message)],
        ..cluster.status.clone().unwrap_or_default()
    };
    preserve_condition_times(cluster, &mut status);
    status
}

fn preserve_condition_times(cluster: &RustQueueCluster, status: &mut RustQueueClusterStatus) {
    let Some(current) = cluster.status.as_ref() else {
        return;
    };
    for condition in &mut status.conditions {
        if let Some(previous) = current.conditions.iter().find(|previous| {
            previous.type_ == condition.type_
                && previous.status == condition.status
                && previous.reason == condition.reason
                && previous.message == condition.message
        }) {
            condition.last_transition_time = previous.last_transition_time.clone();
        }
    }
}

pub fn waiting_upgrade(cluster: &RustQueueCluster, reason: &str) -> UpgradeStatus {
    UpgradeStatus {
        target_image: cluster.spec.image.clone(),
        current_node_id: None,
        started_at: None,
        paused: false,
        reason: reason.into(),
        observed_retry_generation: cluster.spec.upgrade.retry_generation,
    }
}

fn object_node_id(labels: &Option<BTreeMap<String, String>>) -> Option<u64> {
    labels.as_ref()?.get(LABEL_NODE_ID)?.parse().ok()
}

fn annotation_number(annotations: Option<&BTreeMap<String, String>>, key: &str) -> u64 {
    annotations
        .and_then(|values| values.get(key))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn annotation_value(annotations: Option<&BTreeMap<String, String>>, key: &str) -> String {
    annotations
        .and_then(|values| values.get(key))
        .cloned()
        .unwrap_or_default()
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}
