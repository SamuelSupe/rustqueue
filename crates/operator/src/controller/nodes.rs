use crate::crd::RustQueueCluster;
use crate::placement::EligibleNode;
use k8s_openapi::api::core::v1::Node;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};

pub async fn eligible(
    client: Client,
    cluster: &RustQueueCluster,
) -> anyhow::Result<Vec<EligibleNode>> {
    let api = Api::<Node>::all(client);
    let mut result = api
        .list(&ListParams::default())
        .await?
        .items
        .into_iter()
        .filter(|node| matches_selector(node, cluster))
        .filter(|node| {
            ready(node)
                && node
                    .spec
                    .as_ref()
                    .is_none_or(|spec| spec.unschedulable != Some(true))
        })
        .filter(|node| dedicated(node, cluster))
        .map(|node| {
            let name = node.name_any();
            let failure_domain = node
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(&cluster.spec.nodes.failure_domain_label))
                .cloned()
                .unwrap_or_else(|| name.clone());
            EligibleNode {
                name,
                failure_domain,
            }
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(result)
}

fn matches_selector(node: &Node, cluster: &RustQueueCluster) -> bool {
    let labels = node.metadata.labels.as_ref();
    cluster.spec.nodes.selector.iter().all(|(key, value)| {
        labels
            .and_then(|labels| labels.get(key))
            .is_some_and(|actual| actual == value)
    })
}

fn ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn dedicated(node: &Node, cluster: &RustQueueCluster) -> bool {
    if !cluster.spec.nodes.dedicated || cluster.spec.development.allow_single_node {
        return true;
    }
    node.spec
        .as_ref()
        .and_then(|spec| spec.taints.as_ref())
        .is_some_and(|taints| {
            taints.iter().any(|taint| {
                taint.key == cluster.spec.nodes.taint_key
                    && taint.value.as_deref() == Some("true")
                    && taint.effect == "NoSchedule"
            })
        })
}
