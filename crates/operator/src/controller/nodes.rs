use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Api, ListParams};

pub(super) async fn eligible(client: &kube::Client, selector: &str) -> anyhow::Result<usize> {
    let nodes = Api::<Node>::all(client.clone())
        .list(&ListParams::default().labels(selector))
        .await?;
    Ok(nodes
        .items
        .iter()
        .filter(|node| {
            node.spec
                .as_ref()
                .is_none_or(|spec| spec.unschedulable != Some(true))
                && node
                    .status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .is_some_and(|conditions| {
                        conditions.iter().any(|condition| {
                            condition.type_ == "Ready" && condition.status == "True"
                        })
                    })
        })
        .count())
}

pub(super) async fn ready_brokers(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> anyhow::Result<i32> {
    let selector = format!("app.kubernetes.io/instance={name},app.kubernetes.io/component=broker");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(pods
        .items
        .iter()
        .filter(|pod| {
            pod.status
                .as_ref()
                .and_then(|status| status.conditions.as_ref())
                .is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|condition| condition.type_ == "Ready" && condition.status == "True")
                })
        })
        .count() as i32)
}
