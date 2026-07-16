use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Api, ListParams};

pub(super) struct BrokerHealth {
    pub ready: i32,
    pub unavailable: Vec<String>,
}

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
    Ok(broker_health(client, namespace, name).await?.ready)
}

pub(super) async fn broker_health(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> anyhow::Result<BrokerHealth> {
    let selector = format!("app.kubernetes.io/instance={name},app.kubernetes.io/component=broker");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?;
    let mut ready = 0;
    let mut unavailable = Vec::new();
    for pod in &pods.items {
        let is_ready = pod
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            });
        if is_ready {
            ready += 1;
        } else {
            unavailable.push(format!(
                "{}: {}",
                pod.metadata.name.as_deref().unwrap_or("unknown"),
                pod_unavailable_reason(pod)
            ));
        }
    }
    unavailable.sort();
    Ok(BrokerHealth { ready, unavailable })
}

fn pod_unavailable_reason(pod: &Pod) -> String {
    if let Some(waiting) = pod
        .status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|statuses| statuses.first())
        .and_then(|status| status.state.as_ref())
        .and_then(|state| state.waiting.as_ref())
    {
        return format!(
            "{} {}",
            waiting.reason.as_deref().unwrap_or("ContainerWaiting"),
            waiting.message.as_deref().unwrap_or("")
        )
        .trim()
        .to_owned();
    }
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.status != "True")
        })
        .map(|condition| {
            format!(
                "{} {} {}",
                condition.type_,
                condition.reason.as_deref().unwrap_or(""),
                condition.message.as_deref().unwrap_or("")
            )
            .trim()
            .to_owned()
        })
        .unwrap_or_else(|| "Pod has not reported Ready".into())
}
