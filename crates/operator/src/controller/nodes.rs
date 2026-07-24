use k8s_openapi::api::apps::v1::StatefulSet;
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
    // Label membership is the scaling intent. Node readiness and cordons are
    // transient health signals and must not silently trigger a second Broker
    // disruption while the cluster is already degraded.
    Ok(nodes.items.len())
}

pub(super) async fn ready_brokers(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> anyhow::Result<i32> {
    Ok(broker_health(client, namespace, name).await?.ready)
}

pub(super) async fn ready_component(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    component: &str,
) -> anyhow::Result<i32> {
    let statefulset = Api::<StatefulSet>::namespaced(client.clone(), namespace)
        .get(&format!("{name}-{component}"))
        .await?;
    let Some(revision) = observed_update_revision(&statefulset) else {
        return Ok(0);
    };
    let selector =
        format!("app.kubernetes.io/instance={name},app.kubernetes.io/component={component}");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(pods
        .items
        .iter()
        .filter(|pod| pod_current_and_ready(pod, revision))
        .count() as i32)
}

pub(super) async fn ready_component_revision(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    component: &str,
    revision: &str,
) -> anyhow::Result<i32> {
    let selector =
        format!("app.kubernetes.io/instance={name},app.kubernetes.io/component={component}");
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(pods
        .items
        .iter()
        .filter(|pod| pod_ready_on_revision(pod, revision))
        .count() as i32)
}

pub(super) async fn ready_discovery_mode(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    kodo: bool,
) -> anyhow::Result<i32> {
    let mode = if kodo { "kodo" } else { "direct" };
    let selector = format!(
        "app.kubernetes.io/instance={name},app.kubernetes.io/component=discovery,{}={mode}",
        crate::resources::DISCOVERY_MODE_LABEL
    );
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(pods.items.iter().filter(|pod| pod_ready(pod)).count() as i32)
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
        let is_ready = pod_ready(pod);
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

fn pod_ready(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_none()
        && pod
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True")
            })
}

fn pod_current_and_ready(pod: &Pod, revision: &str) -> bool {
    pod_ready(pod)
        && pod
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("controller-revision-hash"))
            .is_some_and(|value| value == revision)
}

fn pod_ready_on_revision(pod: &Pod, revision: &str) -> bool {
    pod_ready(pod)
        && pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("rustqueue.io/revision"))
            .is_some_and(|value| value == revision)
}

fn observed_update_revision(statefulset: &StatefulSet) -> Option<&str> {
    let generation = statefulset.metadata.generation?;
    let status = statefulset.status.as_ref()?;
    (status.observed_generation.unwrap_or_default() >= generation)
        .then_some(status.update_revision.as_deref())
        .flatten()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(revision: &str, ready: bool) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "queue-kodo-gateway-0",
                "labels": {"controller-revision-hash": revision}
            },
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": if ready { "True" } else { "False" }
                }]
            }
        }))
        .unwrap()
    }

    #[test]
    fn component_readiness_requires_the_target_revision() {
        assert!(pod_current_and_ready(&pod("current", true), "current"));
        assert!(!pod_current_and_ready(&pod("old", true), "current"));
        assert!(!pod_current_and_ready(&pod("current", false), "current"));
    }

    #[test]
    fn statefulset_revision_must_observe_its_latest_generation() {
        let statefulset: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "queue-kodo-gateway", "generation": 4},
            "status": {"observedGeneration": 3, "updateRevision": "new"}
        }))
        .unwrap();
        assert_eq!(observed_update_revision(&statefulset), None);
        let mut observed = statefulset;
        observed.status.as_mut().unwrap().observed_generation = Some(4);
        assert_eq!(observed_update_revision(&observed), Some("new"));
    }

    #[test]
    fn component_readiness_requires_the_requested_rustqueue_revision() {
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "queue-kodo-gateway-0",
                "annotations": {"rustqueue.io/revision": "current"}
            },
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }))
        .unwrap();
        assert!(pod_ready_on_revision(&pod, "current"));
        assert!(!pod_ready_on_revision(&pod, "old"));
    }
}
