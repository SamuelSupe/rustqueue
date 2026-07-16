use super::auth::AuthSecret;
use super::ContextData;
use crate::RustQueue;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::ResourceExt;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct DrainStatus {
    draining: bool,
    drained: bool,
}

pub(super) async fn resume_current(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    desired_revision: &str,
    auth: &AuthSecret,
) -> anyhow::Result<()> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
        cluster.name_any()
    );
    for pod in api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
    {
        let current = pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("rustqueue.io/revision"))
            .is_some_and(|revision| revision == desired_revision);
        let Some(ip) = pod.status.and_then(|status| status.pod_ip) else {
            continue;
        };
        if current && drain_status(context, &ip, auth).await?.draining {
            set_drain(context, &ip, auth, false).await?;
        }
    }
    Ok(())
}

pub(super) async fn scale_down_one(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    current: i32,
    auth: &AuthSecret,
) -> anyhow::Result<String> {
    let next = current - 1;
    let pod_name = format!("{}-{next}", cluster.name_any());
    if drain_pod(context, namespace, &pod_name, auth).await? {
        Api::<StatefulSet>::namespaced(context.client.clone(), namespace)
            .patch(
                &cluster.name_any(),
                &PatchParams::default(),
                &Patch::Merge(json!({"spec": {"replicas": next}})),
            )
            .await?;
        Ok(format!("{pod_name} drained; scaling to {next}"))
    } else {
        Ok(format!("waiting for highest ordinal {pod_name} to drain"))
    }
}

pub(super) async fn rollout_one(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    replicas: i32,
    desired_revision: &str,
    auth: &AuthSecret,
) -> anyhow::Result<Option<String>> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
        cluster.name_any()
    );
    let mut outdated: Vec<_> = api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
        .into_iter()
        .filter(|pod| {
            pod.metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get("rustqueue.io/revision"))
                .is_none_or(|revision| revision != desired_revision)
        })
        .collect();
    outdated.sort_by_key(|pod| pod_ordinal(&pod.name_any()).unwrap_or_default());
    let Some(pod) = outdated.pop() else {
        return Ok(None);
    };
    if replicas < 2 {
        return Ok(Some(
            "rolling replacement needs at least two brokers".into(),
        ));
    }
    let name = pod.name_any();
    if drain_pod(context, namespace, &name, auth).await? {
        api.delete(&name, &DeleteParams::default()).await?;
        Ok(Some(format!("deleted drained outdated broker {name}")))
    } else {
        Ok(Some(format!("waiting for outdated broker {name} to drain")))
    }
}

async fn drain_pod(
    context: &ContextData,
    namespace: &str,
    pod_name: &str,
    auth: &AuthSecret,
) -> anyhow::Result<bool> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let Some(pod) = api.get_opt(pod_name).await? else {
        return Ok(true);
    };
    let Some(ip) = pod.status.and_then(|status| status.pod_ip) else {
        return Ok(false);
    };
    set_drain(context, &ip, auth, true).await?;
    Ok(drain_status(context, &ip, auth).await?.drained)
}

async fn set_drain(
    context: &ContextData,
    ip: &str,
    auth: &AuthSecret,
    enabled: bool,
) -> anyhow::Result<()> {
    context
        .http
        .post(format!("{}/v1/drain", origin(ip)))
        .bearer_auth(&auth.admin_token)
        .json(&json!({"enabled": enabled}))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn drain_status(
    context: &ContextData,
    ip: &str,
    auth: &AuthSecret,
) -> anyhow::Result<DrainStatus> {
    Ok(context
        .http
        .get(format!("{}/v1/drain", origin(ip)))
        .bearer_auth(&auth.registry_token)
        .send()
        .await?
        .error_for_status()?
        .json::<DrainStatus>()
        .await?)
}

fn origin(ip: &str) -> String {
    if ip.contains(':') {
        format!("http://[{ip}]:4151")
    } else {
        format!("http://{ip}:4151")
    }
}

fn pod_ordinal(name: &str) -> Option<u32> {
    name.rsplit_once('-')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ordinal_sorting_selects_the_highest_broker() {
        let mut names = vec!["queue-9", "queue-10", "queue-499", "queue-100"];
        names.sort_by_key(|name| pod_ordinal(name).unwrap());
        assert_eq!(names.pop(), Some("queue-499"));
    }

    #[test]
    fn formats_ipv4_and_ipv6_origins() {
        assert_eq!(origin("10.0.0.1"), "http://10.0.0.1:4151");
        assert_eq!(origin("fd00::1"), "http://[fd00::1]:4151");
    }
}
