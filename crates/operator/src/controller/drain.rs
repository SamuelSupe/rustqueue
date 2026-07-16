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

pub(super) struct Progress {
    pub target: Option<String>,
    pub phase: &'static str,
    pub message: String,
}

pub(super) struct RolloutOptions<'a> {
    pub replicas: i32,
    pub desired_revision: &'a str,
    pub paused: bool,
    pub require_canary_approval: bool,
    pub approved_revision: Option<&'a str>,
}

pub(super) async fn resume_current(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    desired_revision: &str,
    auth: &AuthSecret,
) -> anyhow::Result<()> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    for pod in broker_pods(&api, cluster).await? {
        let current = pod_revision(&pod).is_some_and(|revision| revision == desired_revision);
        let Some(ip) = pod
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
        else {
            continue;
        };
        if current && drain_status(context, ip, auth).await?.draining {
            set_drain(context, ip, auth, false).await?;
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
) -> anyhow::Result<Progress> {
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
        Ok(Progress {
            target: Some(pod_name.clone()),
            phase: "Completed",
            message: format!("{pod_name} drained; scaling to {next}"),
        })
    } else {
        Ok(Progress {
            target: Some(pod_name.clone()),
            phase: "Draining",
            message: format!("waiting for highest ordinal {pod_name} to drain"),
        })
    }
}

pub(super) async fn rollout_one(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    options: RolloutOptions<'_>,
    auth: &AuthSecret,
) -> anyhow::Result<Progress> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let pods = broker_pods(&api, cluster).await?;
    let decision = rollout_decision(&pods, &options);
    match decision {
        Decision::Completed => Ok(Progress {
            target: None,
            phase: "Completed",
            message: format!("all {} broker Pods are current and Ready", options.replicas),
        }),
        Decision::Wait(message) => Ok(Progress {
            target: None,
            phase: "WaitingForReady",
            message,
        }),
        Decision::Paused => Ok(Progress {
            target: None,
            phase: "Paused",
            message: "rollout is paused by spec.rollout.paused".into(),
        }),
        Decision::AwaitingApproval => Ok(Progress {
            target: None,
            phase: "AwaitingCanaryApproval",
            message: format!(
                "canary is Ready; set spec.rollout.approvedRevision={} to continue",
                options.desired_revision
            ),
        }),
        Decision::Blocked(message) => Ok(Progress {
            target: None,
            phase: "Blocked",
            message,
        }),
        Decision::Replace(name) => {
            if drain_pod(context, namespace, &name, auth).await? {
                api.delete(&name, &DeleteParams::default()).await?;
                Ok(Progress {
                    target: Some(name.clone()),
                    phase: "Replacing",
                    message: format!("deleted drained outdated broker {name}"),
                })
            } else {
                Ok(Progress {
                    target: Some(name.clone()),
                    phase: "Draining",
                    message: format!("waiting for outdated broker {name} to drain"),
                })
            }
        }
    }
}

pub(super) async fn maintenance(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    broker: &str,
    enabled: bool,
    auth: &AuthSecret,
) -> anyhow::Result<Progress> {
    let prefix = format!("{}-", cluster.name_any());
    anyhow::ensure!(
        broker.starts_with(&prefix) && pod_ordinal(broker).is_some(),
        "maintenance target {broker} is not a Broker of {}",
        cluster.name_any()
    );
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let pod = api
        .get_opt(broker)
        .await?
        .ok_or_else(|| anyhow::anyhow!("maintenance target {broker} does not exist"))?;
    let ip = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
        .ok_or_else(|| anyhow::anyhow!("maintenance target {broker} has no Pod IP"))?;
    set_drain(context, ip, auth, enabled).await?;
    let status = drain_status(context, ip, auth).await?;
    let (phase, message) = if enabled && status.drained {
        (
            "Completed",
            format!("{broker} is drained and held in maintenance"),
        )
    } else if enabled {
        (
            "Draining",
            format!("waiting for {broker} backlog and in-flight work to drain"),
        )
    } else {
        ("Completed", format!("{broker} resumed service"))
    };
    Ok(Progress {
        target: Some(broker.into()),
        phase,
        message,
    })
}

pub(super) async fn previous_image(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    desired_revision: &str,
) -> anyhow::Result<Option<String>> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    Ok(broker_pods(&api, cluster)
        .await?
        .iter()
        .find(|pod| pod_revision(pod) != Some(desired_revision))
        .and_then(pod_image)
        .map(ToOwned::to_owned))
}

async fn broker_pods(api: &Api<Pod>, cluster: &RustQueue) -> anyhow::Result<Vec<Pod>> {
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
        cluster.name_any()
    );
    Ok(api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items)
}

enum Decision {
    Completed,
    Wait(String),
    Paused,
    AwaitingApproval,
    Blocked(String),
    Replace(String),
}

fn rollout_decision(pods: &[Pod], options: &RolloutOptions<'_>) -> Decision {
    if pods.len() < options.replicas as usize {
        return Decision::Wait(format!(
            "waiting for {} of {} broker Pods to exist",
            pods.len(),
            options.replicas
        ));
    }
    let current: Vec<_> = pods
        .iter()
        .filter(|pod| pod_revision(pod) == Some(options.desired_revision))
        .collect();
    if let Some(pod) = current.iter().find(|pod| !pod_ready(pod)) {
        return Decision::Wait(format!(
            "waiting for replacement broker {} to become Ready",
            pod.name_any()
        ));
    }
    let mut outdated: Vec<_> = pods
        .iter()
        .filter(|pod| pod_revision(pod) != Some(options.desired_revision))
        .collect();
    if outdated.is_empty() {
        return Decision::Completed;
    }
    if options.replicas < 2 {
        return Decision::Blocked("rolling replacement needs at least two brokers".into());
    }
    if options.paused {
        return Decision::Paused;
    }
    if options.require_canary_approval
        && !current.is_empty()
        && options.approved_revision != Some(options.desired_revision)
    {
        return Decision::AwaitingApproval;
    }
    outdated.sort_by_key(|pod| pod_ordinal(&pod.name_any()).unwrap_or_default());
    Decision::Replace(outdated.pop().expect("non-empty outdated set").name_any())
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
    let Some(ip) = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
    else {
        return Ok(false);
    };
    set_drain(context, ip, auth, true).await?;
    Ok(drain_status(context, ip, auth).await?.drained)
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

fn pod_revision(pod: &Pod) -> Option<&str> {
    pod.metadata
        .annotations
        .as_ref()?
        .get("rustqueue.io/revision")
        .map(String::as_str)
}

fn pod_image(pod: &Pod) -> Option<&str> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .find(|container| container.name == "broker")?
        .image
        .as_deref()
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
    use kube::api::ObjectMeta;

    fn pod(name: &str, revision: &str, ready: bool) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                annotations: Some(std::collections::BTreeMap::from([(
                    "rustqueue.io/revision".into(),
                    revision.into(),
                )])),
                ..Default::default()
            },
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                conditions: Some(vec![k8s_openapi::api::core::v1::PodCondition {
                    type_: "Ready".into(),
                    status: if ready { "True" } else { "False" }.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn options<'a>(revision: &'a str) -> RolloutOptions<'a> {
        RolloutOptions {
            replicas: 3,
            desired_revision: revision,
            paused: false,
            require_canary_approval: false,
            approved_revision: None,
        }
    }

    #[test]
    fn replacement_waits_for_the_previous_new_pod_to_be_ready() {
        let pods = vec![
            pod("queue-0", "old", true),
            pod("queue-1", "old", true),
            pod("queue-2", "new", false),
        ];
        assert!(matches!(
            rollout_decision(&pods, &options("new")),
            Decision::Wait(_)
        ));
    }

    #[test]
    fn canary_requires_explicit_revision_approval() {
        let pods = vec![
            pod("queue-0", "old", true),
            pod("queue-1", "old", true),
            pod("queue-2", "new", true),
        ];
        let mut options = options("new");
        options.require_canary_approval = true;
        assert!(matches!(
            rollout_decision(&pods, &options),
            Decision::AwaitingApproval
        ));
        options.approved_revision = Some("new");
        assert!(matches!(
            rollout_decision(&pods, &options),
            Decision::Replace(_)
        ));
    }

    #[test]
    fn a_current_single_broker_is_ready_without_a_rollout() {
        let pods = vec![pod("queue-0", "current", true)];
        let mut options = options("current");
        options.replicas = 1;
        assert!(matches!(
            rollout_decision(&pods, &options),
            Decision::Completed
        ));
    }

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
