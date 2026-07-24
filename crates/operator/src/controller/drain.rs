use super::auth::AuthSecret;
use super::ContextData;
use crate::RustQueue;
use futures::{stream, StreamExt};
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
    #[serde(default)]
    quiesced: Option<bool>,
    #[serde(default)]
    delivery_frozen: Option<bool>,
    #[serde(default)]
    empty: Option<bool>,
}

#[derive(Clone, Copy)]
enum DrainGoal {
    Quiesced,
    Empty,
}

impl DrainStatus {
    fn satisfies(&self, goal: DrainGoal) -> bool {
        match goal {
            DrainGoal::Quiesced => match (self.delivery_frozen, self.quiesced) {
                (Some(true), Some(quiesced)) => quiesced,
                (Some(false), _) => false,
                // A broker predating the delivery-freeze contract can keep a
                // fetch active after reporting zero in-flight messages. Its
                // legacy `drained` flag is safe because it also requires the
                // local backlog to be empty; accepting a synthetic quiesce is
                // not.
                _ => self.drained,
            },
            DrainGoal::Empty => self.empty.unwrap_or(self.drained),
        }
    }
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
            set_drain(context, ip, auth, false, false).await?;
        }
    }
    Ok(())
}

pub(super) async fn resume_all(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &AuthSecret,
) -> anyhow::Result<Vec<String>> {
    let api: Api<Pod> = Api::namespaced(context.client.clone(), namespace);
    let checks = broker_pods(&api, cluster)
        .await?
        .into_iter()
        .filter_map(|pod| {
            let name = pod.name_any();
            let ip = pod.status?.pod_ip?;
            Some((name, ip))
        });
    let mut resumed: Vec<_> = stream::iter(checks)
        .map(|(name, ip)| async move {
            match drain_status(context, &ip, auth).await {
                Ok(status) if status.draining => {
                    match set_drain(context, &ip, auth, false, false).await {
                        Ok(()) => Some(name),
                        Err(error) => {
                            tracing::warn!(broker = %name, %error, "could not resume drained Broker");
                            None
                        }
                    }
                }
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(broker = %name, %error, "could not inspect Broker drain state");
                    None
                }
            }
        })
        .buffer_unordered(32)
        .filter_map(async move |name| name)
        .collect()
        .await;
    resumed.sort();
    Ok(resumed)
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
    let pods = broker_pods(
        &Api::<Pod>::namespaced(context.client.clone(), namespace),
        cluster,
    )
    .await?;
    if !other_brokers_ready(&pods, &pod_name, current) {
        resume_one(context, namespace, &pod_name, auth).await?;
        return Ok(Progress {
            target: Some(pod_name.clone()),
            phase: "WaitingForReady",
            message: format!(
                "resumed scale-down target {pod_name}; waiting for every other Broker to become Ready"
            ),
        });
    }
    if drain_pod(context, namespace, &pod_name, DrainGoal::Empty, auth).await? {
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
    let draining =
        intentionally_draining_pods(context, &pods, options.desired_revision, auth).await;
    let decision = rollout_decision(&pods, &options, &draining);
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
        Decision::Resume(names, message) => {
            for name in names {
                resume_one(context, namespace, &name, auth).await?;
            }
            Ok(Progress {
                target: None,
                phase: "WaitingForReady",
                message,
            })
        }
        Decision::Replace(name) => {
            if drain_pod(context, namespace, &name, DrainGoal::Quiesced, auth).await? {
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

pub(super) async fn resume_one(
    context: &ContextData,
    namespace: &str,
    broker: &str,
    auth: &AuthSecret,
) -> anyhow::Result<()> {
    let Some(pod) = Api::<Pod>::namespaced(context.client.clone(), namespace)
        .get_opt(broker)
        .await?
    else {
        return Ok(());
    };
    let Some(ip) = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
    else {
        return Ok(());
    };
    set_drain(context, ip, auth, false, false).await
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
    let pods = broker_pods(&api, cluster).await?;
    let pod = pods
        .iter()
        .find(|pod| pod.name_any() == broker)
        .ok_or_else(|| anyhow::anyhow!("maintenance target {broker} does not exist"))?;
    if enabled {
        let replicas = Api::<StatefulSet>::namespaced(context.client.clone(), namespace)
            .get(&cluster.name_any())
            .await?
            .spec
            .and_then(|spec| spec.replicas)
            .unwrap_or_default();
        if !other_brokers_ready(&pods, broker, replicas) {
            resume_one(context, namespace, broker, auth).await?;
            return Ok(Progress {
                target: Some(broker.into()),
                phase: "Blocked",
                message: format!(
                    "resumed maintenance target {broker}; maintenance is blocked until every other Broker is Ready"
                ),
            });
        }
    }
    let ip = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
        .ok_or_else(|| anyhow::anyhow!("maintenance target {broker} has no Pod IP"))?;
    set_drain(context, ip, auth, enabled, false).await?;
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

pub(super) async fn brokers_current_and_ready(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    desired_revision: &str,
    replicas: i32,
) -> anyhow::Result<bool> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    Ok(broker_set_current_and_ready(
        &broker_pods(&api, cluster).await?,
        desired_revision,
        replicas,
    ))
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

fn broker_set_current_and_ready(pods: &[Pod], desired_revision: &str, replicas: i32) -> bool {
    if !exact_broker_pod_set(pods, replicas) {
        return false;
    }
    let ordinals: std::collections::BTreeSet<_> = pods
        .iter()
        .filter(|pod| pod_revision(pod) == Some(desired_revision) && pod_ready(pod))
        .filter_map(|pod| pod_ordinal(&pod.name_any()))
        .collect();
    ordinals == (0..replicas as u32).collect()
}

enum Decision {
    Completed,
    Wait(String),
    Paused,
    AwaitingApproval,
    Blocked(String),
    Resume(Vec<String>, String),
    Replace(String),
}

fn rollout_decision(
    pods: &[Pod],
    options: &RolloutOptions<'_>,
    intentionally_draining: &std::collections::BTreeSet<String>,
) -> Decision {
    if !exact_broker_pod_set(pods, options.replicas) {
        return Decision::Wait(format!(
            "waiting for the exact {}-Pod Broker ordinal set; observed {} Pods",
            options.replicas,
            pods.len(),
        ));
    }
    let current: Vec<_> = pods
        .iter()
        .filter(|pod| pod_revision(pod) == Some(options.desired_revision))
        .collect();
    let mut outdated: Vec<_> = pods
        .iter()
        .filter(|pod| pod_revision(pod) != Some(options.desired_revision))
        .collect();
    if outdated.is_empty() {
        return current
            .iter()
            .find(|pod| !pod_ready(pod))
            .map_or(Decision::Completed, |pod| {
                Decision::Wait(format!(
                    "waiting for current broker {} to become Ready",
                    pod.name_any()
                ))
            });
    }
    let draining: Vec<_> = outdated
        .iter()
        .filter(|pod| !pod_ready(pod) && intentionally_draining.contains(&pod.name_any()))
        .map(|pod| pod.name_any())
        .collect();
    if let Some(pod) = current.iter().find(|pod| !pod_ready(pod)) {
        if !draining.is_empty() {
            return Decision::Resume(
                draining,
                format!(
                    "resumed an in-progress drain because current broker {} is unavailable",
                    pod.name_any()
                ),
            );
        }
        return Decision::Wait(format!(
            "waiting for replacement broker {} to become Ready",
            pod.name_any()
        ));
    }
    if let Some(pod) = outdated
        .iter()
        .find(|pod| !pod_ready(pod) && !intentionally_draining.contains(&pod.name_any()))
    {
        if !draining.is_empty() {
            return Decision::Resume(
                draining,
                format!(
                    "resumed an in-progress drain because outdated broker {} is also unavailable",
                    pod.name_any()
                ),
            );
        }
        return Decision::Wait(format!(
            "outdated broker {} is not Ready; refusing to make another broker unavailable",
            pod.name_any()
        ));
    }
    if options.replicas < 2 {
        return Decision::Blocked("rolling replacement needs at least two brokers".into());
    }
    if draining.len() > 1 {
        return Decision::Resume(
            draining,
            "resumed multiple in-progress drains; only one Broker may be disrupted".into(),
        );
    }
    if let Some(name) = draining.into_iter().next() {
        return Decision::Replace(name);
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

async fn intentionally_draining_pods(
    context: &ContextData,
    pods: &[Pod],
    desired_revision: &str,
    auth: &AuthSecret,
) -> std::collections::BTreeSet<String> {
    let mut draining = std::collections::BTreeSet::new();
    for pod in pods
        .iter()
        .filter(|pod| !pod_ready(pod) && pod_revision(pod) != Some(desired_revision))
    {
        let Some(ip) = pod
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
        else {
            continue;
        };
        match drain_status(context, ip, auth).await {
            Ok(status) if status.draining => {
                draining.insert(pod.name_any());
            }
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(
                    broker = %pod.name_any(),
                    %error,
                    "could not verify whether unavailable Broker is intentionally draining"
                );
            }
        }
    }
    draining
}

async fn drain_pod(
    context: &ContextData,
    namespace: &str,
    pod_name: &str,
    goal: DrainGoal,
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
    set_drain(context, ip, auth, true, matches!(goal, DrainGoal::Quiesced)).await?;
    Ok(drain_status(context, ip, auth).await?.satisfies(goal))
}

async fn set_drain(
    context: &ContextData,
    ip: &str,
    auth: &AuthSecret,
    enabled: bool,
    freeze_deliveries: bool,
) -> anyhow::Result<()> {
    context
        .http
        .post(format!("{}/v1/drain", origin(ip)))
        .bearer_auth(&auth.admin_token)
        .json(&json!({
            "enabled": enabled,
            "freeze_deliveries": freeze_deliveries,
        }))
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

fn other_brokers_ready(pods: &[Pod], target: &str, replicas: i32) -> bool {
    if !exact_broker_pod_set(pods, replicas) {
        return false;
    }
    let Some(replicas) = u32::try_from(replicas).ok() else {
        return false;
    };
    let Some(target_ordinal) = pod_ordinal(target).filter(|ordinal| *ordinal < replicas) else {
        return false;
    };
    let ready: std::collections::BTreeSet<_> = pods
        .iter()
        .filter(|pod| pod.name_any() != target && pod_ready(pod))
        .filter_map(|pod| pod_ordinal(&pod.name_any()))
        .collect();
    ready
        == (0..replicas)
            .filter(|ordinal| *ordinal != target_ordinal)
            .collect()
}

fn exact_broker_pod_set(pods: &[Pod], replicas: i32) -> bool {
    let Ok(replicas) = u32::try_from(replicas) else {
        return false;
    };
    if replicas == 0 || pods.len() != replicas as usize {
        return false;
    }
    let ordinals: std::collections::BTreeSet<_> = pods
        .iter()
        .filter_map(|pod| pod_ordinal(&pod.name_any()))
        .collect();
    ordinals.len() == pods.len() && ordinals == (0..replicas).collect()
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
            rollout_decision(&pods, &options("new"), &Default::default()),
            Decision::Wait(_)
        ));
    }

    #[test]
    fn rollout_does_not_compound_an_unready_outdated_broker() {
        let pods = vec![
            pod("queue-0", "old", false),
            pod("queue-1", "old", true),
            pod("queue-2", "old", true),
        ];
        assert!(matches!(
            rollout_decision(&pods, &options("new"), &Default::default()),
            Decision::Wait(message) if message.contains("refusing")
        ));
    }

    #[test]
    fn rollout_continues_the_one_intentionally_draining_broker() {
        let pods = vec![
            pod("queue-0", "old", true),
            pod("queue-1", "old", true),
            pod("queue-2", "old", false),
        ];
        assert!(matches!(
            rollout_decision(
                &pods,
                &options("new"),
                &["queue-2".into()].into_iter().collect()
            ),
            Decision::Replace(name) if name == "queue-2"
        ));
    }

    #[test]
    fn rollout_resumes_its_drain_if_another_broker_becomes_unavailable() {
        let pods = vec![
            pod("queue-0", "old", false),
            pod("queue-1", "old", true),
            pod("queue-2", "old", false),
        ];
        assert!(matches!(
            rollout_decision(
                &pods,
                &options("new"),
                &["queue-2".into()].into_iter().collect()
            ),
            Decision::Resume(names, _) if names == ["queue-2"]
        ));
    }

    #[test]
    fn rollout_resumes_its_drain_if_a_current_broker_becomes_unavailable() {
        let pods = vec![
            pod("queue-0", "new", false),
            pod("queue-1", "old", true),
            pod("queue-2", "old", false),
        ];
        assert!(matches!(
            rollout_decision(
                &pods,
                &options("new"),
                &["queue-2".into()].into_iter().collect()
            ),
            Decision::Resume(names, _) if names == ["queue-2"]
        ));
    }

    #[test]
    fn maintenance_and_scale_down_require_every_other_broker_to_be_ready() {
        let pods = vec![
            pod("queue-0", "current", true),
            pod("queue-1", "current", true),
            pod("queue-2", "current", false),
        ];
        assert!(other_brokers_ready(&pods, "queue-2", 3));
        assert!(!other_brokers_ready(&pods, "queue-1", 3));
        assert!(!other_brokers_ready(
            &[pods[0].clone(), pods[2].clone()],
            "queue-2",
            3
        ));
        let mut with_stale = pods.clone();
        with_stale.push(pod("queue-3", "old", true));
        assert!(!other_brokers_ready(&with_stale, "queue-2", 3));
    }

    #[test]
    fn rollout_waits_for_stale_or_malformed_broker_pods_to_disappear() {
        let mut extra = vec![
            pod("queue-0", "old", true),
            pod("queue-1", "old", true),
            pod("queue-2", "old", true),
        ];
        extra.push(pod("queue-3", "old", true));
        assert!(matches!(
            rollout_decision(&extra, &options("new"), &Default::default()),
            Decision::Wait(message) if message.contains("exact")
        ));

        let duplicate = vec![
            pod("queue-0", "old", true),
            pod("another-0", "old", true),
            pod("queue-2", "old", true),
        ];
        assert!(matches!(
            rollout_decision(&duplicate, &options("new"), &Default::default()),
            Decision::Wait(message) if message.contains("exact")
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
            rollout_decision(&pods, &options, &Default::default()),
            Decision::AwaitingApproval
        ));
        options.approved_revision = Some("new");
        assert!(matches!(
            rollout_decision(&pods, &options, &Default::default()),
            Decision::Replace(_)
        ));
    }

    #[test]
    fn a_current_single_broker_is_ready_without_a_rollout() {
        let pods = vec![pod("queue-0", "current", true)];
        let mut options = options("current");
        options.replicas = 1;
        assert!(matches!(
            rollout_decision(&pods, &options, &Default::default()),
            Decision::Completed
        ));
    }

    #[test]
    fn current_revision_is_not_complete_until_every_broker_is_ready() {
        let pods = vec![
            pod("queue-0", "current", true),
            pod("queue-1", "current", false),
            pod("queue-2", "current", true),
        ];
        assert!(matches!(
            rollout_decision(&pods, &options("current"), &Default::default()),
            Decision::Wait(message) if message.contains("queue-1")
        ));
    }

    #[test]
    fn numeric_ordinal_sorting_selects_the_highest_broker() {
        let mut names = vec!["queue-9", "queue-10", "queue-499", "queue-100"];
        names.sort_by_key(|name| pod_ordinal(name).unwrap());
        assert_eq!(names.pop(), Some("queue-499"));
    }

    #[test]
    fn target_broker_readiness_requires_the_complete_current_revision() {
        let ready = vec![
            pod("queue-0", "current", true),
            pod("queue-1", "current", true),
            pod("queue-2", "current", true),
        ];
        assert!(broker_set_current_and_ready(&ready, "current", 3));

        let mut outdated = ready.clone();
        outdated[1] = pod("queue-1", "old", true);
        assert!(!broker_set_current_and_ready(&outdated, "current", 3));

        let mut unready = ready.clone();
        unready[2] = pod("queue-2", "current", false);
        assert!(!broker_set_current_and_ready(&unready, "current", 3));
        assert!(!broker_set_current_and_ready(&ready[..2], "current", 3));
    }

    #[test]
    fn formats_ipv4_and_ipv6_origins() {
        assert_eq!(origin("10.0.0.1"), "http://10.0.0.1:4151");
        assert_eq!(origin("fd00::1"), "http://[fd00::1]:4151");
    }

    #[test]
    fn rollout_accepts_quiesced_backlog_but_scale_down_does_not() {
        let status: DrainStatus = serde_json::from_value(json!({
            "draining": true,
            "drained": false,
            "quiesced": true,
            "delivery_frozen": true,
            "empty": false,
            "in_flight": 0
        }))
        .unwrap();
        assert!(status.satisfies(DrainGoal::Quiesced));
        assert!(!status.satisfies(DrainGoal::Empty));
    }

    #[test]
    fn rollout_requires_an_older_v7_broker_to_be_fully_drained() {
        let status: DrainStatus = serde_json::from_value(json!({
            "draining": true,
            "drained": false,
            "in_flight": 0
        }))
        .unwrap();
        assert!(!status.satisfies(DrainGoal::Quiesced));
        assert!(!status.satisfies(DrainGoal::Empty));

        let status: DrainStatus = serde_json::from_value(json!({
            "draining": true,
            "drained": true,
            "in_flight": 0
        }))
        .unwrap();
        assert!(status.satisfies(DrainGoal::Quiesced));
    }

    #[test]
    fn rollout_rejects_a_new_broker_without_a_delivery_freeze() {
        let status: DrainStatus = serde_json::from_value(json!({
            "draining": true,
            "drained": false,
            "quiesced": true,
            "delivery_frozen": false,
            "in_flight": 0
        }))
        .unwrap();
        assert!(!status.satisfies(DrainGoal::Quiesced));
    }
}
