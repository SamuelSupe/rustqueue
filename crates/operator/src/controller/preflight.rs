use super::auth::AuthSecret;
use super::ContextData;
use crate::RustQueue;
use futures::{stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::{Resource, ResourceExt};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const DATA_FORMAT: u32 = 7;

pub(super) enum Outcome {
    Ready { active_feature_level: u32 },
    Pending(String),
    Blocked(String),
}

#[derive(Clone, Debug, Deserialize)]
struct BinaryCapabilities {
    binary_version: String,
    data_format: u32,
    minimum_reader_feature_level: u32,
    maximum_reader_feature_level: u32,
    maximum_writer_feature_level: u32,
    #[serde(default = "legacy_maximum_message_bytes")]
    maximum_message_bytes: usize,
    #[serde(default = "legacy_maximum_batch_bytes")]
    maximum_batch_bytes: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct CompatibilityState {
    data_format: u32,
    active_writer_feature_level: u32,
    minimum_reader_feature_level: u32,
}

#[derive(Clone, Debug, Deserialize)]
struct CompatibilityReport {
    binary: BinaryCapabilities,
    storage: CompatibilityState,
}

pub(super) async fn target_image(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    target_image: &str,
) -> anyhow::Result<Outcome> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    let contract = probe_contract(cluster, target_image);
    let fingerprint = probe_fingerprint(&contract);
    let name = probe_name(&cluster.name_any(), &contract);
    let owner_uid = cluster.metadata.uid.as_deref();
    let pod = match api.get_opt(&name).await? {
        Some(pod) if !controlled_by(&pod, owner_uid) => {
            return Ok(Outcome::Blocked(format!(
                "target image capability probe {name} is not owned by this RustQueue"
            )))
        }
        Some(pod)
            if !probe_matches(
                &pod,
                target_image,
                &cluster.spec.image_pull_policy,
                &fingerprint,
            ) =>
        {
            api.delete(&name, &DeleteParams::default()).await?;
            return Ok(Outcome::Pending(format!(
                "replacing stale target image capability probe {name}"
            )));
        }
        Some(pod) => pod,
        None => {
            let owner = cluster
                .controller_owner_ref(&())
                .ok_or_else(|| anyhow::anyhow!("RustQueue owner reference is unavailable"))?;
            let pod: Pod = serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {
                    "name": name, "namespace": namespace,
                    "annotations": {
                        "rustqueue.io/preflight-contract": fingerprint
                    },
                    "labels": {
                        "app.kubernetes.io/instance": cluster.name_any(),
                        "app.kubernetes.io/component": "capability-preflight",
                        "app.kubernetes.io/managed-by": "rustqueue-operator"
                    },
                    "ownerReferences": [owner]
                },
                "spec": {
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "probe", "image": target_image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueued"],
                        "args": ["--capabilities-output", "/dev/termination-log"],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}}
                    }]
                }
            }))?;
            api.create(&PostParams::default(), &pod).await?;
            return Ok(Outcome::Pending(format!(
                "waiting for target image capability probe {name}"
            )));
        }
    };

    if let Some(blocked) = waiting_failure(&pod) {
        return Ok(Outcome::Blocked(format!(
            "target image capability probe failed: {blocked}"
        )));
    }
    match pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
    {
        Some("Succeeded") => {
            if let Err(message) = validate_probe_image(&pod) {
                return Ok(Outcome::Blocked(message));
            }
            let message = terminated_message(&pod).ok_or_else(|| {
                anyhow::anyhow!("target image probe succeeded without capabilities")
            })?;
            let capabilities: BinaryCapabilities = serde_json::from_str(message)?;
            validate_binary(
                &capabilities,
                cluster.spec.storage_feature_level,
                cluster.spec.max_message_bytes,
                required_batch_bytes(cluster),
            )
            .map_or_else(
                |message| Ok(Outcome::Blocked(message)),
                |_| {
                    Ok(Outcome::Ready {
                        active_feature_level: cluster.spec.storage_feature_level,
                    })
                },
            )
        }
        Some("Failed") => Ok(Outcome::Blocked(
            "target image capability probe exited unsuccessfully".into(),
        )),
        _ => Ok(Outcome::Pending(format!(
            "waiting for target image capability probe {name}"
        ))),
    }
}

pub(super) async fn current_brokers(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &AuthSecret,
) -> anyhow::Result<Outcome> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
        cluster.name_any()
    );
    let pods = api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    if pods.is_empty() {
        return Ok(Outcome::Pending("waiting for broker Pods".into()));
    }
    let desired = cluster.spec.storage_feature_level;
    let token = auth.registry_token.clone();
    let http = context.http.clone();
    let checks = stream::iter(pods.into_iter().map(|pod| {
        let http = http.clone();
        let token = token.clone();
        async move {
            let name = pod.name_any();
            let ip = pod.status.and_then(|status| status.pod_ip);
            let Some(ip) = ip else {
                return Ok::<_, anyhow::Error>((name, None));
            };
            let response = http
                .get(format!("{}/v1/capabilities", origin(&ip)))
                .bearer_auth(&token)
                .send()
                .await?
                .error_for_status()?;
            let report = super::read_broker_json(response).await?;
            Ok((name, Some(report)))
        }
    }))
    .buffer_unordered(32)
    .collect::<Vec<_>>()
    .await;

    let mut maximum_active = 1;
    for check in checks {
        let (name, report) = match check {
            Ok(value) => value,
            Err(error) => {
                return Ok(Outcome::Pending(format!(
                    "broker capability preflight is waiting: {error}"
                )))
            }
        };
        let Some(report) = report else {
            return Ok(Outcome::Pending(format!(
                "broker capability preflight is waiting for {name}"
            )));
        };
        if let Err(message) = validate_report(
            &report,
            desired,
            cluster.spec.max_message_bytes,
            required_batch_bytes(cluster),
        ) {
            return Ok(Outcome::Blocked(format!("broker {name}: {message}")));
        }
        maximum_active = maximum_active.max(report.storage.active_writer_feature_level);
    }
    Ok(Outcome::Ready {
        active_feature_level: maximum_active,
    })
}

pub(super) async fn cleanup_old_probes(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    target_image: &str,
) -> anyhow::Result<()> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    let current = probe_name(&cluster.name_any(), &probe_contract(cluster, target_image));
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=capability-preflight",
        cluster.name_any()
    );
    let owner_uid = cluster.metadata.uid.as_deref();
    for pod in api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
    {
        if pod.name_any() != current && controlled_by(&pod, owner_uid) {
            api.delete(&pod.name_any(), &DeleteParams::default())
                .await?;
        }
    }
    Ok(())
}

fn controlled_by(pod: &Pod, owner_uid: Option<&str>) -> bool {
    owner_uid.is_some_and(|owner_uid| {
        pod.metadata
            .owner_references
            .as_deref()
            .is_some_and(|owners| {
                owners
                    .iter()
                    .any(|owner| owner.controller == Some(true) && owner.uid == owner_uid)
            })
    })
}

fn validate_binary(
    capabilities: &BinaryCapabilities,
    desired: u32,
    message_bytes: usize,
    batch_bytes: usize,
) -> Result<(), String> {
    if capabilities.binary_version.trim().is_empty() || capabilities.data_format != DATA_FORMAT {
        return Err("target image does not advertise RustQueue format v7".into());
    }
    if desired < capabilities.minimum_reader_feature_level
        || desired > capabilities.maximum_reader_feature_level
        || desired > capabilities.maximum_writer_feature_level
    {
        return Err(format!(
            "target image cannot read and write requested storage feature level {desired}"
        ));
    }
    if message_bytes > capabilities.maximum_message_bytes
        || batch_bytes > capabilities.maximum_batch_bytes
    {
        return Err(format!(
            "target image supports at most {} message bytes and {} batch bytes",
            capabilities.maximum_message_bytes, capabilities.maximum_batch_bytes
        ));
    }
    Ok(())
}

fn validate_report(
    report: &CompatibilityReport,
    desired: u32,
    message_bytes: usize,
    batch_bytes: usize,
) -> Result<(), String> {
    validate_binary(
        &report.binary,
        desired.max(report.storage.active_writer_feature_level),
        message_bytes,
        batch_bytes,
    )?;
    if report.storage.data_format != DATA_FORMAT {
        return Err("PVC compatibility state is not format v7".into());
    }
    if report.storage.minimum_reader_feature_level > report.binary.maximum_reader_feature_level {
        return Err("binary is behind the PVC rollback fence".into());
    }
    Ok(())
}

fn required_batch_bytes(cluster: &RustQueue) -> usize {
    if cluster.spec.kodo_compatibility.enabled {
        128 * 1024 * 1024
    } else {
        (64 * 1024 * 1024).max(cluster.spec.max_message_bytes)
    }
}

fn legacy_maximum_message_bytes() -> usize {
    32 * 1024 * 1024
}

fn legacy_maximum_batch_bytes() -> usize {
    64 * 1024 * 1024
}

fn probe_contract(cluster: &RustQueue, image: &str) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}",
        cluster.metadata.uid.as_deref().unwrap_or_default(),
        image,
        cluster.spec.image_pull_policy,
        cluster.spec.storage_feature_level,
        cluster.spec.max_message_bytes,
        required_batch_bytes(cluster),
        cluster.spec.rollout.retry_nonce,
    )
}

fn probe_fingerprint(contract: &str) -> String {
    let digest = Sha256::digest(contract.as_bytes());
    hex::encode(&digest[..16])
}

fn probe_name(instance: &str, contract: &str) -> String {
    let suffix = format!("-preflight-{}", probe_fingerprint(contract));
    let maximum = 63usize.saturating_sub(suffix.len());
    format!("{}{}", &instance[..instance.len().min(maximum)], suffix)
}

fn probe_matches(
    pod: &Pod,
    target_image: &str,
    image_pull_policy: &str,
    fingerprint: &str,
) -> bool {
    let annotation_matches = pod
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get("rustqueue.io/preflight-contract"))
        .is_some_and(|value| value == fingerprint);
    let container_matches = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.containers.first())
        .is_some_and(|container| {
            container.name == "probe"
                && container.image.as_deref() == Some(target_image)
                && container.image_pull_policy.as_deref() == Some(image_pull_policy)
                && container
                    .command
                    .as_ref()
                    .is_some_and(|values| values.iter().map(String::as_str).eq(["rustqueued"]))
                && container.args.as_ref().is_some_and(|values| {
                    values
                        .iter()
                        .map(String::as_str)
                        .eq(["--capabilities-output", "/dev/termination-log"])
                })
        });
    annotation_matches && container_matches
}

fn validate_probe_image(pod: &Pod) -> Result<(), String> {
    let status = pod
        .status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|statuses| statuses.first())
        .ok_or_else(|| "target image probe has no container image identity".to_owned())?;
    if status.image_id.trim().is_empty() {
        return Err("target image probe has an empty container image identity".into());
    }
    Ok(())
}

fn terminated_message(pod: &Pod) -> Option<&str> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .first()?
        .state
        .as_ref()?
        .terminated
        .as_ref()?
        .message
        .as_deref()
}

fn waiting_failure(pod: &Pod) -> Option<&str> {
    let waiting = pod
        .status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .first()?
        .state
        .as_ref()?
        .waiting
        .as_ref()?;
    match waiting.reason.as_deref()? {
        "ImagePullBackOff" | "ErrImagePull" | "InvalidImageName" => waiting.reason.as_deref(),
        _ => None,
    }
}

fn origin(ip: &str) -> String {
    if ip.contains(':') {
        format!("http://[{ip}]:4151")
    } else {
        format!("http://{ip}:4151")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary(maximum: u32) -> BinaryCapabilities {
        BinaryCapabilities {
            binary_version: "0.8.0".into(),
            data_format: 7,
            minimum_reader_feature_level: 1,
            maximum_reader_feature_level: maximum,
            maximum_writer_feature_level: maximum,
            maximum_message_bytes: 100 * 1024 * 1024,
            maximum_batch_bytes: 128 * 1024 * 1024,
        }
    }

    #[test]
    fn target_image_must_support_the_requested_feature() {
        assert!(validate_binary(&binary(2), 2, 100 * 1024 * 1024, 128 * 1024 * 1024).is_ok());
        assert!(validate_binary(&binary(1), 2, 20 * 1024 * 1024, 64 * 1024 * 1024).is_err());
    }

    #[test]
    fn target_image_must_support_the_requested_protocol_limits() {
        let legacy: BinaryCapabilities = serde_json::from_value(json!({
            "binary_version": "0.8.0",
            "data_format": 7,
            "minimum_reader_feature_level": 1,
            "maximum_reader_feature_level": 2,
            "maximum_writer_feature_level": 2
        }))
        .unwrap();
        assert_eq!(legacy.maximum_message_bytes, legacy_maximum_message_bytes());
        assert_eq!(legacy.maximum_batch_bytes, legacy_maximum_batch_bytes());
        assert!(validate_binary(&legacy, 2, 100 * 1024 * 1024, 128 * 1024 * 1024).is_err());
    }

    #[test]
    fn broker_preflight_recovers_the_live_feature_floor() {
        let report = CompatibilityReport {
            binary: binary(2),
            storage: CompatibilityState {
                data_format: 7,
                active_writer_feature_level: 2,
                minimum_reader_feature_level: 2,
            },
        };
        assert!(validate_report(&report, 1, 20 * 1024 * 1024, 64 * 1024 * 1024).is_ok());
        assert_eq!(report.storage.active_writer_feature_level, 2);
    }

    #[test]
    fn probe_names_are_dns_bounded() {
        assert!(probe_name(&"q".repeat(80), "registry/rustqueue:v7").len() <= 63);
    }

    #[test]
    fn probe_cleanup_requires_the_cluster_owner() {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "queue-preflight-old",
                "ownerReferences": [{
                    "apiVersion": "rustqueue.io/v1alpha1",
                    "kind": "RustQueue",
                    "name": "queue",
                    "uid": "queue-uid",
                    "controller": true
                }]
            }
        }))
        .unwrap();
        assert!(controlled_by(&pod, Some("queue-uid")));
        assert!(!controlled_by(&pod, Some("other-uid")));
    }

    #[test]
    fn cached_probe_must_match_its_contract_and_have_an_image_identity() {
        let digest = "a".repeat(64);
        let image = format!("registry/rustqueue@sha256:{digest}");
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "queue-preflight-current",
                "annotations": {"rustqueue.io/preflight-contract": "deadbeef"}
            },
            "spec": {
                "containers": [{
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["rustqueued"],
                    "args": ["--capabilities-output", "/dev/termination-log"]
                }]
            },
            "status": {
                "containerStatuses": [{
                    "name": "probe",
                    "image": image,
                    "imageID": format!("docker-pullable://registry/rustqueue@sha256:{digest}"),
                    "ready": false,
                    "restartCount": 0
                }]
            }
        }))
        .unwrap();
        assert!(probe_matches(&pod, &image, "IfNotPresent", "deadbeef"));
        assert!(validate_probe_image(&pod).is_ok());
        assert!(!probe_matches(&pod, &image, "IfNotPresent", "stale"));
    }
}
