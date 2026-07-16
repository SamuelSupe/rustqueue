use super::auth::AuthSecret;
use super::ContextData;
use crate::RustQueue;
use futures::{stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::{Resource, ResourceExt};
use serde::Deserialize;
use serde_json::json;

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
) -> anyhow::Result<Outcome> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    let name = probe_name(&cluster.name_any(), &cluster.spec.image);
    let pod = match api.get_opt(&name).await? {
        Some(pod) => pod,
        None => {
            let owner = cluster
                .controller_owner_ref(&())
                .ok_or_else(|| anyhow::anyhow!("RustQueue owner reference is unavailable"))?;
            let pod: Pod = serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {
                    "name": name, "namespace": namespace,
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
                        "name": "probe", "image": cluster.spec.image,
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
            let message = terminated_message(&pod).ok_or_else(|| {
                anyhow::anyhow!("target image probe succeeded without capabilities")
            })?;
            let capabilities: BinaryCapabilities = serde_json::from_str(message)?;
            validate_binary(&capabilities, cluster.spec.storage_feature_level).map_or_else(
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
            let report = http
                .get(format!("{}/v1/capabilities", origin(&ip)))
                .bearer_auth(&token)
                .send()
                .await?
                .error_for_status()?
                .json::<CompatibilityReport>()
                .await?;
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
        if let Err(message) = validate_report(&report, desired) {
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
) -> anyhow::Result<()> {
    let api = Api::<Pod>::namespaced(context.client.clone(), namespace);
    let current = probe_name(&cluster.name_any(), &cluster.spec.image);
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=capability-preflight",
        cluster.name_any()
    );
    for pod in api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
    {
        if pod.name_any() != current {
            api.delete(&pod.name_any(), &DeleteParams::default())
                .await?;
        }
    }
    Ok(())
}

fn validate_binary(capabilities: &BinaryCapabilities, desired: u32) -> Result<(), String> {
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
    Ok(())
}

fn validate_report(report: &CompatibilityReport, desired: u32) -> Result<(), String> {
    validate_binary(&report.binary, desired)?;
    if report.storage.data_format != DATA_FORMAT {
        return Err("PVC compatibility state is not format v7".into());
    }
    if report.storage.minimum_reader_feature_level > report.binary.maximum_reader_feature_level {
        return Err("binary is behind the PVC rollback fence".into());
    }
    if desired < report.storage.active_writer_feature_level {
        return Err(format!(
            "requested feature level {desired} is below PVC rollback fence {}",
            report.storage.active_writer_feature_level
        ));
    }
    Ok(())
}

fn probe_name(instance: &str, image: &str) -> String {
    let suffix = format!("-preflight-{:08x}", crc32c::crc32c(image.as_bytes()));
    let maximum = 63usize.saturating_sub(suffix.len());
    format!("{}{}", &instance[..instance.len().min(maximum)], suffix)
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
            binary_version: "0.7.1".into(),
            data_format: 7,
            minimum_reader_feature_level: 1,
            maximum_reader_feature_level: maximum,
            maximum_writer_feature_level: maximum,
        }
    }

    #[test]
    fn target_image_must_support_the_requested_feature() {
        assert!(validate_binary(&binary(2), 2).is_ok());
        assert!(validate_binary(&binary(1), 2).is_err());
    }

    #[test]
    fn broker_preflight_rejects_a_feature_downgrade() {
        let report = CompatibilityReport {
            binary: binary(2),
            storage: CompatibilityState {
                data_format: 7,
                active_writer_feature_level: 2,
                minimum_reader_feature_level: 2,
            },
        };
        assert!(validate_report(&report, 1).is_err());
    }

    #[test]
    fn probe_names_are_dns_bounded() {
        assert!(probe_name(&"q".repeat(80), "registry/rustqueue:v7").len() <= 63);
    }
}
