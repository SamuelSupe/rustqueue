use crate::RustQueue;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::ResourceExt;
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StorageState {
    Ready,
    Resizing,
    Blocked,
}

pub(super) struct StorageReport {
    pub state: StorageState,
    pub message: String,
    pub orphaned_pvcs: Vec<String>,
}

pub(super) async fn reconcile(
    client: &kube::Client,
    cluster: &RustQueue,
    namespace: &str,
    desired_replicas: i32,
) -> anyhow::Result<StorageReport> {
    let desired = parse_quantity(&cluster.spec.storage_size)?;
    let selector = format!(
        "app.kubernetes.io/instance={},app.kubernetes.io/component=broker",
        cluster.name_any()
    );
    let api = Api::<PersistentVolumeClaim>::namespaced(client.clone(), namespace);
    let claims = api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    let storage_class = Api::<StorageClass>::all(client.clone())
        .get(&cluster.spec.storage_class_name)
        .await?;
    let expansion_supported = storage_class.allow_volume_expansion == Some(true);
    let mut resizing = Vec::new();
    let mut orphaned = Vec::new();

    for claim in claims {
        let name = claim.name_any();
        if claim_ordinal(&name, &cluster.name_any())
            .is_some_and(|ordinal| ordinal >= desired_replicas.max(0) as u32)
        {
            orphaned.push(name.clone());
        }
        let actual_class = claim
            .spec
            .as_ref()
            .and_then(|spec| spec.storage_class_name.as_deref());
        if actual_class != Some(cluster.spec.storage_class_name.as_str()) {
            return Ok(blocked(
                orphaned,
                format!(
                    "PVC {name} uses StorageClass {:?}, expected {}",
                    actual_class, cluster.spec.storage_class_name
                ),
            ));
        }
        let requested_text = claim_request(&claim).unwrap_or("0");
        let requested = parse_quantity(requested_text)?;
        if desired < requested {
            return Ok(blocked(
                orphaned,
                format!(
                    "PVC {name} requests {requested_text}; storage shrink to {} is forbidden",
                    cluster.spec.storage_size
                ),
            ));
        }
        if desired > requested {
            if !expansion_supported {
                return Ok(blocked(
                    orphaned,
                    format!(
                        "StorageClass {} does not allow volume expansion",
                        cluster.spec.storage_class_name
                    ),
                ));
            }
            api.patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({
                    "spec": {"resources": {"requests": {"storage": cluster.spec.storage_size}}}
                })),
            )
            .await?;
            resizing.push(name);
            continue;
        }
        let capacity = claim_capacity(&claim)
            .map(parse_quantity)
            .transpose()?
            .unwrap_or(0);
        if capacity < desired {
            resizing.push(name);
        }
    }
    orphaned.sort();
    if resizing.is_empty() {
        Ok(StorageReport {
            state: StorageState::Ready,
            message: format!(
                "all PVCs satisfy requested capacity {}",
                cluster.spec.storage_size
            ),
            orphaned_pvcs: orphaned,
        })
    } else {
        resizing.sort();
        Ok(StorageReport {
            state: StorageState::Resizing,
            message: format!(
                "waiting for PVC expansion to {}: {}",
                cluster.spec.storage_size,
                resizing.join(", ")
            ),
            orphaned_pvcs: orphaned,
        })
    }
}

pub(super) fn claim_template_size(set: Option<&StatefulSet>, fallback: &str) -> String {
    set.and_then(|set| set.spec.as_ref())
        .and_then(|spec| spec.volume_claim_templates.as_ref())
        .and_then(|templates| templates.first())
        .and_then(claim_request)
        .unwrap_or(fallback)
        .to_owned()
}

fn blocked(orphaned_pvcs: Vec<String>, message: String) -> StorageReport {
    StorageReport {
        state: StorageState::Blocked,
        message,
        orphaned_pvcs,
    }
}

fn claim_request(claim: &PersistentVolumeClaim) -> Option<&str> {
    claim
        .spec
        .as_ref()?
        .resources
        .as_ref()?
        .requests
        .as_ref()?
        .get("storage")
        .map(|quantity| quantity.0.as_str())
}

fn claim_capacity(claim: &PersistentVolumeClaim) -> Option<&str> {
    claim
        .status
        .as_ref()?
        .capacity
        .as_ref()?
        .get("storage")
        .map(|quantity| quantity.0.as_str())
}

fn claim_ordinal(name: &str, cluster: &str) -> Option<u32> {
    name.strip_prefix(&format!("data-{cluster}-"))?.parse().ok()
}

pub(super) fn parse_quantity(value: &str) -> anyhow::Result<u128> {
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    anyhow::ensure!(!number.is_empty(), "invalid resource quantity {value}");
    let number: f64 = number.parse()?;
    anyhow::ensure!(
        number.is_finite() && number >= 0.0,
        "invalid resource quantity {value}"
    );
    let multiplier = match suffix {
        "" => 1_u128,
        "K" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        "Ki" => 1_u128 << 10,
        "Mi" => 1_u128 << 20,
        "Gi" => 1_u128 << 30,
        "Ti" => 1_u128 << 40,
        "Pi" => 1_u128 << 50,
        "Ei" => 1_u128 << 60,
        _ => anyhow::bail!("unsupported resource quantity suffix in {value}"),
    };
    Ok((number * multiplier as f64) as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compares_binary_and_decimal_quantities() {
        assert_eq!(parse_quantity("1Gi").unwrap(), 1 << 30);
        assert_eq!(parse_quantity("1.5Gi").unwrap(), 3 << 29);
        assert!(parse_quantity("1GiB").is_err());
    }

    #[test]
    fn identifies_only_this_statefulsets_retained_claims() {
        assert_eq!(claim_ordinal("data-queue-12", "queue"), Some(12));
        assert_eq!(claim_ordinal("data-other-12", "queue"), None);
    }
}
