use crate::crd::{RustQueueCluster, RustQueueClusterStatus};
use crate::resources::MANAGER;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

pub async fn apply<K>(api: &Api<K>, resource: &K) -> anyhow::Result<K>
where
    K: Clone + Debug + DeserializeOwned + Serialize + Resource<DynamicType = ()>,
{
    let name = resource.name_any();
    Ok(api
        .patch(
            &name,
            &PatchParams::apply(MANAGER).force(),
            &Patch::Apply(resource),
        )
        .await?)
}

pub async fn patch_status(
    client: Client,
    namespace: &str,
    name: &str,
    status: &RustQueueClusterStatus,
) -> anyhow::Result<RustQueueCluster> {
    let api = Api::<RustQueueCluster>::namespaced(client, namespace);
    let value = status_patch_value(status)?;
    Ok(api
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({"status": value})),
        )
        .await?)
}

fn status_patch_value(status: &RustQueueClusterStatus) -> anyhow::Result<serde_json::Value> {
    let mut value = serde_json::to_value(status)?;
    let object = value
        .as_object_mut()
        .expect("RustQueueClusterStatus serializes as an object");
    match &status.upgrade {
        None => {
            object.insert("upgrade".into(), serde_json::Value::Null);
        }
        Some(upgrade) => {
            let upgrade_value = object
                .get_mut("upgrade")
                .and_then(serde_json::Value::as_object_mut)
                .expect("UpgradeStatus serializes as an object");
            if upgrade.current_node_id.is_none() {
                upgrade_value.insert("currentNodeId".into(), serde_json::Value::Null);
            }
            if upgrade.started_at.is_none() {
                upgrade_value.insert("startedAt".into(), serde_json::Value::Null);
            }
        }
    }
    Ok(value)
}

pub async fn delete_pod(client: Client, namespace: &str, name: &str) -> anyhow::Result<()> {
    let api = Api::<Pod>::namespaced(client, namespace);
    api.delete(name, &DeleteParams::default()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::UpgradeStatus;

    #[test]
    fn status_merge_patch_explicitly_clears_optional_rollout_fields() {
        let mut status = RustQueueClusterStatus {
            upgrade: Some(UpgradeStatus {
                target_image: "new".into(),
                ..UpgradeStatus::default()
            }),
            ..RustQueueClusterStatus::default()
        };
        let value = status_patch_value(&status).unwrap();
        assert!(value["upgrade"]["currentNodeId"].is_null());
        assert!(value["upgrade"]["startedAt"].is_null());

        status.upgrade = None;
        assert!(status_patch_value(&status).unwrap()["upgrade"].is_null());
    }
}
