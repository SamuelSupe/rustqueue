use crate::crd::RustQueueCluster;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};

pub async fn resolve_default(
    client: Client,
    cluster: &RustQueueCluster,
) -> anyhow::Result<RustQueueCluster> {
    if !cluster.spec.storage.class_name.trim().is_empty() {
        return Ok(cluster.clone());
    }
    let classes = Api::<StorageClass>::all(client)
        .list(&ListParams::default())
        .await?
        .items;
    let mut defaults = classes
        .iter()
        .filter(|class| {
            class.metadata.annotations.as_ref().is_some_and(|values| {
                [
                    "storageclass.kubernetes.io/is-default-class",
                    "storageclass.beta.kubernetes.io/is-default-class",
                ]
                .iter()
                .any(|key| values.get(*key).is_some_and(|value| value == "true"))
            })
        })
        .map(ResourceExt::name_any)
        .collect::<Vec<_>>();
    defaults.sort();
    anyhow::ensure!(
        defaults.len() == 1,
        "spec.storage.className is empty and Kubernetes has {} default StorageClasses",
        defaults.len()
    );
    let mut resolved = cluster.clone();
    resolved.spec.storage.class_name = defaults.remove(0);
    tracing::debug!(
        storage_class = %resolved.spec.storage.class_name,
        "using Kubernetes default StorageClass"
    );
    Ok(resolved)
}
