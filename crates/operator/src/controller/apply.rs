use crate::crd::RustQueueStatus;
use crate::resources::{ResourceSet, MANAGER};
use crate::RustQueue;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{ConfigMap, Service, ServiceAccount};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::api::{Api, Patch, PatchParams};
use kube::ResourceExt;
use serde_json::json;

pub(super) async fn resources(
    client: &kube::Client,
    namespace: &str,
    set: ResourceSet,
) -> anyhow::Result<()> {
    let params = PatchParams::apply(MANAGER).force();
    Api::<ConfigMap>::namespaced(client.clone(), namespace)
        .patch(&set.config.name_any(), &params, &Patch::Apply(&set.config))
        .await?;
    Api::<ServiceAccount>::namespaced(client.clone(), namespace)
        .patch(
            &set.service_account.name_any(),
            &params,
            &Patch::Apply(&set.service_account),
        )
        .await?;
    Api::<Role>::namespaced(client.clone(), namespace)
        .patch(&set.role.name_any(), &params, &Patch::Apply(&set.role))
        .await?;
    Api::<RoleBinding>::namespaced(client.clone(), namespace)
        .patch(
            &set.role_binding.name_any(),
            &params,
            &Patch::Apply(&set.role_binding),
        )
        .await?;
    Api::<Service>::namespaced(client.clone(), namespace)
        .patch(
            &set.broker_service.name_any(),
            &params,
            &Patch::Apply(&set.broker_service),
        )
        .await?;
    Api::<StatefulSet>::namespaced(client.clone(), namespace)
        .patch(
            &set.brokers.name_any(),
            &params,
            &Patch::Apply(&set.brokers),
        )
        .await?;
    Api::<PodDisruptionBudget>::namespaced(client.clone(), namespace)
        .patch(
            &set.broker_pdb.name_any(),
            &params,
            &Patch::Apply(&set.broker_pdb),
        )
        .await?;
    Api::<Service>::namespaced(client.clone(), namespace)
        .patch(
            &set.discovery_service.name_any(),
            &params,
            &Patch::Apply(&set.discovery_service),
        )
        .await?;
    Api::<Deployment>::namespaced(client.clone(), namespace)
        .patch(
            &set.discovery.name_any(),
            &params,
            &Patch::Apply(&set.discovery),
        )
        .await?;
    Api::<PodDisruptionBudget>::namespaced(client.clone(), namespace)
        .patch(
            &set.discovery_pdb.name_any(),
            &params,
            &Patch::Apply(&set.discovery_pdb),
        )
        .await?;
    Api::<Service>::namespaced(client.clone(), namespace)
        .patch(
            &set.proxy_service.name_any(),
            &params,
            &Patch::Apply(&set.proxy_service),
        )
        .await?;
    Api::<DaemonSet>::namespaced(client.clone(), namespace)
        .patch(&set.proxy.name_any(), &params, &Patch::Apply(&set.proxy))
        .await?;
    Api::<NetworkPolicy>::namespaced(client.clone(), namespace)
        .patch(
            &set.network_policy.name_any(),
            &params,
            &Patch::Apply(&set.network_policy),
        )
        .await?;
    Ok(())
}

pub(super) async fn status(
    client: &kube::Client,
    cluster: &RustQueue,
    status: RustQueueStatus,
) -> anyhow::Result<()> {
    let namespace = cluster.namespace().expect("validated namespace");
    Api::<RustQueue>::namespaced(client.clone(), &namespace)
        .patch_status(
            &cluster.name_any(),
            &PatchParams::apply(MANAGER),
            &Patch::Merge(json!({"status": status})),
        )
        .await?;
    Ok(())
}
