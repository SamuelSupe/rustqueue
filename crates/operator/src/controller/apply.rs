use crate::crd::RustQueueStatus;
use crate::resources::{ResourceSet, MANAGER};
use crate::RustQueue;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{ConfigMap, Service, ServiceAccount};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::api::{Api, DeleteParams, ObjectMeta, Patch, PatchParams};
use kube::ResourceExt;
use serde_json::json;

pub(super) async fn resources(
    client: &kube::Client,
    namespace: &str,
    set: ResourceSet,
) -> anyhow::Result<()> {
    let params = PatchParams::apply(MANAGER).force();
    let cluster_name = set.brokers.name_any();
    let owner_uid = set
        .brokers
        .metadata
        .owner_references
        .as_deref()
        .and_then(|owners| owners.iter().find(|owner| owner.controller == Some(true)))
        .map(|owner| owner.uid.clone());
    let network_policies = Api::<NetworkPolicy>::namespaced(client.clone(), namespace);
    Api::<ConfigMap>::namespaced(client.clone(), namespace)
        .patch(&set.config.name_any(), &params, &Patch::Apply(&set.config))
        .await?;
    network_policies
        .patch(
            &set.broker_network_policy.name_any(),
            &params,
            &Patch::Apply(&set.broker_network_policy),
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
    runtime_resources(client, namespace, &set).await?;

    let services = Api::<Service>::namespaced(client.clone(), namespace);
    let gateway_publish_service_name = format!("{cluster_name}-kodo-publish");
    let statefulsets = Api::<StatefulSet>::namespaced(client.clone(), namespace);
    let gateway_name = format!("{cluster_name}-kodo-gateway");
    if set.kodo_gateway.is_none()
        && !set.retain_existing_kodo_resources
        && statefulsets
            .get_opt(&gateway_name)
            .await?
            .as_ref()
            .is_some_and(|resource| controlled_by(&resource.metadata, owner_uid.as_deref()))
    {
        statefulsets
            .delete(&gateway_name, &DeleteParams::default())
            .await?;
    }
    if !set.retain_existing_kodo_resources
        && set.kodo_gateway_service.is_none()
        && services
            .get_opt(&gateway_publish_service_name)
            .await?
            .as_ref()
            .is_some_and(|resource| controlled_by(&resource.metadata, owner_uid.as_deref()))
    {
        services
            .delete(&gateway_publish_service_name, &DeleteParams::default())
            .await?;
    }
    let gateway_headless_service_name = format!("{cluster_name}-kodo-gateways");
    if !set.retain_existing_kodo_resources
        && set.kodo_gateway_headless_service.is_none()
        && services
            .get_opt(&gateway_headless_service_name)
            .await?
            .as_ref()
            .is_some_and(|resource| controlled_by(&resource.metadata, owner_uid.as_deref()))
    {
        services
            .delete(&gateway_headless_service_name, &DeleteParams::default())
            .await?;
    }
    let pdbs = Api::<PodDisruptionBudget>::namespaced(client.clone(), namespace);
    let gateway_pdb_name = format!("{gateway_name}-pdb");
    if set.kodo_gateway_pdb.is_none()
        && !set.retain_existing_kodo_resources
        && pdbs
            .get_opt(&gateway_pdb_name)
            .await?
            .as_ref()
            .is_some_and(|resource| controlled_by(&resource.metadata, owner_uid.as_deref()))
    {
        pdbs.delete(&gateway_pdb_name, &DeleteParams::default())
            .await?;
    }
    let gateway_policy_name = format!("{cluster_name}-kodo-gateway-ingress");
    if !set.retain_existing_kodo_resources
        && set.kodo_gateway_network_policy.is_none()
        && network_policies
            .get_opt(&gateway_policy_name)
            .await?
            .as_ref()
            .is_some_and(|resource| controlled_by(&resource.metadata, owner_uid.as_deref()))
    {
        network_policies
            .delete(&gateway_policy_name, &DeleteParams::default())
            .await?;
    }
    Ok(())
}

pub(super) async fn runtime_resources(
    client: &kube::Client,
    namespace: &str,
    set: &ResourceSet,
) -> anyhow::Result<()> {
    let params = PatchParams::apply(MANAGER).force();
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

    let network_policies = Api::<NetworkPolicy>::namespaced(client.clone(), namespace);
    network_policies
        .patch(
            &set.network_policy.name_any(),
            &params,
            &Patch::Apply(&set.network_policy),
        )
        .await?;
    if let Some(policy) = &set.kodo_gateway_network_policy {
        network_policies
            .patch(&policy.name_any(), &params, &Patch::Apply(policy))
            .await?;
    }

    let services = Api::<Service>::namespaced(client.clone(), namespace);
    for service in [
        Some(&set.broker_service),
        Some(&set.discovery_service),
        Some(&set.proxy_service),
        set.kodo_gateway_service.as_ref(),
        set.kodo_gateway_headless_service.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        services
            .patch(&service.name_any(), &params, &Patch::Apply(service))
            .await?;
    }

    Api::<Deployment>::namespaced(client.clone(), namespace)
        .patch(
            &set.discovery.name_any(),
            &params,
            &Patch::Apply(&set.discovery),
        )
        .await?;
    Api::<DaemonSet>::namespaced(client.clone(), namespace)
        .patch(&set.proxy.name_any(), &params, &Patch::Apply(&set.proxy))
        .await?;
    if let Some(gateway) = &set.kodo_gateway {
        Api::<StatefulSet>::namespaced(client.clone(), namespace)
            .patch(&gateway.name_any(), &params, &Patch::Apply(gateway))
            .await?;
    }

    let pdbs = Api::<PodDisruptionBudget>::namespaced(client.clone(), namespace);
    pdbs.patch(
        &set.discovery_pdb.name_any(),
        &params,
        &Patch::Apply(&set.discovery_pdb),
    )
    .await?;
    if let Some(pdb) = &set.kodo_gateway_pdb {
        pdbs.patch(&pdb.name_any(), &params, &Patch::Apply(pdb))
            .await?;
    }
    Ok(())
}

fn controlled_by(metadata: &ObjectMeta, owner_uid: Option<&str>) -> bool {
    owner_uid.is_some_and(|owner_uid| {
        metadata.owner_references.as_deref().is_some_and(|owners| {
            owners
                .iter()
                .any(|owner| owner.controller == Some(true) && owner.uid == owner_uid)
        })
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    #[test]
    fn optional_resource_cleanup_requires_the_cluster_owner() {
        let metadata = ObjectMeta {
            owner_references: Some(vec![OwnerReference {
                api_version: "rustqueue.io/v1alpha1".into(),
                kind: "RustQueue".into(),
                name: "queue".into(),
                uid: "queue-uid".into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        };
        assert!(controlled_by(&metadata, Some("queue-uid")));
        assert!(!controlled_by(&metadata, Some("other-uid")));
        assert!(!controlled_by(&metadata, None));
    }
}
