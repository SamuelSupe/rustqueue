use super::reconcile_state::{
    allocated_capacity, blocked_status, build_status, current_placements, pod_states,
    waiting_upgrade,
};
use super::{kube_resources, nodes, rollout, security, storage, Context, ReconcileError};
use crate::broker_config::{self, ConfigInput};
use crate::crd::RustQueueCluster;
use crate::layout::{self, BrokerPlan, ClusterLayout};
use crate::placement;
use crate::resources::{
    self, ANNOTATION_CONFIG_REVISION, ANNOTATION_ROLLOUT_REVISION, ANNOTATION_TARGET_NODE,
    ANNOTATION_TLS_REVISION, LABEL_CLUSTER,
};
use crate::upgrade::RolloutTarget;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Pod, Service, ServiceAccount};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub async fn reconcile(
    cluster: Arc<RustQueueCluster>,
    context: Arc<Context>,
) -> Result<Action, ReconcileError> {
    reconcile_inner(cluster, context).await.map_err(Into::into)
}

async fn reconcile_inner(
    cluster: Arc<RustQueueCluster>,
    context: Arc<Context>,
) -> anyhow::Result<Action> {
    let name = cluster.name_any();
    let generation = cluster.metadata.generation;
    if let Some(owner) = singleton_owner(context.client.clone(), &cluster).await? {
        let status = blocked_status(
            &cluster,
            generation,
            "SingletonReady",
            "MultipleClusters",
            &format!("only one RustQueueCluster is allowed per Kubernetes cluster; active owner is {owner}"),
        );
        persist_status(&context, &cluster, &name, &status).await?;
        context.health.record_success();
        return Ok(Action::requeue(Duration::from_secs(60)));
    }
    let cluster = match storage::resolve_default(context.client.clone(), &cluster).await {
        Ok(cluster) => Arc::new(cluster),
        Err(error) => {
            let status = blocked_status(
                &cluster,
                generation,
                "StorageReady",
                "StorageClassUnresolved",
                &error.to_string(),
            );
            persist_status(&context, &cluster, &name, &status).await?;
            context.health.record_success();
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };
    if let Err(message) = cluster.spec.validate() {
        let status = blocked_status(
            &cluster,
            generation,
            "SpecValid",
            "ValidationFailed",
            &message,
        );
        persist_status(&context, &cluster, &name, &status).await?;
        context.health.record_success();
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    let eligible = nodes::eligible(context.client.clone(), &cluster).await?;
    let allocated = allocated_capacity(&cluster, eligible.len() as u16)?;
    let layout = layout::plan(&name, allocated, &cluster.spec.cells);
    let stateful_sets_api =
        Api::<StatefulSet>::namespaced(context.client.clone(), &context.namespace);
    let pods_api = Api::<Pod>::namespaced(context.client.clone(), &context.namespace);
    let selector = format!("{LABEL_CLUSTER}={name}");
    let stateful_sets = stateful_sets_api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    let pods = pods_api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    let current = current_placements(&stateful_sets, &pods);
    let placements = placement::assign(
        &layout,
        &eligible,
        &current,
        cluster.spec.development.allow_single_node,
    );

    ensure_shared_resources(&context, &cluster, &layout).await?;
    let security = security::ensure(
        context.client.clone(),
        &context.namespace,
        &cluster,
        &layout,
    )
    .await?;
    let desired = ensure_brokers(
        &context,
        &cluster,
        &layout,
        &placements,
        &security.tls_revisions,
        &stateful_sets,
    )
    .await?;
    let pod_states = pod_states(&pods);
    let rollout_target = RolloutTarget {
        image: &cluster.spec.image,
        tls_revisions: &security.tls_revisions,
        config_revisions: &desired.config_revisions,
        target_nodes: &desired.target_nodes,
        rollout_revision: cluster.spec.upgrade.retry_generation,
    };
    let placements_complete = placements.len() == usize::from(layout.active_replicas());
    let mut decision = if placements_complete && desired.configured == layout.active_replicas() {
        rollout::decide(&cluster, &layout, &pod_states, &rollout_target)
    } else {
        rollout::Decision::Settled(Some(waiting_upgrade(
            &cluster,
            "waiting for an eligible node assignment for every Broker",
        )))
    };
    if let rollout::Decision::Release { status, broker } = decision {
        let released = rollout::release(&context, &cluster, &broker, &security.admin_token).await?;
        tracing::info!(
            node_id = broker.node_id,
            "replacement Broker left maintenance"
        );
        let _ = status;
        decision = rollout::Decision::Settled(Some(released));
    }

    let upgrade_status = match &decision {
        rollout::Decision::Settled(status) => status.clone(),
        rollout::Decision::Delete { status, .. } => Some(status.clone()),
        rollout::Decision::Release { .. } => unreachable!("release handled above"),
    };
    let status = build_status(
        &cluster,
        generation,
        allocated,
        &layout,
        &placements,
        desired.configured,
        security.ca_revision,
        &pod_states,
        upgrade_status,
        &rollout_target,
    );
    persist_status(&context, &cluster, &name, &status).await?;

    if let rollout::Decision::Delete {
        broker,
        pod_name,
        needs_maintenance,
        ..
    } = decision
    {
        if needs_maintenance {
            rollout::prepare_delete(&context, &broker, &security.admin_token).await?;
        }
        kube_resources::delete_pod(context.client.clone(), &context.namespace, &pod_name).await?;
        tracing::info!(node_id = broker.node_id, %pod_name, "deleted one Broker Pod for safe rollout");
    }
    context.health.record_success();
    Ok(Action::requeue(Duration::from_secs(5)))
}

async fn persist_status(
    context: &Context,
    cluster: &RustQueueCluster,
    name: &str,
    status: &crate::crd::RustQueueClusterStatus,
) -> anyhow::Result<()> {
    if cluster.status.as_ref().is_some_and(|current| {
        serde_json::to_value(current).ok() == serde_json::to_value(status).ok()
    }) {
        return Ok(());
    }
    kube_resources::patch_status(context.client.clone(), &context.namespace, name, status).await?;
    Ok(())
}

async fn singleton_owner(
    client: kube::Client,
    current: &RustQueueCluster,
) -> anyhow::Result<Option<String>> {
    let mut clusters = Api::<RustQueueCluster>::all(client)
        .list(&ListParams::default())
        .await?
        .items;
    clusters.sort_by_key(|cluster| {
        (
            cluster
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|time| time.0),
            cluster.namespace().unwrap_or_default(),
            cluster.name_any(),
        )
    });
    let Some(owner) = clusters.first() else {
        return Ok(None);
    };
    if owner.metadata.uid == current.metadata.uid {
        Ok(None)
    } else {
        Ok(Some(format!(
            "{}/{}",
            owner.namespace().unwrap_or_default(),
            owner.name_any()
        )))
    }
}

struct DesiredBrokers {
    configured: u16,
    config_revisions: BTreeMap<u64, u64>,
    target_nodes: BTreeMap<u64, String>,
}

async fn ensure_shared_resources(
    context: &Context,
    cluster: &RustQueueCluster,
    layout: &ClusterLayout,
) -> anyhow::Result<()> {
    let service_accounts =
        Api::<ServiceAccount>::namespaced(context.client.clone(), &context.namespace);
    let services = Api::<Service>::namespaced(context.client.clone(), &context.namespace);
    let budgets =
        Api::<PodDisruptionBudget>::namespaced(context.client.clone(), &context.namespace);
    kube_resources::apply(
        &service_accounts,
        &resources::service_account(cluster, &context.namespace)?,
    )
    .await?;
    kube_resources::apply(
        &services,
        &resources::client_service(cluster, &context.namespace)?,
    )
    .await?;
    for cell in &layout.cells {
        kube_resources::apply(
            &services,
            &resources::headless_service(cluster, &context.namespace, cell)?,
        )
        .await?;
        kube_resources::apply(
            &budgets,
            &resources::disruption_budget(cluster, &context.namespace, cell)?,
        )
        .await?;
    }
    Ok(())
}

async fn ensure_brokers(
    context: &Context,
    cluster: &RustQueueCluster,
    layout: &ClusterLayout,
    placements: &BTreeMap<u64, placement::BrokerPlacement>,
    tls_revisions: &BTreeMap<u64, u64>,
    existing_sets: &[StatefulSet],
) -> anyhow::Result<DesiredBrokers> {
    let config_maps = Api::<ConfigMap>::namespaced(context.client.clone(), &context.namespace);
    let stateful_sets = Api::<StatefulSet>::namespaced(context.client.clone(), &context.namespace);
    let existing_maps = config_maps
        .list(&ListParams::default().labels(&format!("{LABEL_CLUSTER}={}", cluster.name_any())))
        .await?
        .items
        .into_iter()
        .map(|map| (map.name_any(), map))
        .collect::<BTreeMap<_, _>>();
    let existing_sets = existing_sets
        .iter()
        .map(|set| (set.name_any(), set))
        .collect::<BTreeMap<_, _>>();
    let failure_domains = placements
        .iter()
        .map(|(node_id, placement)| (*node_id, placement.failure_domain.clone()))
        .collect::<BTreeMap<_, _>>();
    let spec_bytes = serde_json::to_vec(&cluster.spec)?;
    let mut desired = DesiredBrokers {
        configured: 0,
        config_revisions: BTreeMap::new(),
        target_nodes: BTreeMap::new(),
    };
    for broker in layout.brokers() {
        let Some(placement) = placements.get(&broker.node_id) else {
            ensure_pending_map(&config_maps, cluster, context, broker, &existing_maps).await?;
            continue;
        };
        if !visible_brokers_placed(layout, broker, placements) {
            ensure_pending_map(&config_maps, cluster, context, broker, &existing_maps).await?;
            continue;
        }
        let contents = broker_config::render(&ConfigInput {
            cluster_name: &cluster.name_any(),
            namespace: &context.namespace,
            spec: &cluster.spec,
            layout,
            broker,
            failure_domains: &failure_domains,
        })?;
        let revision = crate::pki::revision(&[contents.as_bytes(), &spec_bytes]);
        let config_map = resources::configured_config_map(
            cluster,
            &context.namespace,
            broker,
            &placement.node_name,
            &contents,
        )?;
        if existing_maps
            .get(&broker.config_map)
            .is_none_or(|existing| existing.data != config_map.data)
        {
            kube_resources::apply(&config_maps, &config_map).await?;
        }
        let set = resources::stateful_set(
            cluster,
            &context.namespace,
            broker,
            tls_revisions[&broker.node_id],
            revision,
            &placement.node_name,
        )?;
        if existing_sets
            .get(&broker.stateful_set)
            .is_none_or(|existing| workload_changed(existing, &set))
        {
            kube_resources::apply(&stateful_sets, &set).await?;
        }
        desired.configured += 1;
        desired.config_revisions.insert(broker.node_id, revision);
        desired
            .target_nodes
            .insert(broker.node_id, placement.node_name.clone());
    }
    Ok(desired)
}

async fn ensure_pending_map(
    api: &Api<ConfigMap>,
    cluster: &RustQueueCluster,
    context: &Context,
    broker: &BrokerPlan,
    existing: &BTreeMap<String, ConfigMap>,
) -> anyhow::Result<()> {
    if !existing.contains_key(&broker.config_map) {
        kube_resources::apply(
            api,
            &resources::pending_config_map(cluster, &context.namespace, broker)?,
        )
        .await?;
    }
    Ok(())
}

fn visible_brokers_placed(
    layout: &ClusterLayout,
    broker: &BrokerPlan,
    placements: &BTreeMap<u64, placement::BrokerPlacement>,
) -> bool {
    let root_ready = layout
        .brokers()
        .take(3)
        .all(|root| placements.contains_key(&root.node_id));
    let cell_ready = layout
        .cells
        .iter()
        .find(|cell| cell.id == broker.cell_id)
        .is_some_and(|cell| {
            cell.brokers
                .iter()
                .all(|peer| placements.contains_key(&peer.node_id))
        });
    root_ready && cell_ready
}

fn workload_changed(existing: &StatefulSet, desired: &StatefulSet) -> bool {
    let existing_template = existing.spec.as_ref().map(|spec| &spec.template);
    let desired_template = desired.spec.as_ref().map(|spec| &spec.template);
    [
        ANNOTATION_TLS_REVISION,
        ANNOTATION_CONFIG_REVISION,
        ANNOTATION_TARGET_NODE,
        ANNOTATION_ROLLOUT_REVISION,
    ]
    .iter()
    .any(|key| {
        template_annotation(existing_template, key) != template_annotation(desired_template, key)
    }) || pod_image(existing_template) != pod_image(desired_template)
}

fn template_annotation<'a>(
    template: Option<&'a k8s_openapi::api::core::v1::PodTemplateSpec>,
    key: &str,
) -> Option<&'a str> {
    template?
        .metadata
        .as_ref()?
        .annotations
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

fn pod_image(template: Option<&k8s_openapi::api::core::v1::PodTemplateSpec>) -> Option<&str> {
    template?
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|container| container.name == "rustqueue")?
        .image
        .as_deref()
}
