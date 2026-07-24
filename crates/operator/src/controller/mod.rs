mod apply;
mod auth;
mod drain;
mod kodo_cutover;
mod leadership;
mod nodes;
mod operations;
mod preflight;
mod status;
mod storage;

use crate::resources::{self, BuildInput};
use crate::RustQueue;
use anyhow::{bail, Context as _};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Service;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use status::{OperationUpdate, StatusBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const KODO_BOOTSTRAP_RETENTION_SECONDS: u64 = 180;

pub(super) struct ContextData {
    pub client: Client,
    pub http: reqwest::Client,
    pub leader: Arc<AtomicBool>,
    pub leadership: watch::Receiver<bool>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ReconcileError(#[from] anyhow::Error);

pub async fn run(leader: Arc<AtomicBool>) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let namespace = watch_namespace();
    let (mut leadership, leadership_updates) =
        leadership::start(client.clone(), namespace.clone(), Arc::clone(&leader));
    let context = Arc::new(ContextData {
        client: client.clone(),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        leader,
        leadership: leadership_updates,
    });
    let clusters = Api::<RustQueue>::namespaced(client.clone(), &namespace);
    let stateful_sets = Api::<StatefulSet>::namespaced(client.clone(), &namespace);
    let deployments = Api::<Deployment>::namespaced(client.clone(), &namespace);
    let services = Api::<Service>::namespaced(client, &namespace);
    tracing::info!(%namespace, "share-nothing RustQueue Operator started");
    let controller = Controller::new(clusters, watcher::Config::default())
        .owns(stateful_sets, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(services, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((reference, _)) => tracing::debug!(object = %reference.name, "reconciled"),
                Err(error) => tracing::warn!(%error, "controller stream error"),
            }
        });
    tokio::select! {
        result = &mut leadership => {
            bail!("operator leader election task stopped unexpectedly: {result:?}")
        }
        _ = controller => {
            leadership.abort();
            bail!("operator controller stream stopped unexpectedly")
        }
    }
}

async fn reconcile(
    cluster: Arc<RustQueue>,
    context: Arc<ContextData>,
) -> Result<Action, ReconcileError> {
    if !context.leader.load(Ordering::Acquire) {
        return Ok(Action::requeue(Duration::from_secs(2)));
    }
    let result = tokio::select! {
        result = reconcile_inner(Arc::clone(&cluster), Arc::clone(&context)) => result,
        _ = wait_for_leadership_loss(context.leadership.clone()) => {
            return Ok(Action::requeue(Duration::from_secs(2)));
        }
    };
    if !context.leader.load(Ordering::Acquire) {
        return Ok(Action::requeue(Duration::from_secs(2)));
    }
    match result {
        Ok(action) => Ok(action),
        Err(error) => {
            if context.leader.load(Ordering::Acquire) {
                record_reconcile_error(&context, &cluster, &error).await;
            }
            Err(error.into())
        }
    }
}

async fn wait_for_leadership_loss(mut leadership: watch::Receiver<bool>) {
    if !*leadership.borrow() {
        return;
    }
    while leadership.changed().await.is_ok() {
        if !*leadership.borrow() {
            return;
        }
    }
}

async fn reconcile_inner(
    cluster: Arc<RustQueue>,
    context: Arc<ContextData>,
) -> anyhow::Result<Action> {
    let active_feature_floor = previous_feature_level(&cluster);
    validate(&cluster, active_feature_floor)?;
    let mut cluster = with_storage_feature_floor(cluster, active_feature_floor);
    let namespace = cluster
        .namespace()
        .context("RustQueue must be namespaced")?;
    let effective_image = cluster
        .spec
        .rollout
        .rollback_to_image
        .as_deref()
        .unwrap_or(&cluster.spec.image);
    let effective_image = effective_image.to_owned();
    let statefulsets: Api<StatefulSet> = Api::namespaced(context.client.clone(), &namespace);
    let current_set = statefulsets.get_opt(&cluster.name_any()).await?;
    let current = current_set
        .as_ref()
        .and_then(|set| set.spec.as_ref())
        .and_then(|spec| spec.replicas)
        .unwrap_or(0);
    let current_kodo_gateway = statefulsets
        .get_opt(&format!("{}-kodo-gateway", cluster.name_any()))
        .await?;
    let current_gateways_active = statefulset_has_replicas(current_kodo_gateway.as_ref());
    let decommission_blocked = kodo_decommission_blocked(
        cluster.spec.kodo_compatibility.enabled,
        cluster.spec.kodo_compatibility.decommission_confirmed,
        current_gateways_active,
    );
    let auth = auth::ensure(&context.client, &cluster, &namespace).await?;
    let eligible = nodes::eligible(&context.client, &cluster.spec.eligible_node_selector).await?;
    let ordinary_desired =
        ordinary_desired_brokers(eligible, cluster.spec.min_brokers, cluster.spec.max_brokers);
    let desired = if cluster.spec.kodo_compatibility.enabled {
        3
    } else if current_gateways_active && !cluster.spec.kodo_compatibility.decommission_confirmed {
        ordinary_desired.max(3)
    } else {
        ordinary_desired
    };
    let current_discovery = Api::<Deployment>::namespaced(context.client.clone(), &namespace)
        .get_opt(&format!("{}-discovery", cluster.name_any()))
        .await?;
    let current_discovery_service = Api::<Service>::namespaced(context.client.clone(), &namespace)
        .get_opt(&format!("{}-discovery", cluster.name_any()))
        .await?;
    if decommission_blocked {
        let active_image = statefulset_broker_image(current_set.as_ref())
            .context("current Broker StatefulSet has no broker image")?;
        recover_runtime_resources(
            &context,
            &cluster,
            &namespace,
            &auth,
            active_image,
            current_set.as_ref(),
            current_discovery.as_ref(),
            current_discovery_service.as_ref(),
            current_gateways_active,
        )
        .await?;
        let message = "Kodo Gateway decommission is blocked: stop every Kodo workload using this \
                       Discovery service, then set \
                       spec.kodoCompatibility.decommissionConfirmed=true";
        let ready = nodes::ready_brokers(&context.client, &namespace, &cluster.name_any()).await?;
        let status = StatusBuilder::new(
            &cluster,
            desired,
            ready.min(current),
            previous_feature_level(&cluster),
        )
        .summary("KodoDecommissionBlocked", message)
        .condition("Ready", false, "KodoDecommissionBlocked", message)
        .condition("Progressing", false, "KodoDecommissionBlocked", message)
        .condition("Degraded", true, "KodoDecommissionBlocked", message)
        .condition(
            "KodoGatewaysActive",
            true,
            "RetainedForSafety",
            "Kodo Gateways and their existing Discovery route remain active",
        )
        .condition(
            "KodoDecommissionConfirmed",
            false,
            "ConfirmationRequired",
            message,
        )
        .build();
        apply::status(&context.client, &cluster, status).await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let target_preflight =
        preflight::target_image(&context, &cluster, &namespace, &effective_image).await?;
    if !matches!(target_preflight, preflight::Outcome::Ready { .. }) && current > 0 {
        let active_image = statefulset_broker_image(current_set.as_ref())
            .context("current Broker StatefulSet has no broker image")?;
        recover_runtime_resources(
            &context,
            &cluster,
            &namespace,
            &auth,
            active_image,
            current_set.as_ref(),
            current_discovery.as_ref(),
            current_discovery_service.as_ref(),
            current_gateways_active,
        )
        .await?;
    }
    if preflight_status(
        &context,
        &cluster,
        desired,
        current,
        &effective_image,
        &target_preflight,
    )
    .await?
    {
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let mut active_feature_level = previous_feature_level(&cluster);
    if current > 0 {
        let broker_preflight =
            preflight::current_brokers(&context, &cluster, &namespace, &auth).await?;
        if let preflight::Outcome::Ready {
            active_feature_level: active,
        } = &broker_preflight
        {
            active_feature_level = *active;
            cluster = with_storage_feature_floor(cluster, active_feature_level);
            validate(&cluster, active_feature_level)?;
        } else {
            let active_image = statefulset_broker_image(current_set.as_ref())
                .context("current Broker StatefulSet has no broker image")?;
            recover_runtime_resources(
                &context,
                &cluster,
                &namespace,
                &auth,
                active_image,
                current_set.as_ref(),
                current_discovery.as_ref(),
                current_discovery_service.as_ref(),
                current_gateways_active,
            )
            .await?;
            if preflight_status(
                &context,
                &cluster,
                desired,
                current,
                &effective_image,
                &broker_preflight,
            )
            .await?
            {
                return Ok(Action::requeue(Duration::from_secs(5)));
            }
        }
    }
    preflight::cleanup_old_probes(&context, &cluster, &namespace, &effective_image).await?;

    let storage = storage::reconcile(&context.client, &cluster, &namespace, desired).await?;
    if storage.state != storage::StorageState::Ready {
        if current > 0 {
            let active_image = statefulset_broker_image(current_set.as_ref())
                .context("current Broker StatefulSet has no broker image")?;
            recover_runtime_resources(
                &context,
                &cluster,
                &namespace,
                &auth,
                active_image,
                current_set.as_ref(),
                current_discovery.as_ref(),
                current_discovery_service.as_ref(),
                current_gateways_active,
            )
            .await?;
        }
        let ready = nodes::ready_brokers(&context.client, &namespace, &cluster.name_any()).await?;
        let blocked = storage.state == storage::StorageState::Blocked;
        let phase = if blocked {
            "StorageBlocked"
        } else {
            "StorageResizing"
        };
        let status =
            StatusBuilder::new(&cluster, desired, ready.min(current), active_feature_level)
                .summary(phase, &storage.message)
                .condition("Ready", false, phase, &storage.message)
                .condition("Progressing", !blocked, phase, &storage.message)
                .condition("Degraded", blocked, phase, &storage.message)
                .condition("StorageReady", false, phase, &storage.message)
                .condition(
                    "Upgradeable",
                    true,
                    "PreflightPassed",
                    "binary compatibility checks passed",
                )
                .orphaned_pvcs(storage.orphaned_pvcs)
                .build();
        apply::status(&context.client, &cluster, status).await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let applied_replicas = if current == 0 || desired > current {
        desired
    } else {
        current
    };
    let mounted_secret_revision =
        auth::mounted_secret_revision(&context.client, &cluster, &namespace, &auth).await?;
    let claim_template_size =
        storage::claim_template_size(current_set.as_ref(), &cluster.spec.storage_size);
    let staged = resources::build(BuildInput {
        cluster: &cluster,
        replicas: applied_replicas,
        kodo_gateway_replicas: 0,
        advertise_kodo_gateways: false,
        discovery_service_kodo: Some(false),
        activate_kodo_cleanup: false,
        retain_existing_kodo_resources: !cluster.spec.kodo_compatibility.enabled
            && current_gateways_active,
        image: &effective_image,
        claim_template_size: &claim_template_size,
        secret_name: &auth.name,
        mounted_secret_revision: &mounted_secret_revision,
    })?;
    let target_brokers_ready =
        drain::brokers_current_and_ready(&context, &cluster, &namespace, &staged.revision, desired)
            .await?;
    let ready_brokers =
        nodes::ready_brokers(&context.client, &namespace, &cluster.name_any()).await?;
    let brokers_available_for_kodo = cluster.spec.kodo_compatibility.enabled
        && kodo_gateway_activation_ready(current, ready_brokers);
    let kodo_gateways_active = cluster.spec.kodo_compatibility.enabled
        && (current_gateways_active || brokers_available_for_kodo);
    let current_gateways_ready = if current_gateways_active {
        let updated = nodes::ready_component(
            &context.client,
            &namespace,
            &cluster.name_any(),
            "kodo-gateway",
        )
        .await?;
        let configured = nodes::ready_component_revision(
            &context.client,
            &namespace,
            &cluster.name_any(),
            "kodo-gateway",
            &staged.revision,
        )
        .await?;
        complete_kodo_gateway_set(updated, configured)
    } else {
        false
    };
    let current_discovery_kodo = deployment_requests_kodo_mode(current_discovery.as_ref());
    let current_discovery_route = discovery_service_kodo_route(current_discovery_service.as_ref());
    let direct_inventory_safe =
        cluster.spec.kodo_compatibility.decommission_confirmed || ordinary_desired >= 3;
    let hold_kodo_for_disable = should_hold_kodo_for_disable(
        cluster.spec.kodo_compatibility.enabled,
        current_gateways_active,
        target_brokers_ready,
        direct_inventory_safe,
    );
    let desired_discovery_kodo = cluster.spec.kodo_compatibility.enabled || hold_kodo_for_disable;
    let (advertise_kodo_gateways, discovery_label_adoption) = discovery_target_mode(
        desired_discovery_kodo,
        current_gateways_ready,
        current_discovery.is_some(),
        current_discovery_kodo,
        current_discovery_route,
        deployment_template_has_discovery_mode(current_discovery.as_ref(), current_discovery_kodo),
        deployment_is_fully_ready(current_discovery.as_ref()),
    );
    let ready_target_discovery = nodes::ready_discovery_mode(
        &context.client,
        &namespace,
        &cluster.name_any(),
        advertise_kodo_gateways,
    )
    .await?;
    let ready_fallback_discovery = nodes::ready_discovery_mode(
        &context.client,
        &namespace,
        &cluster.name_any(),
        !advertise_kodo_gateways,
    )
    .await?;
    let discovery_replicas = cluster.spec.discovery_replicas.max(2);
    let discovery_service_kodo = discovery_target_route(
        current_discovery.is_some(),
        discovery_label_adoption,
        current_discovery_route,
        current_discovery_kodo,
        advertise_kodo_gateways,
        (ready_target_discovery, ready_fallback_discovery),
        discovery_replicas,
    );
    let kodo_gateways_advertised = cluster.spec.kodo_compatibility.enabled
        && advertise_kodo_gateways
        && discovery_service_kodo == Some(true)
        && ready_target_discovery >= discovery_replicas;
    let direct_brokers_advertised = !cluster.spec.kodo_compatibility.enabled
        && !advertise_kodo_gateways
        && current_discovery_route == Some(false)
        && deployment_runs_without_kodo(current_discovery.as_ref())
        && ready_target_discovery >= discovery_replicas;
    let kodo_cutover_elapsed = condition_true_for(
        &cluster,
        "KodoGatewaysAdvertised",
        cluster.spec.kodo_compatibility.cutover_grace_seconds,
    );
    let producer_restart = kodo_cutover::producer_restart_fence(
        cluster.spec.kodo_compatibility.enabled,
        kodo_gateways_advertised,
        &cluster.spec.kodo_compatibility.producer_restart_nonce,
        cluster
            .status
            .as_ref()
            .and_then(|status| status.kodo_producer_restart_baseline_nonce.as_deref()),
    );
    let direct_cutover_elapsed = condition_true_for(
        &cluster,
        "KodoDirectBrokersAdvertised",
        cluster.spec.kodo_compatibility.cutover_grace_seconds,
    );
    let retain_existing_kodo_resources = !cluster.spec.kodo_compatibility.enabled
        && current_gateways_active
        && (!direct_inventory_safe
            || !direct_brokers_advertised
            || (!cluster.spec.kodo_compatibility.decommission_confirmed
                && !direct_cutover_elapsed));
    let activate_kodo_cleanup = cluster.spec.kodo_compatibility.enabled
        && cluster.spec.kodo_compatibility.effective_cleanup_enabled()
        && target_brokers_ready;
    let kodo_cleanup_advertised = cluster.spec.kodo_compatibility.enabled
        && cluster.spec.kodo_compatibility.effective_cleanup_enabled()
        && deployment_advertises_kodo_cleanup(current_discovery.as_ref(), &staged.revision);
    let set = resources::build(BuildInput {
        cluster: &cluster,
        replicas: applied_replicas,
        kodo_gateway_replicas: if kodo_gateways_active || retain_existing_kodo_resources {
            3
        } else {
            0
        },
        advertise_kodo_gateways,
        discovery_service_kodo,
        activate_kodo_cleanup,
        retain_existing_kodo_resources,
        image: &effective_image,
        claim_template_size: &claim_template_size,
        secret_name: &auth.name,
        mounted_secret_revision: &mounted_secret_revision,
    })?;
    let revision = set.revision.clone();
    apply::resources(&context.client, &namespace, set).await?;

    let disruptions_allowed = if cluster.spec.kodo_compatibility.enabled {
        current_gateways_ready
            && kodo_gateways_advertised
            && kodo_cutover_elapsed
            && producer_restart.confirmed
    } else if current_gateways_active && direct_brokers_advertised {
        cluster.spec.kodo_compatibility.decommission_confirmed || direct_cutover_elapsed
    } else {
        true
    };
    let (mut phase, mut message, operation) = operations::reconcile(
        &context,
        &cluster,
        &namespace,
        &auth,
        eligible,
        desired,
        current,
        &revision,
        &effective_image,
        disruptions_allowed,
    )
    .await?;
    let broker_health =
        nodes::broker_health(&context.client, &namespace, &cluster.name_any()).await?;
    let ready = broker_health.ready;
    let ready_kodo_gateways = if kodo_gateways_active {
        nodes::ready_component(
            &context.client,
            &namespace,
            &cluster.name_any(),
            "kodo-gateway",
        )
        .await?
    } else {
        0
    };
    let kodo_gateways_ready = !cluster.spec.kodo_compatibility.enabled
        || (kodo_gateways_active && ready_kodo_gateways == 3);
    if phase == "Ready" && !kodo_gateways_ready {
        phase = "WaitingForKodoGateways".into();
        message = format!("{ready_kodo_gateways} of 3 Kodo publish gateways are Ready");
    }
    let kodo_advertisement_ready =
        !cluster.spec.kodo_compatibility.enabled || kodo_gateways_advertised;
    if phase == "Ready" && !kodo_advertisement_ready {
        phase = "WaitingForKodoAdvertisement".into();
        message = "waiting for the replacement Discovery set to become Ready".into();
    }
    let kodo_cutover_ready = !cluster.spec.kodo_compatibility.enabled
        || (current_gateways_ready && kodo_gateways_advertised && kodo_cutover_elapsed);
    if phase == "Ready" && !kodo_cutover_ready {
        phase = "WaitingForKodoCutover".into();
        message = format!(
            "waiting {} seconds for the Gateway Discovery cutover to remain stable",
            cluster.spec.kodo_compatibility.cutover_grace_seconds
        );
    }
    let kodo_producer_restart_ready =
        !cluster.spec.kodo_compatibility.enabled || producer_restart.confirmed;
    let producer_restart_is_only_blocker = cluster.spec.kodo_compatibility.enabled
        && current_gateways_ready
        && kodo_gateways_advertised
        && kodo_cutover_elapsed
        && !producer_restart.confirmed;
    if !kodo_producer_restart_ready
        && (phase == "Ready"
            || (phase == "WaitingForKodoCutover" && producer_restart_is_only_blocker))
    {
        phase = "WaitingForKodoProducerRestart".into();
        message = "restart every Kodo publisher after Gateway advertisement, then change \
                   spec.kodoCompatibility.producerRestartNonce"
            .into();
    }
    let kodo_cleanup_ready = !cluster.spec.kodo_compatibility.enabled
        || !cluster.spec.kodo_compatibility.effective_cleanup_enabled()
        || kodo_cleanup_advertised;
    if phase == "Ready" && !kodo_cleanup_ready {
        phase = "WaitingForKodoCleanup".into();
        message = "waiting for Discovery to advertise cleanup-capable Broker endpoints".into();
    }
    if phase == "Ready" && retain_existing_kodo_resources {
        phase = "DisablingKodo".into();
        message = if !direct_inventory_safe {
            "at least three direct Brokers are required before Kodo Gateways can be removed".into()
        } else if !direct_brokers_advertised {
            "waiting for Discovery to advertise direct Broker addresses".into()
        } else {
            format!(
                "waiting {} seconds before removing the previous Kodo Gateways",
                cluster.spec.kodo_compatibility.cutover_grace_seconds
            )
        };
    }
    let ready_condition = phase == "Ready"
        && ready == desired
        && kodo_gateways_ready
        && kodo_advertisement_ready
        && kodo_cutover_ready
        && kodo_producer_restart_ready
        && kodo_cleanup_ready
        && !retain_existing_kodo_resources;
    let degraded = matches!(
        phase.as_str(),
        "InsufficientNodes" | "MaintenanceBlocked" | "RolloutBlocked" | "RolloutFailed"
    );
    let progressing = !ready_condition && !degraded && phase != "Maintenance";
    let maintenance_enabled = cluster
        .spec
        .maintenance
        .as_ref()
        .is_some_and(|request| request.enabled);
    let mut builder = StatusBuilder::new(&cluster, desired, ready, active_feature_level)
        .summary(&phase, &message)
        .condition("Ready", ready_condition, &phase, &message)
        .condition("Progressing", progressing, &phase, &message)
        .condition("Degraded", degraded, &phase, &message)
        .condition("StorageReady", true, "CapacityReady", &storage.message)
        .condition(
            "Upgradeable",
            true,
            "PreflightPassed",
            "binary compatibility checks passed",
        )
        .condition(
            "Maintenance",
            maintenance_enabled,
            if maintenance_enabled {
                "Requested"
            } else {
                "NotRequested"
            },
            if maintenance_enabled {
                "a Broker is intentionally drained"
            } else {
                "no Broker maintenance is requested"
            },
        )
        .condition(
            "OrphanedPVCs",
            !storage.orphaned_pvcs.is_empty(),
            if storage.orphaned_pvcs.is_empty() {
                "None"
            } else {
                "RetainedAfterScaleDown"
            },
            if storage.orphaned_pvcs.is_empty() {
                "no retained orphan PVCs"
            } else {
                "retained PVCs require an explicit operator decision"
            },
        )
        .condition(
            "BrokersAvailable",
            broker_health.unavailable.is_empty(),
            if broker_health.unavailable.is_empty() {
                "AllReady"
            } else {
                "PodsUnavailable"
            },
            if broker_health.unavailable.is_empty() {
                "all Broker Pods are Ready".into()
            } else {
                broker_health.unavailable.join("; ")
            },
        )
        .condition(
            "KodoGatewaysActive",
            kodo_gateways_active,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if kodo_gateways_active {
                "Active"
            } else {
                "WaitingForBrokers"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled"
            } else if kodo_gateways_active {
                "Kodo publish gateway replicas are activated"
            } else {
                "waiting for two Ready Brokers before starting Kodo Gateways"
            },
        )
        .condition(
            "KodoGatewaysReady",
            cluster.spec.kodo_compatibility.enabled && kodo_gateways_ready,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if kodo_gateways_ready {
                "AllReady"
            } else {
                "PodsUnavailable"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled".into()
            } else {
                format!("{ready_kodo_gateways} of 3 Kodo publish gateways are Ready")
            },
        )
        .condition(
            "KodoGatewaysAdvertised",
            kodo_gateways_advertised,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if kodo_gateways_advertised {
                "Advertised"
            } else {
                "WaitingForGateways"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled"
            } else if kodo_gateways_advertised {
                "Discovery advertises the stable Kodo Gateway addresses"
            } else {
                "Discovery Service has not switched to the stable Kodo Gateway addresses"
            },
        )
        .condition(
            "KodoCutoverReady",
            cluster.spec.kodo_compatibility.enabled && kodo_cutover_ready,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if kodo_cutover_ready {
                "DiscoveryGraceElapsed"
            } else {
                "WaitingForDiscoveryGrace"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled".into()
            } else if kodo_cutover_ready {
                "Kodo Gateway Discovery stability grace has elapsed".into()
            } else {
                format!(
                    "waiting {} seconds after Gateway advertisement",
                    cluster.spec.kodo_compatibility.cutover_grace_seconds
                )
            },
        )
        .condition(
            "KodoProducerRestartConfirmed",
            cluster.spec.kodo_compatibility.enabled && producer_restart.confirmed,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if producer_restart.confirmed {
                "NonceChangedAfterAdvertisement"
            } else if kodo_gateways_advertised {
                "WaitingForRestart"
            } else {
                "WaitingForAdvertisement"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled"
            } else if producer_restart.confirmed {
                "Kodo producer restart was explicitly confirmed after Gateway advertisement"
            } else if kodo_gateways_advertised {
                "restart every Kodo publisher, wait for it to become Ready, then change producerRestartNonce"
            } else {
                "producer restart confirmation is accepted only after Gateway advertisement"
            },
        )
        .condition(
            "KodoDirectBrokersAdvertised",
            direct_brokers_advertised,
            if cluster.spec.kodo_compatibility.enabled {
                "GatewayMode"
            } else if direct_brokers_advertised {
                "Advertised"
            } else {
                "WaitingForSafeInventory"
            },
            if cluster.spec.kodo_compatibility.enabled {
                "Discovery advertises Kodo Gateway addresses"
            } else if direct_brokers_advertised {
                "Discovery advertises direct Broker addresses"
            } else if !direct_inventory_safe {
                "fewer than three direct Brokers would be visible to Kodo"
            } else {
                "Discovery has not completed its direct Broker cutover"
            },
        )
        .condition(
            "KodoCleanupActive",
            cluster.spec.kodo_compatibility.enabled
                && cluster.spec.kodo_compatibility.effective_cleanup_enabled()
                && kodo_cleanup_ready,
            if !cluster.spec.kodo_compatibility.enabled {
                "Disabled"
            } else if !cluster.spec.kodo_compatibility.effective_cleanup_enabled() {
                "NotRequested"
            } else if kodo_cleanup_ready {
                "Advertised"
            } else {
                "WaitingForBrokers"
            },
            if !cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is disabled"
            } else if !cluster.spec.kodo_compatibility.effective_cleanup_enabled() {
                "Kodo automatic cleanup is disabled"
            } else if kodo_cleanup_ready {
                "Discovery advertises authenticated cleanup-capable Broker endpoints"
            } else {
                "cleanup remains inactive until all target Brokers and Discovery are Ready"
            },
        )
        .condition(
            "KodoDecommissionConfirmed",
            !cluster.spec.kodo_compatibility.enabled
                && cluster.spec.kodo_compatibility.decommission_confirmed,
            if cluster.spec.kodo_compatibility.enabled {
                "CompatibilityActive"
            } else if cluster.spec.kodo_compatibility.decommission_confirmed {
                "Confirmed"
            } else {
                "NotRequired"
            },
            if cluster.spec.kodo_compatibility.enabled {
                "Kodo compatibility is active"
            } else if cluster.spec.kodo_compatibility.decommission_confirmed {
                "all Kodo workloads were explicitly confirmed stopped"
            } else {
                "no Kodo Gateway decommission is in progress"
            },
        )
        .orphaned_pvcs(storage.orphaned_pvcs)
        .kodo_producer_restart_baseline_nonce(producer_restart.baseline_nonce);
    if let Some(operation) = &operation {
        operation.audit(&cluster);
        builder = operation.apply(builder);
    }
    apply::status(&context.client, &cluster, builder.build()).await?;
    Ok(Action::requeue(Duration::from_secs(5)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KodoRuntimeRecovery {
    gateway_replicas: i32,
    advertise_gateways: bool,
    discovery_service_kodo: bool,
    retain_gateways: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct KodoRuntimeSignals {
    gateway_active: bool,
    discovery_mode: Option<bool>,
    discovery_route: Option<bool>,
    previously_active: bool,
    previously_advertised: bool,
}

fn kodo_runtime_recovery(
    enabled: bool,
    current_brokers: i32,
    ready_brokers: i32,
    signals: KodoRuntimeSignals,
) -> KodoRuntimeRecovery {
    let gateway_was_active =
        signals.gateway_active || signals.previously_active || signals.previously_advertised;
    let advertise_gateways = signals
        .discovery_mode
        .or(signals.discovery_route)
        .unwrap_or(gateway_was_active);
    let discovery_service_kodo = signals.discovery_route.unwrap_or(advertise_gateways);
    let gateway_was_active = gateway_was_active || advertise_gateways || discovery_service_kodo;
    let gateway_replicas = if gateway_was_active
        || (enabled && kodo_gateway_activation_ready(current_brokers, ready_brokers))
    {
        3
    } else {
        0
    };
    KodoRuntimeRecovery {
        gateway_replicas,
        advertise_gateways,
        discovery_service_kodo,
        retain_gateways: !enabled && gateway_was_active,
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover_runtime_resources(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &auth::AuthSecret,
    image: &str,
    current_set: Option<&StatefulSet>,
    current_discovery: Option<&Deployment>,
    current_discovery_service: Option<&Service>,
    current_gateway_active: bool,
) -> anyhow::Result<()> {
    let resumed = drain::resume_all(context, cluster, namespace, auth).await?;
    if !resumed.is_empty() {
        tracing::warn!(
            brokers = %resumed.join(","),
            "resumed Broker drains while reconciliation is blocked"
        );
    }
    let current_brokers = current_set
        .and_then(|set| set.spec.as_ref())
        .and_then(|spec| spec.replicas)
        .unwrap_or_default();
    let ready_brokers =
        nodes::ready_brokers(&context.client, namespace, &cluster.name_any()).await?;
    let recovery = kodo_runtime_recovery(
        cluster.spec.kodo_compatibility.enabled,
        current_brokers,
        ready_brokers,
        KodoRuntimeSignals {
            gateway_active: current_gateway_active,
            discovery_mode: current_discovery
                .map(|deployment| deployment_requests_kodo_mode(Some(deployment))),
            discovery_route: discovery_service_kodo_route(current_discovery_service),
            previously_active: condition_true(cluster, "KodoGatewaysActive"),
            previously_advertised: condition_true(cluster, "KodoGatewaysAdvertised"),
        },
    );
    let mounted_secret_revision =
        auth::mounted_secret_revision(&context.client, cluster, namespace, auth).await?;
    let claim_template_size = storage::claim_template_size(current_set, &cluster.spec.storage_size);
    let set = resources::build(BuildInput {
        cluster,
        replicas: current_brokers,
        kodo_gateway_replicas: recovery.gateway_replicas,
        advertise_kodo_gateways: recovery.advertise_gateways,
        discovery_service_kodo: Some(recovery.discovery_service_kodo),
        activate_kodo_cleanup: deployment_requests_kodo_cleanup(current_discovery),
        retain_existing_kodo_resources: recovery.retain_gateways,
        image,
        claim_template_size: &claim_template_size,
        secret_name: &auth.name,
        mounted_secret_revision: &mounted_secret_revision,
    })?;
    apply::runtime_resources(&context.client, namespace, &set).await?;
    tracing::warn!(
        ready_brokers,
        "reconciled runtime entrypoints without mutating Brokers"
    );
    Ok(())
}

async fn preflight_status(
    context: &ContextData,
    cluster: &RustQueue,
    desired: i32,
    current: i32,
    target_image: &str,
    outcome: &preflight::Outcome,
) -> anyhow::Result<bool> {
    let (phase, message, blocked) = match outcome {
        preflight::Outcome::Ready { .. } => return Ok(false),
        preflight::Outcome::Pending(message) => ("Preflight", message, false),
        preflight::Outcome::Blocked(message) => ("PreflightBlocked", message, true),
    };
    let ready = nodes::ready_brokers(
        &context.client,
        &cluster.namespace().expect("validated namespace"),
        &cluster.name_any(),
    )
    .await?;
    let kind = if cluster.spec.rollout.rollback_to_image.is_some() {
        "Rollback"
    } else {
        "Rollout"
    };
    let preflight_revision = format!(
        "preflight:{}:{}",
        cluster.spec.storage_feature_level, cluster.spec.rollout.retry_nonce
    );
    let existing = cluster
        .status
        .as_ref()
        .and_then(|status| status.current_operation.as_ref())
        .filter(|operation| operation.kind == kind && operation.target == target_image);
    let revision = existing
        .map(|operation| operation.revision.clone())
        .unwrap_or(preflight_revision);
    let operation_id = existing
        .map(|operation| operation.id.clone())
        .unwrap_or_else(|| {
            status::operation_id(&kind.to_ascii_lowercase(), target_image, &revision)
        });
    let status = StatusBuilder::new(
        cluster,
        desired,
        ready.min(current),
        previous_feature_level(cluster),
    )
    .summary(phase, message)
    .condition("Ready", false, phase, message)
    .condition("Progressing", !blocked, phase, message)
    .condition("Degraded", blocked, phase, message)
    .condition("Upgradeable", false, phase, message)
    .operation(OperationUpdate {
        id: &operation_id,
        kind,
        phase: if blocked { "Blocked" } else { "Preflight" },
        target: target_image,
        revision: &revision,
        message,
        previous_image: existing
            .and_then(|operation| operation.previous_image.clone())
            .or_else(|| {
                cluster
                    .spec
                    .rollout
                    .rollback_to_image
                    .as_ref()
                    .map(|_| cluster.spec.image.clone())
            }),
        current_broker: None,
    })
    .build();
    apply::status(&context.client, cluster, status).await?;
    Ok(true)
}

fn previous_feature_level(cluster: &RustQueue) -> u32 {
    cluster
        .status
        .as_ref()
        .map_or(1, |status| status.active_storage_feature_level.max(1))
}

fn statefulset_has_replicas(statefulset: Option<&StatefulSet>) -> bool {
    statefulset
        .and_then(|set| set.spec.as_ref())
        .and_then(|spec| spec.replicas)
        .is_some_and(|replicas| replicas > 0)
}

fn statefulset_broker_image(statefulset: Option<&StatefulSet>) -> Option<&str> {
    statefulset?
        .spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|container| container.name == "broker")?
        .image
        .as_deref()
}

fn kodo_decommission_blocked(enabled: bool, confirmed: bool, gateways_active: bool) -> bool {
    gateways_active && !enabled && !confirmed
}

fn kodo_gateway_activation_ready(current_brokers: i32, ready_brokers: i32) -> bool {
    current_brokers >= 2 && ready_brokers >= 2
}

fn complete_kodo_gateway_set(updated: i32, configured: i32) -> bool {
    updated == 3 && configured == 3
}

fn condition_true(cluster: &RustQueue, type_: &str) -> bool {
    cluster.status.as_ref().is_some_and(|status| {
        status
            .conditions
            .iter()
            .any(|condition| condition.type_ == type_ && condition.status == "True")
    })
}

fn condition_true_for(cluster: &RustQueue, type_: &str, seconds: u64) -> bool {
    cluster
        .status
        .as_ref()
        .and_then(|status| {
            status
                .conditions
                .iter()
                .find(|condition| condition.type_ == type_ && condition.status == "True")
        })
        .and_then(|condition| status::elapsed_seconds(&condition.last_transition_time))
        .is_some_and(|elapsed| elapsed >= seconds)
}

fn should_hold_kodo_for_disable(
    enabled: bool,
    gateways_active: bool,
    target_brokers_ready: bool,
    direct_inventory_safe: bool,
) -> bool {
    !enabled && gateways_active && (!target_brokers_ready || !direct_inventory_safe)
}

fn deployment_env_value<'a>(deployment: Option<&'a Deployment>, name: &str) -> Option<&'a str> {
    deployment
        .and_then(|deployment| deployment.spec.as_ref())
        .and_then(|spec| spec.template.spec.as_ref())
        .and_then(|spec| {
            spec.containers.iter().find_map(|container| {
                container.env.as_deref().and_then(|environment| {
                    environment
                        .iter()
                        .find(|variable| variable.name == name)
                        .and_then(|variable| variable.value.as_deref())
                })
            })
        })
}

fn deployment_requests_kodo_mode(deployment: Option<&Deployment>) -> bool {
    deployment_env_value(deployment, "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED") == Some("true")
}

fn deployment_requests_kodo_cleanup(deployment: Option<&Deployment>) -> bool {
    deployment_env_value(deployment, "RUSTQUEUE_KODO_CLEANUP_ENABLED") == Some("true")
}

fn deployment_template_has_discovery_mode(deployment: Option<&Deployment>, kodo: bool) -> bool {
    deployment
        .and_then(|deployment| deployment.spec.as_ref())
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.labels.as_ref())
        .and_then(|labels| labels.get(resources::DISCOVERY_MODE_LABEL))
        .is_some_and(|mode| mode == if kodo { "kodo" } else { "direct" })
}

fn discovery_service_kodo_route(service: Option<&Service>) -> Option<bool> {
    match service
        .and_then(|service| service.spec.as_ref())
        .and_then(|spec| spec.selector.as_ref())
        .and_then(|selector| selector.get(resources::DISCOVERY_MODE_LABEL))
        .map(String::as_str)
    {
        Some("kodo") => Some(true),
        Some("direct") => Some(false),
        _ => None,
    }
}

fn discovery_target_mode(
    desired_kodo: bool,
    gateways_ready: bool,
    has_deployment: bool,
    current_kodo: bool,
    current_route: Option<bool>,
    template_mode_matches: bool,
    deployment_fully_ready: bool,
) -> (bool, bool) {
    let adopting_labels = has_deployment
        && current_route.is_none()
        && (!template_mode_matches || !deployment_fully_ready);
    let target_kodo = if adopting_labels {
        current_kodo
    } else {
        desired_kodo && (current_kodo || gateways_ready)
    };
    (target_kodo, adopting_labels)
}

fn discovery_target_route(
    has_deployment: bool,
    adopting_labels: bool,
    current_route: Option<bool>,
    current_kodo: bool,
    target_kodo: bool,
    ready: (i32, i32),
    replicas: i32,
) -> Option<bool> {
    let (target_ready, fallback_ready) = ready;
    if !has_deployment {
        return Some(target_kodo);
    }
    if adopting_labels {
        return None;
    }
    if target_ready >= replicas {
        return Some(target_kodo);
    }
    if current_route == Some(target_kodo) && fallback_ready >= replicas {
        return Some(!target_kodo);
    }
    current_route.or(Some(current_kodo))
}

fn deployment_is_fully_ready(deployment: Option<&Deployment>) -> bool {
    let Some(deployment) = deployment else {
        return false;
    };
    let generation = deployment.metadata.generation.unwrap_or_default();
    let desired = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or_default();
    let ready = deployment
        .status
        .as_ref()
        .and_then(|status| status.ready_replicas)
        .unwrap_or_default();
    let updated = deployment
        .status
        .as_ref()
        .and_then(|status| status.updated_replicas)
        .unwrap_or_default();
    let available = deployment
        .status
        .as_ref()
        .and_then(|status| status.available_replicas)
        .unwrap_or_default();
    let observed = deployment
        .status
        .as_ref()
        .and_then(|status| status.observed_generation)
        .unwrap_or_default();
    desired > 0
        && ready >= desired
        && updated >= desired
        && available >= desired
        && observed >= generation
}

fn deployment_matches_revision(deployment: &Deployment, target_revision: &str) -> bool {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get("rustqueue.io/revision"))
        .is_some_and(|revision| revision == target_revision)
}

fn deployment_advertises_kodo_cleanup(
    deployment: Option<&Deployment>,
    target_revision: &str,
) -> bool {
    let Some(deployment) = deployment else {
        return false;
    };
    deployment_requests_kodo_cleanup(Some(deployment))
        && deployment_matches_revision(deployment, target_revision)
        && deployment_is_fully_ready(Some(deployment))
}

fn deployment_runs_without_kodo(deployment: Option<&Deployment>) -> bool {
    deployment.is_some()
        && !deployment_requests_kodo_mode(deployment)
        && deployment_is_fully_ready(deployment)
}

fn with_storage_feature_floor(
    cluster: Arc<RustQueue>,
    active_feature_floor: u32,
) -> Arc<RustQueue> {
    let effective =
        effective_storage_feature_level(cluster.spec.storage_feature_level, active_feature_floor);
    let (connection_delivery, node_delivery) = effective_delivery_limits(
        effective,
        cluster.spec.max_message_bytes,
        cluster.spec.connection_delivery_inflight_bytes,
        cluster.spec.node_delivery_inflight_bytes,
    );
    if effective == cluster.spec.storage_feature_level
        && connection_delivery == cluster.spec.connection_delivery_inflight_bytes
        && node_delivery == cluster.spec.node_delivery_inflight_bytes
    {
        return cluster;
    }
    let mut adjusted = cluster.as_ref().clone();
    adjusted.spec.storage_feature_level = effective;
    adjusted.spec.connection_delivery_inflight_bytes = connection_delivery;
    adjusted.spec.node_delivery_inflight_bytes = node_delivery;
    Arc::new(adjusted)
}

fn effective_storage_feature_level(requested: u32, active: u32) -> u32 {
    requested.max(active).max(1)
}

fn effective_delivery_limits(
    storage_feature_level: u32,
    max_message_bytes: usize,
    connection: usize,
    node: usize,
) -> (usize, usize) {
    let retained_message_bound = if storage_feature_level >= 2 {
        100 * 1024 * 1024
    } else {
        max_message_bytes
    };
    let connection = connection.max(retained_message_bound);
    (connection, node.max(connection.saturating_mul(2)))
}

async fn record_reconcile_error(context: &ContextData, cluster: &RustQueue, error: &anyhow::Error) {
    if let Some(namespace) = cluster.namespace() {
        match auth::ensure(&context.client, cluster, &namespace).await {
            Ok(auth) => match drain::resume_all(context, cluster, &namespace, &auth).await {
                Ok(resumed) if !resumed.is_empty() => {
                    tracing::warn!(
                        brokers = %resumed.join(","),
                        "resumed Broker drains after reconciliation failure"
                    );
                }
                Ok(_) => {}
                Err(resume_error) => {
                    tracing::warn!(%resume_error, "failed to inspect Broker drains after reconciliation failure");
                }
            },
            Err(auth_error) => {
                tracing::warn!(%auth_error, "could not load Broker credentials for failure recovery");
            }
        }
    }
    let previous = cluster.status.as_ref();
    let desired = previous.map_or(cluster.spec.min_brokers, |status| status.desired_brokers);
    let ready = previous.map_or(0, |status| status.ready_brokers);
    let message = format!("reconciliation failed: {error:#}");
    let status = StatusBuilder::new(cluster, desired, ready, previous_feature_level(cluster))
        .summary("ReconcileError", &message)
        .condition("Ready", false, "ReconcileError", &message)
        .condition("Progressing", false, "ReconcileError", &message)
        .condition("Degraded", true, "ReconcileError", &message)
        .build();
    if let Err(status_error) = apply::status(&context.client, cluster, status).await {
        tracing::warn!(%status_error, "failed to persist reconciliation error status");
    }
}

fn validate(cluster: &RustQueue, active_feature_floor: u32) -> anyhow::Result<()> {
    if cluster.spec.image.trim().is_empty() {
        bail!("spec.image is required");
    }
    if cluster.spec.min_brokers < 1 || cluster.spec.max_brokers < cluster.spec.min_brokers {
        bail!("broker limits must satisfy 1 <= minBrokers <= maxBrokers");
    }
    if cluster.spec.storage_class_name.trim().is_empty()
        || cluster.spec.storage_size.trim().is_empty()
    {
        bail!("storageClassName and storageSize are required");
    }
    if cluster.spec.storage_feature_level == 0 {
        bail!("storageFeatureLevel must be greater than zero");
    }
    if cluster.spec.disk_low_watermark_percent >= cluster.spec.disk_high_watermark_percent
        || cluster.spec.disk_high_watermark_percent > 100
    {
        bail!("disk watermarks must satisfy low < high <= 100");
    }
    if cluster.spec.bootstrap_retention_seconds == 0
        || cluster.spec.max_message_bytes == 0
        || cluster.spec.max_message_bytes > 100 * 1024 * 1024
        || cluster.spec.message_index_cache_bytes == 0
        || cluster.spec.connection_delivery_inflight_bytes < cluster.spec.max_message_bytes
        || cluster
            .spec
            .connection_delivery_inflight_bytes
            .checked_mul(2)
            .is_none_or(|minimum| cluster.spec.node_delivery_inflight_bytes < minimum)
        || cluster.spec.node_delivery_inflight_bytes > u32::MAX as usize
        || cluster.spec.max_topics == 0
        || cluster.spec.max_publish_workers == 0
        || cluster.spec.publish_worker_idle_seconds == 0
        || cluster.spec.max_detailed_metric_series == 0
    {
        bail!("queue limits are outside the stable v7 contract");
    }
    validate_message_storage_contract(
        cluster.spec.max_message_bytes,
        effective_storage_feature_level(cluster.spec.storage_feature_level, active_feature_floor),
    )?;
    if cluster.spec.kodo_compatibility.enabled
        && (cluster.spec.min_brokers != 3
            || cluster.spec.max_brokers != 3
            || cluster.spec.storage_feature_level != 2
            || cluster.spec.bootstrap_retention_seconds < KODO_BOOTSTRAP_RETENTION_SECONDS
            || cluster.spec.max_message_bytes != 100 * 1024 * 1024
            || cluster.spec.connection_delivery_inflight_bytes < 128 * 1024 * 1024
            || cluster.spec.node_delivery_inflight_bytes < 512 * 1024 * 1024)
    {
        bail!(
            "Kodo compatibility requires exactly 3 brokers, storageFeatureLevel 2, \
             bootstrapRetentionSeconds >= 180, \
             maxMessageBytes 104857600, connectionDeliveryInflightBytes >= 134217728, \
             and nodeDeliveryInflightBytes >= 536870912"
        );
    }
    if !(630..=86_400).contains(&cluster.spec.kodo_compatibility.cutover_grace_seconds) {
        bail!("Kodo compatibility cutoverGraceSeconds must be between 630 and 86400");
    }
    if cluster.spec.kodo_compatibility.enabled {
        if cluster.spec.kodo_compatibility.decommission_confirmed {
            bail!("decommissionConfirmed must be false while Kodo compatibility is enabled");
        }
        if cluster.spec.kodo_compatibility.cleanup_enabled {
            bail!(
                "Kodo automatic cleanup is disabled until cluster-wide atomic deletion is available"
            );
        }
        let target_image = cluster
            .spec
            .rollout
            .rollback_to_image
            .as_deref()
            .unwrap_or(&cluster.spec.image);
        if cluster.spec.image_pull_policy != "Never" && !has_sha256_digest(target_image) {
            bail!(
                "Kodo compatibility requires an immutable @sha256 image or imagePullPolicy Never"
            );
        }
        if cluster
            .spec
            .kodo_compatibility
            .allowed_pod_selector
            .is_empty()
            || cluster
                .spec
                .kodo_compatibility
                .allowed_pod_selector
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
            || cluster
                .spec
                .kodo_compatibility
                .allowed_namespace_selector
                .iter()
                .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
        {
            bail!("Kodo compatibility requires a non-empty allowedPodSelector and valid selector labels");
        }
        let memory_request = storage::parse_quantity(&cluster.spec.broker_resources.memory_request)
            .context("parse broker memory request")?;
        if memory_request < 2_u128 << 30 {
            bail!("Kodo compatibility requires brokerResources.memoryRequest >= 2Gi");
        }
        let cpu_request = parse_cpu_millis(&cluster.spec.broker_resources.cpu_request)
            .context("parse broker CPU request")?;
        if cpu_request < 1_000.0 {
            bail!("Kodo compatibility requires brokerResources.cpuRequest >= 1 CPU");
        }
        if let Some(cpu_limit) = cluster.spec.broker_resources.cpu_limit.as_deref() {
            let cpu_limit = parse_cpu_millis(cpu_limit).context("parse broker CPU limit")?;
            if cpu_limit < cpu_request {
                bail!("brokerResources.cpuLimit must be greater than or equal to cpuRequest");
            }
        }
        if let Some(memory_limit) = cluster.spec.broker_resources.memory_limit.as_deref() {
            let memory_limit =
                storage::parse_quantity(memory_limit).context("parse broker memory limit")?;
            if memory_limit < memory_request {
                bail!("brokerResources.memoryLimit must be greater than or equal to memoryRequest");
            }
        }
    }
    if cluster.spec.rollout.timeout_seconds == 0 || cluster.spec.rollout.timeout_seconds > 86_400 {
        bail!("rollout timeoutSeconds must be between 1 and 86400");
    }
    if cluster
        .spec
        .rollout
        .rollback_to_image
        .as_ref()
        .is_some_and(|image| image.trim().is_empty())
    {
        bail!("rollout rollbackToImage cannot be empty");
    }
    if cluster
        .spec
        .broker_scheduling
        .topology_key
        .trim()
        .is_empty()
        || cluster.spec.broker_resources.cpu_request.trim().is_empty()
        || cluster
            .spec
            .broker_resources
            .memory_request
            .trim()
            .is_empty()
    {
        bail!("broker scheduling and resource requests cannot be empty");
    }
    Ok(())
}

fn ordinary_desired_brokers(eligible: usize, minimum: i32, maximum: i32) -> i32 {
    i32::try_from(eligible)
        .unwrap_or(i32::MAX)
        .clamp(minimum, maximum)
}

fn parse_cpu_millis(value: &str) -> anyhow::Result<f64> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix('m') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('u') {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('n') {
        (number, 0.000_001)
    } else {
        (value, 1_000.0)
    };
    let number: f64 = number.parse()?;
    anyhow::ensure!(
        number.is_finite() && number >= 0.0,
        "invalid CPU quantity {value}"
    );
    Ok(number * multiplier)
}

fn validate_message_storage_contract(
    max_message_bytes: usize,
    storage_feature_level: u32,
) -> anyhow::Result<()> {
    const LEGACY_MAX_RECORD_BYTES: usize = 72 * 1024 * 1024;
    const SINGLE_MESSAGE_ENVELOPE_BYTES: usize = 24;
    const MPUB_ENTRY_BYTES: usize = 16;
    const MAX_MPUB_MESSAGES: usize = 65_536;
    let max_body_bytes = (64 * 1024 * 1024).max(max_message_bytes);
    let maximum_record = max_message_bytes
        .saturating_add(SINGLE_MESSAGE_ENVELOPE_BYTES)
        .max(max_body_bytes.saturating_add(MPUB_ENTRY_BYTES.saturating_mul(MAX_MPUB_MESSAGES)));
    if maximum_record > LEGACY_MAX_RECORD_BYTES && storage_feature_level < 2 {
        bail!("messages above the v7 legacy record bound require storageFeatureLevel 2");
    }
    Ok(())
}

fn has_sha256_digest(image: &str) -> bool {
    image.rsplit_once("@sha256:").is_some_and(|(_, digest)| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn error_policy(
    cluster: Arc<RustQueue>,
    error: &ReconcileError,
    _context: Arc<ContextData>,
) -> Action {
    tracing::warn!(cluster = %cluster.name_any(), %error, "reconciliation failed; retrying");
    Action::requeue(Duration::from_secs(10))
}

fn watch_namespace() -> String {
    std::env::var("WATCH_NAMESPACE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace").ok()
        })
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|| "default".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_gateway_replicas_preserve_activation_without_status() {
        let gateway: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "queue-kodo-gateway"},
            "spec": {
                "serviceName": "queue-kodo-gateways",
                "replicas": 3,
                "selector": {"matchLabels": {"app": "gateway"}},
                "template": {
                    "metadata": {"labels": {"app": "gateway"}},
                    "spec": {"containers": []}
                }
            }
        }))
        .unwrap();
        assert!(statefulset_has_replicas(Some(&gateway)));
        assert!(!statefulset_has_replicas(None));
    }

    #[test]
    fn blocked_target_preflight_reuses_the_active_broker_image() {
        let brokers: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "queue"},
            "spec": {
                "serviceName": "queue-brokers",
                "selector": {"matchLabels": {"app": "broker"}},
                "template": {
                    "metadata": {"labels": {"app": "broker"}},
                    "spec": {"containers": [
                        {"name": "sidecar", "image": "sidecar:v1"},
                        {"name": "broker", "image": "rustqueue:active"}
                    ]}
                }
            }
        }))
        .unwrap();
        assert_eq!(
            statefulset_broker_image(Some(&brokers)),
            Some("rustqueue:active")
        );
        assert_eq!(statefulset_broker_image(None), None);
    }

    #[test]
    fn kodo_decommission_requires_confirmation_only_for_a_live_gateway_set() {
        assert!(kodo_decommission_blocked(false, false, true));
        assert!(!kodo_decommission_blocked(false, true, true));
        assert!(!kodo_decommission_blocked(true, false, true));
        assert!(!kodo_decommission_blocked(false, false, false));
    }

    #[test]
    fn kodo_gateways_can_bootstrap_during_one_broker_outage() {
        assert!(kodo_gateway_activation_ready(3, 2));
        assert!(kodo_gateway_activation_ready(2, 2));
        assert!(!kodo_gateway_activation_ready(3, 1));
        assert!(!kodo_gateway_activation_ready(1, 1));
    }

    #[test]
    fn runtime_recovery_recreates_gateways_without_advancing_discovery() {
        assert_eq!(
            kodo_runtime_recovery(
                true,
                3,
                2,
                KodoRuntimeSignals {
                    discovery_mode: Some(false),
                    discovery_route: Some(false),
                    ..Default::default()
                },
            ),
            KodoRuntimeRecovery {
                gateway_replicas: 3,
                advertise_gateways: false,
                discovery_service_kodo: false,
                retain_gateways: false,
            }
        );
    }

    #[test]
    fn runtime_recovery_preserves_an_in_progress_kodo_disable() {
        assert_eq!(
            kodo_runtime_recovery(
                false,
                3,
                2,
                KodoRuntimeSignals {
                    discovery_mode: Some(false),
                    discovery_route: Some(true),
                    previously_active: true,
                    ..Default::default()
                },
            ),
            KodoRuntimeRecovery {
                gateway_replicas: 3,
                advertise_gateways: false,
                discovery_service_kodo: true,
                retain_gateways: true,
            }
        );
    }

    #[test]
    fn runtime_recovery_keeps_live_gateways_advertised_when_discovery_is_missing() {
        assert_eq!(
            kodo_runtime_recovery(
                false,
                3,
                2,
                KodoRuntimeSignals {
                    gateway_active: true,
                    ..Default::default()
                },
            ),
            KodoRuntimeRecovery {
                gateway_replicas: 3,
                advertise_gateways: true,
                discovery_service_kodo: true,
                retain_gateways: true,
            }
        );
    }

    #[test]
    fn runtime_recovery_does_not_resurrect_a_completed_decommission() {
        assert_eq!(
            kodo_runtime_recovery(false, 3, 2, KodoRuntimeSignals::default()),
            KodoRuntimeRecovery {
                gateway_replicas: 0,
                advertise_gateways: false,
                discovery_service_kodo: false,
                retain_gateways: false,
            }
        );
    }

    #[test]
    fn kodo_gateway_readiness_requires_both_revision_fences() {
        assert!(complete_kodo_gateway_set(3, 3));
        assert!(!complete_kodo_gateway_set(2, 3));
        assert!(!complete_kodo_gateway_set(3, 2));
    }

    #[test]
    fn discovery_advertisement_is_recovered_from_the_live_deployment() {
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "queue-discovery", "generation": 4},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": "discovery"}},
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "discovery",
                            "rustqueue.io/discovery-publisher-mode": "kodo"
                        },
                        "annotations": {"rustqueue.io/revision": "current"}
                    },
                    "spec": {
                        "containers": [{
                            "name": "discovery",
                            "image": "rustqueue:test",
                            "env": [{
                                "name": "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED",
                                "value": "true"
                            }, {
                                "name": "RUSTQUEUE_KODO_GATEWAY_ADDRESS",
                                "value": "queue-kodo-publish.test.svc"
                            }, {
                                "name": "RUSTQUEUE_KODO_CLEANUP_ENABLED",
                                "value": "true"
                            }]
                        }]
                    }
                }
            },
            "status": {
                "observedGeneration": 4,
                "readyReplicas": 2,
                "updatedReplicas": 2,
                "availableReplicas": 2
            }
        }))
        .unwrap();
        assert!(deployment_requests_kodo_mode(Some(&deployment)));
        assert!(deployment_requests_kodo_cleanup(Some(&deployment)));
        assert!(deployment_template_has_discovery_mode(
            Some(&deployment),
            true
        ));
        assert!(deployment_advertises_kodo_cleanup(
            Some(&deployment),
            "current"
        ));
        assert!(!deployment_runs_without_kodo(Some(&deployment)));
        let direct_service: Service = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "queue-discovery"},
            "spec": {
                "selector": {
                    "rustqueue.io/discovery-publisher-mode": "direct"
                },
                "ports": [{"port": 4161}]
            }
        }))
        .unwrap();
        assert_eq!(
            discovery_service_kodo_route(Some(&direct_service)),
            Some(false)
        );
        let mut kodo_service = direct_service;
        kodo_service
            .spec
            .as_mut()
            .unwrap()
            .selector
            .as_mut()
            .unwrap()
            .insert(resources::DISCOVERY_MODE_LABEL.into(), "kodo".into());
        assert_eq!(
            discovery_service_kodo_route(Some(&kodo_service)),
            Some(true)
        );
        assert_eq!(discovery_service_kodo_route(None), None);

        let mut unready = deployment.clone();
        unready.status.as_mut().unwrap().ready_replicas = Some(1);
        assert!(!deployment_advertises_kodo_cleanup(
            Some(&unready),
            "current"
        ));

        let mut disabled = deployment.clone();
        disabled
            .spec
            .as_mut()
            .unwrap()
            .template
            .spec
            .as_mut()
            .unwrap()
            .containers[0]
            .env
            .as_mut()
            .unwrap()
            .retain(|variable| !variable.name.starts_with("RUSTQUEUE_KODO_"));
        disabled
            .spec
            .as_mut()
            .unwrap()
            .template
            .metadata
            .as_mut()
            .unwrap()
            .labels
            .as_mut()
            .unwrap()
            .insert(resources::DISCOVERY_MODE_LABEL.into(), "direct".into());
        assert!(deployment_runs_without_kodo(Some(&disabled)));
        disabled.status.as_mut().unwrap().updated_replicas = Some(1);
        assert!(!deployment_runs_without_kodo(Some(&disabled)));
    }

    #[test]
    fn discovery_cutover_never_selects_direct_and_kodo_pods_together() {
        assert_eq!(
            discovery_target_mode(true, true, true, false, None, false, true),
            (false, true),
            "legacy unlabeled Pods are first relabeled without changing topology"
        );
        assert_eq!(
            discovery_target_route(true, true, None, false, false, (2, 0), 2),
            None,
            "label adoption keeps the compatibility selector broad"
        );

        let (target, adopting) = discovery_target_mode(true, true, true, false, None, true, true);
        assert_eq!((target, adopting), (true, false));
        assert_eq!(
            discovery_target_route(true, adopting, None, false, target, (0, 2), 2),
            Some(false),
            "Gateway-mode Pods roll out behind the direct-only Service"
        );
        assert_eq!(
            discovery_target_route(true, adopting, Some(false), false, target, (2, 2), 2),
            Some(true),
            "the Service switches only after every Gateway-mode Pod is Ready"
        );
        assert_eq!(
            discovery_target_route(true, adopting, Some(true), true, target, (0, 2), 2),
            Some(false),
            "the Service rolls back when Gateway-mode Pods fail during the soak"
        );

        let (target, adopting) =
            discovery_target_mode(false, false, true, true, Some(true), true, true);
        assert_eq!((target, adopting), (false, false));
        assert_eq!(
            discovery_target_route(true, adopting, Some(true), true, target, (0, 2), 2),
            Some(true)
        );
        assert_eq!(
            discovery_target_route(true, adopting, Some(true), true, target, (2, 2), 2),
            Some(false),
            "disable switches back only after every direct-mode Pod is Ready"
        );
        assert_eq!(
            discovery_target_route(true, adopting, Some(false), false, target, (0, 2), 2),
            Some(true),
            "the Service restores Gateway mode when direct Pods fail during the soak"
        );
    }

    #[test]
    fn kodo_disable_holds_gateways_until_target_brokers_are_safe() {
        assert!(should_hold_kodo_for_disable(false, true, false, true));
        assert!(should_hold_kodo_for_disable(false, true, true, false));
        assert!(!should_hold_kodo_for_disable(false, true, true, true));
        assert!(!should_hold_kodo_for_disable(true, true, false, false));
    }

    #[test]
    fn kodo_cutover_grace_uses_the_condition_transition_time() {
        let mut cluster: RustQueue = serde_json::from_value(serde_json::json!({
            "apiVersion": "rustqueue.io/v1alpha1",
            "kind": "RustQueue",
            "metadata": {"name": "queue", "namespace": "test"},
            "spec": {"image": "rustqueue:test"},
            "status": {
                "desiredBrokers": 3,
                "readyBrokers": 3,
                "phase": "Ready",
                "message": "ready",
                "activeStorageFeatureLevel": 1,
                "conditions": [{
                    "type": "KodoGatewaysAdvertised",
                    "status": "True",
                    "reason": "Advertised",
                    "message": "ready",
                    "lastTransitionTime": "2020-01-01T00:00:00Z"
                }]
            }
        }))
        .unwrap();
        assert!(condition_true_for(&cluster, "KodoGatewaysAdvertised", 630));
        cluster.status.as_mut().unwrap().conditions[0].status = "False".into();
        assert!(!condition_true_for(&cluster, "KodoGatewaysAdvertised", 630));
    }

    #[test]
    fn active_storage_feature_level_is_a_monotonic_floor() {
        assert_eq!(effective_storage_feature_level(1, 2), 2);
        assert_eq!(effective_storage_feature_level(2, 1), 2);
    }

    #[test]
    fn ordinary_scaling_honors_the_configured_floor_and_ceiling() {
        assert_eq!(ordinary_desired_brokers(0, 3, 10), 3);
        assert_eq!(ordinary_desired_brokers(7, 3, 10), 7);
        assert_eq!(ordinary_desired_brokers(12, 3, 10), 10);
        assert_eq!(ordinary_desired_brokers(usize::MAX, 3, 10), 10);
    }

    #[test]
    fn cpu_quantities_compare_in_millicores() {
        assert_eq!(parse_cpu_millis("1").unwrap(), 1_000.0);
        assert_eq!(parse_cpu_millis("1000m").unwrap(), 1_000.0);
        assert_eq!(parse_cpu_millis("500m").unwrap(), 500.0);
        assert_eq!(parse_cpu_millis("500000u").unwrap(), 500.0);
        assert!(parse_cpu_millis("-1").is_err());
    }

    #[test]
    fn feature_two_retains_large_message_delivery_capacity() {
        assert_eq!(
            effective_delivery_limits(2, 20 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024),
            (100 * 1024 * 1024, 200 * 1024 * 1024)
        );
    }

    #[test]
    fn legacy_feature_rejects_messages_that_cross_the_record_bound() {
        let maximum = 71 * 1024 * 1024;
        assert!(validate_message_storage_contract(maximum, 1).is_ok());
        assert!(validate_message_storage_contract(maximum + 1, 1).is_err());
        assert!(validate_message_storage_contract(100 * 1024 * 1024, 2).is_ok());
    }

    #[test]
    fn immutable_image_detection_requires_a_complete_sha256_digest() {
        let digest = "a".repeat(64);
        assert!(has_sha256_digest(&format!(
            "registry/rustqueue@sha256:{digest}"
        )));
        assert!(!has_sha256_digest(&format!(
            "registry/rustqueue@sha256:{}",
            "A".repeat(64)
        )));
        assert!(!has_sha256_digest("registry/rustqueue:latest"));
        assert!(!has_sha256_digest("registry/rustqueue@sha256:abcd"));
    }

    #[test]
    fn kodo_contract_requires_a_second_lookup_poll_retention_window() {
        let mut cluster: RustQueue = serde_json::from_value(serde_json::json!({
            "apiVersion": "rustqueue.io/v1alpha1",
            "kind": "RustQueue",
            "metadata": {"name": "queue", "namespace": "test"},
            "spec": {
                "image": "rustqueue:test",
                "imagePullPolicy": "Never",
                "minBrokers": 3,
                "maxBrokers": 3,
                "storageFeatureLevel": 2,
                "bootstrapRetentionSeconds": 180,
                "maxMessageBytes": 104857600,
                "connectionDeliveryInflightBytes": 134217728,
                "nodeDeliveryInflightBytes": 536870912,
                "brokerResources": {"cpuRequest": "1", "memoryRequest": "2Gi"},
                "kodoCompatibility": {"enabled": true}
            }
        }))
        .unwrap();

        assert!(validate(&cluster, 2).is_ok());
        cluster.spec.broker_resources.cpu_request = "999m".into();
        assert!(validate(&cluster, 2)
            .unwrap_err()
            .to_string()
            .contains("cpuRequest >= 1 CPU"));
        cluster.spec.broker_resources.cpu_request = "1".into();
        cluster.spec.bootstrap_retention_seconds = KODO_BOOTSTRAP_RETENTION_SECONDS - 1;
        assert!(validate(&cluster, 2)
            .unwrap_err()
            .to_string()
            .contains("bootstrapRetentionSeconds >= 180"));
    }
}
