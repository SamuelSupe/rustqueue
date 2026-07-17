use super::apply::ApplyRequest;
pub use super::operation_state::{action_name, StartedOperation};
use super::operation_state::{
    final_phase, new_operation, phase_for, retry_operation, started_channel, started_topic,
    update_paused, update_tombstone,
};
use super::ManagementError;
use crate::app::AppState;
use crate::resources;
use crate::session::{now_ms, ActionChallenge};
use kube::api::{Api, ObjectMeta, PostParams};
use kube::{Resource, ResourceExt};
use rustqueue_operator::{
    ManagedResourcePhase, RustQueue, RustQueueChannel, RustQueueChannelSpec, RustQueueTopic,
    RustQueueTopicSpec,
};
use std::collections::BTreeMap;

pub async fn verify_subject(
    state: &AppState,
    challenge: &ActionChallenge,
) -> Result<(), ManagementError> {
    let namespace = &state.config.namespace;
    let queue = &state.config.queue_name;
    let current = match challenge.subject_kind.as_str() {
        "channel" => resources::get_channel(
            &state.client,
            namespace,
            queue,
            &challenge.topic,
            challenge.channel.as_deref().expect("channel challenge"),
        )
        .await
        .map_err(kube_unavailable)?
        .map(identity),
        "topic" => resources::get_topic(&state.client, namespace, queue, &challenge.topic)
            .await
            .map_err(kube_unavailable)?
            .map(identity),
        "catalog" => {
            let cluster = Api::<RustQueue>::namespaced(state.client.clone(), namespace)
                .get(queue)
                .await
                .map_err(|error| ManagementError::unavailable(error.to_string()))?;
            let catalog = resources::list(&state.client, namespace, queue)
                .await
                .map_err(kube_unavailable)?;
            Some((cluster.uid().unwrap_or_default(), catalog.fences().revision))
        }
        _ => None,
    };
    if current.as_ref()
        != Some(&(
            challenge.subject_uid.clone(),
            challenge.resource_version.clone(),
        ))
    {
        return Err(ManagementError::conflict(
            "E_RESOURCE_CHANGED",
            "resource UID or revision changed after preview",
        ));
    }
    if challenge.subject_kind == "catalog"
        && resources::get_topic(&state.client, namespace, queue, &challenge.topic)
            .await
            .map_err(kube_unavailable)?
            .is_some()
    {
        return Err(ManagementError::conflict(
            "E_RESOURCE_CHANGED",
            "target resource appeared after preview",
        ));
    }
    if challenge.kind == "channel"
        && challenge.subject_kind != "channel"
        && resources::get_channel(
            &state.client,
            namespace,
            queue,
            &challenge.topic,
            challenge.channel.as_deref().expect("channel challenge"),
        )
        .await
        .map_err(kube_unavailable)?
        .is_some()
    {
        return Err(ManagementError::conflict(
            "E_RESOURCE_CHANGED",
            "channel appeared after preview",
        ));
    }
    Ok(())
}

pub async fn begin_topic(
    state: &AppState,
    request: &ApplyRequest,
    challenge: &ActionChallenge,
    tombstone_until_ms: Option<i64>,
) -> Result<StartedOperation, ManagementError> {
    let api = Api::<RustQueueTopic>::namespaced(state.client.clone(), &state.config.namespace);
    let current = resources::get_topic(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
        &request.topic,
    )
    .await
    .map_err(kube_unavailable)?;
    let now = now_ms() as i64;
    let mut resource = match current {
        Some(resource) => resource,
        None if request.action == "create" => {
            let operation = new_operation(request, challenge, now)?;
            let mut resource = RustQueueTopic::new(
                &resources::topic_resource_name(&state.config.queue_name, &request.topic),
                RustQueueTopicSpec {
                    queue: state.config.queue_name.clone(),
                    topic: request.topic.clone(),
                    owners: challenge.owners.clone(),
                    phase: phase_for(operation.action),
                    revision: 1,
                    paused: false,
                    tombstone_until_ms: None,
                    last_error: None,
                    operation: Some(operation),
                },
            );
            attach_metadata(state, &mut resource.metadata).await?;
            let created = api
                .create(&PostParams::default(), &resource)
                .await
                .map_err(kube_conflict)?;
            return Ok(started_topic(&created));
        }
        None => return Err(not_found("topic")),
    };
    ensure_topic_children_idle(state, &request.topic).await?;
    resource.spec.owners = challenge.owners.clone();
    if request.action == "retry" {
        retry_operation(&mut resource.spec.operation, &resource.spec.phase, now)?;
    } else {
        resource.spec.operation = Some(new_operation(request, challenge, now)?);
    }
    let action = resource
        .spec
        .operation
        .as_ref()
        .expect("operation set")
        .action;
    resource.spec.phase = phase_for(action);
    resource.spec.revision = resource.spec.revision.saturating_add(1);
    resource.spec.last_error = None;
    update_tombstone(
        &mut resource.spec.tombstone_until_ms,
        action,
        tombstone_until_ms,
    );
    let replaced = api
        .replace(&resource.name_any(), &PostParams::default(), &resource)
        .await
        .map_err(kube_conflict)?;
    Ok(started_topic(&replaced))
}

pub async fn begin_channel(
    state: &AppState,
    request: &ApplyRequest,
    challenge: &ActionChallenge,
    tombstone_until_ms: Option<i64>,
) -> Result<StartedOperation, ManagementError> {
    let channel = request.channel.as_deref().expect("validated channel");
    let api = Api::<RustQueueChannel>::namespaced(state.client.clone(), &state.config.namespace);
    let parent = resources::get_topic(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
        &request.topic,
    )
    .await
    .map_err(kube_unavailable)?
    .ok_or_else(|| not_found("topic"))?;
    if parent.spec.phase != ManagedResourcePhase::Active || parent.spec.operation.is_some() {
        return Err(ManagementError::conflict(
            "E_TOPIC_OPERATION_IN_PROGRESS",
            "channel operations require an active parent topic",
        ));
    }
    let current = resources::get_channel(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
        &request.topic,
        channel,
    )
    .await
    .map_err(kube_unavailable)?;
    let now = now_ms() as i64;
    let mut resource = match current {
        Some(resource) => resource,
        None if request.action == "create" => {
            let operation = new_operation(request, challenge, now)?;
            let mut resource = RustQueueChannel::new(
                &resources::channel_resource_name(
                    &state.config.queue_name,
                    &request.topic,
                    channel,
                ),
                RustQueueChannelSpec {
                    queue: state.config.queue_name.clone(),
                    topic: request.topic.clone(),
                    channel: channel.into(),
                    owners: challenge.owners.clone(),
                    phase: phase_for(operation.action),
                    revision: 1,
                    paused: false,
                    ephemeral: false,
                    tombstone_until_ms: None,
                    last_error: None,
                    operation: Some(operation),
                },
            );
            attach_metadata(state, &mut resource.metadata).await?;
            let created = api
                .create(&PostParams::default(), &resource)
                .await
                .map_err(kube_conflict)?;
            return Ok(started_channel(&created));
        }
        None => return Err(not_found("channel")),
    };
    if resource.spec.ephemeral {
        return Err(ManagementError::conflict(
            "E_EPHEMERAL_UNMANAGED",
            "ephemeral channels are observation-only",
        ));
    }
    resource.spec.owners = challenge.owners.clone();
    if request.action == "retry" {
        retry_operation(&mut resource.spec.operation, &resource.spec.phase, now)?;
    } else {
        resource.spec.operation = Some(new_operation(request, challenge, now)?);
    }
    let action = resource
        .spec
        .operation
        .as_ref()
        .expect("operation set")
        .action;
    resource.spec.phase = phase_for(action);
    resource.spec.revision = resource.spec.revision.saturating_add(1);
    resource.spec.last_error = None;
    update_tombstone(
        &mut resource.spec.tombstone_until_ms,
        action,
        tombstone_until_ms,
    );
    let replaced = api
        .replace(&resource.name_any(), &PostParams::default(), &resource)
        .await
        .map_err(kube_conflict)?;
    Ok(started_channel(&replaced))
}

pub async fn record_topic_owner(
    state: &AppState,
    mut resource: RustQueueTopic,
    owner: &str,
) -> Result<(), ManagementError> {
    let operation = resource
        .spec
        .operation
        .as_mut()
        .ok_or_else(|| ManagementError::internal("topic operation disappeared"))?;
    if !operation
        .completed_owners
        .iter()
        .any(|value| value == owner)
    {
        operation.completed_owners.push(owner.into());
    }
    operation.updated_at_ms = now_ms() as i64;
    resource.spec.last_error = None;
    replace_topic(state, resource).await.map(|_| ())
}

pub async fn record_channel_owner(
    state: &AppState,
    mut resource: RustQueueChannel,
    owner: &str,
) -> Result<(), ManagementError> {
    let operation = resource
        .spec
        .operation
        .as_mut()
        .ok_or_else(|| ManagementError::internal("channel operation disappeared"))?;
    if !operation
        .completed_owners
        .iter()
        .any(|value| value == owner)
    {
        operation.completed_owners.push(owner.into());
    }
    operation.updated_at_ms = now_ms() as i64;
    resource.spec.last_error = None;
    replace_channel(state, resource).await.map(|_| ())
}

pub async fn finish_topic(
    state: &AppState,
    mut resource: RustQueueTopic,
) -> Result<u64, ManagementError> {
    let action = resource
        .spec
        .operation
        .as_ref()
        .ok_or_else(|| ManagementError::internal("topic operation disappeared"))?
        .action;
    resource.spec.phase = final_phase(action);
    update_paused(&mut resource.spec.paused, action);
    resource.spec.last_error = None;
    resource.spec.operation = None;
    Ok(replace_topic(state, resource).await?.spec.revision)
}

pub async fn finish_channel(
    state: &AppState,
    mut resource: RustQueueChannel,
) -> Result<u64, ManagementError> {
    let action = resource
        .spec
        .operation
        .as_ref()
        .ok_or_else(|| ManagementError::internal("channel operation disappeared"))?
        .action;
    resource.spec.phase = final_phase(action);
    update_paused(&mut resource.spec.paused, action);
    resource.spec.last_error = None;
    resource.spec.operation = None;
    Ok(replace_channel(state, resource).await?.spec.revision)
}

pub async fn fail_topic(state: &AppState, mut resource: RustQueueTopic, detail: &str) {
    resource.spec.phase = ManagedResourcePhase::Failed;
    resource.spec.last_error = Some(truncate_error(detail));
    if let Some(operation) = resource.spec.operation.as_mut() {
        operation.updated_at_ms = now_ms() as i64;
    }
    let _ = replace_topic(state, resource).await;
}

pub async fn fail_channel(state: &AppState, mut resource: RustQueueChannel, detail: &str) {
    resource.spec.phase = ManagedResourcePhase::Failed;
    resource.spec.last_error = Some(truncate_error(detail));
    if let Some(operation) = resource.spec.operation.as_mut() {
        operation.updated_at_ms = now_ms() as i64;
    }
    let _ = replace_channel(state, resource).await;
}

pub async fn note_topic_error(state: &AppState, mut resource: RustQueueTopic, detail: &str) {
    resource.spec.last_error = Some(truncate_error(detail));
    if let Some(operation) = resource.spec.operation.as_mut() {
        operation.updated_at_ms = now_ms() as i64;
    }
    let _ = replace_topic(state, resource).await;
}

pub async fn note_channel_error(state: &AppState, mut resource: RustQueueChannel, detail: &str) {
    resource.spec.last_error = Some(truncate_error(detail));
    if let Some(operation) = resource.spec.operation.as_mut() {
        operation.updated_at_ms = now_ms() as i64;
    }
    let _ = replace_channel(state, resource).await;
}

pub async fn tombstone_topic_channels(
    state: &AppState,
    topic: &str,
    tombstone_until_ms: Option<i64>,
) -> Result<(), ManagementError> {
    let api = Api::<RustQueueChannel>::namespaced(state.client.clone(), &state.config.namespace);
    let catalog = resources::list(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
    )
    .await
    .map_err(kube_unavailable)?;
    for mut channel in catalog
        .channels
        .into_iter()
        .filter(|channel| channel.spec.topic == topic)
    {
        channel.spec.phase = ManagedResourcePhase::Tombstoned;
        channel.spec.tombstone_until_ms = tombstone_until_ms;
        channel.spec.last_error = None;
        channel.spec.operation = None;
        channel.spec.revision = channel.spec.revision.saturating_add(1);
        api.replace(&channel.name_any(), &PostParams::default(), &channel)
            .await
            .map_err(kube_conflict)?;
    }
    Ok(())
}

async fn replace_topic(
    state: &AppState,
    resource: RustQueueTopic,
) -> Result<RustQueueTopic, ManagementError> {
    Api::<RustQueueTopic>::namespaced(state.client.clone(), &state.config.namespace)
        .replace(&resource.name_any(), &PostParams::default(), &resource)
        .await
        .map_err(kube_conflict)
}

async fn replace_channel(
    state: &AppState,
    resource: RustQueueChannel,
) -> Result<RustQueueChannel, ManagementError> {
    Api::<RustQueueChannel>::namespaced(state.client.clone(), &state.config.namespace)
        .replace(&resource.name_any(), &PostParams::default(), &resource)
        .await
        .map_err(kube_conflict)
}

async fn attach_metadata(
    state: &AppState,
    metadata: &mut ObjectMeta,
) -> Result<(), ManagementError> {
    let cluster = Api::<RustQueue>::namespaced(state.client.clone(), &state.config.namespace)
        .get(&state.config.queue_name)
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
    metadata.labels = Some(BTreeMap::from([(
        "rustqueue.io/queue".into(),
        state.config.queue_name.clone(),
    )]));
    metadata.owner_references = Some(vec![cluster
        .controller_owner_ref(&())
        .ok_or_else(|| ManagementError::internal("RustQueue owner reference is unavailable"))?]);
    Ok(())
}

fn identity<K: ResourceExt>(resource: K) -> (String, String) {
    (
        resource.uid().unwrap_or_default(),
        resource.resource_version().unwrap_or_default(),
    )
}

fn truncate_error(detail: &str) -> String {
    detail.chars().take(1024).collect()
}

async fn ensure_topic_children_idle(state: &AppState, topic: &str) -> Result<(), ManagementError> {
    let catalog = resources::list(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
    )
    .await
    .map_err(kube_unavailable)?;
    if catalog.channels.iter().any(|channel| {
        channel.spec.topic == topic
            && !matches!(
                channel.spec.phase,
                ManagedResourcePhase::Active | ManagedResourcePhase::Tombstoned
            )
    }) {
        return Err(ManagementError::conflict(
            "E_CHANNEL_OPERATION_IN_PROGRESS",
            "topic has an unfinished channel operation",
        ));
    }
    Ok(())
}

fn not_found(kind: &str) -> ManagementError {
    ManagementError::conflict(
        "E_NOT_FOUND",
        format!("{kind} control resource does not exist"),
    )
}

fn kube_unavailable(error: anyhow::Error) -> ManagementError {
    ManagementError::unavailable(error.to_string())
}

fn kube_conflict(error: kube::Error) -> ManagementError {
    if matches!(&error, kube::Error::Api(response) if response.code == 409) {
        ManagementError::conflict(
            "E_RESOURCE_CHANGED",
            "Kubernetes resource revision changed during the operation",
        )
    } else {
        ManagementError::unavailable(error.to_string())
    }
}
