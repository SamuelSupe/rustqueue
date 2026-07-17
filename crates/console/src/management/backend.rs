use super::ManagementError;
use crate::app::AppState;
use crate::model::{BrokerView, Snapshot};
use crate::resources;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct OwnerOperation<'a> {
    pub owner: &'a str,
    pub operation_id: &'a str,
    pub topic: &'a str,
    pub channel: Option<&'a str>,
    pub action: &'a str,
    pub tombstone_until_ms: Option<i64>,
}

pub async fn apply_owner(
    state: &AppState,
    snapshot: &Snapshot,
    operation: OwnerOperation<'_>,
) -> Result<(), ManagementError> {
    let token = console_token(state).await?;
    let resource = if operation.channel.is_some() {
        "channels"
    } else {
        "topics"
    };
    let path = format!("/v1/manage/{resource}/{}", operation.action);
    let (broker, revision) = target(snapshot, operation.owner)?;
    post(
        state,
        &token,
        &broker,
        &path,
        json!({
            "operation_id": operation.operation_id,
            "topic": operation.topic,
            "channel": operation.channel,
            "expected_revision": revision,
            "tombstone_until_ms": operation.tombstone_until_ms,
        }),
    )
    .await
}

pub async fn sync_fences(state: &AppState, snapshot: &Snapshot) -> Result<(), ManagementError> {
    let token = Arc::<str>::from(console_token(state).await?);
    let managed = resources::list(
        &state.client,
        &state.config.namespace,
        &state.config.queue_name,
    )
    .await
    .map_err(|error| ManagementError::unavailable(error.to_string()))?;
    let fences = serde_json::to_value(managed.fences())
        .map_err(|error| ManagementError::internal(error.to_string()))?;
    stream::iter(snapshot.brokers.clone())
        .map(|broker| {
            let token = Arc::clone(&token);
            let fences = fences.clone();
            async move {
                post(
                    state,
                    token.as_ref(),
                    &broker,
                    "/v1/manage/fences/sync",
                    fences,
                )
                .await
            }
        })
        .buffer_unordered(64)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn console_token(state: &AppState) -> Result<String, ManagementError> {
    let token = tokio::fs::read_to_string(&state.config.console_token_file)
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
    let token = token.trim();
    if token.is_empty() {
        return Err(ManagementError::unavailable("console token is empty"));
    }
    Ok(token.into())
}

fn broker<'a>(snapshot: &'a Snapshot, owner: &str) -> Result<&'a BrokerView, ManagementError> {
    snapshot
        .brokers
        .iter()
        .find(|broker| broker.name == owner)
        .ok_or_else(|| ManagementError::unavailable(format!("owner {owner} is absent")))
}

fn registry_revision(broker: &BrokerView, owner: &str) -> Result<u64, ManagementError> {
    broker
        .observation
        .as_ref()
        .map(|observation| observation.registry_revision)
        .ok_or_else(|| ManagementError::unavailable(format!("owner {owner} is unobserved")))
}

fn target(snapshot: &Snapshot, owner: &str) -> Result<(BrokerView, u64), ManagementError> {
    let broker = broker(snapshot, owner)?;
    Ok((broker.clone(), registry_revision(broker, owner)?))
}

async fn post(
    state: &AppState,
    token: &str,
    broker: &BrokerView,
    path: &str,
    body: serde_json::Value,
) -> Result<(), ManagementError> {
    if broker.pod_ip.is_empty() {
        return Err(ManagementError::unavailable(format!(
            "broker {} has no Pod IP",
            broker.name
        )));
    }
    let response = state
        .http
        .post(format!(
            "http://{}:{}{}",
            broker.pod_ip, state.config.broker_http_port, path
        ))
        .bearer_auth(token)
        .timeout(Duration::from_secs(15))
        .json(&body)
        .send()
        .await
        .map_err(|error| ManagementError::unavailable(format!("{}: {error}", broker.name)))?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let detail = response.text().await.unwrap_or_default();
    if status.as_u16() == 409 {
        Err(ManagementError::conflict(
            "E_BROKER_STATE_DRIFT",
            format!("{} rejected stale state: {detail}", broker.name),
        ))
    } else if matches!(status.as_u16(), 400 | 404 | 422) {
        Err(ManagementError::conflict(
            "E_BROKER_REJECTED",
            format!("{} rejected the operation: {detail}", broker.name),
        ))
    } else {
        Err(ManagementError::unavailable(format!(
            "{} returned {status}: {detail}",
            broker.name
        )))
    }
}
