mod admission;
mod auth;
mod delivery;

pub(super) use admission::WebSocketRuntime;
use admission::{AdmissionError, ConnectionAdmission};
use auth::{
    authenticate_header, ensure_authorized, observe_auth_failure, read_auth, refresh_auth,
    SessionAuth,
};
use delivery::{deliver_one, reliable_policy_changed, FRAME_HEADER_BYTES};

use super::*;
use crate::subscriptions::ClientIdentity;
use crate::tcp::EphemeralConsumers;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::Path;
use rustqueue_queue::{BrokerError, DeliveryMode};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::debug;

const SUBPROTOCOL: &str = "rustqueue.live.v1";
const MAX_CONTROL_BYTES: usize = 64 * 1024;
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) fn lookup_cors(allowed_origins: &[String]) -> Option<CorsLayer> {
    let origins = allowed_origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect::<Vec<_>>();
    (!origins.is_empty()).then(|| {
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([axum::http::Method::GET])
            .allow_headers([header::ACCEPT])
            .max_age(Duration::from_secs(600))
    })
}

pub(super) async fn subscribe_topic(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    uri: axum::http::Uri,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    if uri.query().is_some() {
        return Err(ApiError::bad_request(
            "E_WS_QUERY_FORBIDDEN",
            "WebSocket subscriptions do not accept query parameters",
        ));
    }
    ensure_origin_allowed(&state.config.websocket.allowed_origins, &headers)?;
    if !ws
        .requested_protocols()
        .any(|protocol| protocol == SUBPROTOCOL)
    {
        return Err(ApiError::bad_request(
            "E_WS_SUBPROTOCOL",
            format!("Sec-WebSocket-Protocol must include {SUBPROTOCOL}"),
        ));
    }
    let reliable_policy_epoch = state.broker.topic_reliable_policy_epoch(&topic)?;
    let policy = state.broker.topic_policy(&topic)?;
    if policy.delivery_mode != DeliveryMode::TtlDiscard {
        state
            .metrics
            .websocket_non_ttl_rejections
            .fetch_add(1, Ordering::Relaxed);
        return Err(ApiError::conflict(
            "E_WS_TTL_REQUIRED",
            "WebSocket subscriptions require a TTL_DISCARD Topic",
        ));
    }
    if !state.delivering.load(Ordering::Acquire) {
        return Err(ApiError::unavailable(
            "E_NOT_READY",
            "Broker is not accepting new subscriptions",
        ));
    }
    let (id, admission) = state.websocket.admit(&topic).map_err(|error| {
        state
            .metrics
            .websocket_capacity_rejections
            .fetch_add(1, Ordering::Relaxed);
        match error {
            AdmissionError::Global => ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "E_WS_CAPACITY",
                detail: "WebSocket connection capacity is exhausted".into(),
            },
            AdmissionError::Topic => ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "E_WS_CAPACITY",
                detail: "WebSocket Topic connection capacity is exhausted".into(),
            },
        }
    })?;
    let session_auth = authenticate_header(&state, peer, &topic, &headers).await?;
    let max_write_buffer = state
        .config
        .queue
        .max_message_bytes
        .saturating_add(FRAME_HEADER_BYTES + 1024);
    Ok(ws
        .protocols([SUBPROTOCOL])
        .write_buffer_size(0)
        .max_write_buffer_size(max_write_buffer)
        .max_message_size(MAX_CONTROL_BYTES)
        .max_frame_size(MAX_CONTROL_BYTES)
        .on_upgrade(move |socket| {
            run_session(
                socket,
                state,
                peer,
                topic,
                id,
                admission,
                session_auth,
                reliable_policy_epoch,
            )
        })
        .into_response())
}

fn ensure_origin_allowed(allowed: &[String], headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return Ok(());
    };
    let allowed = origin
        .to_str()
        .ok()
        .is_some_and(|origin| allowed.iter().any(|candidate| candidate == origin));
    if allowed {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "E_WS_ORIGIN",
            detail: "WebSocket Origin is not allowed".into(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    mut socket: WebSocket,
    state: AppState,
    peer: SocketAddr,
    topic: String,
    id: u64,
    _admission: ConnectionAdmission,
    mut auth: SessionAuth,
    reliable_policy_epoch: u64,
) {
    state
        .metrics
        .websocket_connections
        .fetch_add(1, Ordering::Relaxed);
    let result = run_session_inner(
        &mut socket,
        &state,
        peer,
        &topic,
        id,
        &mut auth,
        reliable_policy_epoch,
    )
    .await;
    state
        .metrics
        .websocket_connections
        .fetch_sub(1, Ordering::Relaxed);
    if let Err(error) = result {
        debug!(%peer, %topic, connection_id = id, %error, "WebSocket subscriber disconnected");
    }
}

async fn run_session_inner(
    socket: &mut WebSocket,
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
    id: u64,
    auth: &mut SessionAuth,
    reliable_policy_epoch: u64,
) -> anyhow::Result<()> {
    if state.authenticator.is_some() && auth.session.is_none() {
        send_control(socket, json!({"type": "auth_required"})).await?;
        match read_auth(socket, state, peer, topic).await {
            Ok(session) => *auth = session,
            Err(error) => {
                observe_auth_failure(state);
                close_control(socket, error.code, &error.detail, 1008).await;
                anyhow::bail!(error.detail);
            }
        }
    }
    ensure_authorized(auth.session.as_ref(), topic)?;
    let mut policy_changes = match state.broker.subscribe_topic_policy(topic) {
        Ok(changes) => changes,
        Err(error) => {
            handle_broker_error(socket, error).await;
            anyhow::bail!("Topic became unavailable before subscription readiness");
        }
    };
    if policy_changes.borrow().delivery_mode != DeliveryMode::TtlDiscard {
        close_control(
            socket,
            "E_WS_TTL_REQUIRED",
            "Topic no longer uses TTL_DISCARD",
            1008,
        )
        .await;
        anyhow::bail!("Topic stopped using TTL_DISCARD before subscription readiness");
    }
    let channel: Arc<str> = Arc::from(format!("ws.{}.{}#ephemeral", state.config.node.id, id));
    let cleanup = match EphemeralCleanup::register(
        state.ephemeral_consumers.clone(),
        Arc::clone(&state.broker),
        topic,
        channel.as_ref(),
    )
    .await
    {
        Ok(cleanup) => cleanup,
        Err(error) => {
            let (code, close) = match &error {
                BrokerError::ChannelLimit => {
                    state
                        .metrics
                        .websocket_capacity_rejections
                        .fetch_add(1, Ordering::Relaxed);
                    ("E_WS_CAPACITY", 1013)
                }
                BrokerError::TopicNotFound
                | BrokerError::TopicRetiring
                | BrokerError::TopicTombstoned => ("E_TOPIC_CLOSED", 1001),
                BrokerError::ManagementUnavailable => ("E_OWNER_CHANGED", 1012),
                _ => ("E_BROKER", 1011),
            };
            close_control(socket, code, &error.to_string(), close).await;
            return Err(error.into());
        }
    };
    if policy_changes.has_changed().is_err() {
        close_control(
            socket,
            "E_TOPIC_CLOSED",
            "Topic was removed while creating the live Channel",
            1001,
        )
        .await;
        anyhow::bail!("Topic was removed while creating the live Channel");
    }
    let current_policy = match state.broker.topic_policy(topic) {
        Ok(policy) => policy,
        Err(error) => {
            handle_broker_error(socket, error).await;
            anyhow::bail!("Topic became unavailable while creating the live Channel");
        }
    };
    if current_policy.delivery_mode != DeliveryMode::TtlDiscard {
        close_control(
            socket,
            "E_WS_TTL_REQUIRED",
            "Topic no longer uses TTL_DISCARD",
            1008,
        )
        .await;
        anyhow::bail!("Topic stopped using TTL_DISCARD while creating the live Channel");
    }
    if reliable_policy_changed(state, topic, reliable_policy_epoch)? {
        close_control(
            socket,
            "E_WS_TTL_REQUIRED",
            "Topic switched to RELIABLE before subscription readiness",
            1008,
        )
        .await;
        anyhow::bail!("Topic switched to RELIABLE before subscription readiness");
    }
    if state.broker.topic_policy(topic)?.delivery_mode != DeliveryMode::TtlDiscard {
        close_control(
            socket,
            "E_WS_TTL_REQUIRED",
            "Topic no longer uses TTL_DISCARD",
            1008,
        )
        .await;
        anyhow::bail!("Topic stopped using TTL_DISCARD before subscription readiness");
    }
    let identity = ClientIdentity {
        client_id: format!("ws-{id}"),
        hostname: "websocket".into(),
        remote_address: peer.to_string(),
        user_agent: "rustqueue.live.v1".into(),
        authed: auth.session.is_some(),
        ..ClientIdentity::default()
    };
    let lease = state
        .subscriptions
        .register(topic, channel.as_ref(), identity)
        .map_err(|_| anyhow::anyhow!("subscription deletion is in progress"))?;
    lease.update_flow(1, 0);
    send_control(
        socket,
        json!({
            "type": "ready",
            "topic": topic,
            "subscription_id": id.to_string(),
            "start": "tail",
            "ack": "auto"
        }),
    )
    .await?;

    let mut shutdown = state.shutdown.clone();
    let heartbeat = Duration::from_millis(state.config.limits.heartbeat_interval_ms);
    let mut heartbeat_tick = tokio::time::interval(heartbeat);
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat_tick.tick().await;
    let mut last_peer_activity = Instant::now();
    loop {
        if !state.delivering.load(Ordering::Acquire) {
            close_control(socket, "E_DRAINING", "Broker is draining", 1001).await;
            break;
        }
        if reliable_policy_changed(state, topic, reliable_policy_epoch)? {
            close_control(
                socket,
                "E_WS_TTL_REQUIRED",
                "Topic switched to RELIABLE",
                1008,
            )
            .await;
            break;
        }
        if let Err(error) = refresh_auth(auth, state, peer, topic).await {
            close_control(socket, error.code, &error.detail, 1008).await;
            anyhow::bail!(error.detail);
        }
        let fetch = state.broker.fetch_batch_retained(
            topic,
            channel.as_ref(),
            1,
            state.config.queue.max_message_bytes,
            Duration::from_secs(1),
            Some(
                state
                    .config
                    .message_timeout()
                    .saturating_add(Duration::from_millis(
                        state.config.limits.tcp_command_timeout_ms,
                    )),
            ),
        );
        tokio::pin!(fetch);
        let batch = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                close_control(socket, "E_SHUTDOWN", "Broker is shutting down", 1001).await;
                break;
            }
            changed = policy_changes.changed() => {
                if changed.is_err() {
                    close_control(socket, "E_TOPIC_CLOSED", "Topic was removed", 1001).await;
                    break;
                }
                if policy_changes.borrow_and_update().delivery_mode != DeliveryMode::TtlDiscard {
                    close_control(socket, "E_WS_TTL_REQUIRED", "Topic no longer uses TTL_DISCARD", 1008).await;
                    break;
                }
                continue;
            }
            incoming = socket.recv() => {
                if !handle_incoming(socket, incoming, &mut last_peer_activity).await? {
                    break;
                }
                continue;
            }
            _ = heartbeat_tick.tick() => {
                if last_peer_activity.elapsed() >= heartbeat.saturating_mul(2) {
                    state.metrics.websocket_slow_disconnects.fetch_add(1, Ordering::Relaxed);
                    close_control(socket, "E_WS_HEARTBEAT", "WebSocket heartbeat timed out", 1008).await;
                    break;
                }
                let ping = tokio::time::timeout(
                    Duration::from_millis(state.config.limits.tcp_command_timeout_ms),
                    socket.send(Message::Ping(bytes::Bytes::new())),
                )
                .await;
                match ping {
                    Ok(result) => result?,
                    Err(_) => {
                        state.metrics.websocket_slow_disconnects.fetch_add(1, Ordering::Relaxed);
                        anyhow::bail!("WebSocket heartbeat write timed out");
                    }
                }
                continue;
            }
            result = &mut fetch => match result {
                Ok(batch) => batch,
                Err(error) => {
                    handle_broker_error(socket, error).await;
                    break;
                }
            },
        };
        let (mut deliveries, mut guard) = batch.into_parts();
        let Some(delivery) = deliveries.pop() else {
            continue;
        };
        lease.update_flow(0, 1);
        let outcome = deliver_one(
            socket,
            state,
            topic,
            &channel,
            delivery,
            &mut guard,
            &mut policy_changes,
            reliable_policy_epoch,
        )
        .await;
        lease.update_flow(1, 0);
        match outcome {
            Ok(true) => {
                lease.observe_delivery();
                lease.observe_finish();
            }
            Ok(false) => continue,
            Err(error) => {
                return Err(error);
            }
        }
    }
    drop(lease);
    drop(cleanup);
    Ok(())
}

async fn handle_incoming(
    socket: &mut WebSocket,
    incoming: Option<Result<Message, axum::Error>>,
    last_peer_activity: &mut Instant,
) -> anyhow::Result<bool> {
    let Some(message) = incoming else {
        return Ok(false);
    };
    let message = message?;
    *last_peer_activity = Instant::now();
    match message {
        Message::Close(_) => Ok(false),
        Message::Ping(_) => Ok(true),
        Message::Pong(_) => Ok(true),
        Message::Text(_) | Message::Binary(_) => {
            close_control(socket, "E_WS_CONTROL", "Unexpected client data frame", 1008).await;
            Ok(false)
        }
    }
}

async fn handle_broker_error(socket: &mut WebSocket, error: BrokerError) {
    let (code, close) = match &error {
        BrokerError::TopicNotFound | BrokerError::TopicRetiring | BrokerError::TopicTombstoned => {
            ("E_TOPIC_CLOSED", 1001)
        }
        BrokerError::ManagementUnavailable => ("E_OWNER_CHANGED", 1012),
        _ => ("E_BROKER", 1011),
    };
    close_control(socket, code, &error.to_string(), close).await;
}

async fn send_control(socket: &mut WebSocket, value: Value) -> anyhow::Result<()> {
    tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        socket.send(Message::Text(serde_json::to_string(&value)?.into())),
    )
    .await
    .map_err(|_| anyhow::anyhow!("WebSocket control write timed out"))??;
    Ok(())
}

async fn close_control(socket: &mut WebSocket, code: &'static str, detail: &str, close_code: u16) {
    let _ = tokio::time::timeout(CONTROL_WRITE_TIMEOUT, async {
        send_control(
            socket,
            json!({"type": "error", "code": code, "detail": detail}),
        )
        .await?;
        socket
            .send(Message::Close(Some(CloseFrame {
                code: close_code,
                reason: Utf8Bytes::from_static(code),
            })))
            .await?;
        anyhow::Ok(())
    })
    .await;
}

fn remaining_until(deadline_ns: i64) -> Duration {
    let remaining = deadline_ns.saturating_sub(now_ns());
    Duration::from_nanos(remaining.max(0) as u64)
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

struct EphemeralCleanup {
    consumers: EphemeralConsumers,
    broker: Arc<Broker>,
    topic: String,
    channel: String,
}

impl EphemeralCleanup {
    async fn register(
        consumers: EphemeralConsumers,
        broker: Arc<Broker>,
        topic: &str,
        channel: &str,
    ) -> Result<Self, BrokerError> {
        consumers.register_existing(&broker, topic, channel).await?;
        Ok(Self {
            consumers,
            broker,
            topic: topic.into(),
            channel: channel.into(),
        })
    }
}

impl Drop for EphemeralCleanup {
    fn drop(&mut self) {
        let consumers = self.consumers.clone();
        let broker = Arc::clone(&self.broker);
        let topic = self.topic.clone();
        let channel = self.channel.clone();
        tokio::spawn(async move {
            consumers.unregister(&broker, &topic, &channel).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_check_allows_missing_and_exact_origin_only() {
        let allowed = ["https://app.example".to_owned()];
        let missing = HeaderMap::new();
        assert!(ensure_origin_allowed(&allowed, &missing).is_ok());

        let mut exact = HeaderMap::new();
        exact.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://app.example"),
        );
        assert!(ensure_origin_allowed(&allowed, &exact).is_ok());

        let mut near_miss = HeaderMap::new();
        near_miss.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://app.example.evil"),
        );
        let error = ensure_origin_allowed(&allowed, &near_miss).unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(error.code, "E_WS_ORIGIN");
    }
}
