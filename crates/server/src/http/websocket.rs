use super::*;
use crate::auth::{AuthError, AuthSession, Authenticator};
use crate::subscriptions::ClientIdentity;
use crate::tcp::EphemeralConsumers;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::Path;
use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use rustqueue_queue::{BrokerError, Delivery, DeliveryGuard, DeliveryMode, TopicPolicy};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::debug;

const SUBPROTOCOL: &str = "rustqueue.live.v1";
const AUTH_CHANNEL: &str = "websocket#ephemeral";
const FRAME_MAGIC: &[u8; 4] = b"RQW1";
const FRAME_HEADER_BYTES: usize = 24;
const MAX_CONTROL_BYTES: usize = 64 * 1024;
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(super) struct WebSocketRuntime {
    connections: Arc<Semaphore>,
    frame_bytes: Arc<Semaphore>,
    topics: Arc<Mutex<HashMap<String, usize>>>,
    max_connections_per_topic: usize,
    next_id: Arc<AtomicU64>,
}

pub(super) struct ConnectionAdmission {
    runtime: WebSocketRuntime,
    topic: String,
    _connection: OwnedSemaphorePermit,
}

#[derive(Debug)]
enum AdmissionError {
    Global,
    Topic,
}

#[derive(Deserialize)]
struct AuthControl {
    #[serde(rename = "type")]
    kind: String,
    secret: String,
}

struct SessionAuth {
    session: Option<AuthSession>,
    secret: Option<String>,
}

struct ControlAuthError {
    code: &'static str,
    detail: String,
}

impl WebSocketRuntime {
    pub(super) fn new(config: &crate::config::WebSocketConfig) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(config.max_connections)),
            frame_bytes: Arc::new(Semaphore::new(config.frame_inflight_bytes)),
            topics: Arc::new(Mutex::new(HashMap::new())),
            max_connections_per_topic: config.max_connections_per_topic,
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn admit(&self, topic: &str) -> Result<(u64, ConnectionAdmission), AdmissionError> {
        let connection = Arc::clone(&self.connections)
            .try_acquire_owned()
            .map_err(|_| AdmissionError::Global)?;
        let mut topics = self.topics.lock();
        let count = topics.entry(topic.to_owned()).or_default();
        if *count >= self.max_connections_per_topic {
            return Err(AdmissionError::Topic);
        }
        *count += 1;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        Ok((
            id,
            ConnectionAdmission {
                runtime: self.clone(),
                topic: topic.to_owned(),
                _connection: connection,
            },
        ))
    }

    fn reserve_frame(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let permits = u32::try_from(bytes.max(1)).ok()?;
        Arc::clone(&self.frame_bytes)
            .try_acquire_many_owned(permits)
            .ok()
    }
}

impl Drop for ConnectionAdmission {
    fn drop(&mut self) {
        let mut topics = self.runtime.topics.lock();
        let remove = topics.get_mut(&self.topic).is_some_and(|count| {
            *count = count.saturating_sub(1);
            *count == 0
        });
        if remove {
            topics.remove(&self.topic);
        }
    }
}

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

async fn authenticate_header(
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
    headers: &HeaderMap,
) -> Result<SessionAuth, ApiError> {
    let Some(authenticator) = state.authenticator.as_deref() else {
        return Ok(SessionAuth {
            session: None,
            secret: None,
        });
    };
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(SessionAuth {
            session: None,
            secret: None,
        });
    };
    let secret = value
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|secret| !secret.is_empty())
        .ok_or_else(|| {
            observe_auth_failure(state);
            ApiError {
                status: StatusCode::UNAUTHORIZED,
                code: "E_AUTH_FAILED",
                detail: "Authorization must use a non-empty Bearer token".into(),
            }
        })?
        .to_owned();
    let session = authenticate(authenticator, peer, topic, &secret)
        .await
        .map_err(|error| auth_api_error(state, error))?;
    Ok(SessionAuth {
        session: Some(session),
        secret: Some(secret),
    })
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

#[allow(clippy::too_many_arguments)]
async fn deliver_one(
    socket: &mut WebSocket,
    state: &AppState,
    topic: &str,
    channel: &Arc<str>,
    delivery: Delivery,
    guard: &mut DeliveryGuard,
    policy_changes: &mut tokio::sync::watch::Receiver<TopicPolicy>,
    reliable_policy_epoch: u64,
) -> anyhow::Result<bool> {
    let Some(expiration_ns) = state
        .broker
        .delivery_expiration_ns(topic, delivery.timestamp_ns)?
    else {
        anyhow::bail!("Topic stopped using TTL_DISCARD");
    };
    if remaining_until(expiration_ns).is_zero() {
        return Ok(false);
    }
    let frame_bytes = FRAME_HEADER_BYTES.saturating_add(delivery.body.len());
    let Some(_frame_hold) = state.websocket.reserve_frame(frame_bytes) else {
        state
            .metrics
            .websocket_capacity_rejections
            .fetch_add(1, Ordering::Relaxed);
        close_control(
            socket,
            "E_WS_CAPACITY",
            "WebSocket frame memory capacity is exhausted",
            1013,
        )
        .await;
        anyhow::bail!("WebSocket frame memory capacity is exhausted");
    };
    let frame = encode_delivery(&delivery);
    let write_deadline = Instant::now()
        .checked_add(Duration::from_millis(
            state.config.limits.tcp_command_timeout_ms,
        ))
        .unwrap_or_else(Instant::now);
    let send = socket.send(Message::Binary(frame));
    tokio::pin!(send);
    let mut shutdown = state.shutdown.clone();
    loop {
        let expiration_ns = state
            .broker
            .delivery_expiration_ns(topic, delivery.timestamp_ns)?
            .ok_or_else(|| anyhow::anyhow!("Topic stopped using TTL_DISCARD"))?;
        let ttl_remaining = remaining_until(expiration_ns);
        let socket_remaining = write_deadline.saturating_duration_since(Instant::now());
        if ttl_remaining.is_zero() {
            anyhow::bail!("Message TTL expired during WebSocket frame write");
        }
        if socket_remaining.is_zero() {
            state
                .metrics
                .websocket_slow_disconnects
                .fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("WebSocket frame write timed out");
        }
        let timeout = ttl_remaining.min(socket_remaining);
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                anyhow::bail!("Broker shut down during WebSocket frame write");
            }
            changed = policy_changes.changed() => {
                if changed.is_err() {
                    anyhow::bail!("Topic closed during WebSocket frame write");
                }
                if reliable_policy_changed(state, topic, reliable_policy_epoch)? {
                    anyhow::bail!("Topic switched to RELIABLE during WebSocket frame write");
                }
                if policy_changes.borrow_and_update().delivery_mode != DeliveryMode::TtlDiscard {
                    anyhow::bail!("Topic stopped using TTL_DISCARD during WebSocket frame write");
                }
                continue;
            }
            result = &mut send => {
                result?;
                break;
            }
            _ = tokio::time::sleep(timeout) => {
                if socket_remaining <= ttl_remaining {
                    state.metrics.websocket_slow_disconnects.fetch_add(1, Ordering::Relaxed);
                    anyhow::bail!("WebSocket frame write timed out");
                }
                anyhow::bail!("Message TTL expired during WebSocket frame write");
            }
        }
    }
    let Some(token) = guard.accept_with_token(delivery.id) else {
        anyhow::bail!("WebSocket delivery reservation disappeared");
    };
    state
        .broker
        .finish_delivery_shared(topic, Arc::clone(channel), delivery.id, token)
        .await?;
    state
        .metrics
        .websocket_messages
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .websocket_bytes
        .fetch_add(delivery.body.len() as u64, Ordering::Relaxed);
    Ok(true)
}

fn reliable_policy_changed(
    state: &AppState,
    topic: &str,
    expected_epoch: u64,
) -> Result<bool, BrokerError> {
    Ok(state.broker.topic_reliable_policy_epoch(topic)? != expected_epoch)
}

fn encode_delivery(delivery: &Delivery) -> bytes::Bytes {
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_BYTES + delivery.body.len());
    frame.extend_from_slice(FRAME_MAGIC);
    frame.put_u16(FRAME_HEADER_BYTES as u16);
    frame.put_u16(delivery.attempts);
    frame.put_i64(delivery.timestamp_ns);
    frame.put_u64(delivery.id);
    frame.extend_from_slice(&delivery.body);
    frame.freeze()
}

async fn read_auth(
    socket: &mut WebSocket,
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
) -> Result<SessionAuth, ControlAuthError> {
    let message = tokio::time::timeout(
        Duration::from_millis(state.config.limits.auth_timeout_ms),
        socket.recv(),
    )
    .await
    .map_err(|_| ControlAuthError {
        code: "E_AUTH_TIMEOUT",
        detail: "WebSocket AUTH timed out".into(),
    })?
    .ok_or_else(|| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: "WebSocket closed before AUTH".into(),
    })?
    .map_err(|error| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: error.to_string(),
    })?;
    let Message::Text(text) = message else {
        return Err(ControlAuthError {
            code: "E_BAD_AUTH",
            detail: "WebSocket AUTH must be a text control frame".into(),
        });
    };
    let request: AuthControl =
        serde_json::from_str(text.as_str()).map_err(|error| ControlAuthError {
            code: "E_BAD_AUTH",
            detail: error.to_string(),
        })?;
    if request.kind != "auth" || request.secret.is_empty() {
        return Err(ControlAuthError {
            code: "E_BAD_AUTH",
            detail: "WebSocket AUTH frame is invalid".into(),
        });
    }
    let authenticator = state
        .authenticator
        .as_deref()
        .ok_or_else(|| ControlAuthError {
            code: "E_AUTH_DISABLED",
            detail: "WebSocket AUTH is disabled".into(),
        })?;
    let session = authenticate(authenticator, peer, topic, &request.secret)
        .await
        .map_err(|error| ControlAuthError {
            code: auth_error_code(&error),
            detail: "WebSocket AUTH failed".into(),
        })?;
    Ok(SessionAuth {
        session: Some(session),
        secret: Some(request.secret),
    })
}

async fn refresh_auth(
    auth: &mut SessionAuth,
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
) -> Result<(), ControlAuthError> {
    let Some(session) = auth.session.as_ref() else {
        return Ok(());
    };
    if !session.is_expired() {
        ensure_authorized(Some(session), topic).map_err(|_| ControlAuthError {
            code: "E_UNAUTHORIZED",
            detail: "WebSocket subscription is no longer authorized".into(),
        })?;
        return Ok(());
    }
    let authenticator = state
        .authenticator
        .as_deref()
        .ok_or_else(|| ControlAuthError {
            code: "E_AUTH_FAILED",
            detail: "WebSocket AUTH configuration is unavailable".into(),
        })?;
    let secret = auth.secret.as_deref().ok_or_else(|| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: "WebSocket AUTH state is unavailable".into(),
    })?;
    let session = authenticate(authenticator, peer, topic, secret)
        .await
        .map_err(|error| {
            observe_auth_failure(state);
            ControlAuthError {
                code: auth_error_code(&error),
                detail: "WebSocket AUTH refresh failed".into(),
            }
        })?;
    auth.session = Some(session);
    Ok(())
}

async fn authenticate(
    authenticator: &Authenticator,
    peer: SocketAddr,
    topic: &str,
    secret: &str,
) -> Result<AuthSession, AuthError> {
    let session = authenticator
        .authenticate(&peer.ip().to_string(), false, "", secret.as_bytes())
        .await?;
    if !session.can_subscribe(topic, AUTH_CHANNEL) {
        return Err(AuthError::Unauthorized);
    }
    Ok(session)
}

fn ensure_authorized(session: Option<&AuthSession>, topic: &str) -> anyhow::Result<()> {
    if session.is_some_and(|session| !session.can_subscribe(topic, AUTH_CHANNEL)) {
        anyhow::bail!("WebSocket subscription is not authorized");
    }
    Ok(())
}

fn auth_api_error(state: &AppState, error: AuthError) -> ApiError {
    observe_auth_failure(state);
    let (status, code, detail) = match error {
        AuthError::Unauthorized => (
            StatusCode::FORBIDDEN,
            "E_UNAUTHORIZED",
            "AUTH does not permit this Topic subscription",
        ),
        AuthError::Overloaded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "E_AUTH_OVERLOADED",
            "AUTH capacity is exhausted",
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "E_AUTH_FAILED",
            "AUTH service failed",
        ),
    };
    ApiError {
        status,
        code,
        detail: detail.into(),
    }
}

fn observe_auth_failure(state: &AppState) {
    state
        .metrics
        .websocket_auth_failures
        .fetch_add(1, Ordering::Relaxed);
    state.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
}

fn auth_error_code(error: &AuthError) -> &'static str {
    match error {
        AuthError::Unauthorized => "E_UNAUTHORIZED",
        AuthError::Overloaded => "E_AUTH_OVERLOADED",
        _ => "E_AUTH_FAILED",
    }
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

    fn runtime(max_connections: usize, max_connections_per_topic: usize) -> WebSocketRuntime {
        let config = crate::config::WebSocketConfig {
            max_connections,
            max_connections_per_topic,
            ..Default::default()
        };
        WebSocketRuntime::new(&config)
    }

    #[test]
    fn encodes_rqw1_golden_frame() {
        let delivery = Delivery {
            id: 0x0102_0304_0506_0708,
            timestamp_ns: -0x0102_0304_0506_0708,
            attempts: 0x090a,
            body: Arc::<[u8]>::from(&b"hello"[..]),
        };

        let frame = encode_delivery(&delivery);

        assert_eq!(
            frame.as_ref(),
            &[
                b'R', b'Q', b'W', b'1', 0, 24, 9, 10, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8,
                0xf8, 1, 2, 3, 4, 5, 6, 7, 8, b'h', b'e', b'l', b'l', b'o',
            ]
        );
    }

    #[test]
    fn admission_enforces_global_and_per_topic_limits_and_releases() {
        let runtime = runtime(2, 1);
        let (_, first) = runtime.admit("events").unwrap();
        assert!(matches!(
            runtime.admit("events"),
            Err(AdmissionError::Topic)
        ));
        let (_, second) = runtime.admit("other").unwrap();
        assert!(matches!(
            runtime.admit("third"),
            Err(AdmissionError::Global)
        ));

        drop(first);
        let (_, replacement) = runtime.admit("events").unwrap();
        assert!(matches!(
            runtime.admit("other"),
            Err(AdmissionError::Global)
        ));

        drop(second);
        drop(replacement);
        assert!(runtime.admit("events").is_ok());
    }

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
