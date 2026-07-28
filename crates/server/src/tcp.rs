mod authorization;
mod channel_ops;
mod codec;
mod commands;
mod dead_letter;
mod ephemeral;
mod session;
mod time;
mod writer;

use authorization::*;
use channel_ops::*;
use codec::*;
use commands::*;
use dead_letter::*;
use ephemeral::*;
use session::*;
use time::*;
use writer::*;

use crate::admission::{ConnectionBudget, PublishAdmission, PublishReservation};
use crate::auth::{AuthError, AuthSession, Authenticator};
use crate::compression::{self, BoxIo};
use crate::config::Config;
use crate::metrics::Metrics;
use crate::subscriptions::{ClientIdentity, SubscriptionLease, SubscriptionRegistry};
use crate::tls;
use anyhow::Context;
use bytes::Bytes;
use rustqueue_protocol::{
    encode_frame, encode_message_header, parse_mpub_bytes, Command, CommandError, FrameType,
    IdentifyRequest, IdentifyResponse, CLOSE_WAIT, HEARTBEAT, MAGIC_V2, OK,
};
use rustqueue_queue::{Broker, BrokerError, DeliveryGuard};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

pub(crate) fn broker_storage_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::StorageUnavailable
            | BrokerError::Storage(_)
            | BrokerError::Io(_)
            | BrokerError::InvalidRecord(_)
    )
}

fn renew_delivery_lease(
    broker: &Broker,
    topic: &str,
    channel: &str,
    id: u64,
    token: u64,
    timeout: Duration,
) -> Result<Instant, BrokerError> {
    // Keep the session deadline slightly earlier than the broker deadline so a
    // client operation cannot pass the local check after its broker lease has
    // already expired.
    let deadline = Instant::now() + timeout;
    broker.touch_delivery(topic, channel, id, token, Some(timeout))?;
    Ok(deadline)
}

#[derive(Debug)]
struct ParsedCommand {
    command: Command,
    body: Option<Bytes>,
    publish_reservation: Option<PublishReservation>,
}

#[derive(Clone, Copy)]
enum Compression {
    Snappy,
    Deflate(i32),
}

struct Subscription {
    topic: String,
    channel: String,
    lease: SubscriptionLease,
}

const MAX_FETCH_MESSAGES: u16 = 64;
const DEFAULT_FETCH_WAIT_MS: u32 = 100;

#[derive(Clone, Debug)]
struct FetchRequest {
    topic: String,
    channel: String,
    timeout_ms: u64,
    max_messages: u16,
    max_bytes: u32,
    wait_ms: u32,
}

struct FetchResponse {
    deliveries: Vec<RemoteDelivery>,
    delivery_guard: DeliveryGuard,
}

#[derive(Clone, Debug)]
struct RemoteDelivery {
    id: u64,
    timestamp_ns: i64,
    attempts: u16,
    body: Bytes,
}

#[derive(Clone, Copy, Debug)]
struct InFlightDelivery {
    deadline: Instant,
    token: u64,
}

struct SessionState {
    identified: bool,
    encrypted: bool,
    tls_common_name: String,
    heartbeat: Option<Duration>,
    message_timeout: Duration,
    output_buffer_size: usize,
    output_buffer_timeout: Option<Duration>,
    sample_rate: u8,
    sample_cursor: u8,
    auth: Option<AuthSession>,
    auth_secret: Option<Bytes>,
    _auth_reservation: Option<PublishReservation>,
    subscription: Option<Subscription>,
    rdy: u64,
    in_flight: HashMap<u64, InFlightDelivery>,
    pending_channel_ops: HashSet<u64>,
    closing: bool,
    client_identity: ClientIdentity,
}

impl SessionState {
    fn accept_sample(&mut self) -> bool {
        if self.sample_rate == 0 {
            return true;
        }
        let selected = self.sample_cursor < self.sample_rate;
        self.sample_cursor = (self.sample_cursor + 1) % 100;
        selected
    }

    fn update_subscription_flow(&self) {
        if let Some(subscription) = &self.subscription {
            subscription
                .lease
                .update_flow(self.rdy, self.in_flight.len());
        }
    }

    fn delivery_for_operation(&self, id: u64) -> Option<InFlightDelivery> {
        if self.pending_channel_ops.contains(&id) {
            return None;
        }
        self.in_flight.get(&id).copied()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
    mut shutdown: watch::Receiver<bool>,
    shutdown_grace: Duration,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.network.tcp_address).await?;
    let tls_acceptor = tls::acceptor(config.security.tls.as_ref())?;
    let authenticator = Authenticator::new(&config)
        .map_err(anyhow::Error::msg)?
        .map(Arc::new);
    let permits = Arc::new(Semaphore::new(config.limits.max_connections));
    let ephemeral_consumers = EphemeralConsumers::default();
    info!(address = %config.network.tcp_address, "NSQ TCP listener ready");

    let mut sessions = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => Some(result?),
            result = sessions.join_next(), if !sessions.is_empty() => {
                log_session_result(result);
                None
            }
            _ = shutdown.changed() => break,
        };
        let Some((stream, peer)) = accepted else {
            continue;
        };
        if *shutdown.borrow() {
            break;
        }
        let permit = match Arc::clone(&permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(%peer, "rejecting connection because the connection limit is reached");
                continue;
            }
        };
        let config = Arc::clone(&config);
        let broker = Arc::clone(&broker);
        let metrics = Arc::clone(&metrics);
        let tls_acceptor = tls_acceptor.clone();
        let authenticator = authenticator.clone();
        let ephemeral_consumers = ephemeral_consumers.clone();
        let accepting = Arc::clone(&accepting);
        let delivering = Arc::clone(&delivering);
        let publish_admission = Arc::clone(&publish_admission);
        let subscriptions = subscriptions.clone();
        sessions.spawn(async move {
            let _permit = permit;
            metrics.tcp_connections.fetch_add(1, Ordering::Relaxed);
            if let Err(error) = handle_connection(
                stream,
                peer,
                &config,
                &broker,
                &metrics,
                tls_acceptor,
                authenticator,
                ephemeral_consumers,
                accepting,
                delivering,
                publish_admission,
                subscriptions,
            )
            .await
            {
                debug!(%peer, %error, "client connection closed with error");
            }
            metrics.tcp_connections.fetch_sub(1, Ordering::Relaxed);
        });
    }
    info!("NSQ TCP listener stopped accepting new connections");
    if tokio::time::timeout(shutdown_grace, drain_sessions(&mut sessions))
        .await
        .is_err()
    {
        warn!(
            active_sessions = sessions.len(),
            "NSQ TCP shutdown grace expired"
        );
        sessions.abort_all();
        drain_sessions(&mut sessions).await;
    }
    Ok(())
}

async fn drain_sessions(sessions: &mut JoinSet<()>) {
    while let Some(result) = sessions.join_next().await {
        log_session_result(Some(result));
    }
}

fn log_session_result(result: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = result {
        warn!(%error, "NSQ TCP session task failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    config: &Config,
    broker: &Broker,
    metrics: &Metrics,
    tls_acceptor: Option<TlsAcceptor>,
    authenticator: Option<Arc<Authenticator>>,
    ephemeral_consumers: EphemeralConsumers,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;
    let handshake_timeout = Duration::from_millis(config.limits.client_handshake_timeout_ms);
    let mut magic = [0u8; 4];
    tokio::time::timeout(handshake_timeout, stream.read_exact(&mut magic))
        .await
        .context("client magic timeout")??;
    if &magic != MAGIC_V2 {
        anyhow::bail!("unsupported protocol magic");
    }

    let connection_budget = Arc::new(ConnectionBudget::new(
        config.limits.connection_publish_inflight_bytes,
    ));
    let first =
        match read_initial_command(&mut stream, config, &publish_admission, &connection_budget)
            .await
        {
            Ok(command) => command,
            Err(CommandReadError::Io(error)) => return Err(error.into()),
            Err(CommandReadError::Protocol { code, detail }) => {
                if disconnect_on_retriable_protocol_error(config, code) {
                    return Ok(());
                }
                write_error(&mut stream, code, &detail).await?;
                return Ok(());
            }
        };
    let mut state = SessionState {
        identified: false,
        encrypted: false,
        tls_common_name: String::new(),
        heartbeat: Some(Duration::from_millis(config.limits.heartbeat_interval_ms)),
        message_timeout: config.message_timeout(),
        output_buffer_size: config.limits.output_buffer_size,
        output_buffer_timeout: Some(Duration::from_millis(
            config.limits.output_buffer_timeout_ms,
        )),
        sample_rate: 0,
        sample_cursor: 0,
        auth: None,
        auth_secret: None,
        _auth_reservation: None,
        subscription: None,
        rdy: 0,
        in_flight: HashMap::new(),
        pending_channel_ops: HashSet::new(),
        closing: false,
        client_identity: ClientIdentity {
            remote_address: peer.to_string(),
            ..ClientIdentity::default()
        },
    };

    let (mut io, pending, negotiated): (BoxIo, Option<ParsedCommand>, Option<Compression>) =
        if matches!(first.command, Command::Identify) {
            let identify = match parse_identify(first.body.as_deref().unwrap_or_default(), config) {
                Ok(identify) => identify,
                Err(error) => {
                    write_error(&mut stream, "E_BAD_BODY", &error.to_string()).await?;
                    return Ok(());
                }
            };
            state.identified = true;
            let delivery_settings = (|| {
                Ok::<_, anyhow::Error>((
                    identify_heartbeat(&identify, config)?,
                    identify_message_timeout(&identify, config)?,
                    identify_output_buffer(&identify, config)?,
                    identify_sample_rate(&identify)?,
                ))
            })();
            let (heartbeat, message_timeout, output, sample_rate) = match delivery_settings {
                Ok(settings) => settings,
                Err(error) => {
                    write_error(&mut stream, "E_BAD_BODY", &error.to_string()).await?;
                    return Ok(());
                }
            };
            state.heartbeat = heartbeat;
            state.message_timeout = message_timeout;
            state.output_buffer_size = output.size;
            state.output_buffer_timeout = output.timeout;
            state.sample_rate = sample_rate;
            state.client_identity.client_id = identify.client_id.clone();
            state.client_identity.hostname = identify.hostname.clone();
            state.client_identity.user_agent = identify.user_agent.clone();
            state.client_identity.sample_rate = sample_rate;
            let use_tls = identify.feature_negotiation && identify.tls_v1 && tls_acceptor.is_some();
            let negotiated = match negotiate_compression(&identify, config) {
                Ok(compression) => compression,
                Err(error) => {
                    write_error(&mut stream, "E_IDENTIFY_FAILED", &error.to_string()).await?;
                    return Ok(());
                }
            };
            let response = identify_response(config, use_tls, negotiated, &state);
            if identify.feature_negotiation {
                write_frame(
                    &mut stream,
                    FrameType::Response,
                    &serde_json::to_vec(&response)?,
                )
                .await?;
            } else {
                write_frame(&mut stream, FrameType::Response, OK).await?;
            }
            if use_tls {
                let acceptor = tls_acceptor.expect("TLS availability was checked");
                let tls_stream = tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
                    .await
                    .context("TLS handshake timeout")?
                    .context("TLS handshake")?;
                state.tls_common_name = tls::peer_common_name(&tls_stream);
                let mut io: BoxIo = Box::new(tls_stream);
                write_frame(&mut io, FrameType::Response, OK).await?;
                state.encrypted = true;
                state.client_identity.tls = true;
                (io, None, negotiated)
            } else {
                (Box::new(stream), None, negotiated)
            }
        } else {
            (Box::new(stream), Some(first), None)
        };

    if config.security.tls.as_ref().is_some_and(|tls| tls.required) && !state.encrypted {
        write_frame(&mut io, FrameType::Error, b"E_TLS_REQUIRED TLS is required").await?;
        return Ok(());
    }

    if let Some(compression) = negotiated {
        state.client_identity.snappy = matches!(compression, Compression::Snappy);
        state.client_identity.deflate = matches!(compression, Compression::Deflate(_));
        io = match compression {
            Compression::Snappy => compression::snappy(io),
            Compression::Deflate(level) => compression::deflate(io, level),
        };
        write_frame(&mut io, FrameType::Response, OK).await?;
    }

    run_session(
        io,
        pending,
        peer,
        config,
        broker,
        metrics,
        authenticator,
        ephemeral_consumers,
        accepting,
        delivering,
        publish_admission,
        connection_budget,
        subscriptions,
        state,
    )
    .await
}

fn disconnect_on_retriable_protocol_error(config: &Config, code: &str) -> bool {
    config.limits.disconnect_on_retriable_publish_error && code == "E_THROTTLED"
}
