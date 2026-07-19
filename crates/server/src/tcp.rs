mod authorization;
mod codec;
mod commands;
mod dead_letter;
mod ephemeral;
mod session;
mod time;
mod writer;

use authorization::*;
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
use crate::tls;
use anyhow::Context;
use bytes::Bytes;
use rustqueue_protocol::{
    encode_frame, encode_message_header, parse_mpub_bytes, Command, CommandError, FrameType,
    IdentifyRequest, IdentifyResponse, CLOSE_WAIT, HEARTBEAT, MAGIC_V2, OK,
};
use rustqueue_queue::{Broker, BrokerError, DeliveryGuard};
use serde_json::json;
use std::collections::HashMap;
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
use tokio::sync::Semaphore;
use tokio::time::{interval, MissedTickBehavior};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

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
    auth_secret: Option<Vec<u8>>,
    subscription: Option<Subscription>,
    rdy: u64,
    in_flight: HashMap<u64, Instant>,
    closing: bool,
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
}

pub async fn serve(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.network.tcp_address).await?;
    let tls_acceptor = tls::acceptor(config.security.tls.as_ref())?;
    let authenticator = Authenticator::new(&config)
        .map_err(anyhow::Error::msg)?
        .map(Arc::new);
    let permits = Arc::new(Semaphore::new(config.limits.max_connections));
    let ephemeral_consumers = EphemeralConsumers::default();
    info!(address = %config.network.tcp_address, "NSQ TCP listener ready");

    loop {
        let (stream, peer) = listener.accept().await?;
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
        tokio::spawn(async move {
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
            )
            .await
            {
                debug!(%peer, %error, "client connection closed with error");
            }
            metrics.tcp_connections.fetch_sub(1, Ordering::Relaxed);
        });
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
    let first = match tokio::time::timeout(
        handshake_timeout,
        read_initial_command(&mut stream, config, &publish_admission, &connection_budget),
    )
    .await
    .context("initial command timeout")?
    {
        Ok(command) => command,
        Err(CommandReadError::Io(error)) => return Err(error.into()),
        Err(CommandReadError::Protocol { code, detail }) => {
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
        subscription: None,
        rdy: 0,
        in_flight: HashMap::new(),
        closing: false,
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
        state,
    )
    .await
}
