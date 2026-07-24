use crate::backend::{Backend, BackendPool};
use crate::metrics::ProxyMetrics;
use axum::body::Bytes;
use rustqueue_protocol::{
    encode_frame, Command, FrameType, IdentifyRequest, IdentifyResponse, HEARTBEAT, MAGIC_V2, OK,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

const MAX_COMMAND_LINE_BYTES: usize = 1024;
const MAX_CONTROL_BODY_BYTES: usize = 1024 * 1024;
const MAX_BACKEND_FRAME_BYTES: usize = 1024 * 1024;
const BACKEND_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_IDENTIFY_TIMEOUT: Duration = Duration::from_secs(5);
// Kodo's go-nsq producer waits at most 60 seconds for the response. Two
// pre-commit attempts, including both handshakes and the final response, must
// remain inside that client envelope while still allowing a 100 MiB body to
// cross a temporarily congested cluster network.
const BACKEND_WRITE_TIMEOUT: Duration = Duration::from_secs(12);
const BACKEND_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_PUBLISH_ATTEMPTS: usize = 3;
const PUBLISH_DEADLINE: Duration = Duration::from_secs(55);
const NEW_ATTEMPT_BUDGET: Duration = Duration::from_secs(34);
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);

struct ClientCommand {
    command: Command,
    line: Vec<u8>,
    body: Option<Bytes>,
    _permit: Option<OwnedSemaphorePermit>,
}

struct BrokerSession {
    backend: Backend,
    stream: TcpStream,
}

pub(super) struct Limits {
    pub max_message_bytes: usize,
    pub max_body_bytes: usize,
    pub command_timeout: Duration,
    pub inflight_bytes: Arc<Semaphore>,
}

enum Attempt {
    Success,
    Retry(Vec<u8>),
    Final(Vec<u8>),
    Ambiguous(String),
}

#[derive(Debug, Eq, PartialEq)]
enum PublishFailure {
    Final(Vec<u8>),
    RetryByReconnect,
    Ambiguous(Vec<u8>),
}

pub(super) async fn run(
    mut client: TcpStream,
    pool: BackendPool,
    limits: Limits,
    metrics: ProxyMetrics,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut magic = [0; 4];
    tokio::select! {
        biased;
        _ = shutdown.changed() => return Ok(()),
        result = tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut magic)) => {
            result??;
        }
    }
    if &magic != MAGIC_V2 {
        return Ok(());
    }
    let (mut reader, mut writer) = client.into_split();
    let (commands, mut incoming) = tokio::sync::mpsc::channel(1);
    let reader_task = tokio::spawn(async move {
        loop {
            let command = read_command(
                &mut reader,
                limits.max_message_bytes,
                limits.max_body_bytes,
                limits.command_timeout,
                &limits.inflight_bytes,
            )
            .await;
            let failed = command.is_err();
            if commands.send(command).await.is_err() || failed {
                break;
            }
        }
    });
    let _reader = AbortOnDrop(reader_task.abort_handle());
    let mut identified = false;
    let mut heartbeat_interval = Some(DEFAULT_HEARTBEAT);
    let mut heartbeat = Box::pin(tokio::time::sleep(DEFAULT_HEARTBEAT));
    let mut backend_identify = default_backend_identify();
    let mut backend_session = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            command = incoming.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command = match command {
                    Ok(command) => command,
                    Err(ReadError::RetryByReconnect(detail)) => {
                        tracing::debug!(
                            detail,
                            "closing Kodo producer connection for safe Gateway failover"
                        );
                        break;
                    }
                    Err(error) => {
                        write_error(&mut writer, error.code(), &error.to_string()).await?;
                        break;
                    }
                };
                reset_heartbeat(&mut heartbeat, heartbeat_interval);
                match command.command {
                    Command::Identify if !identified => {
                        let body = command.body.as_deref().unwrap_or_default();
                        let request: IdentifyRequest = match serde_json::from_slice(body) {
                            Ok(request) => request,
                            Err(_) => {
                                write_error(&mut writer, "E_BAD_BODY", "IDENTIFY body is invalid").await?;
                                break;
                            }
                        };
                        if request.tls_v1 || request.snappy || request.deflate {
                            write_error(
                                &mut writer,
                                "E_IDENTIFY_FAILED",
                                "Kodo publish gateways do not negotiate TLS or compression",
                            )
                            .await?;
                            break;
                        }
                        heartbeat_interval = match request.heartbeat_interval {
                            Some(-1) => None,
                            Some(value) if (1_000..=300_000).contains(&value) => {
                                Some(Duration::from_millis(value as u64))
                            }
                            Some(_) => {
                                write_error(
                                    &mut writer,
                                    "E_IDENTIFY_FAILED",
                                    "heartbeat_interval is outside 1000..=300000",
                                )
                                .await?;
                                break;
                            }
                            None => Some(DEFAULT_HEARTBEAT),
                        };
                        backend_identify = backend_identify_body(body)?;
                        identified = true;
                        reset_heartbeat(&mut heartbeat, heartbeat_interval);
                        let response = IdentifyResponse {
                            max_rdy_count: 2_500,
                            version: env!("CARGO_PKG_VERSION").into(),
                            max_msg_timeout: 900_000,
                            msg_timeout: request.msg_timeout.unwrap_or(60_000).max(1_000),
                            tls_v1: false,
                            deflate: false,
                            deflate_level: 0,
                            max_deflate_level: 0,
                            snappy: false,
                            sample_rate: 0,
                            auth_required: false,
                            output_buffer_size: 16_384,
                            output_buffer_timeout: 250,
                        };
                        write_frame(
                            &mut writer,
                            FrameType::Response,
                            &serde_json::to_vec(&response)?,
                        )
                        .await?;
                    }
                    Command::Identify => {
                        write_error(&mut writer, "E_INVALID", "IDENTIFY may only be sent once").await?;
                        break;
                    }
                    Command::Publish { .. }
                    | Command::MultiPublish { .. }
                    | Command::DeferredPublish { .. } => {
                        let response = publish(
                            &pool,
                            &backend_identify,
                            &command,
                            &mut backend_session,
                            &metrics,
                        )
                        .await;
                        match response {
                            Ok(()) => write_frame(&mut writer, FrameType::Response, OK).await?,
                            Err(PublishFailure::Final(error)) => {
                                write_frame(&mut writer, FrameType::Error, &error).await?;
                            }
                            // Kodo retries another advertised Gateway only for a
                            // connection error. Every backend attempt represented
                            // here failed before commit, so closing is both safe
                            // and necessary for cross-Gateway failover.
                            Err(PublishFailure::RetryByReconnect) => break,
                            Err(PublishFailure::Ambiguous(error)) => {
                                write_frame(&mut writer, FrameType::Error, &error).await?;
                                break;
                            }
                        }
                    }
                    Command::Noop => {}
                    Command::Auth => {
                        write_error(&mut writer, "E_AUTH_DISABLED", "AUTH disabled").await?;
                        break;
                    }
                    _ => {
                        write_error(
                            &mut writer,
                            "E_INVALID",
                            "publish Gateway accepts only IDENTIFY, PUB, MPUB, DPUB, and NOP",
                        )
                        .await?;
                        break;
                    }
                }
            }
            _ = &mut heartbeat, if heartbeat_interval.is_some() => {
                write_frame(&mut writer, FrameType::Response, HEARTBEAT).await?;
                reset_heartbeat(&mut heartbeat, heartbeat_interval);
            }
        }
    }
    Ok(())
}

async fn publish(
    pool: &BackendPool,
    identify: &[u8],
    command: &ClientCommand,
    current: &mut Option<BrokerSession>,
    metrics: &ProxyMetrics,
) -> Result<(), PublishFailure> {
    let deadline = Instant::now() + PUBLISH_DEADLINE;
    let available = pool.all();
    let mut attempted = BTreeSet::new();
    let mut last_retry = None;

    if current
        .as_ref()
        .is_some_and(|session| available.contains(&session.backend))
    {
        let mut session = current.take().expect("checked current session");
        attempted.insert(session.backend.node_id);
        match attempt(&mut session.stream, command).await {
            Attempt::Success => {
                *current = Some(session);
                return Ok(());
            }
            Attempt::Retry(error) => {
                metrics.producer_retries.fetch_add(1, Ordering::Relaxed);
                last_retry = Some(error);
            }
            Attempt::Final(error) => return Err(PublishFailure::Final(error)),
            Attempt::Ambiguous(detail) => {
                metrics
                    .producer_ambiguous_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(PublishFailure::Ambiguous(
                    format!("E_AMBIGUOUS {detail}").into_bytes(),
                ));
            }
        }
    } else {
        *current = None;
    }

    for backend in pool.shuffled(MAX_PUBLISH_ATTEMPTS) {
        if attempted.len() >= MAX_PUBLISH_ATTEMPTS
            || deadline.saturating_duration_since(Instant::now()) < NEW_ATTEMPT_BUDGET
        {
            break;
        }
        if !attempted.insert(backend.node_id) {
            continue;
        }
        let mut session = match connect_backend(backend, identify, metrics).await {
            Ok(session) => session,
            Err(error) => {
                tracing::debug!(%error, "publish Gateway backend handshake failed");
                continue;
            }
        };
        match attempt(&mut session.stream, command).await {
            Attempt::Success => {
                *current = Some(session);
                return Ok(());
            }
            Attempt::Retry(error) => {
                metrics.producer_retries.fetch_add(1, Ordering::Relaxed);
                last_retry = Some(error);
            }
            Attempt::Final(error) => return Err(PublishFailure::Final(error)),
            Attempt::Ambiguous(detail) => {
                metrics
                    .producer_ambiguous_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(PublishFailure::Ambiguous(
                    format!("E_AMBIGUOUS {detail}").into_bytes(),
                ));
            }
        }
    }
    if let Some(error) = last_retry {
        tracing::debug!(
            error = %String::from_utf8_lossy(&error),
            "publish Gateway exhausted pre-commit Broker retries"
        );
    } else {
        tracing::debug!("publish Gateway has no publish-ready Broker");
    }
    Err(PublishFailure::RetryByReconnect)
}

async fn connect_backend(
    backend: Backend,
    identify: &[u8],
    metrics: &ProxyMetrics,
) -> anyhow::Result<BrokerSession> {
    let _timer = metrics.backend.timer();
    let mut stream = tokio::time::timeout(
        BACKEND_CONNECT_TIMEOUT,
        TcpStream::connect(backend.tcp_address()),
    )
    .await??;
    stream.set_nodelay(true)?;
    stream.write_all(MAGIC_V2).await?;
    stream.write_all(b"IDENTIFY\n").await?;
    stream.write_u32(identify.len() as u32).await?;
    stream.write_all(identify).await?;
    stream.flush().await?;
    let (frame_type, body) =
        tokio::time::timeout(BACKEND_IDENTIFY_TIMEOUT, read_frame(&mut stream)).await??;
    if frame_type == FrameType::Error as i32 {
        anyhow::bail!(
            "backend IDENTIFY failed: {}",
            String::from_utf8_lossy(&body)
        );
    }
    if frame_type != FrameType::Response as i32 {
        anyhow::bail!("backend IDENTIFY returned an invalid frame");
    }
    Ok(BrokerSession { backend, stream })
}

async fn attempt(stream: &mut TcpStream, command: &ClientCommand) -> Attempt {
    if send_before_commit(stream, command, BACKEND_WRITE_TIMEOUT)
        .await
        .is_err()
    {
        return Attempt::Retry(
            b"E_PUB_RETRY Broker connection failed before the publish body was complete".to_vec(),
        );
    }
    if !matches!(
        tokio::time::timeout(BACKEND_WRITE_TIMEOUT, stream.flush()).await,
        Ok(Ok(()))
    ) {
        return Attempt::Ambiguous("Broker connection failed after sending the body".into());
    }
    match tokio::time::timeout(BACKEND_TIMEOUT, read_frame(stream)).await {
        Ok(Ok((frame_type, body))) if frame_type == FrameType::Response as i32 && body == OK => {
            Attempt::Success
        }
        Ok(Ok((frame_type, body))) if frame_type == FrameType::Error as i32 => {
            if retriable_error(&body) {
                Attempt::Retry(body)
            } else {
                Attempt::Final(body)
            }
        }
        Ok(Ok(_)) => Attempt::Ambiguous("Broker returned an invalid publish response".into()),
        Ok(Err(error)) => Attempt::Ambiguous(format!("Broker response failed: {error}")),
        Err(_) => Attempt::Ambiguous("Broker publish response timed out".into()),
    }
}

async fn send_before_commit<W>(
    stream: &mut W,
    command: &ClientCommand,
    timeout: Duration,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, async {
        stream.write_all(&command.line).await?;
        if let Some(body) = command.body.as_deref() {
            stream.write_u32(body.len() as u32).await?;
            stream.write_all(body).await?;
        }
        io::Result::Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend publish write timed out"))?
}

fn retriable_error(body: &[u8]) -> bool {
    let error = String::from_utf8_lossy(body);
    error.starts_with("E_DRAINING")
        || error.starts_with("E_CLOSING")
        || error.starts_with("E_THROTTLED")
        || error.starts_with("E_PUB_RETRY")
}

fn backend_identify_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(body)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("IDENTIFY body must be an object"))?;
    object.insert("feature_negotiation".into(), json!(true));
    object.insert("heartbeat_interval".into(), json!(-1));
    object.insert("tls_v1".into(), json!(false));
    object.insert("snappy".into(), json!(false));
    object.insert("deflate".into(), json!(false));
    Ok(serde_json::to_vec(&value)?)
}

fn default_backend_identify() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "feature_negotiation": true,
        "heartbeat_interval": -1,
        "tls_v1": false,
        "snappy": false,
        "deflate": false,
        "user_agent": "rustqueue-kodo-gateway"
    }))
    .expect("static IDENTIFY body")
}

fn reset_heartbeat(heartbeat: &mut std::pin::Pin<Box<Sleep>>, interval: Option<Duration>) {
    heartbeat.as_mut().reset(
        Instant::now() + interval.unwrap_or_else(|| Duration::from_secs(365 * 24 * 60 * 60)),
    );
}

async fn read_command<R>(
    reader: &mut R,
    max_message_bytes: usize,
    max_body_bytes: usize,
    command_timeout: Duration,
    inflight_bytes: &Arc<Semaphore>,
) -> Result<ClientCommand, ReadError>
where
    R: AsyncRead + Unpin,
{
    tokio::time::timeout(command_timeout, async {
        let first = reader.read_u8().await?;
        read_started_command(
            reader,
            first,
            max_message_bytes,
            max_body_bytes,
            inflight_bytes,
        )
        .await
    })
    .await
    .map_err(|_| ReadError::Protocol("E_INVALID", "command read timed out".into()))?
}

async fn read_started_command<R>(
    reader: &mut R,
    first: u8,
    max_message_bytes: usize,
    max_body_bytes: usize,
    inflight_bytes: &Arc<Semaphore>,
) -> Result<ClientCommand, ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut line = Vec::with_capacity(64);
    line.push(first);
    loop {
        if line.last() == Some(&b'\n') {
            break;
        }
        line.push(reader.read_u8().await?);
        if line.len() > MAX_COMMAND_LINE_BYTES {
            return Err(ReadError::Protocol(
                "E_INVALID",
                "command line exceeds limit".into(),
            ));
        }
    }
    let command = Command::parse(&line)
        .map_err(|error| ReadError::Protocol("E_INVALID", error.to_string()))?;
    let limit = match command {
        Command::Identify | Command::Auth => Some(("E_BAD_BODY", MAX_CONTROL_BODY_BYTES)),
        Command::Publish { .. } | Command::DeferredPublish { .. } => {
            Some(("E_BAD_MESSAGE", max_message_bytes))
        }
        Command::MultiPublish { .. } => Some(("E_BAD_BODY", max_body_bytes)),
        _ => None,
    };
    let Some((limit_code, maximum)) = limit else {
        return Ok(ClientCommand {
            command,
            line,
            body: None,
            _permit: None,
        });
    };
    let length = reader.read_u32().await? as usize;
    if length == 0 || length > maximum {
        return Err(ReadError::Protocol(
            limit_code,
            format!("command body size {length} is outside 1..={maximum}"),
        ));
    }
    let permit = Arc::clone(inflight_bytes)
        .try_acquire_many_owned(length as u32)
        .map_err(|_| ReadError::RetryByReconnect("publish Gateway byte budget is exhausted"))?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    let body = Bytes::from(body);
    if matches!(command, Command::MultiPublish { .. }) {
        rustqueue_protocol::parse_mpub_bytes(body.clone(), max_message_bytes)
            .map_err(|error| ReadError::Protocol(error.code(), error.to_string()))?;
    }
    Ok(ClientCommand {
        command,
        line,
        body: Some(body),
        _permit: Some(permit),
    })
}

async fn read_frame<R>(reader: &mut R) -> io::Result<(i32, Vec<u8>)>
where
    R: AsyncRead + Unpin,
{
    let size = reader.read_u32().await? as usize;
    if !(4..=MAX_BACKEND_FRAME_BYTES).contains(&size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backend frame size is invalid",
        ));
    }
    let frame_type = reader.read_i32().await?;
    let mut body = vec![0; size - 4];
    reader.read_exact(&mut body).await?;
    Ok((frame_type, body))
}

async fn write_frame<W>(writer: &mut W, frame_type: FrameType, body: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&encode_frame(frame_type, body)).await?;
    writer.flush().await
}

async fn write_error<W>(writer: &mut W, code: &str, detail: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let detail = detail.replace(['\r', '\n'], " ");
    write_frame(
        writer,
        FrameType::Error,
        format!("{code} {detail}").as_bytes(),
    )
    .await
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug)]
enum ReadError {
    Io(io::Error),
    Protocol(&'static str, String),
    RetryByReconnect(&'static str),
}

impl ReadError {
    fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "E_INVALID",
            Self::Protocol(code, _) => code,
            Self::RetryByReconnect(_) => "E_THROTTLED",
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Protocol(_, detail) => detail.fmt(formatter),
            Self::RetryByReconnect(detail) => detail.fmt(formatter),
        }
    }
}

impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_line(stream: &mut TcpStream) -> Vec<u8> {
        let mut line = Vec::new();
        loop {
            let byte = stream.read_u8().await.unwrap();
            line.push(byte);
            if byte == b'\n' {
                return line;
            }
        }
    }

    async fn mock_backend(listener: tokio::net::TcpListener, response: (FrameType, &'static [u8])) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut magic = [0; 4];
        stream.read_exact(&mut magic).await.unwrap();
        assert_eq!(&magic, MAGIC_V2);
        assert_eq!(read_line(&mut stream).await, b"IDENTIFY\n");
        let length = stream.read_u32().await.unwrap() as usize;
        let mut identify = vec![0; length];
        stream.read_exact(&mut identify).await.unwrap();
        write_frame(&mut stream, FrameType::Response, b"{}")
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream).await, b"PUB events\n");
        let length = stream.read_u32().await.unwrap() as usize;
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(body, b"payload");
        write_frame(&mut stream, response.0, response.1)
            .await
            .unwrap();
    }

    async fn mock_large_backend(listener: tokio::net::TcpListener, expected: usize) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut magic = [0; 4];
        stream.read_exact(&mut magic).await.unwrap();
        assert_eq!(&magic, MAGIC_V2);
        assert_eq!(read_line(&mut stream).await, b"IDENTIFY\n");
        let length = stream.read_u32().await.unwrap() as usize;
        let mut identify = vec![0; length];
        stream.read_exact(&mut identify).await.unwrap();
        write_frame(&mut stream, FrameType::Response, b"{}")
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream).await, b"PUB large\n");
        assert_eq!(stream.read_u32().await.unwrap() as usize, expected);
        let mut remaining = expected;
        let mut buffer = vec![0; 1024 * 1024];
        while remaining > 0 {
            let count = remaining.min(buffer.len());
            stream.read_exact(&mut buffer[..count]).await.unwrap();
            assert!(buffer[..count].iter().all(|byte| *byte == 0x5a));
            remaining -= count;
        }
        write_frame(&mut stream, FrameType::Response, OK)
            .await
            .unwrap();
    }

    fn backend(listener: &tokio::net::TcpListener, node_id: u64) -> Backend {
        Backend {
            broadcast_address: listener.local_addr().unwrap().ip().to_string(),
            tcp_port: listener.local_addr().unwrap().port(),
            http_port: 4151,
            node_id,
        }
    }

    #[test]
    fn only_explicit_node_local_rejections_are_retried() {
        assert!(retriable_error(b"E_DRAINING broker is draining"));
        assert!(retriable_error(b"E_CLOSING broker is shutting down"));
        assert!(retriable_error(
            b"E_THROTTLED local disk is above its publish watermark"
        ));
        assert!(retriable_error(
            b"E_PUB_RETRY active topic publish worker limit reached"
        ));
        assert!(!retriable_error(
            b"E_PUB_FAILED local storage is isolated after an earlier failure"
        ));
        assert!(!retriable_error(
            b"E_PUB_FAILED message exceeds configured limit"
        ));
        assert!(!retriable_error(
            b"E_PUB_FAILED topic is protected by an active deletion tombstone"
        ));
    }

    #[tokio::test]
    async fn no_backend_requests_a_safe_cross_gateway_reconnect() {
        let pool = BackendPool::default();
        let command = ClientCommand {
            command: Command::Publish {
                topic: "events".into(),
            },
            line: b"PUB events\n".to_vec(),
            body: Some(Bytes::from_static(b"payload")),
            _permit: None,
        };
        let mut current = None;
        assert_eq!(
            publish(
                &pool,
                &default_backend_identify(),
                &command,
                &mut current,
                &ProxyMetrics::default(),
            )
            .await,
            Err(PublishFailure::RetryByReconnect)
        );
    }

    #[tokio::test]
    async fn final_backend_rejections_remain_protocol_errors() {
        let rejected = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rejected_backend = backend(&rejected, 1);
        let backend_task = tokio::spawn(mock_backend(
            rejected,
            (
                FrameType::Error,
                b"E_PUB_FAILED message exceeds configured limit",
            ),
        ));
        let pool = BackendPool::default();
        pool.replace(vec![rejected_backend]);
        let command = ClientCommand {
            command: Command::Publish {
                topic: "events".into(),
            },
            line: b"PUB events\n".to_vec(),
            body: Some(Bytes::from_static(b"payload")),
            _permit: None,
        };
        let mut current = None;
        assert_eq!(
            publish(
                &pool,
                &default_backend_identify(),
                &command,
                &mut current,
                &ProxyMetrics::default(),
            )
            .await,
            Err(PublishFailure::Final(
                b"E_PUB_FAILED message exceeds configured limit".to_vec()
            ))
        );
        backend_task.await.unwrap();
    }

    #[tokio::test]
    async fn idle_client_command_is_bounded_before_the_first_byte() {
        let (_client, mut gateway) = tokio::io::duplex(1024);
        let budget = Arc::new(Semaphore::new(4));
        let error = match read_command(&mut gateway, 1024, 1024, Duration::from_millis(10), &budget)
            .await
        {
            Ok(_) => panic!("idle client command was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, ReadError::Protocol("E_INVALID", _)));
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn partial_client_body_releases_its_budget_after_timeout() {
        let (mut client, mut gateway) = tokio::io::duplex(1024);
        client.write_all(b"PUB events\n\0\0\0\x04x").await.unwrap();
        let budget = Arc::new(Semaphore::new(4));
        let error = match read_command(&mut gateway, 1024, 1024, Duration::from_millis(10), &budget)
            .await
        {
            Ok(_) => panic!("partial publish body was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, ReadError::Protocol("E_INVALID", _)));
        assert_eq!(budget.available_permits(), 4);
    }

    #[tokio::test]
    async fn exhausted_ingress_budget_closes_without_a_protocol_error() {
        let pool = BackendPool::default();
        let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway_address = gateway_listener.local_addr().unwrap();
        let gateway_task = tokio::spawn(async move {
            let (client, _) = gateway_listener.accept().await.unwrap();
            let (_shutdown_tx, shutdown) = watch::channel(false);
            run(
                client,
                pool,
                Limits {
                    max_message_bytes: 1024,
                    max_body_bytes: 1024,
                    command_timeout: Duration::from_secs(1),
                    inflight_bytes: Arc::new(Semaphore::new(1)),
                },
                ProxyMetrics::default(),
                shutdown,
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(gateway_address).await.unwrap();
        client.write_all(MAGIC_V2).await.unwrap();
        client.write_all(b"PUB events\n\0\0\0\x02").await.unwrap();
        let mut frame = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.read(&mut frame))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        gateway_task.await.unwrap();
    }

    #[tokio::test]
    async fn single_message_limit_is_independent_from_the_batch_body_limit() {
        let (mut client, mut gateway) = tokio::io::duplex(1024);
        client
            .write_all(b"PUB events\n\0\0\0\x05hello")
            .await
            .unwrap();
        let budget = Arc::new(Semaphore::new(64));
        let error = match read_command(&mut gateway, 4, 64, Duration::from_secs(1), &budget).await {
            Ok(_) => panic!("oversized PUB was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, ReadError::Protocol("E_BAD_MESSAGE", _)));
        assert_eq!(budget.available_permits(), 64);
    }

    #[tokio::test]
    async fn mpub_rejects_an_entry_above_the_single_message_limit() {
        let mut body = 1u32.to_be_bytes().to_vec();
        body.extend_from_slice(&5u32.to_be_bytes());
        body.extend_from_slice(b"hello");
        let (mut client, mut gateway) = tokio::io::duplex(1024);
        client.write_all(b"MPUB events\n").await.unwrap();
        client.write_u32(body.len() as u32).await.unwrap();
        client.write_all(&body).await.unwrap();
        let budget = Arc::new(Semaphore::new(64));
        let error = match read_command(&mut gateway, 4, 64, Duration::from_secs(1), &budget).await {
            Ok(_) => panic!("MPUB entry above the message limit was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, ReadError::Protocol("E_BAD_MESSAGE", _)));
        assert_eq!(budget.available_permits(), 64);
    }

    #[tokio::test]
    async fn stalled_backend_write_is_bounded_before_commit() {
        let (mut gateway, _backend) = tokio::io::duplex(1);
        let command = ClientCommand {
            command: Command::Publish {
                topic: "events".into(),
            },
            line: b"PUB events\n".to_vec(),
            body: Some(Bytes::from(vec![0x5a; 4096])),
            _permit: None,
        };
        let error = send_before_commit(&mut gateway, &command, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn explicit_pre_commit_rejection_retries_another_broker() {
        let rejected = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let accepted = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rejected_backend = backend(&rejected, 1);
        let accepted_backend = backend(&accepted, 2);
        let rejected_task = tokio::spawn(mock_backend(
            rejected,
            (FrameType::Error, b"E_DRAINING broker is draining"),
        ));
        let accepted_task = tokio::spawn(mock_backend(accepted, (FrameType::Response, OK)));
        let metrics = ProxyMetrics::default();
        let identify = default_backend_identify();
        let first = connect_backend(rejected_backend.clone(), &identify, &metrics)
            .await
            .unwrap();
        let pool = BackendPool::default();
        pool.replace(vec![rejected_backend, accepted_backend]);
        let command = ClientCommand {
            command: Command::Publish {
                topic: "events".into(),
            },
            line: b"PUB events\n".to_vec(),
            body: Some(Bytes::from_static(b"payload")),
            _permit: None,
        };
        let mut current = Some(first);
        publish(&pool, &identify, &command, &mut current, &metrics)
            .await
            .unwrap();
        assert_eq!(current.as_ref().unwrap().backend.node_id, 2);
        assert_eq!(metrics.producer_retries.load(Ordering::Relaxed), 1);
        rejected_task.await.unwrap();
        accepted_task.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_finishes_the_inflight_publish_but_rejects_the_queued_publish() {
        let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend(&backend_listener, 1);
        let (body_received_tx, body_received_rx) = tokio::sync::oneshot::channel();
        let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
        let backend_task = tokio::spawn(async move {
            let (mut stream, _) = backend_listener.accept().await.unwrap();
            let mut magic = [0; 4];
            stream.read_exact(&mut magic).await.unwrap();
            assert_eq!(&magic, MAGIC_V2);
            assert_eq!(read_line(&mut stream).await, b"IDENTIFY\n");
            let length = stream.read_u32().await.unwrap() as usize;
            let mut identify = vec![0; length];
            stream.read_exact(&mut identify).await.unwrap();
            write_frame(&mut stream, FrameType::Response, b"{}")
                .await
                .unwrap();
            assert_eq!(read_line(&mut stream).await, b"PUB events\n");
            let length = stream.read_u32().await.unwrap() as usize;
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(body, b"payload");
            body_received_tx.send(()).unwrap();
            respond_rx.await.unwrap();
            write_frame(&mut stream, FrameType::Response, OK)
                .await
                .unwrap();
            let mut next = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), stream.read(&mut next))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let pool = BackendPool::default();
        pool.replace(vec![backend_address]);
        let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway_address = gateway_listener.local_addr().unwrap();
        let (shutdown_tx, shutdown) = watch::channel(false);
        let gateway_task = tokio::spawn(async move {
            let (client, _) = gateway_listener.accept().await.unwrap();
            run(
                client,
                pool,
                Limits {
                    max_message_bytes: 1024,
                    max_body_bytes: 1024,
                    command_timeout: Duration::from_secs(1),
                    inflight_bytes: Arc::new(Semaphore::new(1024)),
                },
                ProxyMetrics::default(),
                shutdown,
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(gateway_address).await.unwrap();
        client.write_all(MAGIC_V2).await.unwrap();
        client.write_all(b"IDENTIFY\n\0\0\0\x02{}").await.unwrap();
        assert_eq!(read_frame(&mut client).await.unwrap().0, 0);
        client
            .write_all(b"PUB events\n\0\0\0\x07payload")
            .await
            .unwrap();
        body_received_rx.await.unwrap();
        client
            .write_all(b"PUB events\n\0\0\0\x06queued")
            .await
            .unwrap();
        shutdown_tx.send(true).unwrap();
        respond_tx.send(()).unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!((response.0, response.1.as_slice()), (0, OK));
        let mut closed = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.read(&mut closed))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        tokio::time::timeout(Duration::from_secs(1), gateway_task)
            .await
            .unwrap()
            .unwrap();
        backend_task.await.unwrap();
    }

    #[tokio::test]
    async fn gateway_accepts_a_hundred_mebibyte_publish() {
        const MESSAGE_BYTES: usize = 100 * 1024 * 1024;
        let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendPool::default();
        pool.replace(vec![backend(&backend_listener, 1)]);
        let backend_task = tokio::spawn(mock_large_backend(backend_listener, MESSAGE_BYTES));

        let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway_address = gateway_listener.local_addr().unwrap();
        let gateway_task = tokio::spawn(async move {
            let (client, _) = gateway_listener.accept().await.unwrap();
            let (_shutdown_tx, shutdown) = watch::channel(false);
            run(
                client,
                pool,
                Limits {
                    max_message_bytes: MESSAGE_BYTES,
                    max_body_bytes: 128 * 1024 * 1024,
                    command_timeout: Duration::from_secs(120),
                    inflight_bytes: Arc::new(Semaphore::new(512 * 1024 * 1024)),
                },
                ProxyMetrics::default(),
                shutdown,
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(gateway_address).await.unwrap();
        client.write_all(MAGIC_V2).await.unwrap();
        let identify = serde_json::to_vec(&json!({
            "feature_negotiation": true,
            "heartbeat_interval": -1
        }))
        .unwrap();
        client.write_all(b"IDENTIFY\n").await.unwrap();
        client.write_u32(identify.len() as u32).await.unwrap();
        client.write_all(&identify).await.unwrap();
        assert_eq!(read_frame(&mut client).await.unwrap().0, 0);
        client.write_all(b"PUB large\n").await.unwrap();
        client.write_u32(MESSAGE_BYTES as u32).await.unwrap();
        client.write_all(&vec![0x5a; MESSAGE_BYTES]).await.unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!((response.0, response.1.as_slice()), (0, OK));
        drop(client);
        backend_task.await.unwrap();
        gateway_task.await.unwrap();
    }

    #[test]
    fn two_slow_precommit_attempts_fit_the_kodo_read_timeout() {
        let handshake = BACKEND_CONNECT_TIMEOUT + BACKEND_IDENTIFY_TIMEOUT;
        let first_precommit_failure = handshake + BACKEND_WRITE_TIMEOUT;
        let total = first_precommit_failure + NEW_ATTEMPT_BUDGET;
        assert!(total < PUBLISH_DEADLINE);
        assert!(PUBLISH_DEADLINE < Duration::from_secs(60));
        assert!(NEW_ATTEMPT_BUDGET >= handshake + BACKEND_WRITE_TIMEOUT + BACKEND_TIMEOUT);
    }
}
