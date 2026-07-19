use super::*;
use std::fmt;

const MAX_CONTROL_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub(super) enum CommandReadError {
    Io(std::io::Error),
    Protocol { code: &'static str, detail: String },
}

impl CommandReadError {
    fn protocol(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Protocol {
            code,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for CommandReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Protocol { code, detail } => write!(formatter, "{code} {detail}"),
        }
    }
}

impl std::error::Error for CommandReadError {}

impl From<std::io::Error> for CommandReadError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub(super) async fn read_initial_command(
    stream: &mut TcpStream,
    config: &Config,
    admission: &PublishAdmission,
    connection_budget: &ConnectionBudget,
) -> Result<ParsedCommand, CommandReadError> {
    let mut line = Vec::with_capacity(64);
    loop {
        let byte = stream.read_u8().await?;
        line.push(byte);
        if byte == b'\n' {
            break;
        }
        if line.len() > 1024 {
            return Err(CommandReadError::protocol(
                "E_INVALID",
                "command line exceeds limit",
            ));
        }
    }
    let command = parse_command(&line)?;
    let (body, reservation) =
        read_command_body(stream, &command, config, admission, connection_budget).await?;
    Ok(ParsedCommand {
        command,
        body,
        publish_reservation: reservation,
    })
}

pub(super) async fn read_command(
    reader: &mut BufReader<ReadHalf<BoxIo>>,
    config: &Config,
    admission: &PublishAdmission,
    connection_budget: &ConnectionBudget,
) -> Result<ParsedCommand, CommandReadError> {
    // Waiting for the first byte remains unbounded so an explicitly idle
    // connection (including one with heartbeats disabled) stays compatible.
    // Once a command starts, bound both its line and optional body so a
    // partial publish cannot pin a connection or its byte reservation.
    let first = reader.read_u8().await?;
    tokio::time::timeout(
        Duration::from_millis(config.limits.tcp_command_timeout_ms),
        read_started_command(reader, first, config, admission, connection_budget),
    )
    .await
    .map_err(|_| CommandReadError::protocol("E_INVALID", "command read timed out"))?
}

async fn read_started_command<R>(
    reader: &mut R,
    first: u8,
    config: &Config,
    admission: &PublishAdmission,
    connection_budget: &ConnectionBudget,
) -> Result<ParsedCommand, CommandReadError>
where
    R: AsyncRead + Unpin,
{
    let mut line = Vec::with_capacity(64);
    line.push(first);
    while line.last() != Some(&b'\n') {
        line.push(reader.read_u8().await?);
        if line.len() > 1024 {
            return Err(CommandReadError::protocol(
                "E_INVALID",
                "command line exceeds limit",
            ));
        }
    }
    let command = parse_command(&line)?;
    let (body, reservation) =
        read_command_body(reader, &command, config, admission, connection_budget).await?;
    Ok(ParsedCommand {
        command,
        body,
        publish_reservation: reservation,
    })
}

async fn read_command_body<R>(
    reader: &mut R,
    command: &Command,
    config: &Config,
    admission: &PublishAdmission,
    connection_budget: &ConnectionBudget,
) -> Result<(Option<Bytes>, Option<PublishReservation>), CommandReadError>
where
    R: AsyncRead + Unpin,
{
    if !matches!(
        command,
        Command::Identify
            | Command::Auth
            | Command::Publish { .. }
            | Command::MultiPublish { .. }
            | Command::DeferredPublish { .. }
    ) {
        return Ok((None, None));
    }
    let (code, name, maximum) = match command {
        Command::Identify => ("E_BAD_BODY", "IDENTIFY", MAX_CONTROL_BODY_BYTES),
        Command::Auth => ("E_BAD_BODY", "AUTH", MAX_CONTROL_BODY_BYTES),
        Command::Publish { .. } => ("E_BAD_MESSAGE", "PUB", config.queue.max_message_bytes),
        Command::MultiPublish { .. } => ("E_BAD_BODY", "MPUB", config.limits.max_body_bytes),
        Command::DeferredPublish { .. } => {
            ("E_BAD_MESSAGE", "DPUB", config.queue.max_message_bytes)
        }
        _ => unreachable!(),
    };
    let length = reader.read_u32().await.map_err(|error| {
        CommandReadError::protocol(code, format!("{name} failed to read body size: {error}"))
    })? as usize;
    if length == 0 {
        return Err(CommandReadError::protocol(
            code,
            format!("{name} invalid body size 0"),
        ));
    }
    if length > maximum {
        return Err(CommandReadError::protocol(
            code,
            format!("{name} body too big {length} > {maximum}"),
        ));
    }
    let reservation = if matches!(
        command,
        Command::Publish { .. } | Command::MultiPublish { .. } | Command::DeferredPublish { .. }
    ) {
        let shape = if matches!(command, Command::MultiPublish { .. }) {
            crate::admission::PublishShape::Multi
        } else {
            crate::admission::PublishShape::Single
        };
        Some(
            admission
                .try_reserve_connection_publish(length, shape, connection_budget)
                .ok_or_else(|| {
                    CommandReadError::protocol(
                        "E_THROTTLED",
                        format!("{name} publish byte budget is exhausted; retry later"),
                    )
                })?,
        )
    } else {
        None
    };
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await.map_err(|error| {
        CommandReadError::protocol(code, format!("{name} failed to read body: {error}"))
    })?;
    Ok((Some(Bytes::from(body)), reservation))
}

fn parse_command(line: &[u8]) -> Result<Command, CommandReadError> {
    Command::parse(line).map_err(|error| match error {
        CommandError::Invalid(detail) => CommandReadError::protocol("E_INVALID", detail),
        CommandError::BadTopic => CommandReadError::protocol("E_BAD_TOPIC", "invalid topic"),
        CommandError::BadChannel => CommandReadError::protocol("E_BAD_CHANNEL", "invalid channel"),
    })
}

pub(super) fn parse_identify(body: &[u8], config: &Config) -> anyhow::Result<IdentifyRequest> {
    if body.len() > config.limits.max_body_bytes.min(MAX_CONTROL_BODY_BYTES) {
        anyhow::bail!("IDENTIFY body exceeds limit");
    }
    let request: IdentifyRequest = serde_json::from_slice(body).context("parse IDENTIFY body")?;
    Ok(request)
}

pub(super) fn identify_heartbeat(
    request: &IdentifyRequest,
    config: &Config,
) -> anyhow::Result<Option<Duration>> {
    match request.heartbeat_interval {
        Some(-1) => Ok(None),
        Some(value)
            if value >= 1_000 && value <= config.limits.max_heartbeat_interval_ms as i64 =>
        {
            Ok(Some(Duration::from_millis(value as u64)))
        }
        Some(_) => anyhow::bail!("heartbeat_interval is outside configured range"),
        None => Ok(Some(Duration::from_millis(
            config.limits.heartbeat_interval_ms,
        ))),
    }
}

pub(super) fn identify_message_timeout(
    request: &IdentifyRequest,
    config: &Config,
) -> anyhow::Result<Duration> {
    match request.msg_timeout {
        Some(0) => Ok(config.message_timeout()),
        Some(value) if value >= 1_000 && value <= config.queue.max_message_timeout_ms as i64 => {
            Ok(Duration::from_millis(value as u64))
        }
        Some(_) => anyhow::bail!("msg_timeout is outside configured range"),
        None => Ok(config.message_timeout()),
    }
}

pub(super) struct OutputBufferSettings {
    pub size: usize,
    pub timeout: Option<Duration>,
}

pub(super) fn identify_output_buffer(
    request: &IdentifyRequest,
    config: &Config,
) -> anyhow::Result<OutputBufferSettings> {
    let mut timeout = match request.output_buffer_timeout {
        Some(-1) => None,
        Some(0) | None => Some(Duration::from_millis(
            config.limits.output_buffer_timeout_ms,
        )),
        Some(value)
            if value >= config.limits.min_output_buffer_timeout_ms as i64
                && value <= config.limits.max_output_buffer_timeout_ms as i64 =>
        {
            Some(Duration::from_millis(value as u64))
        }
        Some(_) => anyhow::bail!("output buffer timeout is outside configured range"),
    };
    let size = match request.output_buffer_size {
        Some(-1) => {
            timeout = None;
            1
        }
        Some(0) | None => config.limits.output_buffer_size,
        Some(value) if value >= 64 && value <= config.limits.max_output_buffer_size as i64 => {
            value as usize
        }
        Some(_) => anyhow::bail!("output buffer size is outside configured range"),
    };
    Ok(OutputBufferSettings { size, timeout })
}

pub(super) fn identify_sample_rate(request: &IdentifyRequest) -> anyhow::Result<u8> {
    match request.sample_rate.unwrap_or(0) {
        value @ 0..=99 => Ok(value as u8),
        _ => anyhow::bail!("sample rate must fit 0..=99"),
    }
}

pub(super) fn negotiate_compression(
    request: &IdentifyRequest,
    config: &Config,
) -> anyhow::Result<Option<Compression>> {
    if !request.feature_negotiation {
        return Ok(None);
    }
    let snappy = request.snappy && config.network.snappy_enabled;
    let deflate = request.deflate && config.network.deflate_enabled;
    if snappy && deflate {
        anyhow::bail!("cannot enable both deflate and snappy compression");
    }
    if snappy {
        return Ok(Some(Compression::Snappy));
    }
    if deflate {
        let requested = request.deflate_level.unwrap_or(6);
        let level = if requested > 0 { requested } else { 6 };
        return Ok(Some(Compression::Deflate(
            level.min(config.network.max_deflate_level),
        )));
    }
    Ok(None)
}

pub(super) fn identify_response(
    config: &Config,
    tls_enabled: bool,
    compression: Option<Compression>,
    state: &SessionState,
) -> IdentifyResponse {
    IdentifyResponse {
        max_rdy_count: config.limits.max_rdy_count,
        version: env!("CARGO_PKG_VERSION").into(),
        max_msg_timeout: config.queue.max_message_timeout_ms as i64,
        msg_timeout: state.message_timeout.as_millis().min(i64::MAX as u128) as i64,
        tls_v1: tls_enabled,
        deflate: matches!(compression, Some(Compression::Deflate(_))),
        deflate_level: match compression {
            Some(Compression::Deflate(level)) => level,
            _ => 0,
        },
        max_deflate_level: config.network.max_deflate_level,
        snappy: matches!(compression, Some(Compression::Snappy)),
        sample_rate: state.sample_rate as i32,
        auth_required: !config.security.auth_http_addresses.is_empty(),
        output_buffer_size: state.output_buffer_size as i64,
        output_buffer_timeout: state.output_buffer_timeout.map_or(0, |timeout| {
            timeout.as_millis().min(i64::MAX as u128) as i64
        }),
    }
}

pub(super) async fn write_broker_error<W>(
    writer: &mut W,
    code: &str,
    error: BrokerError,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_error(writer, code, &error.to_string()).await
}

pub(super) async fn write_error<W>(writer: &mut W, code: &str, detail: &str) -> anyhow::Result<()>
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

pub(super) async fn write_frame<W>(
    writer: &mut W,
    frame_type: FrameType,
    body: &[u8],
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&encode_frame(frame_type, body)).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn applies_identify_delivery_settings() {
        let config = Config::default();
        let request = IdentifyRequest {
            output_buffer_size: Some(4096),
            output_buffer_timeout: Some(100),
            sample_rate: Some(25),
            msg_timeout: Some(2_000),
            ..Default::default()
        };
        let output = identify_output_buffer(&request, &config).unwrap();
        assert_eq!(output.size, 4096);
        assert_eq!(output.timeout, Some(Duration::from_millis(100)));
        assert_eq!(identify_sample_rate(&request).unwrap(), 25);
        assert_eq!(
            identify_message_timeout(&request, &config).unwrap(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn supports_disabling_output_buffering() {
        let config = Config::default();
        let request = IdentifyRequest {
            output_buffer_size: Some(-1),
            output_buffer_timeout: Some(100),
            ..Default::default()
        };
        let output = identify_output_buffer(&request, &config).unwrap();
        assert_eq!(output.size, 1);
        assert_eq!(output.timeout, None);
    }

    #[test]
    fn rejects_invalid_identify_delivery_settings() {
        let config = Config::default();
        assert!(identify_output_buffer(
            &IdentifyRequest {
                output_buffer_size: Some(63),
                ..Default::default()
            },
            &config
        )
        .is_err());
        assert!(identify_sample_rate(&IdentifyRequest {
            sample_rate: Some(100),
            ..Default::default()
        })
        .is_err());
    }

    #[tokio::test]
    async fn reports_nsq_error_code_for_an_empty_publish() {
        let (mut peer, server) = tokio::io::duplex(1024);
        peer.write_all(b"PUB events\n\0\0\0\0").await.unwrap();
        let io: BoxIo = Box::new(server);
        let (read, _) = tokio::io::split(io);
        let mut reader = BufReader::new(read);
        let config = Config::default();
        let metrics = Arc::new(Metrics::default());
        let admission = PublishAdmission::new(config.limits.node_publish_inflight_bytes, metrics);
        let connection = ConnectionBudget::new(config.limits.connection_publish_inflight_bytes);
        let error = read_command(&mut reader, &config, &admission, &connection)
            .await
            .unwrap_err();
        match error {
            CommandReadError::Protocol { code, .. } => assert_eq!(code, "E_BAD_MESSAGE"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn rejects_an_unterminated_command_before_it_can_grow_unbounded() {
        let (mut peer, server) = tokio::io::duplex(4096);
        peer.write_all(&vec![b'X'; 1025]).await.unwrap();
        let io: BoxIo = Box::new(server);
        let (read, _) = tokio::io::split(io);
        let mut reader = BufReader::new(read);
        let config = Config::default();
        let metrics = Arc::new(Metrics::default());
        let admission = PublishAdmission::new(config.limits.node_publish_inflight_bytes, metrics);
        let connection = ConnectionBudget::new(config.limits.connection_publish_inflight_bytes);
        let error = read_command(&mut reader, &config, &admission, &connection)
            .await
            .unwrap_err();
        match error {
            CommandReadError::Protocol { code, detail } => {
                assert_eq!(code, "E_INVALID");
                assert!(detail.contains("line exceeds limit"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn times_out_a_partial_publish_body_without_requiring_heartbeats() {
        let (mut peer, server) = tokio::io::duplex(1024);
        peer.write_all(b"PUB events\n\0\0\0\x04x").await.unwrap();
        let io: BoxIo = Box::new(server);
        let (read, _) = tokio::io::split(io);
        let mut reader = BufReader::new(read);
        let mut config = Config::default();
        config.limits.tcp_command_timeout_ms = 10;
        let metrics = Arc::new(Metrics::default());
        let admission = PublishAdmission::new(config.limits.node_publish_inflight_bytes, metrics);
        let connection = ConnectionBudget::new(config.limits.connection_publish_inflight_bytes);
        let error = read_command(&mut reader, &config, &admission, &connection)
            .await
            .unwrap_err();
        match error {
            CommandReadError::Protocol { code, detail } => {
                assert_eq!(code, "E_INVALID");
                assert!(detail.contains("timed out"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
