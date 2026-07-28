mod backend;
mod discovery;
mod http;
mod kodo;
mod metrics;
mod tcp;

use backend::BackendPool;
use clap::Parser;
use metrics::ProxyMetrics;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "rustqueue-proxy", version)]
struct Cli {
    #[arg(
        long,
        env = "RUSTQUEUE_DISCOVERY_URLS",
        value_delimiter = ',',
        default_value = "http://rustqueue-discovery:4161"
    )]
    discovery_urls: Vec<String>,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_TCP_ADDRESS",
        default_value = "0.0.0.0:4150"
    )]
    tcp_address: SocketAddr,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_HTTP_ADDRESS",
        default_value = "0.0.0.0:4151"
    )]
    http_address: SocketAddr,
    #[arg(long, env = "RUSTQUEUE_PROXY_MAX_BODY_BYTES", default_value_t = 64 * 1024 * 1024)]
    max_body_bytes: usize,
    #[arg(long, env = "RUSTQUEUE_PROXY_MAX_MESSAGE_BYTES", default_value_t = 20 * 1024 * 1024)]
    max_message_bytes: usize,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES",
        default_value_t = 512 * 1024 * 1024
    )]
    max_inflight_bytes: usize,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_MAX_CONNECTIONS",
        default_value_t = 10_000
    )]
    max_connections: usize,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_HTTP_BODY_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    http_body_timeout_ms: u64,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS",
        default_value_t = 300
    )]
    tcp_max_connection_age_seconds: u64,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_TCP_COMMAND_TIMEOUT_MS",
        default_value_t = 120_000
    )]
    tcp_command_timeout_ms: u64,
    #[arg(
        long,
        env = "RUSTQUEUE_PROXY_SHUTDOWN_GRACE_SECONDS",
        default_value_t = 30
    )]
    shutdown_grace_seconds: u64,
    #[arg(
        long,
        env = "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED",
        default_value_t = false
    )]
    kodo_compatibility_enabled: bool,
    #[arg(long, env = "RUSTQUEUE_KODO_GATEWAY_ORDINAL")]
    kodo_gateway_ordinal: Option<usize>,
    #[arg(long, env = "RUSTQUEUE_KODO_CLEANUP_ENABLED", default_value_t = false)]
    kodo_cleanup_enabled: bool,
    #[arg(long, env = "RUSTQUEUE_KODO_CLEANUP_TOKEN_FILE")]
    kodo_cleanup_token_file: Option<PathBuf>,
    #[arg(long, env = "RUSTQUEUE_REGISTRY_TOKEN_FILE")]
    registry_token_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    validate_proxy_limits(&cli)?;
    let publish_pool = BackendPool::default();
    let broker_pool = BackendPool::default();
    let metrics = ProxyMetrics::default();
    let kodo = build_kodo_config(&cli)?;
    let terminate_producer_protocol = kodo.is_some();
    let inflight_bytes = Arc::new(tokio::sync::Semaphore::new(cli.max_inflight_bytes));
    let shutdown_grace = Duration::from_secs(cli.shutdown_grace_seconds);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let discovery = discovery::run(
        publish_pool.clone(),
        broker_pool.clone(),
        cli.discovery_urls,
        metrics.clone(),
    );
    let tcp = tcp::serve(
        cli.tcp_address,
        publish_pool.clone(),
        tcp::Limits {
            max_connections: cli.max_connections,
            max_connection_age: Duration::from_secs(cli.tcp_max_connection_age_seconds),
            terminate_producer_protocol,
            max_message_bytes: cli.max_message_bytes,
            max_body_bytes: cli.max_body_bytes,
            command_timeout: Duration::from_millis(cli.tcp_command_timeout_ms),
            inflight_bytes: Arc::clone(&inflight_bytes),
        },
        metrics.clone(),
        shutdown_rx.clone(),
        shutdown_grace,
    );
    let http = http::serve(
        cli.http_address,
        publish_pool,
        broker_pool,
        http::Limits {
            max_body_bytes: cli.max_body_bytes,
            inflight_bytes,
            body_timeout: Duration::from_millis(cli.http_body_timeout_ms),
        },
        metrics,
        kodo,
        shutdown_rx,
    );
    tokio::pin!(discovery, tcp, http);
    let (completed_task, terminal_result) = tokio::select! {
        result = &mut discovery => (
            Some(RuntimeTask::Discovery),
            unexpected_task_exit("discovery", result),
        ),
        result = &mut tcp => (
            Some(RuntimeTask::Tcp),
            unexpected_task_exit("TCP", result),
        ),
        result = &mut http => (
            Some(RuntimeTask::Http),
            unexpected_task_exit("HTTP", result),
        ),
        _ = shutdown_signal() => {
            info!("proxy shutdown signal received");
            (None, Ok(()))
        }
    };
    let _ = shutdown_tx.send(true);
    let graceful = async {
        match completed_task {
            Some(RuntimeTask::Tcp) => (&mut http).await,
            Some(RuntimeTask::Http) => (&mut tcp).await,
            Some(RuntimeTask::Discovery) | None => {
                tokio::try_join!(async { (&mut tcp).await }, async { (&mut http).await },)?;
                Ok::<(), anyhow::Error>(())
            }
        }
    };
    let cleanup_result =
        match tokio::time::timeout(shutdown_grace + Duration::from_secs(2), graceful).await {
            Ok(result) => result,
            Err(_) => {
                warn!("proxy shutdown grace expired");
                Err(anyhow::anyhow!("proxy shutdown grace expired"))
            }
        };
    combine_results(terminal_result, cleanup_result)
}

fn validate_proxy_limits(cli: &Cli) -> anyhow::Result<()> {
    if cli.max_body_bytes == 0
        || cli.max_message_bytes == 0
        || cli.max_message_bytes > cli.max_body_bytes
        || cli.max_message_bytes > rustqueue_protocol::MAX_MESSAGE_BYTES
        || cli.max_body_bytes > rustqueue_protocol::MAX_BATCH_BYTES
        || cli.max_inflight_bytes == 0
        || cli.max_inflight_bytes < cli.max_body_bytes
        || cli.max_inflight_bytes > u32::MAX as usize
        || cli.max_connections == 0
        || cli.http_body_timeout_ms == 0
        || cli.tcp_command_timeout_ms == 0
        || cli.shutdown_grace_seconds == 0
    {
        anyhow::bail!(
            "proxy limits must be non-zero, fit the 100 MiB message and 128 MiB batch contract, and fit the inflight byte budget"
        );
    }
    if cli.max_connections > tokio::sync::Semaphore::MAX_PERMITS
        || cli.max_inflight_bytes > tokio::sync::Semaphore::MAX_PERMITS
    {
        anyhow::bail!("proxy limits exceed the runtime semaphore capacity");
    }
    let now = std::time::Instant::now();
    let shutdown_timeout =
        Duration::from_secs(cli.shutdown_grace_seconds).checked_add(Duration::from_secs(2));
    let timers = [
        Duration::from_millis(cli.http_body_timeout_ms),
        Duration::from_millis(cli.tcp_command_timeout_ms),
        Duration::from_secs(cli.tcp_max_connection_age_seconds),
    ];
    if shutdown_timeout
        .and_then(|duration| now.checked_add(duration))
        .is_none()
        || timers
            .into_iter()
            .any(|duration| now.checked_add(duration).is_none())
    {
        anyhow::bail!("proxy timeouts exceed the platform timer range");
    }
    if cli.kodo_compatibility_enabled
        && (cli.max_message_bytes != rustqueue_protocol::MAX_MESSAGE_BYTES
            || cli.max_body_bytes != rustqueue_protocol::MAX_BATCH_BYTES
            || cli.max_inflight_bytes
                < tcp::maximum_gateway_working_set(cli.max_message_bytes, cli.max_body_bytes))
    {
        anyhow::bail!(
            "Kodo compatibility requires the 100 MiB message and 128 MiB batch limits plus their parsing working set"
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RuntimeTask {
    Discovery,
    Tcp,
    Http,
}

fn unexpected_task_exit(name: &str, result: anyhow::Result<()>) -> anyhow::Result<()> {
    match result {
        Ok(()) => anyhow::bail!("{name} task stopped unexpectedly"),
        Err(error) => Err(anyhow::anyhow!("{name} task failed: {error:#}")),
    }
}

fn combine_results(
    terminal: anyhow::Result<()>,
    cleanup: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match (terminal, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(terminal), Err(cleanup)) => Err(anyhow::anyhow!(
            "{terminal:#}; shutdown cleanup also failed: {cleanup:#}"
        )),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

fn build_kodo_config(cli: &Cli) -> anyhow::Result<Option<kodo::KodoConfig>> {
    if cli.kodo_cleanup_enabled {
        anyhow::bail!(
            "Kodo automatic cleanup is disabled until cluster-wide atomic deletion is available"
        );
    }
    if !cli.kodo_compatibility_enabled {
        if cli.kodo_cleanup_enabled || cli.kodo_gateway_ordinal.is_some() {
            anyhow::bail!("Kodo cleanup and gateway ordinal require Kodo compatibility");
        }
        return Ok(None);
    }
    let ordinal = cli
        .kodo_gateway_ordinal
        .or_else(|| {
            std::env::var("POD_NAME")
                .ok()
                .and_then(|name| name.rsplit_once('-')?.1.parse().ok())
        })
        .ok_or_else(|| anyhow::anyhow!("Kodo gateway ordinal is required"))?;
    if ordinal >= 3 {
        anyhow::bail!("Kodo gateway ordinal must be in 0..3");
    }
    let cleanup_token = read_token(cli.kodo_cleanup_token_file.as_deref())?;
    let registry_token = read_token(cli.registry_token_file.as_deref())?;
    if cli.kodo_cleanup_enabled && (cleanup_token.is_none() || registry_token.is_none()) {
        anyhow::bail!("Kodo cleanup requires non-empty cleanup and registry token files");
    }
    if cli.kodo_cleanup_enabled && cleanup_token == registry_token {
        anyhow::bail!("Kodo cleanup and registry tokens must be distinct");
    }
    Ok(Some(kodo::KodoConfig {
        ordinal,
        cleanup_enabled: cli.kodo_cleanup_enabled,
        cleanup_token,
        registry_token,
    }))
}

fn read_token(path: Option<&std::path::Path>) -> anyhow::Result<Option<Arc<str>>> {
    path.map(std::fs::read_to_string)
        .transpose()
        .map(|token| {
            token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty())
                .map(Arc::<str>::from)
        })
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cli() -> Cli {
        Cli::try_parse_from(["rustqueue-proxy"]).unwrap()
    }

    #[test]
    fn proxy_limits_enforce_the_wire_and_memory_contract() {
        let mut cli = default_cli();
        assert!(validate_proxy_limits(&cli).is_ok());

        cli.max_message_bytes = rustqueue_protocol::MAX_MESSAGE_BYTES + 1;
        cli.max_body_bytes = rustqueue_protocol::MAX_BATCH_BYTES;
        assert!(validate_proxy_limits(&cli).is_err());

        let mut cli = default_cli();
        cli.max_inflight_bytes = cli.max_body_bytes - 1;
        assert!(validate_proxy_limits(&cli).is_err());
    }

    #[test]
    fn proxy_limits_reject_runtime_panics() {
        let mut cli = default_cli();
        cli.max_connections = tokio::sync::Semaphore::MAX_PERMITS + 1;
        assert!(validate_proxy_limits(&cli).is_err());

        let mut cli = default_cli();
        cli.shutdown_grace_seconds = u64::MAX;
        assert!(validate_proxy_limits(&cli).is_err());
    }

    #[test]
    fn kodo_mode_requires_the_full_hundred_mebibyte_profile() {
        let mut cli = default_cli();
        cli.kodo_compatibility_enabled = true;
        cli.max_message_bytes = rustqueue_protocol::MAX_MESSAGE_BYTES;
        cli.max_body_bytes = rustqueue_protocol::MAX_BATCH_BYTES;
        assert!(validate_proxy_limits(&cli).is_ok());

        cli.max_message_bytes -= 1;
        assert!(validate_proxy_limits(&cli).is_err());

        let mut cli = default_cli();
        cli.kodo_compatibility_enabled = true;
        cli.max_message_bytes = rustqueue_protocol::MAX_MESSAGE_BYTES;
        cli.max_body_bytes = rustqueue_protocol::MAX_BATCH_BYTES;
        cli.max_inflight_bytes = cli.max_body_bytes;
        assert!(validate_proxy_limits(&cli).is_err());
    }
}
