use clap::Parser;
use rustqueue_discovery::{run_refresh_loop, serve, Directory, RefreshConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "rustqueue-discovery", version)]
struct Cli {
    #[arg(long, env = "POD_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(
        long,
        env = "RUSTQUEUE_BROKER_SERVICE",
        default_value = "rustqueue-brokers"
    )]
    broker_service: String,
    #[arg(
        long,
        env = "RUSTQUEUE_DISCOVERY_ADDRESS",
        default_value = "0.0.0.0:4161"
    )]
    address: SocketAddr,
    #[arg(long, env = "RUSTQUEUE_BROKER_HTTP_PORT", default_value_t = 4151)]
    broker_http_port: u16,
    #[arg(
        long,
        env = "RUSTQUEUE_ENDPOINT_SLICE_TIMEOUT_MS",
        default_value_t = 1500
    )]
    endpoint_slice_timeout_ms: u64,
    #[arg(long, env = "RUSTQUEUE_REGISTRY_TOKEN_FILE")]
    registry_token_file: Option<PathBuf>,
    #[arg(
        long,
        env = "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED",
        default_value_t = false
    )]
    kodo_compatibility_enabled: bool,
    #[arg(long, env = "RUSTQUEUE_KODO_GATEWAY_ADDRESS")]
    kodo_gateway_address: Option<String>,
    #[arg(long, env = "RUSTQUEUE_KODO_CLEANUP_ENABLED", default_value_t = false)]
    kodo_cleanup_enabled: bool,
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
    if cli.endpoint_slice_timeout_ms == 0 {
        anyhow::bail!("EndpointSlice timeout must be greater than zero");
    }
    if cli.kodo_cleanup_enabled {
        anyhow::bail!(
            "Kodo automatic cleanup is disabled until cluster-wide atomic deletion is available"
        );
    }
    if cli.kodo_compatibility_enabled {
        if cli
            .kodo_gateway_address
            .as_deref()
            .is_none_or(|address| address.trim().is_empty())
        {
            anyhow::bail!("Kodo Gateway address is required and cannot be empty");
        }
    } else if cli.kodo_cleanup_enabled || cli.kodo_gateway_address.is_some() {
        anyhow::bail!("Kodo cleanup and Gateway address require Kodo compatibility");
    }
    let directory = Directory::default();
    if cli.kodo_compatibility_enabled {
        directory.configure_kodo(
            cli.kodo_gateway_address
                .map(|address| {
                    let address = address.trim().to_owned();
                    (0..3)
                        .map(|ordinal| {
                            rustqueue_discovery::Producer::gateway(address.clone(), ordinal)
                        })
                        .collect()
                })
                .expect("validated Kodo Gateway address"),
            cli.kodo_cleanup_enabled,
        );
    }
    let refresh = run_refresh_loop(
        directory.clone(),
        RefreshConfig {
            namespace: cli.namespace,
            service_name: cli.broker_service,
            fallback_http_port: cli.broker_http_port,
            poll_interval: Duration::from_secs(2),
            endpoint_slice_timeout: Duration::from_millis(cli.endpoint_slice_timeout_ms),
            stale_after: Duration::from_secs(5),
            registry_token_file: cli.registry_token_file,
            max_parallel_polls: 128,
        },
    );
    tokio::select! {
        result = refresh => result,
        result = serve(cli.address, directory) => result,
    }
}
