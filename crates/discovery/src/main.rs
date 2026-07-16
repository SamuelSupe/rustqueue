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
    let token = cli
        .registry_token_file
        .map(std::fs::read_to_string)
        .transpose()?
        .map(|token| token.trim().to_owned());
    let directory = Directory::default();
    let refresh = run_refresh_loop(
        directory.clone(),
        RefreshConfig {
            namespace: cli.namespace,
            service_name: cli.broker_service,
            fallback_http_port: cli.broker_http_port,
            poll_interval: Duration::from_secs(2),
            stale_after: Duration::from_secs(5),
            registry_token: token,
            max_parallel_polls: 128,
        },
    );
    tokio::select! {
        result = refresh => result,
        result = serve(cli.address, directory) => result,
    }
}
