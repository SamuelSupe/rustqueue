mod backend;
mod discovery;
mod http;
mod tcp;

use backend::BackendPool;
use clap::Parser;
use std::net::SocketAddr;
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
    if cli.max_body_bytes == 0
        || cli.max_inflight_bytes == 0
        || cli.max_inflight_bytes > u32::MAX as usize
        || cli.max_connections == 0
    {
        anyhow::bail!("proxy limits must be non-zero and inflight bytes must fit u32");
    }
    let pool = BackendPool::default();
    tokio::select! {
        result = discovery::run(pool.clone(), cli.discovery_urls) => result,
        result = tcp::serve(cli.tcp_address, pool.clone(), cli.max_connections) => result,
        result = http::serve(cli.http_address, pool, cli.max_body_bytes, cli.max_inflight_bytes) => result,
    }
}
