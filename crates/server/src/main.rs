mod admission;
mod auth;
mod compression;
mod config;
mod disk_guard;
mod http;
mod metrics;
mod tcp;
mod tls;

use admission::PublishAdmission;
use anyhow::Context;
use clap::Parser;
use config::Config;
use metrics::Metrics;
use rustqueue_queue::{Broker, BrokerConfig};
use rustqueue_storage::DataDirectoryLock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "rustqueued",
    version,
    about = "NSQ-compatible share-nothing broker"
)]
struct Cli {
    #[arg(long, env = "RUSTQUEUE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long)]
    check_config: bool,
    #[arg(long)]
    capabilities_output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();
    if let Some(path) = cli.capabilities_output.as_deref() {
        write_binary_capabilities(path)?;
        return Ok(());
    }
    let config = Config::load(cli.config.as_deref())?;
    init_tracing(&config.log_format)?;
    config.validate()?;
    if cli.check_config {
        println!("configuration is valid");
        return Ok(());
    }

    let _data_lock = DataDirectoryLock::acquire(&config.storage.data_path)
        .context("lock RustQueue data directory")?;
    let broker = Arc::new(Broker::open(BrokerConfig {
        data_path: config.storage.data_path.clone(),
        node_id: config.node.id,
        max_segment_bytes: config.storage.max_segment_bytes,
        max_message_bytes: config.queue.max_message_bytes,
        message_timeout: config.message_timeout(),
        bootstrap_retention: std::time::Duration::from_secs(
            config.queue.bootstrap_retention_seconds,
        ),
        max_ack_gap: config.queue.max_ack_gap,
        max_topics: config.queue.max_topics,
        max_publish_workers: config.queue.max_publish_workers,
        publish_worker_idle: std::time::Duration::from_secs(
            config.queue.publish_worker_idle_seconds,
        ),
        entry_cache_bytes: config.storage.entry_cache_bytes,
        message_index_cache_bytes: config.storage.message_index_cache_bytes,
        payload_read_workers: config.storage.payload_read_workers,
        payload_read_queue: config.storage.payload_read_queue,
        delivery_inflight_bytes: config.limits.node_delivery_inflight_bytes,
        scrub_bytes_per_second: config.storage.scrub_bytes_per_second,
        storage_feature_level: config.storage.feature_level,
        require_management_fence_sync: config.security.console_management_enabled,
    })?);
    let config = Arc::new(config);
    let metrics = Arc::new(Metrics::default());
    let accepting = Arc::new(AtomicBool::new(true));
    let delivering = Arc::new(AtomicBool::new(true));
    let publish_admission = Arc::new(PublishAdmission::new(
        config.limits.node_publish_inflight_bytes,
        Arc::clone(&metrics),
    ));
    let initially_pressured = disk_guard::initialize(&config, &publish_admission, &metrics)?;

    info!(
        node_id = config.node.id,
        tcp = %config.network.tcp_address,
        http = %config.network.http_address,
        format = 7,
        "starting share-nothing RustQueue broker"
    );

    let tcp_task = tokio::spawn(tcp::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        Arc::clone(&accepting),
        Arc::clone(&delivering),
        Arc::clone(&publish_admission),
    ));
    let http_task = tokio::spawn(http::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        Arc::clone(&accepting),
        Arc::clone(&delivering),
        Arc::clone(&publish_admission),
    ));
    let scrub_task = tokio::spawn(run_scrubber(
        Arc::clone(&broker),
        std::time::Duration::from_secs(config.storage.maintenance_startup_delay_seconds),
        std::time::Duration::from_secs(config.storage.scrub_interval_seconds),
    ));
    let disk_task = tokio::spawn(disk_guard::run(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&publish_admission),
        Arc::clone(&metrics),
        initially_pressured,
    ));
    let storage_health_task = tokio::spawn(monitor_storage_health(Arc::clone(&broker)));

    tokio::select! {
        result = tcp_task => result.context("TCP task join")??,
        result = http_task => result.context("HTTP task join")??,
        result = scrub_task => result.context("storage scrub task join")??,
        result = disk_task => result.context("disk guard task join")??,
        result = storage_health_task => result.context("storage health task join")??,
        _ = shutdown_signal() => {
            info!("shutdown signal received");
            accepting.store(false, Ordering::Release);
            delivering.store(false, Ordering::Release);
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_secs(config.shutdown.grace_seconds);
            while metrics.tcp_connections.load(Ordering::Acquire) > 0
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let released = broker.release_all_in_flight();
            broker.flush().await.context("flush queue state")?;
            info!(released, "share-nothing broker stopped cleanly");
        },
    }
    Ok(())
}

fn write_binary_capabilities(path: &std::path::Path) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(&rustqueue_storage::binary_capabilities())?;
    std::fs::write(path, bytes).with_context(|| format!("write capabilities to {}", path.display()))
}

async fn monitor_storage_health(broker: Arc<Broker>) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if !broker.storage_healthy() {
            anyhow::bail!("broker storage was isolated after an I/O or integrity failure");
        }
    }
}

async fn run_scrubber(
    broker: Arc<Broker>,
    startup_delay: std::time::Duration,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    tokio::time::sleep(startup_delay).await;
    loop {
        let records = broker.scrub().await.context("background queue scrub")?;
        tracing::info!(records, "background data scrub completed");
        tokio::time::sleep(interval).await;
    }
}

fn init_tracing(format: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(%error, "failed to install Ctrl-C handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => error!(%error, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
