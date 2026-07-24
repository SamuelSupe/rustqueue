mod admission;
mod auth;
mod compression;
mod config;
mod disk_guard;
mod http;
mod metrics;
mod subscriptions;
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
use subscriptions::SubscriptionRegistry;
use tracing::{error, info, warn};
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
    let subscriptions = SubscriptionRegistry::default();
    let initially_pressured = disk_guard::initialize(&config, &publish_admission, &metrics)?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    info!(
        node_id = config.node.id,
        tcp = %config.network.tcp_address,
        http = %config.network.http_address,
        format = 7,
        "starting share-nothing RustQueue broker"
    );

    let mut tcp_task = tokio::spawn(tcp::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        Arc::clone(&accepting),
        Arc::clone(&delivering),
        Arc::clone(&publish_admission),
        subscriptions.clone(),
        shutdown_rx.clone(),
        std::time::Duration::from_secs(config.shutdown.grace_seconds)
            .saturating_sub(std::time::Duration::from_millis(250)),
    ));
    let mut http_task = tokio::spawn(http::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        Arc::clone(&accepting),
        Arc::clone(&delivering),
        Arc::clone(&publish_admission),
        subscriptions.clone(),
        shutdown_rx.clone(),
    ));
    let mut kodo_http_task = tokio::spawn(http::serve_kodo_compat(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        Arc::clone(&accepting),
        Arc::clone(&delivering),
        Arc::clone(&publish_admission),
        subscriptions,
        shutdown_rx,
    ));
    let mut scrub_task = tokio::spawn(run_scrubber(
        Arc::clone(&broker),
        std::time::Duration::from_secs(config.storage.maintenance_startup_delay_seconds),
        std::time::Duration::from_secs(config.storage.scrub_interval_seconds),
    ));
    let mut disk_task = tokio::spawn(disk_guard::run(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&publish_admission),
        Arc::clone(&metrics),
        initially_pressured,
    ));
    let mut storage_health_task = tokio::spawn(monitor_storage_health(Arc::clone(&broker)));

    let (completed_task, terminal_result) = tokio::select! {
        result = &mut tcp_task => (
            Some(RuntimeTask::Tcp),
            unexpected_task_exit("TCP", result),
        ),
        result = &mut http_task => (
            Some(RuntimeTask::Http),
            unexpected_task_exit("HTTP", result),
        ),
        result = &mut kodo_http_task => (
            Some(RuntimeTask::KodoHttp),
            unexpected_task_exit("Kodo HTTP", result),
        ),
        result = &mut scrub_task => (
            Some(RuntimeTask::Scrub),
            unexpected_task_exit("storage scrub", result),
        ),
        result = &mut disk_task => (
            Some(RuntimeTask::DiskGuard),
            unexpected_task_exit("disk guard", result),
        ),
        result = &mut storage_health_task => (
            Some(RuntimeTask::StorageHealth),
            unexpected_task_exit("storage health", result),
        ),
        _ = shutdown_signal() => {
            info!("shutdown signal received");
            (None, Ok(()))
        },
    };
    accepting.store(false, Ordering::Release);
    delivering.store(false, Ordering::Release);
    let _ = shutdown_tx.send(true);
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(config.shutdown.grace_seconds);
    while shutdown_work_in_progress(
        &metrics,
        tcp_task.is_finished() && http_task.is_finished() && kodo_http_task.is_finished(),
    ) && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    if shutdown_work_in_progress(
        &metrics,
        tcp_task.is_finished() && http_task.is_finished() && kodo_http_task.is_finished(),
    ) {
        warn!(
            tcp_connections = metrics.tcp_connections.load(Ordering::Acquire),
            publish_inflight_bytes = metrics.publish_inflight_bytes.load(Ordering::Acquire),
            "broker shutdown grace expired with client work in progress"
        );
    }
    let mut cleanup_error = None;
    if completed_task != Some(RuntimeTask::Tcp) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_service_task(&mut tcp_task, "TCP").await,
        );
    }
    if completed_task != Some(RuntimeTask::Http) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_service_task(&mut http_task, "HTTP").await,
        );
    }
    if completed_task != Some(RuntimeTask::KodoHttp) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_service_task(&mut kodo_http_task, "Kodo HTTP").await,
        );
    }
    if completed_task != Some(RuntimeTask::Scrub) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_background_task(&mut scrub_task, "storage scrub").await,
        );
    }
    if completed_task != Some(RuntimeTask::DiskGuard) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_background_task(&mut disk_task, "disk guard").await,
        );
    }
    if completed_task != Some(RuntimeTask::StorageHealth) {
        record_cleanup_error(
            &mut cleanup_error,
            stop_background_task(&mut storage_health_task, "storage health").await,
        );
    }
    let released = broker.release_all_in_flight();
    record_cleanup_error(
        &mut cleanup_error,
        broker.flush().await.context("flush queue state"),
    );
    info!(released, "share-nothing broker stopped cleanly");
    combine_shutdown_results(terminal_result, cleanup_error)
}

fn shutdown_work_in_progress(metrics: &Metrics, services_finished: bool) -> bool {
    !services_finished || metrics.publish_inflight_bytes.load(Ordering::Acquire) > 0
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RuntimeTask {
    Tcp,
    Http,
    KodoHttp,
    Scrub,
    DiskGuard,
    StorageHealth,
}

fn unexpected_task_exit(
    name: &str,
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Result<()> {
    match result {
        Ok(Ok(())) => anyhow::bail!("{name} task stopped unexpectedly"),
        Ok(Err(error)) => Err(error).with_context(|| format!("{name} task stopped with an error")),
        Err(error) => Err(error).with_context(|| format!("{name} task join")),
    }
}

fn record_cleanup_error(first: &mut Option<anyhow::Error>, result: anyhow::Result<()>) {
    if let Err(error) = result {
        warn!(%error, "broker shutdown cleanup step failed");
        if first.is_none() {
            *first = Some(error);
        }
    }
}

fn combine_shutdown_results(
    terminal: anyhow::Result<()>,
    cleanup: Option<anyhow::Error>,
) -> anyhow::Result<()> {
    match (terminal, cleanup) {
        (Ok(()), None) => Ok(()),
        (Err(error), None) | (Ok(()), Some(error)) => Err(error),
        (Err(terminal), Some(cleanup)) => Err(anyhow::anyhow!(
            "{terminal:#}; shutdown cleanup also failed: {cleanup:#}"
        )),
    }
}

async fn stop_service_task(
    task: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    name: &str,
) -> anyhow::Result<()> {
    if !task.is_finished() {
        warn!(
            service = name,
            "service did not stop before the broker deadline; aborting it"
        );
        task.abort();
    }
    match task.await {
        Ok(result) => result.with_context(|| format!("{name} service stopped with an error")),
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(error).with_context(|| format!("{name} service task join")),
    }
}

async fn stop_background_task(
    task: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    name: &str,
) -> anyhow::Result<()> {
    task.abort();
    match task.await {
        Ok(result) => result.with_context(|| format!("{name} task stopped with an error")),
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(error).with_context(|| format!("{name} task join")),
    }
}

fn write_binary_capabilities(path: &std::path::Path) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(&config::runtime_capabilities())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_waits_for_service_completion_and_publish_reservations() {
        let metrics = Metrics::default();
        assert!(!shutdown_work_in_progress(&metrics, true));
        assert!(shutdown_work_in_progress(&metrics, false));

        metrics.tcp_connections.store(1, Ordering::Release);
        assert!(!shutdown_work_in_progress(&metrics, true));
        metrics.tcp_connections.store(0, Ordering::Release);

        metrics
            .publish_inflight_bytes
            .store(1024, Ordering::Release);
        assert!(shutdown_work_in_progress(&metrics, true));
    }
}
