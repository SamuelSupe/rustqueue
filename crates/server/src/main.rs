mod admission;
mod auth;
mod compression;
mod config;
mod discovery;
mod http;
mod internal;
mod metrics;
mod snapshot_cli;
mod tcp;
mod tls;

use admission::PublishAdmission;
use anyhow::Context;
use clap::{Parser, Subcommand};
use config::Config;
use metrics::Metrics;
use rustqueue_consensus::{
    AutomationOptions, BasicNode, ClusterRuntime, ControlPlaneOptions, MetadataCatalog,
    NodeDescriptor, RetentionOptions,
};
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
    about = "NSQ-compatible durable message queue"
)]
struct Cli {
    #[arg(long, env = "RUSTQUEUE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long)]
    check_config: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Snapshot(snapshot_cli::SnapshotCommand),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();
    if let Some(Command::Snapshot(command)) = cli.command {
        return snapshot_cli::run(command);
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

    let broker = Broker::open(BrokerConfig {
        data_path: config.storage.data_path.clone(),
        default_partitions: config.queue.default_partitions,
        max_segment_bytes: config.storage.max_segment_bytes,
        max_message_bytes: config.queue.max_message_bytes,
        message_timeout: config.message_timeout(),
        max_ack_gap: config.queue.max_ack_gap,
        max_backlog_messages_per_partition: config.queue.max_backlog_messages_per_partition,
        projection_only: config.cluster.enabled,
        entry_cache_bytes: config.storage.entry_cache_bytes,
        payload_read_workers: config.storage.payload_read_workers,
        payload_read_queue: config.storage.payload_read_queue,
        dedup_max_entries: config.storage.dedup_max_entries,
        dedup_ttl: std::time::Duration::from_secs(config.storage.dedup_ttl_seconds),
        cell_id: config.local_cell_id().0,
    })
    .context("open durable queue")?;
    broker.scrub().context("startup data scrub")?;

    let config = Arc::new(config);
    let metrics = Arc::new(Metrics::default());
    let publish_admission = Arc::new(PublishAdmission::new(
        config.limits.node_publish_inflight_bytes,
        Arc::clone(&metrics),
    ));
    let consensus = if config.cluster.enabled {
        let tls = config
            .security
            .internal_tls
            .as_ref()
            .expect("validated cluster TLS configuration");
        let client = tls::internal_http_client(tls)?;
        let snapshot_client = tls::internal_snapshot_http_client(tls)?;
        let placement_nodes = config.placement_nodes();
        // Every partition Raft instance knows the static federation allowlist.
        // Cell metadata still places replicas only on `placement_nodes`, while
        // a live migration may temporarily add learners from another Cell.
        let nodes = config
            .cluster
            .nodes
            .iter()
            .map(|(id, node)| {
                Ok((
                    id.parse::<u64>()?,
                    BasicNode::new(node.raft_address.clone()),
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        let metadata_nodes = placement_nodes
            .iter()
            .map(|(id, node)| {
                let id = id.parse::<u64>()?;
                Ok((
                    id,
                    NodeDescriptor {
                        id,
                        raft_address: node.raft_address.clone(),
                        broadcast_address: node.broadcast_address.clone(),
                        tcp_port: node.tcp_port,
                        http_port: node.http_port,
                        tls_server_name: node.tls_server_name.clone(),
                        failure_domain: node.failure_domain.clone(),
                        peer_id: None,
                        cell_id: config.local_cell_id(),
                        federation_router: config.is_federation_router(id),
                    },
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        let metadata = Arc::new(
            MetadataCatalog::new_federated_in_cell(
                config.local_cell_id(),
                metadata_nodes,
                config.queue.default_partitions,
                config.cluster.default_replication_factor,
                config.cluster.federation.max_home_cells_per_topic,
            )
            .map_err(anyhow::Error::msg)?,
        );
        let control_options = if config.cluster.federation.enabled {
            let descriptors = config
                .cluster
                .nodes
                .iter()
                .map(|(id, node)| {
                    let id = id.parse::<u64>()?;
                    Ok((
                        id,
                        NodeDescriptor {
                            id,
                            raft_address: node.raft_address.clone(),
                            broadcast_address: node.broadcast_address.clone(),
                            tcp_port: node.tcp_port,
                            http_port: node.http_port,
                            tls_server_name: node.tls_server_name.clone(),
                            failure_domain: node.failure_domain.clone(),
                            peer_id: None,
                            cell_id: rustqueue_consensus::CellId(
                                node.cell_id.unwrap_or(config.local_cell_id().0),
                            ),
                            federation_router: node.federation_router,
                        },
                    ))
                })
                .collect::<anyhow::Result<_>>()?;
            ControlPlaneOptions {
                enabled: true,
                nodes: descriptors,
                voters: config.control_voters(),
                max_home_cells_per_topic: config.cluster.federation.max_home_cells_per_topic,
                route_cache_ms: config.cluster.federation.route_cache_ms,
                retry_after_ms: config.cluster.federation.retry_after_ms,
            }
        } else {
            ControlPlaneOptions::default()
        };
        Some(
            ClusterRuntime::open(
                config.node.id,
                &config.cluster.name,
                nodes,
                config.storage.data_path.join("consensus"),
                Arc::clone(&broker),
                metadata,
                client,
                snapshot_client,
                control_options,
                AutomationOptions {
                    enabled: config.cluster.automation.enabled,
                    node_stabilization_seconds: config
                        .cluster
                        .automation
                        .node_stabilization_seconds,
                    node_down_grace_seconds: config.cluster.automation.node_down_grace_seconds,
                    group_cooldown_seconds: config.cluster.automation.group_cooldown_seconds,
                    max_concurrent_migrations: config.cluster.automation.max_concurrent_migrations,
                    max_migrations_per_node: config.cluster.automation.max_migrations_per_node,
                    operation_history_limit: config.cluster.automation.operation_history_limit,
                    auto_replace_metadata: config.cluster.automation.auto_replace_metadata,
                    disk_high_watermark_percent: config.storage.disk_high_watermark_percent,
                    disk_low_watermark_percent: config.storage.disk_low_watermark_percent,
                    min_free_bytes: config.storage.min_free_bytes,
                    protective_eviction_enabled: config.storage.protective_eviction_enabled,
                    disk_pressure_grace_seconds: config.storage.disk_pressure_grace_seconds,
                },
                RetentionOptions {
                    message_retention_seconds: config.queue.message_retention_seconds,
                    dead_letter_suffix: config.queue.dead_letter_suffix.clone(),
                    max_groups_per_cycle: 32,
                },
            )
            .await
            .context("open Raft node")?,
        )
    } else {
        None
    };
    info!(
        node_id = config.node.id,
        tcp = %config.network.tcp_address,
        http = %config.network.http_address,
        "starting RustQueue"
    );

    let accepting = Arc::new(AtomicBool::new(true));
    let federation_peers = Arc::new(discovery::Directory::default());
    let tcp_task = tokio::spawn(tcp::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        consensus.clone(),
        Arc::clone(&accepting),
        Arc::clone(&publish_admission),
    ));
    let http_task = tokio::spawn(http::serve(
        Arc::clone(&config),
        Arc::clone(&broker),
        Arc::clone(&metrics),
        consensus.clone(),
        Arc::clone(&accepting),
        publish_admission,
        Arc::clone(&federation_peers),
    ));
    let internal_task = if let Some(node) = consensus.clone() {
        tokio::spawn(internal::serve(Arc::clone(&config), node))
    } else {
        tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        })
    };
    let (discovery_task, discovery_admission_task) = if config.cluster.discovery.enabled {
        let runtime = consensus
            .clone()
            .expect("discovery validation requires cluster mode");
        let local = config
            .cluster
            .nodes
            .get(&config.node.id.to_string())
            .expect("validated local cluster node");
        let descriptor = NodeDescriptor {
            id: config.node.id,
            raft_address: local.raft_address.clone(),
            broadcast_address: local.broadcast_address.clone(),
            tcp_port: local.tcp_port,
            http_port: local.http_port,
            tls_server_name: local.tls_server_name.clone(),
            failure_domain: local.failure_domain.clone(),
            peer_id: None,
            cell_id: config.local_cell_id(),
            federation_router: config.is_federation_router(config.node.id),
        };
        let options = discovery::Options {
            config: config.cluster.discovery.clone(),
            identity_path: config
                .cluster
                .discovery
                .identity_path(&config.storage.data_path),
            cluster_name: config.cluster.name.clone(),
            descriptor,
            join_token: config.cluster.discovery.read_join_token()?,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(128);
        (
            Some(tokio::spawn(discovery::run(options, sender))),
            Some(tokio::spawn(run_discovery_admission(
                runtime,
                config.local_cell_id(),
                Arc::clone(&federation_peers),
                std::time::Duration::from_secs(
                    config.cluster.automation.node_stabilization_seconds,
                ),
                receiver,
            ))),
        )
    } else {
        (None, None)
    };
    let scrub_task = tokio::spawn(run_scrubber(
        Arc::clone(&broker),
        consensus.clone(),
        std::time::Duration::from_secs(config.storage.scrub_interval_seconds),
    ));
    let reconcile_task = consensus.clone().map(|runtime| {
        tokio::spawn(run_reconciler(
            runtime,
            std::time::Duration::from_secs(config.cluster.automation.poll_interval_seconds),
        ))
    });
    let clock_task = consensus
        .clone()
        .map(|runtime| tokio::spawn(run_clock_monitor(runtime)));
    if config.cluster.bootstrap {
        if let Some(node) = consensus.clone() {
            let voter_ids: std::collections::BTreeSet<u64> =
                if config.cluster.initial_voters.is_empty() {
                    config
                        .placement_nodes()
                        .keys()
                        .take(config.cluster.metadata_replication_factor as usize)
                        .map(|id| id.parse().expect("validated cluster node ID"))
                        .collect()
                } else {
                    config.cluster.initial_voters.iter().copied().collect()
                };
            let members = config
                .placement_nodes()
                .iter()
                .filter(|(id, _)| {
                    voter_ids.contains(&id.parse::<u64>().expect("validated cluster node ID"))
                })
                .map(|(id, node)| {
                    (
                        id.parse::<u64>().expect("validated cluster node ID"),
                        BasicNode::new(node.raft_address.clone()),
                    )
                })
                .collect();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Err(error) = node.initialize_metadata(members).await {
                    tracing::warn!(%error, "Raft bootstrap skipped or failed");
                }
                if let Err(error) = node.initialize_control_groups().await {
                    tracing::warn!(%error, "Root/Catalog bootstrap skipped or failed");
                }
                let learners: Vec<_> = node
                    .metadata()
                    .snapshot()
                    .nodes
                    .keys()
                    .filter(|id| !voter_ids.contains(id))
                    .copied()
                    .collect();
                for learner in learners {
                    let mut last_error = None;
                    for _ in 0..30 {
                        match node.add_metadata_learner(learner).await {
                            Ok(()) => {
                                last_error = None;
                                break;
                            }
                            Err(error) => last_error = Some(error),
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    if let Some(error) = last_error {
                        tracing::warn!(node_id = learner, %error, "metadata learner add failed");
                    }
                }
            });
        }
    }

    tokio::select! {
        result = tcp_task => result.context("TCP task join")??,
        result = http_task => result.context("HTTP task join")??,
        result = internal_task => result.context("internal Raft task join")??,
        result = scrub_task => result.context("storage scrub task join")??,
        result = async {
            match reconcile_task {
                Some(task) => task.await.context("metadata reconciler task join")?,
                None => std::future::pending().await,
            }
        } => result?,
        result = async {
            match clock_task {
                Some(task) => task.await.context("clock monitor task join")?,
                None => std::future::pending().await,
            }
        } => result?,
        result = async {
            match discovery_task {
                Some(task) => task.await.context("node discovery task join")?,
                None => std::future::pending().await,
            }
        } => result?,
        result = async {
            match discovery_admission_task {
                Some(task) => task.await.context("node discovery admission task join")?,
                None => std::future::pending().await,
            }
        } => result?,
        _ = shutdown_signal() => {
            info!("shutdown signal received");
            accepting.store(false, Ordering::Release);
            if let Some(runtime) = &consensus {
                runtime.begin_shutdown();
                runtime.evacuate_local_leaders().await;
            }
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_secs(config.cluster.shutdown.grace_seconds);
            while metrics.tcp_connections.load(Ordering::Acquire) > 0
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let released = broker.release_all_in_flight();
            info!(released, "released remaining in-flight leases");
            if let Some(runtime) = &consensus {
                runtime.shutdown().await.context("shutdown Raft groups")?;
            }
            broker.finish_replicated_batch().context("flush queue state")?;
            info!("graceful shutdown completed");
        },
    }
    Ok(())
}

async fn run_discovery_admission(
    runtime: Arc<ClusterRuntime>,
    local_cell: rustqueue_consensus::CellId,
    directory: Arc<discovery::Directory>,
    stabilization: std::time::Duration,
    mut receiver: tokio::sync::mpsc::Receiver<discovery::DiscoveredNode>,
) -> anyhow::Result<()> {
    let mut first_seen = std::collections::BTreeMap::new();
    while let Some(discovered) = receiver.recv().await {
        directory.observe(discovered.clone());
        if discovered.descriptor.id == runtime.node_id() {
            continue;
        }
        if discovered.descriptor.cell_id != local_cell {
            tracing::debug!(
                node_id = discovered.descriptor.id,
                peer_cell = %discovered.descriptor.cell_id,
                local_cell = %local_cell,
                "remote Cell peer retained by discovery but not admitted to local Raft placement"
            );
            continue;
        }
        let first = first_seen
            .entry(discovered.descriptor.id)
            .or_insert_with(std::time::Instant::now);
        if first.elapsed() < stabilization {
            continue;
        }
        match runtime
            .admit_discovered_node(discovered.descriptor.clone())
            .await
        {
            Ok(true) => tracing::info!(
                node_id = discovered.descriptor.id,
                peer_id = %discovered.peer_id,
                addresses = ?discovered.addresses,
                "discovered node admitted through metadata Raft"
            ),
            Ok(false) => {}
            Err(error) => tracing::debug!(
                node_id = discovered.descriptor.id,
                peer_id = %discovered.peer_id,
                %error,
                "discovered node admission will retry"
            ),
        }
    }
    anyhow::bail!("node discovery service stopped")
}

async fn run_reconciler(
    runtime: Arc<ClusterRuntime>,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(error) = runtime.reconcile_once().await {
            tracing::warn!(%error, "metadata reconciliation will retry");
        }
    }
}

async fn run_clock_monitor(runtime: Arc<ClusterRuntime>) -> anyhow::Result<()> {
    loop {
        match runtime.check_clock_once().await {
            Ok(status) if !status.healthy => {
                tracing::warn!(
                    offset_ms = status.offset_ms,
                    reason = status.reason,
                    "clock guard stopped publish and delivery"
                );
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "clock check failed"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn run_scrubber(
    broker: Arc<Broker>,
    consensus: Option<Arc<ClusterRuntime>>,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(interval).await;
        let queue_records = broker.scrub().context("background queue scrub")?;
        let cluster = match &consensus {
            Some(consensus) => consensus.scrub_and_repair().await?,
            None => Default::default(),
        };
        tracing::info!(
            records = queue_records + cluster.records_checked,
            repairs = cluster.replicas_repaired,
            "background data scrub completed"
        );
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
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
