#[path = "tcp/gateway.rs"]
mod gateway;

use crate::backend::BackendPool;
use crate::metrics::ProxyMetrics;
use rand::Rng;
use rustqueue_protocol::{Command, MAX_MPUB_MESSAGES};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

pub(crate) const MAX_CONTROL_BODY_BYTES: usize = 1024 * 1024;
const IDENTIFY_WORKING_SET_COPIES: usize = 3;
const IDENTIFY_FIXED_WORKING_BYTES: usize = 4096;
const MPUB_MESSAGE_WORKING_BYTES: usize = 128;

fn command_working_set(command: &Command, bytes: usize) -> usize {
    match command {
        Command::Identify => bytes
            .saturating_mul(IDENTIFY_WORKING_SET_COPIES)
            .saturating_add(IDENTIFY_FIXED_WORKING_BYTES),
        Command::MultiPublish { .. } => {
            bytes.saturating_add(MAX_MPUB_MESSAGES.saturating_mul(MPUB_MESSAGE_WORKING_BYTES))
        }
        _ => bytes,
    }
}

pub(crate) fn maximum_gateway_working_set(
    max_message_bytes: usize,
    max_body_bytes: usize,
) -> usize {
    command_working_set(&Command::Identify, MAX_CONTROL_BODY_BYTES)
        .max(command_working_set(
            &Command::MultiPublish {
                topic: String::new(),
            },
            max_body_bytes,
        ))
        .max(max_message_bytes)
}

pub struct Limits {
    pub max_connections: usize,
    pub max_connection_age: Duration,
    pub terminate_producer_protocol: bool,
    pub max_message_bytes: usize,
    pub max_body_bytes: usize,
    pub command_timeout: Duration,
    pub inflight_bytes: Arc<Semaphore>,
}

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    limits: Limits,
    metrics: ProxyMetrics,
    mut shutdown: watch::Receiver<bool>,
    shutdown_grace: Duration,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let mut sessions = JoinSet::new();
    tracing::info!(%address, "producer TCP proxy listening");
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => Some(result?),
            result = sessions.join_next(), if !sessions.is_empty() => {
                log_session_result(result);
                None
            }
            _ = shutdown.changed() => break,
        };
        let Some((mut client, peer)) = accepted else {
            continue;
        };
        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
            tracing::debug!(%peer, "rejecting producer because proxy connection limit is reached");
            continue;
        };
        let metrics = metrics.clone();
        if limits.terminate_producer_protocol {
            let pool = pool.clone();
            let gateway_limits = gateway::Limits {
                max_message_bytes: limits.max_message_bytes,
                max_body_bytes: limits.max_body_bytes,
                command_timeout: limits.command_timeout,
                inflight_bytes: Arc::clone(&limits.inflight_bytes),
            };
            let session_shutdown = shutdown.clone();
            sessions.spawn(async move {
                let _permit = permit;
                let _ = client.set_nodelay(true);
                if let Err(error) =
                    gateway::run(client, pool, gateway_limits, metrics, session_shutdown).await
                {
                    tracing::debug!(%peer, %error, "producer Gateway session closed");
                }
            });
            continue;
        }
        let Some(backend) = pool.lease() else {
            tracing::debug!(%peer, "rejecting producer because no broker is ready");
            continue;
        };
        let connection_age = jittered_connection_age(limits.max_connection_age);
        sessions.spawn(async move {
            let _permit = permit;
            let _backend_lease = backend;
            let mut invalidation = _backend_lease.invalidation();
            let connect_timer = metrics.backend.timer();
            match tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(_backend_lease.tcp_address()),
            )
            .await
            {
                Ok(Ok(mut broker)) => {
                    drop(connect_timer);
                    let _ = client.set_nodelay(true);
                    let _ = broker.set_nodelay(true);
                    let tunnel = tokio::io::copy_bidirectional(&mut client, &mut broker);
                    tokio::select! {
                        result = tunnel => {
                            if let Err(error) = result {
                                tracing::debug!(%peer, %error, "producer TCP proxy closed");
                            }
                        }
                        _ = invalidation.changed() => {
                            metrics.tcp_connection_rotations.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                %peer,
                                node_id = _backend_lease.node_id,
                                "closing producer tunnel because backend left discovery"
                            );
                        }
                        _ = async {
                            if let Some(connection_age) = connection_age {
                                tokio::time::sleep(connection_age).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {
                            metrics.tcp_connection_rotations.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                %peer,
                                node_id = _backend_lease.node_id,
                                max_age_seconds = connection_age.map_or(0, |age| age.as_secs()),
                                "rotating producer TCP connection"
                            );
                        }
                    }
                }
                Ok(Err(error)) => {
                    drop(connect_timer);
                    tracing::debug!(%peer, %error, node_id = _backend_lease.node_id, "broker connect failed")
                }
                Err(_) => {
                    drop(connect_timer);
                    tracing::debug!(%peer, node_id = _backend_lease.node_id, "broker connect timed out")
                }
            }
        });
    }
    tracing::info!("producer TCP proxy stopped accepting new connections");
    if tokio::time::timeout(shutdown_grace, drain_sessions(&mut sessions))
        .await
        .is_err()
    {
        tracing::warn!(
            active_sessions = sessions.len(),
            "producer TCP proxy shutdown grace expired"
        );
        sessions.abort_all();
        drain_sessions(&mut sessions).await;
    }
    Ok(())
}

async fn drain_sessions(sessions: &mut JoinSet<()>) {
    while let Some(result) = sessions.join_next().await {
        log_session_result(Some(result));
    }
}

fn log_session_result(result: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = result {
        tracing::warn!(%error, "producer proxy session task failed");
    }
}

fn jittered_connection_age(base: Duration) -> Option<Duration> {
    if base.is_zero() {
        return None;
    }
    let seconds = base.as_secs().max(1);
    let jitter = (seconds / 10).max(1).min(seconds);
    let lower = seconds.saturating_sub(jitter).max(1);
    let upper = seconds.saturating_add(jitter);
    Some(Duration::from_secs(
        rand::thread_rng().gen_range(lower..=upper),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_age_disables_rotation() {
        assert_eq!(jittered_connection_age(Duration::ZERO), None);
    }

    #[test]
    fn connection_age_has_bounded_jitter() {
        for _ in 0..100 {
            let age = jittered_connection_age(Duration::from_secs(300)).unwrap();
            assert!((270..=330).contains(&age.as_secs()));
        }
    }
}
