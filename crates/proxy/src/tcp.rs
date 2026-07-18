use crate::backend::BackendPool;
use crate::metrics::ProxyMetrics;
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    max_connections: usize,
    max_connection_age: Duration,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let connections = Arc::new(Semaphore::new(max_connections));
    tracing::info!(%address, "producer TCP proxy listening");
    loop {
        let (mut client, peer) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
            tracing::debug!(%peer, "rejecting producer because proxy connection limit is reached");
            continue;
        };
        let Some(backend) = pool.lease() else {
            tracing::debug!(%peer, "rejecting producer because no broker is ready");
            continue;
        };
        let metrics = metrics.clone();
        let connection_age = jittered_connection_age(max_connection_age);
        tokio::spawn(async move {
            let _permit = permit;
            let _backend_lease = backend;
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
                    if let Some(connection_age) = connection_age {
                        match tokio::time::timeout(
                            connection_age,
                            tokio::io::copy_bidirectional(&mut client, &mut broker),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                tracing::debug!(%peer, %error, "producer TCP proxy closed")
                            }
                            Err(_) => {
                                metrics
                                    .tcp_connection_rotations
                                    .fetch_add(1, Ordering::Relaxed);
                                tracing::debug!(
                                    %peer,
                                    node_id = _backend_lease.node_id,
                                    max_age_seconds = connection_age.as_secs(),
                                    "rotating producer TCP connection"
                                );
                            }
                        }
                    } else if let Err(error) =
                        tokio::io::copy_bidirectional(&mut client, &mut broker).await
                    {
                        tracing::debug!(%peer, %error, "producer TCP proxy closed");
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
