use crate::backend::BackendPool;
use crate::metrics::ProxyMetrics;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    max_connections: usize,
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
                    if let Err(error) =
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
