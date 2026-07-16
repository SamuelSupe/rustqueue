use crate::backend::BackendPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    max_connections: usize,
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
        let Some(backend) = pool.shuffled(1).pop() else {
            tracing::debug!(%peer, "rejecting producer because no broker is ready");
            continue;
        };
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::net::TcpStream::connect(backend.tcp_address()),
            )
            .await
            {
                Ok(Ok(mut broker)) => {
                    let _ = client.set_nodelay(true);
                    let _ = broker.set_nodelay(true);
                    if let Err(error) =
                        tokio::io::copy_bidirectional(&mut client, &mut broker).await
                    {
                        tracing::debug!(%peer, %error, "producer TCP proxy closed");
                    }
                }
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %error, node_id = backend.node_id, "broker connect failed")
                }
                Err(_) => {
                    tracing::debug!(%peer, node_id = backend.node_id, "broker connect timed out")
                }
            }
        });
    }
}
