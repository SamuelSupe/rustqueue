use super::*;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::io::BufWriter;

pub(super) struct ClientWriter {
    inner: BufWriter<WriteHalf<BoxIo>>,
    buffering: bool,
    dirty: bool,
}

impl ClientWriter {
    pub fn new(inner: WriteHalf<BoxIo>, output_buffer_size: usize) -> Self {
        let buffering = output_buffer_size > 1;
        Self {
            inner: BufWriter::with_capacity(output_buffer_size.max(1), inner),
            buffering,
            dirty: false,
        }
    }

    pub async fn write_message_parts(&mut self, header: &[u8], body: &[u8]) -> anyhow::Result<()> {
        self.write_all(header).await?;
        self.write_all(body).await?;
        if !self.buffering {
            self.flush().await?;
        }
        Ok(())
    }

    pub fn has_pending(&self) -> bool {
        self.dirty
    }

    pub async fn flush_pending(&mut self) -> anyhow::Result<()> {
        if self.dirty {
            self.flush().await?;
        }
        Ok(())
    }
}

pub(super) fn delivery_write_timeout(heartbeat: Option<Duration>) -> Duration {
    heartbeat
        .unwrap_or(Duration::from_secs(30))
        .saturating_mul(2)
        .max(Duration::from_secs(1))
}

pub(super) fn delivery_visibility_timeout(
    message_timeout: Duration,
    output_buffer_timeout: Option<Duration>,
) -> Duration {
    message_timeout.saturating_add(output_buffer_timeout.unwrap_or_default())
}

pub(super) fn connection_progress_timeout(heartbeat: Option<Duration>) -> Duration {
    heartbeat
        .map(|interval| interval.saturating_mul(2))
        .unwrap_or(Duration::from_secs(60))
        .max(Duration::from_secs(5))
}

pub(super) async fn write_error_timed(
    writer: &mut ClientWriter,
    heartbeat: Option<Duration>,
    code: &str,
    detail: &str,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        connection_progress_timeout(heartbeat),
        write_error(writer, code, detail),
    )
    .await
    .map_err(|_| anyhow::anyhow!("client error write timed out"))??;
    Ok(())
}

pub(super) async fn flush_timed(
    writer: &mut ClientWriter,
    heartbeat: Option<Duration>,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        connection_progress_timeout(heartbeat),
        writer.flush_pending(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("client output flush timed out"))??;
    Ok(())
}

impl AsyncWrite for ClientWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(context, buffer) {
            Poll::Ready(Ok(written)) => {
                self.dirty |= written > 0;
                Poll::Ready(Ok(written))
            }
            result => result,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                self.dirty = false;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn buffers_messages_until_flushed() {
        let (mut peer, server) = tokio::io::duplex(1024);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 128);

        writer.write_message_parts(b"", b"message").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), peer.read_u8())
                .await
                .is_err()
        );

        writer.flush_pending().await.unwrap();
        let mut received = [0; 7];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"message");
    }

    #[tokio::test]
    async fn disabled_buffering_flushes_each_message() {
        let (mut peer, server) = tokio::io::duplex(1024);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 1);

        writer.write_message_parts(b"", b"message").await.unwrap();
        let mut received = [0; 7];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"message");
    }

    #[tokio::test]
    async fn writes_message_header_and_body_without_a_combined_buffer() {
        let (mut peer, server) = tokio::io::duplex(1024);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 1);

        writer.write_message_parts(b"head", b"body").await.unwrap();
        let mut received = [0; 8];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"headbody");
    }

    #[test]
    fn client_write_timeouts_are_bounded_when_heartbeats_are_disabled() {
        assert_eq!(delivery_write_timeout(None), Duration::from_secs(60));
        assert_eq!(
            delivery_write_timeout(Some(Duration::from_millis(100))),
            Duration::from_secs(1)
        );
        assert_eq!(connection_progress_timeout(None), Duration::from_secs(60));
        assert_eq!(
            connection_progress_timeout(Some(Duration::from_millis(100))),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn initial_delivery_lease_covers_output_buffering() {
        assert_eq!(
            delivery_visibility_timeout(Duration::from_secs(1), Some(Duration::from_secs(30))),
            Duration::from_secs(31)
        );
        assert_eq!(
            delivery_visibility_timeout(Duration::from_secs(1), None),
            Duration::from_secs(1)
        );
    }
}
