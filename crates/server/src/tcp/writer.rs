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

pub(super) async fn write_message_timed(
    writer: &mut ClientWriter,
    header: &[u8],
    body: &[u8],
    write_timeout: Duration,
    expiration_ns: Option<i64>,
    policy_changes: &mut tokio::sync::watch::Receiver<rustqueue_queue::TopicPolicy>,
) -> anyhow::Result<()> {
    let effective_timeout = expiration_ns
        .map(remaining_until_ns)
        .map_or(write_timeout, |remaining| write_timeout.min(remaining));
    let write_frame = async {
        writer.write_message_parts(header, body).await?;
        if expiration_ns.is_some() {
            writer.flush_pending().await?;
        }
        anyhow::Ok(())
    };
    tokio::select! {
        biased;
        changed = policy_changes.changed() => {
            match changed {
                Ok(()) => Err(anyhow::anyhow!("Topic delivery policy changed during consumer write")),
                Err(_) => Err(anyhow::anyhow!("Topic closed during consumer write")),
            }
        }
        result = tokio::time::timeout(effective_timeout, write_frame) => {
            result
                .map_err(|_| anyhow::anyhow!("consumer delivery write timed out"))?
        }
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
    policy_changes: Option<&mut tokio::sync::watch::Receiver<rustqueue_queue::TopicPolicy>>,
) -> anyhow::Result<()> {
    if !writer.has_pending() {
        return Ok(());
    }
    let timeout = connection_progress_timeout(heartbeat);
    if let Some(policy_changes) = policy_changes {
        tokio::select! {
            biased;
            changed = policy_changes.changed() => {
                match changed {
                    Ok(()) => Err(anyhow::anyhow!("Topic delivery policy changed with buffered consumer output")),
                    Err(_) => Err(anyhow::anyhow!("Topic closed with buffered consumer output")),
                }
            }
            result = tokio::time::timeout(timeout, writer.flush_pending()) => {
                result
                    .map_err(|_| anyhow::anyhow!("client output flush timed out"))?
            }
        }
    } else {
        tokio::time::timeout(timeout, writer.flush_pending())
            .await
            .map_err(|_| anyhow::anyhow!("client output flush timed out"))?
    }
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

    #[tokio::test]
    async fn ttl_deadline_stops_a_partial_frame_and_requires_connection_close() {
        let (mut peer, server) = tokio::io::duplex(32);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 1);
        let body = vec![0xab; 1024 * 1024];
        let (_, mut policy_changes) =
            tokio::sync::watch::channel(rustqueue_queue::TopicPolicy::default());
        let result = write_message_timed(
            &mut writer,
            b"header",
            &body,
            Duration::from_secs(1),
            Some(now_ns().saturating_add(20_000_000)),
            &mut policy_changes,
        )
        .await;
        assert!(result.is_err());

        drop(writer);
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(received.len() < b"header".len() + body.len());
    }

    #[tokio::test]
    async fn policy_change_stops_a_partial_frame_and_requires_connection_close() {
        let (mut peer, server) = tokio::io::duplex(32);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 1);
        let body = vec![0xcd; 1024 * 1024];
        let (policy, mut policy_changes) =
            tokio::sync::watch::channel(rustqueue_queue::TopicPolicy::default());
        {
            let write = write_message_timed(
                &mut writer,
                b"header",
                &body,
                Duration::from_secs(1),
                None,
                &mut policy_changes,
            );
            tokio::pin!(write);
            tokio::select! {
                result = &mut write => panic!("partial frame completed before policy change: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            policy.send_replace(rustqueue_queue::TopicPolicy {
                delivery_mode: rustqueue_queue::DeliveryMode::TtlDiscard,
                message_ttl_seconds: Some(1),
            });
            assert!(write.as_mut().await.is_err());
        }
        drop(writer);

        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(!received.is_empty());
        assert!(received.len() < b"header".len() + body.len());
    }

    #[tokio::test]
    async fn policy_change_discards_a_buffered_reliable_frame_before_flush() {
        let (mut peer, server) = tokio::io::duplex(1024);
        let io: BoxIo = Box::new(server);
        let (_, write) = tokio::io::split(io);
        let mut writer = ClientWriter::new(write, 4096);
        let (policy, mut policy_changes) =
            tokio::sync::watch::channel(rustqueue_queue::TopicPolicy::default());
        writer
            .write_message_parts(b"header", b"body")
            .await
            .unwrap();
        policy.send_replace(rustqueue_queue::TopicPolicy {
            delivery_mode: rustqueue_queue::DeliveryMode::TtlDiscard,
            message_ttl_seconds: Some(1),
        });

        assert!(flush_timed(&mut writer, None, Some(&mut policy_changes))
            .await
            .is_err());
        drop(writer);
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(received.is_empty());
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
