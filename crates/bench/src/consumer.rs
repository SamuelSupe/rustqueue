use anyhow::Context;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinSet;

const RDY_COUNT: u64 = 2_500;
const RDY_REFILL_AT: u64 = RDY_COUNT / 4;
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct IdRanges {
    ranges: BTreeMap<u64, u64>,
}

impl IdRanges {
    fn insert(&mut self, id: u64) -> bool {
        let previous = self
            .ranges
            .range(..=id)
            .next_back()
            .map(|(&start, &end)| (start, end));
        if previous.is_some_and(|(_, end)| id <= end) {
            return false;
        }
        let next = self
            .ranges
            .range(id..)
            .next()
            .map(|(&start, &end)| (start, end));
        let joins_previous = previous.is_some_and(|(_, end)| end.checked_add(1) == Some(id));
        let joins_next = next.is_some_and(|(start, _)| id.checked_add(1) == Some(start));

        match (previous, next, joins_previous, joins_next) {
            (Some((start, _)), Some((next_start, next_end)), true, true) => {
                *self.ranges.get_mut(&start).expect("previous range exists") = next_end;
                self.ranges.remove(&next_start);
            }
            (Some((start, _)), _, true, false) => {
                *self.ranges.get_mut(&start).expect("previous range exists") = id;
            }
            (_, Some((next_start, next_end)), false, true) => {
                self.ranges.remove(&next_start);
                self.ranges.insert(id, next_end);
            }
            _ => {
                self.ranges.insert(id, id);
            }
        }
        true
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DeliverySnapshot {
    pub(crate) unique: u64,
    pub(crate) total: u64,
}

impl DeliverySnapshot {
    pub(crate) fn duplicates(self) -> u64 {
        self.total.saturating_sub(self.unique)
    }
}

pub(crate) struct DeliveryWait {
    pub(crate) snapshot: DeliverySnapshot,
    pub(crate) complete: bool,
}

pub(crate) struct ConsumerProgress {
    ids: Mutex<IdRanges>,
    unique: AtomicU64,
    total: AtomicU64,
    changed: Notify,
}

impl Default for ConsumerProgress {
    fn default() -> Self {
        Self {
            ids: Mutex::new(IdRanges::default()),
            unique: AtomicU64::new(0),
            total: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }
}

impl ConsumerProgress {
    fn observe(&self, id: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        let inserted = self
            .ids
            .lock()
            .expect("consumer progress lock poisoned")
            .insert(id);
        if inserted {
            self.unique.fetch_add(1, Ordering::Release);
            self.changed.notify_one();
        }
    }

    pub(crate) fn snapshot(&self) -> DeliverySnapshot {
        DeliverySnapshot {
            unique: self.unique.load(Ordering::Acquire),
            total: self.total.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn wait_for(&self, target: u64, timeout: Duration) -> DeliveryWait {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.changed.notified();
            let snapshot = self.snapshot();
            if snapshot.unique >= target {
                return DeliveryWait {
                    snapshot,
                    complete: true,
                };
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return DeliveryWait {
                    snapshot: self.snapshot(),
                    complete: false,
                };
            }
        }
    }
}

pub(crate) struct ConsumerGroup {
    stop: watch::Sender<bool>,
    tasks: JoinSet<anyhow::Result<()>>,
    failures: mpsc::UnboundedReceiver<String>,
}

impl ConsumerGroup {
    pub(crate) async fn failure(&mut self) -> String {
        self.failures
            .recv()
            .await
            .unwrap_or_else(|| "all consumers exited unexpectedly".into())
    }

    pub(crate) async fn stop(mut self) -> anyhow::Result<()> {
        let _ = self.stop.send(true);
        match tokio::time::timeout(STOP_TIMEOUT, join_consumers(&mut self.tasks)).await {
            Ok(result) => result,
            Err(_) => {
                self.tasks.abort_all();
                while self.tasks.join_next().await.is_some() {}
                anyhow::bail!("benchmark consumers did not stop within {STOP_TIMEOUT:?}");
            }
        }
    }
}

async fn join_consumers(tasks: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result
            .context("benchmark consumer panicked")
            .and_then(|result| result);
        if let Err(error) = result {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) async fn start_consumers(
    address: &str,
    topic: &str,
    channel: &str,
    count: usize,
    progress: Arc<ConsumerProgress>,
) -> anyhow::Result<ConsumerGroup> {
    let (stop, stop_rx) = watch::channel(false);
    let (ready, mut ready_rx) = mpsc::channel(count);
    let (failure_tx, mut failures) = mpsc::unbounded_channel();
    let mut tasks = JoinSet::new();
    for _ in 0..count {
        let failure_tx = failure_tx.clone();
        let worker = consume_worker(
            address.to_owned(),
            topic.to_owned(),
            channel.to_owned(),
            ready.clone(),
            stop_rx.clone(),
            Arc::clone(&progress),
        );
        tasks.spawn(async move {
            let result = worker.await;
            if let Err(error) = &result {
                let _ = failure_tx.send(format!("{error:#}"));
            }
            result
        });
    }
    drop(ready);
    drop(failure_tx);
    for _ in 0..count {
        tokio::select! {
            ready = ready_rx.recv() => {
                if ready.is_none() {
                    let group = ConsumerGroup { stop, tasks, failures };
                    let _ = group.stop().await;
                    anyhow::bail!("consumer exited before subscription became ready");
                }
            }
            failure = failures.recv() => {
                let failure =
                    failure.unwrap_or_else(|| "consumer exited before subscription became ready".into());
                let group = ConsumerGroup { stop, tasks, failures };
                let _ = group.stop().await;
                anyhow::bail!("consumer failed before subscription became ready: {failure}");
            }
        }
    }
    Ok(ConsumerGroup {
        stop,
        tasks,
        failures,
    })
}

async fn consume_worker(
    address: String,
    topic: String,
    channel: String,
    ready: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
    progress: Arc<ConsumerProgress>,
) -> anyhow::Result<()> {
    let stream = TcpStream::connect(&address)
        .await
        .with_context(|| format!("connect consumer to {address}"))?;
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(b"  V2").await?;
    writer
        .write_all(format!("SUB {topic} {channel}\n").as_bytes())
        .await?;
    wait_for_ok(&mut reader, &mut writer).await?;
    writer
        .write_all(format!("RDY {RDY_COUNT}\n").as_bytes())
        .await?;
    ready.send(()).await.ok();
    drop(ready);

    let mut remaining_rdy = RDY_COUNT;
    loop {
        let (stopping, frame) = {
            let frame = read_frame(&mut reader);
            tokio::pin!(frame);
            tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        writer.write_all(b"CLS\n").await?;
                        writer.flush().await?;
                        (true, frame.await)
                    } else {
                        (false, frame.await)
                    }
                }
                frame = &mut frame => (false, frame),
            }
        };
        let (frame_type, response) = match frame {
            Ok(frame) => frame,
            Err(_) if stopping => return Ok(()),
            Err(error) => return Err(error),
        };
        if stopping {
            return close_consumer(&mut reader, &mut writer, Some((frame_type, response))).await;
        }
        match frame_type {
            0 if response == b"_heartbeat_" => writer.write_all(b"NOP\n").await?,
            2 if response.len() >= 26 => {
                let id: [u8; 16] = response[10..26]
                    .try_into()
                    .expect("message frame ID length was checked");
                let numeric_id = parse_message_id(&id)?;
                writer.write_all(b"FIN ").await?;
                writer.write_all(&id).await?;
                writer.write_all(b"\n").await?;
                remaining_rdy = remaining_rdy.saturating_sub(1);
                if remaining_rdy <= RDY_REFILL_AT {
                    writer
                        .write_all(format!("RDY {RDY_COUNT}\n").as_bytes())
                        .await?;
                    remaining_rdy = RDY_COUNT;
                }
                progress.observe(numeric_id);
            }
            1 => anyhow::bail!("consumer error: {}", String::from_utf8_lossy(&response)),
            _ => {}
        }
    }
}

async fn close_consumer<R, W>(
    reader: &mut R,
    writer: &mut W,
    mut pending: Option<(i32, Vec<u8>)>,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let (frame_type, response) = match pending.take() {
            Some(frame) => frame,
            None => read_frame(reader).await?,
        };
        match frame_type {
            0 if response == b"CLOSE_WAIT" => return Ok(()),
            0 if response == b"_heartbeat_" => writer.write_all(b"NOP\n").await?,
            2 if response.len() >= 26 => {
                writer.write_all(b"FIN ").await?;
                writer.write_all(&response[10..26]).await?;
                writer.write_all(b"\n").await?;
            }
            1 => anyhow::bail!(
                "consumer close failed: {}",
                String::from_utf8_lossy(&response)
            ),
            _ => {}
        }
    }
}

fn parse_message_id(id: &[u8; 16]) -> anyhow::Result<u64> {
    let id = std::str::from_utf8(id).context("message ID is not ASCII")?;
    u64::from_str_radix(id, 16).context("message ID is not hexadecimal")
}

async fn wait_for_ok<R, W>(reader: &mut R, writer: &mut W) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let (frame_type, response) = read_frame(reader).await?;
        if frame_type == 0 && response == b"_heartbeat_" {
            writer.write_all(b"NOP\n").await?;
        } else if frame_type == 0 && response == b"OK" {
            return Ok(());
        } else if frame_type == 1 {
            anyhow::bail!("subscribe failed: {}", String::from_utf8_lossy(&response));
        }
    }
}

async fn read_frame<R>(reader: &mut R) -> anyhow::Result<(i32, Vec<u8>)>
where
    R: AsyncRead + Unpin,
{
    let size = reader.read_u32().await? as usize;
    if !(4..=16 * 1024 * 1024).contains(&size) {
        anyhow::bail!("server returned invalid frame size {size}");
    }
    let frame_type = reader.read_i32().await?;
    let mut response = vec![0; size - 4];
    reader.read_exact(&mut response).await?;
    Ok((frame_type, response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[test]
    fn progress_counts_unique_deliveries_and_duplicates() {
        let progress = ConsumerProgress::default();
        progress.observe(1);
        progress.observe(1);
        progress.observe(2);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.unique, 2);
        assert_eq!(snapshot.total, 3);
        assert_eq!(snapshot.duplicates(), 1);
    }

    #[test]
    fn id_ranges_merge_out_of_order_ids_without_per_message_storage() {
        let mut ids = IdRanges::default();
        assert!(ids.insert(7));
        assert!(ids.insert(9));
        assert!(ids.insert(8));
        assert!(!ids.insert(8));
        assert_eq!(ids.ranges, BTreeMap::from([(7, 9)]));
    }

    #[test]
    fn parses_rustqueue_and_nsq_hex_ids() {
        assert_eq!(parse_message_id(b"000000000123abcd").unwrap(), 0x0123_abcd);
        assert!(parse_message_id(b"not-a-message-id").is_err());
    }

    #[tokio::test]
    async fn wait_reports_an_incomplete_drain() {
        let progress = ConsumerProgress::default();
        let result = progress.wait_for(1, Duration::from_millis(1)).await;

        assert!(!result.complete);
        assert_eq!(result.snapshot.unique, 0);
    }

    #[tokio::test]
    async fn stopping_during_a_partial_frame_preserves_the_frame_boundary() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (partial_tx, partial_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            let mut magic = [0; 4];
            reader.read_exact(&mut magic).await.unwrap();
            assert_eq!(&magic, b"  V2");

            let mut command = String::new();
            reader.read_line(&mut command).await.unwrap();
            assert!(command.starts_with("SUB "));
            writer.write_all(&test_frame(0, b"OK")).await.unwrap();
            writer.flush().await.unwrap();

            command.clear();
            reader.read_line(&mut command).await.unwrap();
            assert!(command.starts_with("RDY "));

            let message = test_message_frame(1, &[b'x'; 1024]);
            writer.write_all(&message[..20]).await.unwrap();
            writer.flush().await.unwrap();
            partial_tx.send(()).unwrap();
            continue_rx.await.unwrap();
            writer.write_all(&message[20..]).await.unwrap();
            writer.flush().await.unwrap();

            let mut saw_close = false;
            let mut saw_finish = false;
            while !saw_close || !saw_finish {
                command.clear();
                reader.read_line(&mut command).await.unwrap();
                saw_close |= command == "CLS\n";
                saw_finish |= command == "FIN 0000000000000001\n";
            }
            writer
                .write_all(&test_frame(0, b"CLOSE_WAIT"))
                .await
                .unwrap();
            writer.flush().await.unwrap();
        });

        let (stop, stop_rx) = watch::channel(false);
        let (ready, mut ready_rx) = mpsc::channel(1);
        let client = tokio::spawn(consume_worker(
            address.to_string(),
            "events".into(),
            "workers".into(),
            ready,
            stop_rx,
            Arc::new(ConsumerProgress::default()),
        ));
        ready_rx.recv().await.unwrap();
        partial_rx.await.unwrap();
        stop.send(true).unwrap();
        tokio::task::yield_now().await;
        continue_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), client)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    fn test_frame(frame_type: i32, body: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(body.len() + 8);
        frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(&frame_type.to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    fn test_message_frame(id: u64, body: &[u8]) -> Vec<u8> {
        let mut message = Vec::with_capacity(body.len() + 26);
        message.extend_from_slice(&0i64.to_be_bytes());
        message.extend_from_slice(&1u16.to_be_bytes());
        message.extend_from_slice(format!("{id:016x}").as_bytes());
        message.extend_from_slice(body);
        test_frame(2, &message)
    }
}
