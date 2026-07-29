use super::{Broker, BrokerError, BrokerInner, PublishAckMode};
use crate::model::PublishGroupCommitStats;
use crate::topic::PendingSync;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 1_024;
const MAX_GROUP_REQUESTS: usize = 64;
const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
const COALESCE_DELAY: Duration = Duration::from_millis(1);

type PublishResult = Result<Vec<u64>, BrokerError>;
type PublishGuard = Box<dyn Send + 'static>;

pub(super) struct PublishGroups {
    senders: Mutex<HashMap<String, WorkerEntry>>,
    max_workers: usize,
    idle_timeout: Duration,
    next_worker_id: AtomicU64,
    commits: AtomicU64,
    requests: AtomicU64,
    max_batch_requests: AtomicU64,
    retired_workers: AtomicU64,
    rejected_workers: AtomicU64,
}

struct WorkerEntry {
    id: u64,
    sender: mpsc::Sender<PublishRequest>,
}

struct PublishRequest {
    bodies: Vec<Bytes>,
    delay: Duration,
    encoded_bytes: usize,
    enqueued_at: Instant,
    reply: oneshot::Sender<PublishResult>,
    guard: Option<PublishGuard>,
}

struct PendingPublish {
    ids: Vec<u64>,
    reply: oneshot::Sender<PublishResult>,
    _guard: Option<PublishGuard>,
}

impl PublishGroups {
    pub(super) fn new(max_workers: usize, idle_timeout: Duration) -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            max_workers,
            idle_timeout,
            next_worker_id: AtomicU64::new(1),
            commits: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            max_batch_requests: AtomicU64::new(0),
            retired_workers: AtomicU64::new(0),
            rejected_workers: AtomicU64::new(0),
        }
    }

    fn sender(
        &self,
        broker: &Broker,
        topic: &str,
    ) -> Result<mpsc::Sender<PublishRequest>, BrokerError> {
        let mut senders = self.senders.lock();
        senders.retain(|_, entry| !entry.sender.is_closed());
        if let Some(entry) = senders.get(topic) {
            return Ok(entry.sender.clone());
        }
        if senders.len() >= self.max_workers {
            self.rejected_workers.fetch_add(1, Ordering::Relaxed);
            return Err(BrokerError::PublishWorkerLimit);
        }
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        senders.insert(
            topic.to_owned(),
            WorkerEntry {
                id: worker_id,
                sender: sender.clone(),
            },
        );
        tokio::spawn(run_worker(
            Arc::downgrade(&broker.inner),
            topic.to_owned(),
            worker_id,
            self.idle_timeout,
            receiver,
        ));
        Ok(sender)
    }

    fn retire_idle(&self, topic: &str, worker_id: u64) -> bool {
        let mut senders = self.senders.lock();
        let removable = senders
            .get(topic)
            .is_some_and(|entry| entry.id == worker_id && entry.sender.strong_count() == 1);
        if removable {
            senders.remove(topic);
            self.retired_workers.fetch_add(1, Ordering::Relaxed);
        }
        removable
    }

    fn record(&self, requests: usize) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.requests.fetch_add(requests as u64, Ordering::Relaxed);
        self.max_batch_requests
            .fetch_max(requests as u64, Ordering::Relaxed);
    }

    pub(super) fn stats(&self) -> PublishGroupCommitStats {
        let active_workers = {
            let mut senders = self.senders.lock();
            senders.retain(|_, entry| !entry.sender.is_closed());
            senders.len() as u64
        };
        PublishGroupCommitStats {
            commits: self.commits.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            max_batch_requests: self.max_batch_requests.load(Ordering::Relaxed),
            active_workers,
            retired_workers: self.retired_workers.load(Ordering::Relaxed),
            rejected_workers: self.rejected_workers.load(Ordering::Relaxed),
        }
    }
}

impl Broker {
    pub async fn publish<B>(
        &self,
        topic: &str,
        bodies: Vec<B>,
        delay: Duration,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: Into<Bytes> + Send + 'static,
    {
        self.publish_inner(topic, bodies, delay, None).await
    }

    /// Keeps `guard` alive until the request crosses the configured publish
    /// acknowledgement boundary, including cancellation after it is queued.
    pub async fn publish_guarded<B, G>(
        &self,
        topic: &str,
        bodies: Vec<B>,
        delay: Duration,
        guard: G,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: Into<Bytes> + Send + 'static,
        G: Send + 'static,
    {
        self.publish_inner(topic, bodies, delay, Some(Box::new(guard)))
            .await
    }

    async fn publish_inner<B>(
        &self,
        topic: &str,
        bodies: Vec<B>,
        delay: Duration,
        guard: Option<PublishGuard>,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: Into<Bytes> + Send + 'static,
    {
        let _ack_timer = self.inner.metrics.publish_ack.timer();
        self.ensure_storage_healthy()?;
        let bodies: Vec<Bytes> = bodies.into_iter().map(Into::into).collect();
        let encoded_bytes = self.validate_publish_request(topic, &bodies)?;
        let sender = self.inner.publish_groups.sender(self, topic)?;
        let (reply, result) = oneshot::channel();
        sender
            .send(PublishRequest {
                bodies,
                delay,
                encoded_bytes,
                enqueued_at: Instant::now(),
                reply,
                guard,
            })
            .await
            .map_err(|_| BrokerError::StorageUnavailable)?;
        match result.await {
            Ok(result) => result,
            Err(_) => {
                self.inner.storage_healthy.store(false, Ordering::Release);
                Err(BrokerError::StorageUnavailable)
            }
        }
    }

    fn commit_publish_group(
        &self,
        topic: &str,
        requests: Vec<PublishRequest>,
    ) -> Result<(), BrokerError> {
        if let Err(error) = self.ensure_storage_healthy() {
            fail_requests(requests, &error);
            return Err(error);
        }
        let message_count = requests
            .iter()
            .map(|request| request.bodies.len())
            .sum::<usize>();
        let mut metadata = match self.reserve_message_metadata(message_count) {
            Ok(reservation) => reservation,
            Err(error) => {
                fail_requests(requests, &error);
                return self.worker_result(error);
            }
        };
        let handle = match self.get_or_create_topic(topic) {
            Ok(handle) => handle,
            Err(error) => {
                fail_requests(requests, &error);
                return self.worker_result(error);
            }
        };
        let commit_gate = handle.commit_gate.lock();
        if let Err(error) = self.ensure_storage_healthy() {
            fail_requests(requests, &error);
            return Err(error);
        }
        if let Err(error) = self.ensure_management_access(topic, None) {
            fail_requests(requests, &error);
            return Ok(());
        }
        let topic_lock_started = Instant::now();
        let mut topic_state = handle.state.lock();
        self.inner
            .metrics
            .publish_topic_lock_wait
            .observe(topic_lock_started.elapsed());
        let topic_lock_hold = self.inner.metrics.publish_topic_lock_hold.timer();
        let deliverable_before = topic_state.deliverable_position();
        let mut pending = Vec::with_capacity(requests.len());
        let mut requests = requests.into_iter();

        while let Some(request) = requests.next() {
            let PublishRequest {
                bodies,
                delay,
                encoded_bytes,
                enqueued_at,
                reply,
                guard,
            } = request;
            self.inner
                .metrics
                .group_commit_wait
                .observe(enqueued_at.elapsed());
            match self.append_publish_to_topic(
                &mut topic_state,
                &bodies,
                delay,
                false,
                &mut metadata,
            ) {
                Ok(ids) => {
                    topic_state.record_unsynced(bodies.len(), encoded_bytes);
                    pending.push(PendingPublish {
                        ids,
                        reply,
                        _guard: guard,
                    });
                }
                Err(error) if is_storage_error(&error) => {
                    let _ = reply.send(Err(copy_error(&error)));
                    fail_pending(pending);
                    fail_requests(requests, &BrokerError::StorageUnavailable);
                    return self.worker_result(error);
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
        }

        if pending.is_empty() {
            return Ok(());
        }
        drop(metadata);
        if self.inner.message_index_cache.over_budget() {
            if let Err(error) = topic_state.spill_message_metadata() {
                fail_pending(pending);
                return self.worker_result(error);
            }
        }
        let ack_mode = self.inner.config.publish_ack_mode;
        let visibility_advanced;
        if ack_mode == PublishAckMode::Durable {
            let durable_through = topic_state.last_position();
            let sync_file = match topic_state.clone_log_for_sync() {
                Ok(file) => file,
                Err(error) => {
                    fail_pending(pending);
                    return self.worker_result(error);
                }
            };
            drop(topic_state);
            drop(topic_lock_hold);
            rustqueue_storage::crash_failpoint("publish_after_append_before_fsync");
            let sync_result = {
                let _timer = self.inner.metrics.fsync.timer();
                sync_file.sync_data().map_err(BrokerError::from)
            };
            if let Err(error) = sync_result {
                let topic_lock_started = Instant::now();
                let topic_state = handle.state.lock();
                self.inner
                    .metrics
                    .publish_topic_lock_wait
                    .observe(topic_lock_started.elapsed());
                let _topic_lock_hold = self.inner.metrics.publish_topic_lock_hold.timer();
                topic_state.mark_log_sync_failed();
                fail_pending(pending);
                return self.worker_result(error);
            }
            let topic_lock_started = Instant::now();
            let mut topic_state = handle.state.lock();
            self.inner
                .metrics
                .publish_topic_lock_wait
                .observe(topic_lock_started.elapsed());
            let topic_lock_hold = self.inner.metrics.publish_topic_lock_hold.timer();
            topic_state.mark_durable_through(durable_through);
            visibility_advanced = topic_state.deliverable_position() > deliverable_before;
            drop(topic_state);
            drop(topic_lock_hold);
            rustqueue_storage::crash_failpoint("publish_after_fsync_before_reply");
        } else {
            if ack_mode.exposes_unsynced() {
                let deliverable_through = topic_state.last_position();
                topic_state.mark_deliverable_through(deliverable_through);
            }
            visibility_advanced = topic_state.deliverable_position() > deliverable_before;
            drop(topic_state);
            drop(topic_lock_hold);
        }

        drop(commit_gate);
        self.inner.publish_groups.record(pending.len());
        if visibility_advanced {
            handle.signal();
        }
        for pending in pending {
            let _ = pending.reply.send(Ok(pending.ids));
        }
        Ok(())
    }

    fn sync_pending_topic(&self, topic: &str) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        let handle = match self.topic(topic) {
            Ok(handle) => handle,
            Err(BrokerError::TopicNotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        let commit_gate = handle.commit_gate.lock();
        let topic_lock_started = Instant::now();
        let topic_state = handle.state.lock();
        self.inner
            .metrics
            .publish_topic_lock_wait
            .observe(topic_lock_started.elapsed());
        let topic_lock_hold = self.inner.metrics.publish_topic_lock_hold.timer();
        if topic_state.pending_sync().is_none() {
            return Ok(());
        }
        let durable_through = topic_state.last_position();
        let sync_file = topic_state.clone_log_for_sync()?;
        drop(topic_state);
        drop(topic_lock_hold);
        rustqueue_storage::crash_failpoint("publish_after_append_before_fsync");
        let sync_result = {
            let _timer = self.inner.metrics.fsync.timer();
            sync_file.sync_data().map_err(BrokerError::from)
        };
        if let Err(error) = sync_result {
            let topic_state = handle.state.lock();
            topic_state.mark_log_sync_failed();
            drop(topic_state);
            return self.observe_storage_result(Err(error));
        }
        let mut topic_state = handle.state.lock();
        topic_state.mark_durable_through(durable_through);
        drop(topic_state);
        drop(commit_gate);
        handle.signal();
        Ok(())
    }

    fn pending_topic_sync(&self, topic: &str) -> Option<PendingSync> {
        self.inner
            .topics
            .read()
            .get(topic)
            .and_then(|handle| handle.state.lock().pending_sync())
    }

    fn worker_result(&self, error: BrokerError) -> Result<(), BrokerError> {
        if is_storage_error(&error) {
            let _ = self.observe_storage_result::<()>(Err(copy_error(&error)));
            Err(BrokerError::StorageUnavailable)
        } else {
            Ok(())
        }
    }
}

async fn run_worker(
    broker: Weak<BrokerInner>,
    topic: String,
    worker_id: u64,
    idle_timeout: Duration,
    mut receiver: mpsc::Receiver<PublishRequest>,
) {
    let mut carry = None;
    let mut pending_sync = None;
    loop {
        let Some(inner) = broker.upgrade() else {
            return;
        };
        let relaxed_ack_mode = inner.config.publish_ack_mode.is_relaxed();
        let sync_interval = inner.config.relaxed_sync_interval;
        let sync_messages = inner.config.relaxed_sync_messages as u64;
        let sync_bytes = inner.config.relaxed_sync_bytes as u64;
        drop(inner);

        if pending_sync
            .is_some_and(|pending| sync_is_due(pending, sync_messages, sync_bytes, sync_interval))
        {
            if !run_pending_sync(&broker, &topic).await {
                return;
            }
            pending_sync = pending_topic_sync(&broker, &topic, relaxed_ack_mode);
            continue;
        }

        let first = match carry.take() {
            Some(request) => request,
            None => match pending_sync {
                Some(pending) => {
                    let deadline = tokio::time::Instant::from_std(
                        pending
                            .since
                            .checked_add(sync_interval)
                            .expect("validated relaxed sync interval"),
                    );
                    tokio::select! {
                        request = receiver.recv() => match request {
                            Some(request) => request,
                            None => {
                                let _ = run_pending_sync(&broker, &topic).await;
                                return;
                            }
                        },
                        _ = tokio::time::sleep_until(deadline) => {
                            if !run_pending_sync(&broker, &topic).await {
                                return;
                            }
                            pending_sync =
                                pending_topic_sync(&broker, &topic, relaxed_ack_mode);
                            continue;
                        }
                    }
                }
                None => match tokio::time::timeout(idle_timeout, receiver.recv()).await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(_) => {
                        let Some(inner) = broker.upgrade() else {
                            return;
                        };
                        if inner.publish_groups.retire_idle(&topic, worker_id) {
                            return;
                        }
                        continue;
                    }
                },
            },
        };
        let (requests, next) = collect_group(first, &mut receiver).await;
        carry = next;
        let Some(inner) = broker.upgrade() else {
            return;
        };
        let worker_broker = Broker { inner };
        let worker_topic = topic.clone();
        match tokio::task::spawn_blocking(move || {
            worker_broker.commit_publish_group(&worker_topic, requests)
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                if let Some(inner) = broker.upgrade() {
                    inner.storage_healthy.store(false, Ordering::Release);
                }
                return;
            }
        }
        pending_sync = pending_topic_sync(&broker, &topic, relaxed_ack_mode);
    }
}

fn pending_topic_sync(
    broker: &Weak<BrokerInner>,
    topic: &str,
    relaxed_ack_mode: bool,
) -> Option<PendingSync> {
    if !relaxed_ack_mode {
        return None;
    }
    broker
        .upgrade()
        .and_then(|inner| Broker { inner }.pending_topic_sync(topic))
}

async fn run_pending_sync(broker: &Weak<BrokerInner>, topic: &str) -> bool {
    let Some(inner) = broker.upgrade() else {
        return false;
    };
    let worker_broker = Broker { inner };
    let worker_topic = topic.to_owned();
    match tokio::task::spawn_blocking(move || worker_broker.sync_pending_topic(&worker_topic)).await
    {
        Ok(Ok(())) => true,
        Ok(Err(_)) | Err(_) => {
            if let Some(inner) = broker.upgrade() {
                inner.storage_healthy.store(false, Ordering::Release);
            }
            false
        }
    }
}

fn sync_is_due(
    pending: PendingSync,
    message_limit: u64,
    byte_limit: u64,
    interval: Duration,
) -> bool {
    pending.messages >= message_limit
        || pending.bytes >= byte_limit
        || pending.since.elapsed() >= interval
}

async fn collect_group(
    first: PublishRequest,
    receiver: &mut mpsc::Receiver<PublishRequest>,
) -> (Vec<PublishRequest>, Option<PublishRequest>) {
    let mut bytes = first.encoded_bytes;
    let mut requests = vec![first];
    loop {
        if requests.len() >= MAX_GROUP_REQUESTS {
            return (requests, None);
        }
        let next = match receiver.try_recv() {
            Ok(request) => Some(request),
            Err(mpsc::error::TryRecvError::Disconnected) => None,
            Err(mpsc::error::TryRecvError::Empty) if requests.len() == 1 => {
                tokio::time::timeout(COALESCE_DELAY, receiver.recv())
                    .await
                    .ok()
                    .flatten()
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
        };
        let Some(next) = next else {
            return (requests, None);
        };
        if bytes.saturating_add(next.encoded_bytes) > MAX_GROUP_BYTES {
            return (requests, Some(next));
        }
        bytes = bytes.saturating_add(next.encoded_bytes);
        requests.push(next);
    }
}

fn fail_pending(pending: Vec<PendingPublish>) {
    for pending in pending {
        let _ = pending.reply.send(Err(BrokerError::StorageUnavailable));
    }
}

fn fail_requests(requests: impl IntoIterator<Item = PublishRequest>, error: &BrokerError) {
    for request in requests {
        let _ = request.reply.send(Err(copy_error(error)));
    }
}

pub(super) fn is_storage_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::StorageUnavailable
            | BrokerError::Storage(_)
            | BrokerError::Io(_)
            | BrokerError::InvalidRecord(_)
    )
}

pub(super) fn copy_error(error: &BrokerError) -> BrokerError {
    match error {
        BrokerError::InvalidTopic => BrokerError::InvalidTopic,
        BrokerError::InvalidChannel => BrokerError::InvalidChannel,
        BrokerError::TopicNotFound => BrokerError::TopicNotFound,
        BrokerError::TopicRetiring => BrokerError::TopicRetiring,
        BrokerError::TopicTombstoned => BrokerError::TopicTombstoned,
        BrokerError::ChannelNotFound => BrokerError::ChannelNotFound,
        BrokerError::ChannelTombstoned => BrokerError::ChannelTombstoned,
        BrokerError::ChannelNotIdle {
            depth,
            in_flight,
            deferred,
        } => BrokerError::ChannelNotIdle {
            depth: *depth,
            in_flight: *in_flight,
            deferred: *deferred,
        },
        BrokerError::ManagementUnavailable => BrokerError::ManagementUnavailable,
        BrokerError::RevisionConflict { expected, actual } => BrokerError::RevisionConflict {
            expected: *expected,
            actual: *actual,
        },
        BrokerError::OperationConflict => BrokerError::OperationConflict,
        BrokerError::InvalidTombstone => BrokerError::InvalidTombstone,
        BrokerError::MessageNotFound => BrokerError::MessageNotFound,
        BrokerError::MessageNotInFlight => BrokerError::MessageNotInFlight,
        BrokerError::MessageTooLarge => BrokerError::MessageTooLarge,
        BrokerError::BatchTooLarge => BrokerError::BatchTooLarge,
        BrokerError::TopicLimit => BrokerError::TopicLimit,
        BrokerError::PublishWorkerLimit => BrokerError::PublishWorkerLimit,
        BrokerError::ChannelWorkerLimit => BrokerError::ChannelWorkerLimit,
        BrokerError::ChannelLimit => BrokerError::ChannelLimit,
        BrokerError::SequenceExhausted => BrokerError::SequenceExhausted,
        BrokerError::StorageUnavailable | BrokerError::Storage(_) | BrokerError::Io(_) => {
            BrokerError::StorageUnavailable
        }
        BrokerError::InvalidRecord(message) => BrokerError::InvalidRecord(message.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(encoded_bytes: usize) -> PublishRequest {
        let (reply, _) = oneshot::channel();
        PublishRequest {
            bodies: Vec::new(),
            delay: Duration::ZERO,
            encoded_bytes,
            enqueued_at: Instant::now(),
            reply,
            guard: None,
        }
    }

    #[tokio::test]
    async fn group_size_limit_leaves_the_next_request_queued() {
        let (sender, mut receiver) = mpsc::channel(128);
        for _ in 0..=MAX_GROUP_REQUESTS {
            sender.send(request(1)).await.unwrap();
        }
        let first = receiver.recv().await.unwrap();
        let (group, carry) = collect_group(first, &mut receiver).await;
        assert_eq!(group.len(), MAX_GROUP_REQUESTS);
        assert!(carry.is_none());
        assert!(receiver.try_recv().is_ok());
    }

    #[tokio::test]
    async fn group_byte_limit_carries_the_request_without_dropping_it() {
        let (sender, mut receiver) = mpsc::channel(2);
        sender.send(request(3 * 1024 * 1024)).await.unwrap();
        let (group, carry) = collect_group(request(6 * 1024 * 1024), &mut receiver).await;
        assert_eq!(group.len(), 1);
        assert_eq!(carry.unwrap().encoded_bytes, 3 * 1024 * 1024);
    }

    #[test]
    fn relaxed_sync_uses_the_first_reached_threshold() {
        let interval = Duration::from_millis(10);
        let now = Instant::now();
        assert!(sync_is_due(
            PendingSync {
                messages: 2_500,
                bytes: 1,
                since: now,
            },
            2_500,
            8 * 1024 * 1024,
            interval,
        ));
        assert!(sync_is_due(
            PendingSync {
                messages: 1,
                bytes: 8 * 1024 * 1024,
                since: now,
            },
            2_500,
            8 * 1024 * 1024,
            interval,
        ));
        assert!(sync_is_due(
            PendingSync {
                messages: 1,
                bytes: 1,
                since: now - interval,
            },
            2_500,
            8 * 1024 * 1024,
            interval,
        ));
    }
}
