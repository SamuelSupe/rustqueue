use super::group_commit::{copy_error, is_storage_error};
use super::{Broker, BrokerError, BrokerInner};
use crate::model::ChannelGroupCommitStats;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 1024;
const MAX_GROUP_REQUESTS: usize = 64;
const COALESCE_DELAY: Duration = Duration::from_millis(1);

pub(super) enum ChannelOperation {
    Finish {
        id: u64,
        require_in_flight: bool,
        token: Option<u64>,
    },
    Requeue {
        id: u64,
        available_at_ms: i64,
        token: Option<u64>,
    },
}

pub(super) struct ChannelGroups {
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
    sender: mpsc::Sender<ChannelRequest>,
}

struct ChannelRequest {
    channel: String,
    operation: ChannelOperation,
    enqueued_at: Instant,
    reply: oneshot::Sender<Result<(), BrokerError>>,
}

struct PendingChannel {
    reply: oneshot::Sender<Result<(), BrokerError>>,
}

impl ChannelGroups {
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

    pub(super) async fn submit(
        &self,
        broker: &Broker,
        topic: &str,
        channel: String,
        operation: ChannelOperation,
    ) -> Result<(), BrokerError> {
        let sender = self.sender(broker, topic)?;
        let (reply, result) = oneshot::channel();
        sender
            .send(ChannelRequest {
                channel,
                operation,
                enqueued_at: Instant::now(),
                reply,
            })
            .await
            .map_err(|_| BrokerError::StorageUnavailable)?;
        result.await.map_err(|_| BrokerError::StorageUnavailable)?
    }

    fn sender(
        &self,
        broker: &Broker,
        topic: &str,
    ) -> Result<mpsc::Sender<ChannelRequest>, BrokerError> {
        let mut senders = self.senders.lock();
        senders.retain(|_, entry| !entry.sender.is_closed());
        if let Some(entry) = senders.get(topic) {
            return Ok(entry.sender.clone());
        }
        if senders.len() >= self.max_workers {
            self.rejected_workers.fetch_add(1, Ordering::Relaxed);
            return Err(BrokerError::ChannelWorkerLimit);
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

    pub(super) fn stats(&self) -> ChannelGroupCommitStats {
        let active_workers = {
            let mut senders = self.senders.lock();
            senders.retain(|_, entry| !entry.sender.is_closed());
            senders.len() as u64
        };
        ChannelGroupCommitStats {
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
    fn commit_channel_group(&self, topic: &str, requests: Vec<ChannelRequest>) {
        if let Err(error) = self.ensure_storage_healthy() {
            fail_requests(requests, &error);
            return;
        }
        let handle = match self.topic(topic) {
            Ok(handle) => handle,
            Err(error) => {
                fail_requests(requests, &error);
                return;
            }
        };
        let mut topic_state = handle.state.lock();
        let mut pending = Vec::with_capacity(requests.len());
        let mut touched = BTreeSet::new();
        let mut requests = requests.into_iter();

        while let Some(request) = requests.next() {
            let ChannelRequest {
                channel,
                operation,
                enqueued_at,
                reply,
            } = request;
            self.inner
                .metrics
                .channel_group_commit_wait
                .observe(enqueued_at.elapsed());
            if let Err(error) = self.ensure_management_access(topic, Some(&channel)) {
                let _ = reply.send(Err(error));
                continue;
            }
            let result = match operation {
                ChannelOperation::Finish {
                    id,
                    require_in_flight,
                    token,
                } => topic_state.finish_buffered(&channel, id, require_in_flight, token),
                ChannelOperation::Requeue {
                    id,
                    available_at_ms,
                    token,
                } => topic_state.requeue_buffered(&channel, id, available_at_ms, token),
            };
            match result {
                Ok(()) => {
                    touched.insert(channel);
                    pending.push(PendingChannel { reply });
                }
                Err(error) if is_storage_error(&error) => {
                    self.observe_storage_result::<()>(Err(copy_error(&error)))
                        .ok();
                    let _ = reply.send(Err(copy_error(&error)));
                    fail_pending(pending);
                    fail_requests(requests, &BrokerError::StorageUnavailable);
                    return;
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
        }
        if pending.is_empty() {
            return;
        }

        rustqueue_storage::crash_failpoint("channel_group_after_append_before_fsync");
        let sync_result = {
            let _timer = self.inner.metrics.channel_fsync.timer();
            topic_state.sync_channel_wals(touched.iter())
        };
        if let Err(error) = sync_result {
            self.observe_storage_result::<()>(Err(copy_error(&error)))
                .ok();
            fail_pending(pending);
            return;
        }
        if let Err(error) = topic_state.checkpoint_channels_if_needed(touched.iter()) {
            self.observe_storage_result::<()>(Err(copy_error(&error)))
                .ok();
            fail_pending(pending);
            return;
        }
        rustqueue_storage::crash_failpoint("channel_group_after_fsync_before_reply");
        drop(topic_state);
        self.inner.channel_groups.record(pending.len());
        handle.signal();
        for pending in pending {
            let _ = pending.reply.send(Ok(()));
        }
    }
}

async fn run_worker(
    broker: Weak<BrokerInner>,
    topic: String,
    worker_id: u64,
    idle_timeout: Duration,
    mut receiver: mpsc::Receiver<ChannelRequest>,
) {
    loop {
        let first = match tokio::time::timeout(idle_timeout, receiver.recv()).await {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(_) => {
                let Some(inner) = broker.upgrade() else {
                    return;
                };
                if inner.channel_groups.retire_idle(&topic, worker_id) {
                    return;
                }
                continue;
            }
        };
        let requests = collect_group(first, &mut receiver).await;
        let Some(inner) = broker.upgrade() else {
            return;
        };
        let worker_broker = Broker { inner };
        let worker_topic = topic.clone();
        if tokio::task::spawn_blocking(move || {
            worker_broker.commit_channel_group(&worker_topic, requests)
        })
        .await
        .is_err()
        {
            if let Some(inner) = broker.upgrade() {
                inner.storage_healthy.store(false, Ordering::Release);
            }
            return;
        }
    }
}

async fn collect_group(
    first: ChannelRequest,
    receiver: &mut mpsc::Receiver<ChannelRequest>,
) -> Vec<ChannelRequest> {
    let deadline = tokio::time::Instant::now() + COALESCE_DELAY;
    let mut requests = vec![first];
    loop {
        if requests.len() >= MAX_GROUP_REQUESTS {
            return requests;
        }
        let next = match receiver.try_recv() {
            Ok(request) => Some(request),
            Err(mpsc::error::TryRecvError::Disconnected) => None,
            Err(mpsc::error::TryRecvError::Empty) => {
                tokio::time::timeout_at(deadline, receiver.recv())
                    .await
                    .ok()
                    .flatten()
            }
        };
        let Some(next) = next else {
            return requests;
        };
        requests.push(next);
    }
}

fn fail_pending(pending: Vec<PendingChannel>) {
    for pending in pending {
        let _ = pending.reply.send(Err(BrokerError::StorageUnavailable));
    }
}

fn fail_requests(requests: impl IntoIterator<Item = ChannelRequest>, error: &BrokerError) {
    for request in requests {
        let _ = request.reply.send(Err(copy_error(error)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ChannelRequest {
        let (reply, _) = oneshot::channel();
        ChannelRequest {
            channel: "workers".into(),
            operation: ChannelOperation::Finish {
                id: 1,
                require_in_flight: true,
                token: None,
            },
            enqueued_at: Instant::now(),
            reply,
        }
    }

    #[tokio::test]
    async fn group_size_is_bounded() {
        let (sender, mut receiver) = mpsc::channel(128);
        for _ in 0..=MAX_GROUP_REQUESTS {
            sender.send(request()).await.unwrap();
        }
        let first = receiver.recv().await.unwrap();
        assert_eq!(
            collect_group(first, &mut receiver).await.len(),
            MAX_GROUP_REQUESTS
        );
        assert!(receiver.try_recv().is_ok());
    }

    #[tokio::test]
    async fn group_collects_requests_arriving_anytime_within_the_window() {
        let (sender, mut receiver) = mpsc::channel(8);
        sender.send(request()).await.unwrap();
        sender.send(request()).await.unwrap();
        let late_sender = tokio::spawn(async move {
            tokio::task::yield_now().await;
            sender.send(request()).await.unwrap();
        });

        let first = receiver.recv().await.unwrap();
        assert_eq!(collect_group(first, &mut receiver).await.len(), 3);
        late_sender.await.unwrap();
    }
}
