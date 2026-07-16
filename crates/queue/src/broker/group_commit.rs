use super::{Broker, BrokerError, BrokerInner};
use crate::model::PublishGroupCommitStats;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 1024;
const MAX_GROUP_REQUESTS: usize = 64;
const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
const COALESCE_DELAY: Duration = Duration::from_millis(1);

type PublishResult = Result<Vec<u64>, BrokerError>;

pub(super) struct PublishGroups {
    senders: Mutex<HashMap<String, mpsc::Sender<PublishRequest>>>,
    commits: AtomicU64,
    requests: AtomicU64,
    max_batch_requests: AtomicU64,
}

impl Default for PublishGroups {
    fn default() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            commits: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            max_batch_requests: AtomicU64::new(0),
        }
    }
}

struct PublishRequest {
    bodies: Vec<Bytes>,
    delay: Duration,
    encoded_bytes: usize,
    reply: oneshot::Sender<PublishResult>,
}

struct PendingPublish {
    ids: Vec<u64>,
    reply: oneshot::Sender<PublishResult>,
}

impl PublishGroups {
    fn sender(&self, broker: &Broker, topic: &str) -> mpsc::Sender<PublishRequest> {
        let mut senders = self.senders.lock();
        if let Some(sender) = senders.get(topic).filter(|sender| !sender.is_closed()) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        senders.insert(topic.to_owned(), sender.clone());
        tokio::spawn(run_worker(
            Arc::downgrade(&broker.inner),
            topic.to_owned(),
            receiver,
        ));
        sender
    }

    fn record(&self, requests: usize) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.requests.fetch_add(requests as u64, Ordering::Relaxed);
        self.max_batch_requests
            .fetch_max(requests as u64, Ordering::Relaxed);
    }

    pub(super) fn stats(&self) -> PublishGroupCommitStats {
        PublishGroupCommitStats {
            commits: self.commits.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            max_batch_requests: self.max_batch_requests.load(Ordering::Relaxed),
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
        self.ensure_storage_healthy()?;
        let bodies: Vec<Bytes> = bodies.into_iter().map(Into::into).collect();
        let encoded_bytes = self.validate_publish_request(topic, &bodies)?;
        let sender = self.inner.publish_groups.sender(self, topic);
        let (reply, result) = oneshot::channel();
        sender
            .send(PublishRequest {
                bodies,
                delay,
                encoded_bytes,
                reply,
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

    fn commit_publish_group(&self, topic: &str, requests: Vec<PublishRequest>) {
        if let Err(error) = self.ensure_storage_healthy() {
            fail_requests(requests, &error);
            return;
        }
        let handle = match self.get_or_create_topic(topic) {
            Ok(handle) => handle,
            Err(error) => {
                self.observe_storage_result::<()>(Err(copy_error(&error)))
                    .ok();
                fail_requests(requests, &error);
                return;
            }
        };
        let mut topic_state = handle.state.lock();
        let mut pending = Vec::with_capacity(requests.len());
        let mut requests = requests.into_iter();

        while let Some(request) = requests.next() {
            let PublishRequest {
                bodies,
                delay,
                reply,
                ..
            } = request;
            match self.append_publish_to_topic(&mut topic_state, &bodies, delay, false) {
                Ok(ids) => pending.push(PendingPublish { ids, reply }),
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
        if let Err(error) = topic_state.sync_log() {
            self.observe_storage_result::<()>(Err(copy_error(&error)))
                .ok();
            fail_pending(pending);
            return;
        }
        drop(topic_state);
        self.inner.publish_groups.record(pending.len());
        handle.signal();
        for pending in pending {
            let _ = pending.reply.send(Ok(pending.ids));
        }
    }
}

async fn run_worker(
    broker: Weak<BrokerInner>,
    topic: String,
    mut receiver: mpsc::Receiver<PublishRequest>,
) {
    let mut carry = None;
    loop {
        let first = match carry.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => return,
            },
        };
        let (requests, next) = collect_group(first, &mut receiver).await;
        carry = next;
        let Some(inner) = broker.upgrade() else {
            return;
        };
        let worker_broker = Broker { inner };
        let worker_topic = topic.clone();
        if tokio::task::spawn_blocking(move || {
            worker_broker.commit_publish_group(&worker_topic, requests)
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

fn is_storage_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::StorageUnavailable | BrokerError::Storage(_) | BrokerError::Io(_)
    )
}

fn copy_error(error: &BrokerError) -> BrokerError {
    match error {
        BrokerError::InvalidTopic => BrokerError::InvalidTopic,
        BrokerError::InvalidChannel => BrokerError::InvalidChannel,
        BrokerError::TopicNotFound => BrokerError::TopicNotFound,
        BrokerError::TopicRetiring => BrokerError::TopicRetiring,
        BrokerError::ChannelNotFound => BrokerError::ChannelNotFound,
        BrokerError::MessageNotFound => BrokerError::MessageNotFound,
        BrokerError::MessageNotInFlight => BrokerError::MessageNotInFlight,
        BrokerError::MessageTooLarge => BrokerError::MessageTooLarge,
        BrokerError::BatchTooLarge => BrokerError::BatchTooLarge,
        BrokerError::BacklogLimit => BrokerError::BacklogLimit,
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
            reply,
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
}
