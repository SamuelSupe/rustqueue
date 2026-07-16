use super::*;
use crate::batch;
use crate::metadata::store_atomic;
use crate::model::Delivery;
use crate::outbox::OutboxEntry;
use crate::topic::Topic;
use bytes::Bytes;
use rustqueue_storage::MAX_RECORD_BYTES;

const MAX_BATCH_MESSAGES: usize = 10_000;
const MAX_SEQUENCE: u64 = (1u64 << 48) - 1;
pub(super) const SEQUENCE_RESERVATION: u64 = 1 << 20;

impl Broker {
    pub async fn fetch_batch(
        &self,
        topic: &str,
        channel: &str,
        max_messages: usize,
        max_bytes: usize,
        wait: Duration,
        timeout: Option<Duration>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        self.ensure_storage_healthy()?;
        let handle = self.topic(topic)?;
        let mut wake = handle.wake.subscribe();
        let timeout = timeout.unwrap_or(self.inner.config.message_timeout);
        let (mut reservations, mut lease) = {
            let mut topic = handle.state.lock();
            let reservations = topic.reserve_batch(
                channel,
                max_messages.clamp(1, 64),
                max_bytes.max(1),
                timeout,
            )?;
            let lease = (!reservations.is_empty()).then(|| {
                self.inner.payload_reader.retain(
                    reservations
                        .iter()
                        .map(|item| item.payload.clone())
                        .collect(),
                )
            });
            (reservations, lease)
        };
        if reservations.is_empty() && !wait.is_zero() {
            let _ = tokio::time::timeout(wait.min(Duration::from_secs(1)), wake.changed()).await;
            (reservations, lease) = {
                let mut topic = handle.state.lock();
                let reservations = topic.reserve_batch(
                    channel,
                    max_messages.clamp(1, 64),
                    max_bytes.max(1),
                    timeout,
                )?;
                let lease = (!reservations.is_empty()).then(|| {
                    self.inner.payload_reader.retain(
                        reservations
                            .iter()
                            .map(|item| item.payload.clone())
                            .collect(),
                    )
                });
                (reservations, lease)
            };
        }
        if reservations.is_empty() {
            return Ok(Vec::new());
        }
        let bodies = match self
            .inner
            .payload_reader
            .read_retained(
                lease
                    .take()
                    .expect("non-empty reservation has a payload lease"),
            )
            .await
        {
            Ok(bodies) => bodies,
            Err(error) => {
                handle.state.lock().cancel(channel, &reservations);
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    return Ok(Vec::new());
                }
                return self.observe_storage_result(Err(error.into()));
            }
        };
        Ok(reservations
            .drain(..)
            .zip(bodies)
            .map(|(reservation, body)| Delivery {
                id: reservation.id,
                timestamp_ns: reservation.timestamp_ns,
                attempts: reservation.attempts,
                body,
            })
            .collect())
    }

    pub async fn next_message(
        &self,
        topic: &str,
        channel: &str,
        timeout: Option<Duration>,
    ) -> Result<Option<Delivery>, BrokerError> {
        Ok(self
            .fetch_batch(topic, channel, 1, usize::MAX, Duration::ZERO, timeout)
            .await?
            .pop())
    }

    pub async fn finish(&self, topic: &str, channel: &str, id: u64) -> Result<(), BrokerError> {
        self.finish_inner(topic, channel, id, true).await
    }

    async fn finish_inner(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            broker
                .topic(&topic)?
                .state
                .lock()
                .finish(&channel, id, require_in_flight)
        })
        .await
    }

    pub async fn requeue(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        let handle = self.topic(topic)?;
        let work = Arc::clone(&handle);
        let channel = channel.to_owned();
        let available = now_ms().saturating_add(delay.as_millis().min(i64::MAX as u128) as i64);
        self.storage_task(move || work.state.lock().requeue(&channel, id, available))
            .await?;
        handle.signal();
        Ok(())
    }

    pub fn touch(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        timeout: Option<Duration>,
    ) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        self.topic(topic)?.state.lock().touch(
            channel,
            id,
            timeout.unwrap_or(self.inner.config.message_timeout),
        )
    }

    pub fn release(&self, topic: &str, channel: &str, ids: &[u64]) {
        if let Ok(handle) = self.topic(topic) {
            handle.state.lock().release(channel, ids);
            handle.signal();
        }
    }

    pub async fn move_to_dead_letter(
        &self,
        source_topic: &str,
        source_channel: &str,
        message_id: u64,
        target_topic: &str,
        body: Bytes,
    ) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        let entry = OutboxEntry {
            source_topic: source_topic.into(),
            source_channel: source_channel.into(),
            message_id,
            target_topic: target_topic.into(),
            body,
        };
        let directory = self.inner.config.data_path.join("dlq-outbox");
        let entry_for_write = entry.clone();
        let path = self
            .storage_task(move || crate::outbox::store(&directory, &entry_for_write))
            .await?;
        self.publish(
            &entry.target_topic,
            vec![entry.body.clone()],
            Duration::ZERO,
        )
        .await?;
        self.finish_inner(
            &entry.source_topic,
            &entry.source_channel,
            entry.message_id,
            false,
        )
        .await?;
        self.storage_task(move || crate::outbox::remove(&path))
            .await
    }

    pub(super) fn publish_sync(
        &self,
        topic: &str,
        bodies: &[Bytes],
        delay: Duration,
    ) -> Result<Vec<u64>, BrokerError> {
        self.validate_publish_request(topic, bodies)?;
        let handle = self.get_or_create_topic(topic)?;
        let ids = self.append_publish_to_topic(&mut handle.state.lock(), bodies, delay, true)?;
        handle.signal();
        Ok(ids)
    }

    pub(super) fn validate_publish_request(
        &self,
        topic: &str,
        bodies: &[Bytes],
    ) -> Result<usize, BrokerError> {
        validate_name(topic).map_err(|_| BrokerError::InvalidTopic)?;
        if bodies.is_empty() || bodies.len() > MAX_BATCH_MESSAGES {
            return Err(BrokerError::BatchTooLarge);
        }
        if bodies
            .iter()
            .any(|body| body.is_empty() || body.len() > self.inner.config.max_message_bytes)
        {
            return Err(BrokerError::MessageTooLarge);
        }
        let encoded_bytes = bodies.iter().try_fold(4usize, |total, body| {
            total.checked_add(20)?.checked_add(body.len())
        });
        if encoded_bytes.is_none_or(|bytes| bytes > MAX_RECORD_BYTES) {
            return Err(BrokerError::BatchTooLarge);
        }
        Ok(encoded_bytes.expect("validated encoded length"))
    }

    pub(super) fn append_publish_to_topic(
        &self,
        state: &mut Topic,
        bodies: &[Bytes],
        delay: Duration,
        durable: bool,
    ) -> Result<Vec<u64>, BrokerError> {
        let first_position = state.next_position();
        let first_id = self.reserve_ids(bodies.len())?;
        let batch = batch::encode(first_position, first_id, bodies)?;
        let timestamp = now_ns();
        let available = now_ms().saturating_add(delay.as_millis().min(i64::MAX as u128) as i64);
        state.append_batch(first_id, timestamp, available, batch, durable)
    }

    pub(super) fn reserve_ids(&self, count: usize) -> Result<u64, BrokerError> {
        let mut sequence = self.inner.sequence.lock();
        let first = sequence.next;
        let required_exclusive = first
            .checked_add(count as u64)
            .filter(|value| *value <= MAX_SEQUENCE.saturating_add(1))
            .ok_or(BrokerError::SequenceExhausted)?;
        if required_exclusive > sequence.reserved_exclusive {
            let reserved_exclusive = first
                .saturating_add(SEQUENCE_RESERVATION.max(count as u64))
                .min(MAX_SEQUENCE.saturating_add(1));
            let mut meta = self.inner.meta.lock();
            let mut durable = meta.clone();
            durable.next_sequence = reserved_exclusive;
            store_atomic(&self.inner.meta_path, &durable)?;
            *meta = durable;
            sequence.reserved_exclusive = reserved_exclusive;
        }
        sequence.next = required_exclusive;
        Ok((self.inner.config.node_id << 48) | first)
    }

    pub(super) fn recover_outbox(&self) -> Result<(), BrokerError> {
        for (path, entry) in
            crate::outbox::load_all(&self.inner.config.data_path.join("dlq-outbox"))?
        {
            self.publish_sync(&entry.target_topic, &[entry.body], Duration::ZERO)?;
            let finish = self.topic(&entry.source_topic).and_then(|topic| {
                topic
                    .state
                    .lock()
                    .finish(&entry.source_channel, entry.message_id, false)
            });
            match finish {
                Ok(())
                | Err(
                    BrokerError::TopicNotFound
                    | BrokerError::ChannelNotFound
                    | BrokerError::MessageNotFound,
                ) => {}
                Err(error) => return Err(error),
            }
            crate::outbox::remove(&path)?;
        }
        Ok(())
    }
}
