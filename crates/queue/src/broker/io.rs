use super::*;
use crate::batch;
use crate::metadata::store_atomic;
use crate::outbox::OutboxEntry;
use crate::topic::Topic;
use bytes::Bytes;
use rustqueue_storage::MAX_RECORD_BYTES;

const MAX_SEQUENCE: u64 = (1u64 << 48) - 1;
pub(super) const SEQUENCE_RESERVATION: u64 = 1 << 20;

impl Broker {
    pub async fn finish(&self, topic: &str, channel: &str, id: u64) -> Result<(), BrokerError> {
        self.finish_with_token(topic, channel, id, None).await
    }

    pub async fn finish_delivery(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        token: u64,
    ) -> Result<(), BrokerError> {
        self.finish_with_token(topic, channel, id, Some(token))
            .await
    }

    async fn finish_with_token(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        token: Option<u64>,
    ) -> Result<(), BrokerError> {
        let _timer = self.inner.metrics.channel_ack.timer();
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        self.inner
            .channel_groups
            .submit(
                self,
                topic,
                channel.to_owned(),
                super::channel_commit::ChannelOperation::Finish {
                    id,
                    require_in_flight: true,
                    token,
                },
            )
            .await
    }

    async fn finish_outbox_source(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
    ) -> Result<(), BrokerError> {
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        let result = self
            .storage_task(move || {
                broker
                    .topic(&topic)?
                    .state
                    .lock()
                    .finish(&channel, id, false)
            })
            .await;
        match result {
            Ok(())
            | Err(
                BrokerError::TopicNotFound
                | BrokerError::ChannelNotFound
                | BrokerError::MessageNotFound,
            ) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn source_message_is_unacknowledged(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
    ) -> Result<Option<bool>, BrokerError> {
        match self.topic(topic) {
            Ok(topic) => topic.state.lock().message_is_unacknowledged(channel, id),
            Err(BrokerError::TopicNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn requeue(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        self.requeue_with_token(topic, channel, id, None, delay)
            .await
    }

    pub async fn requeue_delivery(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        token: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        self.requeue_with_token(topic, channel, id, Some(token), delay)
            .await
    }

    async fn requeue_with_token(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        token: Option<u64>,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        let _timer = self.inner.metrics.channel_ack.timer();
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        let available = now_ms().saturating_add(delay.as_millis().min(i64::MAX as u128) as i64);
        let result = self
            .inner
            .channel_groups
            .submit(
                self,
                topic,
                channel.to_owned(),
                super::channel_commit::ChannelOperation::Requeue {
                    id,
                    available_at_ms: available,
                    token,
                },
            )
            .await;
        result
    }

    pub fn touch(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        timeout: Option<Duration>,
    ) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        let handle = self.topic(topic)?;
        let mut topic_state = handle.state.lock();
        self.ensure_management_access(topic, Some(channel))?;
        topic_state.touch(
            channel,
            id,
            timeout.unwrap_or(self.inner.config.message_timeout),
        )
    }

    pub fn touch_delivery(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        token: u64,
        timeout: Option<Duration>,
    ) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        let handle = self.topic(topic)?;
        let mut topic_state = handle.state.lock();
        self.ensure_management_access(topic, Some(channel))?;
        topic_state.touch_with_token(
            channel,
            id,
            Some(token),
            timeout.unwrap_or(self.inner.config.message_timeout),
        )
    }

    pub fn touch_deliveries(
        &self,
        topic: &str,
        channel: &str,
        deliveries: &[(u64, u64)],
        timeout: Option<Duration>,
    ) -> Result<(), BrokerError> {
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        let handle = self.topic(topic)?;
        let mut topic_state = handle.state.lock();
        self.ensure_management_access(topic, Some(channel))?;
        topic_state.touch_deliveries_with_tokens(
            channel,
            deliveries,
            timeout.unwrap_or(self.inner.config.message_timeout),
        )
    }

    pub fn release(&self, topic: &str, channel: &str, ids: &[u64]) {
        if let Ok(handle) = self.topic(topic) {
            handle.state.lock().release(channel, ids);
            handle.signal();
        }
    }

    pub fn release_deliveries(&self, topic: &str, channel: &str, deliveries: &[(u64, u64)]) {
        if let Ok(handle) = self.topic(topic) {
            handle.state.lock().release_with_tokens(channel, deliveries);
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
    ) -> Result<bool, BrokerError> {
        self.ensure_storage_healthy()?;
        let broker = self.clone();
        let source_topic = source_topic.to_owned();
        let source_channel = source_channel.to_owned();
        let target_topic = target_topic.to_owned();
        let task = tokio::spawn(async move {
            let move_guard = broker.inner.outbox_moves.lock().await;
            let result = broker
                .move_to_dead_letter_transaction(
                    &source_topic,
                    &source_channel,
                    message_id,
                    &target_topic,
                    body,
                )
                .await;
            drop(move_guard);
            result
        });
        match task.await {
            Ok(result) => result,
            Err(_) => self.observe_storage_result(Err(BrokerError::StorageUnavailable)),
        }
    }

    async fn move_to_dead_letter_transaction(
        &self,
        source_topic: &str,
        source_channel: &str,
        message_id: u64,
        target_topic: &str,
        body: Bytes,
    ) -> Result<bool, BrokerError> {
        if !self
            .source_message_is_unacknowledged(source_topic, source_channel, message_id)?
            .unwrap_or(false)
        {
            return Ok(false);
        }
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
        let broker = self.clone();
        let target_topic = entry.target_topic.clone();
        let body = entry.body.clone();
        self.storage_task(move || {
            broker.publish_durable_body_sync(&target_topic, &[body], Duration::ZERO)
        })
        .await?;
        self.finish_outbox_source(&entry.source_topic, &entry.source_channel, entry.message_id)
            .await?;
        self.storage_task(move || crate::outbox::remove(&path))
            .await?;
        Ok(true)
    }

    fn publish_durable_body_sync(
        &self,
        topic: &str,
        bodies: &[Bytes],
        delay: Duration,
    ) -> Result<Vec<u64>, BrokerError> {
        self.validate_publish_request_with_limit(topic, bodies, self.durable_message_read_limit())?;
        let mut metadata = self.reserve_message_metadata(bodies.len())?;
        let handle = self.get_or_create_topic(topic)?;
        let mut state = handle.state.lock();
        self.ensure_management_access(topic, None)?;
        let ids = self.append_publish_to_topic(&mut state, bodies, delay, true, &mut metadata)?;
        if self.inner.message_index_cache.over_budget() {
            state.spill_message_metadata()?;
        }
        drop(state);
        handle.signal();
        Ok(ids)
    }

    pub(super) fn validate_publish_request(
        &self,
        topic: &str,
        bodies: &[Bytes],
    ) -> Result<usize, BrokerError> {
        self.validate_publish_request_with_limit(topic, bodies, self.inner.config.max_message_bytes)
    }

    fn validate_publish_request_with_limit(
        &self,
        topic: &str,
        bodies: &[Bytes],
        max_message_bytes: usize,
    ) -> Result<usize, BrokerError> {
        validate_name(topic).map_err(|_| BrokerError::InvalidTopic)?;
        self.ensure_management_access(topic, None)?;
        if bodies.is_empty() || bodies.len() > batch::MAX_MESSAGES {
            return Err(BrokerError::BatchTooLarge);
        }
        if bodies
            .iter()
            .any(|body| body.is_empty() || body.len() > max_message_bytes)
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

    fn durable_message_read_limit(&self) -> usize {
        if self.inner.compatibility.minimum_reader_feature_level >= 2 {
            100 * 1024 * 1024
        } else {
            rustqueue_storage::LEGACY_MAX_RECORD_BYTES.saturating_sub(24)
        }
    }

    pub(super) fn append_publish_to_topic(
        &self,
        state: &mut Topic,
        bodies: &[Bytes],
        delay: Duration,
        durable: bool,
        metadata: &mut crate::topic::index::MetadataReservation,
    ) -> Result<Vec<u64>, BrokerError> {
        let first_position = state.next_position();
        let first_id = self.reserve_ids(bodies.len())?;
        let batch = batch::encode(first_position, first_id, bodies)?;
        let timestamp = now_ns();
        let available = now_ms().saturating_add(delay.as_millis().min(i64::MAX as u128) as i64);
        state.append_batch(first_id, timestamp, available, batch, durable, metadata)
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
        for path in crate::outbox::paths(&self.inner.config.data_path.join("dlq-outbox"))? {
            let entry = crate::outbox::load(&path)?;
            if self.source_message_is_unacknowledged(
                &entry.source_topic,
                &entry.source_channel,
                entry.message_id,
            )? == Some(false)
            {
                crate::outbox::remove(&path)?;
                continue;
            }
            if let Err(error) =
                self.publish_durable_body_sync(&entry.target_topic, &[entry.body], Duration::ZERO)
            {
                if matches!(
                    error,
                    BrokerError::ManagementUnavailable
                        | BrokerError::TopicRetiring
                        | BrokerError::TopicTombstoned
                        | BrokerError::TopicLimit
                ) {
                    // Keep both the durable intent and its source payload. A
                    // consumer retry can complete the move after the
                    // management fence or capacity constraint is cleared.
                    continue;
                }
                return Err(error);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn durable_internal_publish_rechecks_fences_after_acquiring_topic() {
        let root = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: root.path().into(),
            ..BrokerConfig::default()
        })
        .unwrap();
        broker.create_topic("dead-letter").await.unwrap();

        let topic = broker.topic("dead-letter").unwrap();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_holder = {
            let topic = Arc::clone(&topic);
            std::thread::spawn(move || {
                let _topic_state = topic.state.lock();
                locked_tx.send(()).unwrap();
                let _ = release_rx.recv();
            })
        };
        locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let baseline_references = Arc::strong_count(&topic);
        let publisher = {
            let broker = broker.clone();
            std::thread::spawn(move || {
                broker.publish_durable_body_sync(
                    "dead-letter",
                    &[Bytes::from_static(b"poison")],
                    Duration::ZERO,
                )
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&topic) <= baseline_references {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        {
            let mut fences = broker.inner.fences.lock();
            fences.set_topic("dead-letter", now_ms() + 60_000);
            fences.store(&broker.inner.fences_path).unwrap();
        }
        release_tx.send(()).unwrap();
        lock_holder.join().unwrap();

        assert!(matches!(
            publisher.join().unwrap(),
            Err(BrokerError::TopicTombstoned)
        ));
        assert_eq!(broker.stats().topics[0].message_count, 0);
    }
}
