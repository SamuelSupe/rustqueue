use super::*;
use crate::delivery_guard::DeliveryGuard;
use crate::model::{Delivery, DeliveryBatch, ReservedDelivery};
use crate::payload_reader::PathLease;
use crate::topic::delivery::ReserveBatch;

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
        Ok(self
            .fetch_batch_retained(topic, channel, max_messages, max_bytes, wait, timeout)
            .await?
            .into_deliveries())
    }

    pub async fn fetch_batch_retained(
        &self,
        topic: &str,
        channel: &str,
        max_messages: usize,
        max_bytes: usize,
        wait: Duration,
        timeout: Option<Duration>,
    ) -> Result<DeliveryBatch, BrokerError> {
        self.ensure_storage_healthy()?;
        self.ensure_management_access(topic, Some(channel))?;
        self.expire_channel_in_flight(topic, channel).await?;
        let handle = self.topic(topic)?;
        let mut wake = handle.wake.subscribe();
        let timeout = timeout.unwrap_or(self.inner.config.message_timeout);
        let max_bytes = max_bytes
            .max(1)
            .min(self.inner.delivery_budget.max_payload_bytes());
        let mut batch = self
            .reserve_deliveries(
                topic,
                Arc::clone(&handle),
                channel,
                max_messages.clamp(1, 64),
                max_bytes,
                timeout,
            )
            .await?;
        if batch.is_empty() && !wait.is_zero() {
            let _ = tokio::time::timeout(wait.min(Duration::from_secs(1)), wake.changed()).await;
            batch = self
                .reserve_deliveries(
                    topic,
                    Arc::clone(&handle),
                    channel,
                    max_messages.clamp(1, 64),
                    max_bytes,
                    timeout,
                )
                .await?;
        }
        if batch.is_empty() {
            return Ok(DeliveryBatch::new(Vec::new(), DeliveryGuard::empty()));
        }
        let bytes = batch.payload_bytes();
        let hold = self.inner.delivery_budget.acquire(bytes).await?;
        let lease = self.inner.payload_reader.retain(batch.payloads());
        let (bodies, hold) = match self
            .inner
            .payload_reader
            .read_retained(lease, Some(hold))
            .await
        {
            Ok(result) => result,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(DeliveryBatch::new(Vec::new(), DeliveryGuard::empty()));
            }
            Err(error) => return self.observe_storage_result(Err(error.into())),
        };
        let hold = hold.expect("delivery payload read returns its byte-budget hold");
        let handle = Arc::clone(&batch.handle);
        let channel = batch.channel.clone();
        let reservations = batch.disarm();
        let deliveries = reservations
            .iter()
            .zip(bodies)
            .map(|(reservation, body)| Delivery {
                id: reservation.id,
                timestamp_ns: reservation.timestamp_ns,
                attempts: reservation.attempts,
                body,
            })
            .collect();
        let guard = DeliveryGuard::new(handle, channel, reservations, hold);
        Ok(DeliveryBatch::new(deliveries, guard))
    }

    async fn reserve_deliveries(
        &self,
        topic: &str,
        handle: Arc<TopicHandle>,
        channel: &str,
        max_messages: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<ReservedBatch, BrokerError> {
        let mut batch = ReservedBatch::new(handle, channel);
        for _ in 0..=max_messages {
            let remaining_messages = max_messages.saturating_sub(batch.len());
            if remaining_messages == 0 {
                break;
            }
            let handle = Arc::clone(&batch.handle);
            let action = {
                let topic_lock_started = Instant::now();
                let mut topic_state = handle.state.lock();
                self.inner
                    .metrics
                    .delivery_topic_lock_wait
                    .observe(topic_lock_started.elapsed());
                let _topic_lock_hold = self.inner.metrics.delivery_topic_lock_hold.timer();
                self.ensure_management_access(topic, Some(channel))?;
                let action = topic_state.reserve_batch(
                    channel,
                    remaining_messages,
                    max_bytes.saturating_sub(batch.payload_bytes()).max(1),
                    timeout,
                )?;
                let mut paths = match &action {
                    ReserveBatch::Ready(reserved) => reserved
                        .iter()
                        .map(|reservation| reservation.payload.path.as_ref().clone())
                        .collect::<Vec<_>>(),
                    ReserveBatch::Load { reserved, request } => {
                        let mut paths = reserved
                            .iter()
                            .map(|reservation| reservation.payload.path.as_ref().clone())
                            .collect::<Vec<_>>();
                        paths.push(request.segment_path().to_path_buf());
                        paths
                    }
                };
                paths.sort();
                paths.dedup();
                if !paths.is_empty() {
                    batch
                        .path_leases
                        .push(self.inner.payload_reader.retain_paths(paths));
                }
                action
            };
            match action {
                ReserveBatch::Ready(reservations) => {
                    let done = reservations.is_empty()
                        || batch.len().saturating_add(reservations.len()) >= max_messages
                        || batch
                            .payload_bytes()
                            .saturating_add(payload_bytes(&reservations))
                            >= max_bytes;
                    batch.extend(reservations);
                    if done {
                        break;
                    }
                }
                ReserveBatch::Load { reserved, request } => {
                    batch.extend(reserved);
                    if batch.payload_bytes() >= max_bytes || batch.len() >= max_messages {
                        break;
                    }
                    let read_lease = self
                        .inner
                        .payload_reader
                        .retain_paths(vec![request.segment_path().to_path_buf()]);
                    if let Err(error) = self
                        .inner
                        .message_index_cache
                        .load(request, read_lease)
                        .await
                    {
                        if matches!(&error, BrokerError::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock)
                        {
                            return Ok(ReservedBatch::new(Arc::clone(&batch.handle), channel));
                        }
                        return self.observe_storage_result(Err(error));
                    }
                }
            }
        }
        Ok(batch)
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
}

struct ReservedBatch {
    handle: Arc<TopicHandle>,
    channel: String,
    items: Vec<ReservedDelivery>,
    path_leases: Vec<PathLease>,
    armed: bool,
}

impl ReservedBatch {
    fn new(handle: Arc<TopicHandle>, channel: &str) -> Self {
        Self {
            handle,
            channel: channel.to_owned(),
            items: Vec::new(),
            path_leases: Vec::new(),
            armed: true,
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn payload_bytes(&self) -> usize {
        payload_bytes(&self.items)
    }

    fn payloads(&self) -> Vec<rustqueue_storage::PayloadRef> {
        self.items.iter().map(|item| item.payload.clone()).collect()
    }

    fn extend(&mut self, items: Vec<ReservedDelivery>) {
        self.items.extend(items);
    }

    fn disarm(mut self) -> Vec<ReservedDelivery> {
        self.armed = false;
        std::mem::take(&mut self.items)
    }
}

impl Drop for ReservedBatch {
    fn drop(&mut self) {
        if self.armed && !self.items.is_empty() {
            self.handle.state.lock().cancel(&self.channel, &self.items);
            self.handle.signal();
        }
    }
}

fn payload_bytes(items: &[ReservedDelivery]) -> usize {
    items
        .iter()
        .map(|item| item.payload.len as usize)
        .fold(0usize, usize::saturating_add)
}
