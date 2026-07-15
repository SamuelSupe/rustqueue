use super::*;

const MAX_FETCH_MESSAGES: usize = 64;
const MAX_FETCH_BYTES: usize = 1024 * 1024;
const MAX_FETCH_WAIT: Duration = Duration::from_secs(1);

struct BatchReservationGuard {
    partition: Arc<Mutex<Partition>>,
    channel: String,
    reservations: Vec<ReservedDelivery>,
}

impl BatchReservationGuard {
    fn new(
        partition: Arc<Mutex<Partition>>,
        channel: &str,
        reservations: Vec<ReservedDelivery>,
    ) -> Self {
        Self {
            partition,
            channel: channel.to_owned(),
            reservations,
        }
    }

    fn reservations(&self) -> &[ReservedDelivery] {
        &self.reservations
    }

    fn complete(mut self, bodies: Vec<Arc<[u8]>>) -> Result<Vec<Delivery>, BrokerError> {
        let reservations = std::mem::take(&mut self.reservations);
        let mut partition = self.partition.lock();
        reservations
            .into_iter()
            .zip(bodies)
            .filter_map(|(reservation, body)| {
                partition
                    .complete_delivery(&self.channel, &reservation, body)
                    .transpose()
            })
            .collect()
    }
}

impl Drop for BatchReservationGuard {
    fn drop(&mut self) {
        if self.reservations.is_empty() {
            return;
        }
        let mut partition = self.partition.lock();
        for reservation in &self.reservations {
            partition.cancel_delivery(&self.channel, reservation);
        }
    }
}

impl Partition {
    pub(super) fn signal_delivery(&self) {
        self.delivery_wake
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

impl Broker {
    pub async fn wait_partition_ready(
        &self,
        topic_name: &str,
        channel: &str,
        partition_number: u16,
        wait: Duration,
    ) -> Result<bool, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(false);
        }
        let partition = topic.partition_by_number(partition_number)?;
        let mut wake = partition.lock().delivery_wake.subscribe();
        let wait = wait.min(MAX_FETCH_WAIT);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let blocked_until = {
                let mut state = partition.lock();
                if state.has_ready_message(channel, now_ms())? {
                    return Ok(true);
                }
                state.delivery_blocked_until(channel)?
            };
            if wait.is_zero() {
                return Ok(false);
            }
            let check_at = delivery_check_at(deadline, blocked_until);
            match tokio::time::timeout_at(check_at, wake.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Ok(false),
                Err(_) if check_at < deadline => {}
                Err(_) => return Ok(false),
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_batch(
        &self,
        topic_name: &str,
        channel: &str,
        partition_cursor: &mut usize,
        max_messages: usize,
        max_bytes: usize,
        wait: Duration,
        message_timeout: Option<Duration>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let partitions = topic.partitions();
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let index = *partition_cursor % partitions.len();
        *partition_cursor = (*partition_cursor + 1) % partitions.len();
        self.fetch_from_partition(
            partitions[index].clone(),
            channel,
            max_messages,
            max_bytes,
            wait,
            message_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_batch_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition_number: u16,
        max_messages: usize,
        max_bytes: usize,
        wait: Duration,
        message_timeout: Option<Duration>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let partition = topic.partition_by_number(partition_number)?;
        self.fetch_from_partition(
            partition,
            channel,
            max_messages,
            max_bytes,
            wait,
            message_timeout,
        )
        .await
    }

    async fn fetch_from_partition(
        &self,
        partition: Arc<Mutex<Partition>>,
        channel: &str,
        max_messages: usize,
        max_bytes: usize,
        wait: Duration,
        message_timeout: Option<Duration>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let max_messages = max_messages.clamp(1, MAX_FETCH_MESSAGES);
        let max_bytes = max_bytes.clamp(1, MAX_FETCH_BYTES);
        let wait = wait.min(MAX_FETCH_WAIT);
        let deadline = tokio::time::Instant::now() + wait;
        let mut wake = partition.lock().delivery_wake.subscribe();
        loop {
            let deliveries = self
                .fetch_from_partition_now(
                    Arc::clone(&partition),
                    channel,
                    max_messages,
                    max_bytes,
                    message_timeout,
                    None,
                )
                .await?;
            if !deliveries.is_empty() || wait.is_zero() {
                return Ok(deliveries);
            }
            let blocked_until = partition.lock().delivery_blocked_until(channel)?;
            let check_at = delivery_check_at(deadline, blocked_until);
            match tokio::time::timeout_at(check_at, wake.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Ok(Vec::new()),
                Err(_) if check_at < deadline => {}
                Err(_) => return Ok(Vec::new()),
            }
        }
    }

    async fn fetch_from_partition_now(
        &self,
        partition: Arc<Mutex<Partition>>,
        channel: &str,
        max_messages: usize,
        max_bytes: usize,
        message_timeout: Option<Duration>,
        expired_before_ns: Option<i64>,
    ) -> Result<Vec<Delivery>, BrokerError> {
        let timeout = message_timeout.unwrap_or(self.config.message_timeout);
        let reservations = {
            let mut state = partition.lock();
            let mut reservations = Vec::with_capacity(max_messages);
            let mut body_bytes = 0usize;
            for _ in 0..max_messages {
                let reservation = match expired_before_ns {
                    Some(cutoff) => state.reserve_expired_message(channel, cutoff, timeout)?,
                    None => state.reserve_next_message(channel, timeout)?,
                };
                let Some(reservation) = reservation else {
                    break;
                };
                let next_bytes = reservation.payload.len as usize;
                if !reservations.is_empty() && body_bytes.saturating_add(next_bytes) > max_bytes {
                    state.cancel_delivery(channel, &reservation);
                    break;
                }
                body_bytes = body_bytes.saturating_add(next_bytes);
                reservations.push(reservation);
            }
            reservations
        };
        if reservations.is_empty() {
            return Ok(Vec::new());
        }
        let guard = BatchReservationGuard::new(partition, channel, reservations);
        let payloads: Vec<_> = guard
            .reservations()
            .iter()
            .map(|reservation| reservation.payload.clone())
            .collect();
        let bodies = match self.payload_reader.read_many(&payloads).await {
            Ok(bodies) => bodies,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error.into()),
        };
        guard.complete(bodies)
    }

    pub async fn fetch_expired_batch_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition_number: u16,
        expired_before_ns: i64,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<Delivery>, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let partition = topic.partition_by_number(partition_number)?;
        self.fetch_from_partition_now(
            partition,
            channel,
            max_messages.clamp(1, MAX_FETCH_MESSAGES),
            max_bytes.clamp(1, MAX_FETCH_BYTES),
            None,
            Some(expired_before_ns),
        )
        .await
    }
}

fn delivery_check_at(
    deadline: tokio::time::Instant,
    blocked_until_ms: i64,
) -> tokio::time::Instant {
    if blocked_until_ms == i64::MAX {
        return deadline;
    }
    let delay_ms = blocked_until_ms.saturating_sub(now_ms()).max(0) as u64;
    (tokio::time::Instant::now() + Duration::from_millis(delay_ms)).min(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn open_broker(path: &Path) -> Arc<Broker> {
        Broker::open(BrokerConfig {
            data_path: path.to_path_buf(),
            default_partitions: 1,
            max_segment_bytes: 1024 * 1024,
            max_message_bytes: 1024,
            message_timeout: Duration::from_secs(1),
            max_ack_gap: 65_536,
            max_backlog_messages_per_partition: 10_000_000,
            projection_only: false,
            entry_cache_bytes: 1024 * 1024,
            payload_read_workers: 1,
            payload_read_queue: 128,
            dedup_max_entries: 1024,
            dedup_ttl: Duration::from_secs(60),
            cell_id: 1,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn fetches_up_to_the_batch_limit() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        broker
            .publish(
                "events",
                (0..80).map(|_| vec![7; 32]).collect(),
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();

        let deliveries = broker
            .fetch_batch_partition(
                "events",
                "workers",
                0,
                80,
                MAX_FETCH_BYTES,
                Duration::ZERO,
                None,
            )
            .await
            .unwrap();
        assert_eq!(deliveries.len(), MAX_FETCH_MESSAGES);
    }

    #[tokio::test]
    async fn publish_wakes_a_long_poll_without_periodic_scanning() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let waiting = Arc::clone(&broker);
        let started = tokio::time::Instant::now();
        let fetch = tokio::spawn(async move {
            waiting
                .fetch_batch_partition(
                    "events",
                    "workers",
                    0,
                    64,
                    MAX_FETCH_BYTES,
                    Duration::from_secs(1),
                    None,
                )
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        broker
            .publish(
                "events",
                vec![b"wake".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();

        let deliveries = fetch.await.unwrap();
        assert_eq!(deliveries.len(), 1);
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn readiness_wait_wakes_without_reserving_the_message() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let waiting = Arc::clone(&broker);
        let ready = tokio::spawn(async move {
            waiting
                .wait_partition_ready("events", "workers", 0, Duration::from_secs(1))
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        broker
            .publish(
                "events",
                vec![b"ready".to_vec()],
                Duration::ZERO,
                Some(0),
                None,
            )
            .unwrap();

        assert!(ready.await.unwrap());
        let deliveries = broker
            .fetch_batch_partition(
                "events",
                "workers",
                0,
                1,
                MAX_FETCH_BYTES,
                Duration::ZERO,
                None,
            )
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
    }

    #[tokio::test]
    async fn deferred_delivery_wakes_at_its_deadline_without_polling() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        broker
            .publish(
                "events",
                vec![b"later".to_vec()],
                Duration::from_millis(50),
                Some(0),
                None,
            )
            .unwrap();
        let started = tokio::time::Instant::now();
        let deliveries = broker
            .fetch_batch_partition(
                "events",
                "workers",
                0,
                1,
                MAX_FETCH_BYTES,
                Duration::from_millis(500),
                None,
            )
            .await
            .unwrap();
        assert_eq!(1, deliveries.len());
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert!(started.elapsed() < Duration::from_millis(300));
    }

    #[tokio::test]
    async fn retention_fetch_reserves_only_expired_messages() {
        let directory = tempdir().unwrap();
        let broker = open_broker(directory.path());
        broker.create_channel("events", "workers").unwrap();
        let old = broker
            .publish_replicated(1, "events", vec![b"old".to_vec()], 10, 0, Some(0), None)
            .unwrap()[0];
        broker
            .publish_replicated(2, "events", vec![b"new".to_vec()], 1_000, 0, Some(0), None)
            .unwrap();
        let expired = broker
            .fetch_expired_batch_partition("events", "workers", 0, 100, 64, 1024)
            .await
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, old);
        broker.release("events", "workers", &[old]);
    }
}
