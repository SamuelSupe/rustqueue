use super::*;

struct ReservationGuard {
    partition: Arc<Mutex<Partition>>,
    channel: String,
    reservation: Option<ReservedDelivery>,
}

impl ReservationGuard {
    fn new(partition: Arc<Mutex<Partition>>, channel: &str, reservation: ReservedDelivery) -> Self {
        Self {
            partition,
            channel: channel.to_owned(),
            reservation: Some(reservation),
        }
    }

    fn reservation(&self) -> &ReservedDelivery {
        self.reservation.as_ref().unwrap()
    }

    fn complete(mut self, body: Arc<[u8]>) -> Result<Option<Delivery>, BrokerError> {
        let reservation = self.reservation.take().unwrap();
        self.partition
            .lock()
            .complete_delivery(&self.channel, &reservation, body)
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(reservation) = &self.reservation {
            self.partition
                .lock()
                .cancel_delivery(&self.channel, reservation);
        }
    }
}

impl Broker {
    pub fn create_channel(&self, topic_name: &str, channel: &str) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        if !self.topics.read().contains_key(topic_name) {
            self.create_topic(topic_name, None)?;
        }
        let topic = self.topic(topic_name)?;
        for partition in topic.partitions() {
            partition.lock().create_channel(channel)?;
        }
        Ok(())
    }

    pub fn create_channel_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition: u16,
    ) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        if !self.topics.read().contains_key(topic_name) {
            self.create_topic(topic_name, None)?;
        }
        let partition = self.partition(topic_name, partition)?;
        let result = partition.lock().create_channel(channel);
        result
    }

    pub fn delete_channel(&self, topic_name: &str, channel: &str) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        for partition in topic.partitions() {
            partition.lock().delete_channel(channel)?;
        }
        Ok(())
    }

    pub fn delete_channel_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition: u16,
    ) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let partition = self.partition(topic_name, partition)?;
        let result = partition.lock().delete_channel(channel);
        result
    }

    pub fn set_channel_paused(
        &self,
        topic_name: &str,
        channel: &str,
        paused: bool,
    ) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        for partition in topic.partitions() {
            partition.lock().set_channel_paused(channel, paused)?;
        }
        Ok(())
    }

    pub fn set_channel_paused_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition: u16,
        paused: bool,
    ) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let partition = self.partition(topic_name, partition)?;
        let result = partition.lock().set_channel_paused(channel, paused);
        result
    }

    pub fn set_topic_paused(&self, topic_name: &str, paused: bool) -> Result<(), BrokerError> {
        let topic = self.topic(topic_name)?;
        let mut catalog = self.catalog.lock();
        let definition = catalog
            .topics
            .get_mut(topic_name)
            .ok_or(BrokerError::TopicNotFound)?;
        if definition.paused == paused {
            return Ok(());
        }
        definition.paused = paused;
        self.catalog_store.store(&catalog)?;
        topic.paused.store(paused, Ordering::Release);
        Ok(())
    }

    pub fn empty_channel(&self, topic_name: &str, channel: &str) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        for partition in topic.partitions() {
            partition.lock().empty_channel(channel)?;
        }
        Ok(())
    }

    pub fn empty_channel_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition: u16,
    ) -> Result<(), BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let partition = self.partition(topic_name, partition)?;
        let result = partition.lock().empty_channel(channel);
        result
    }

    pub fn empty_topic(&self, topic_name: &str) -> Result<(), BrokerError> {
        let topic = self.topic(topic_name)?;
        let channels = topic.channel_names();
        for channel in channels {
            self.empty_channel(topic_name, &channel)?;
        }
        Ok(())
    }

    pub fn empty_topic_partition(
        &self,
        topic_name: &str,
        partition: u16,
    ) -> Result<(), BrokerError> {
        let topic = self.topic(topic_name)?;
        let partition = topic.partition_by_number(partition)?;
        let channels: Vec<_> = partition.lock().channels.keys().cloned().collect();
        for channel in channels {
            partition.lock().empty_channel(&channel)?;
        }
        Ok(())
    }

    pub async fn next_message(
        &self,
        topic_name: &str,
        channel: &str,
        partition_cursor: &mut usize,
        message_timeout: Option<Duration>,
    ) -> Result<Option<Delivery>, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(None);
        }
        let partitions = topic.partitions();
        for _ in 0..partitions.len() {
            let index = *partition_cursor % partitions.len();
            *partition_cursor = (*partition_cursor + 1) % partitions.len();
            let timeout = message_timeout.unwrap_or(self.config.message_timeout);
            let reservation = partitions[index]
                .lock()
                .reserve_next_message(channel, timeout)?;
            let Some(reservation) = reservation else {
                continue;
            };
            let guard = ReservationGuard::new(Arc::clone(&partitions[index]), channel, reservation);
            match self.payload_reader.read(&guard.reservation().payload).await {
                Ok(body) => {
                    if let Some(delivery) = guard.complete(body)? {
                        return Ok(Some(delivery));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(None)
    }

    pub async fn next_message_partition(
        &self,
        topic_name: &str,
        channel: &str,
        partition: u16,
        message_timeout: Option<Duration>,
    ) -> Result<Option<Delivery>, BrokerError> {
        validate_name(channel).map_err(|_| BrokerError::InvalidChannel)?;
        let topic = self.topic(topic_name)?;
        if topic.paused.load(Ordering::Acquire) {
            return Ok(None);
        }
        let partition = topic.partition_by_number(partition)?;
        let timeout = message_timeout.unwrap_or(self.config.message_timeout);
        let reservation = partition.lock().reserve_next_message(channel, timeout)?;
        let Some(reservation) = reservation else {
            return Ok(None);
        };
        let guard = ReservationGuard::new(partition, channel, reservation);
        match self.payload_reader.read(&guard.reservation().payload).await {
            Ok(body) => guard.complete(body),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn finish(&self, topic: &str, channel: &str, id: u64) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?
            .lock()
            .finish(channel, id, true)
    }

    pub fn commit_finish(&self, topic: &str, channel: &str, id: u64) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?
            .lock()
            .finish(channel, id, false)
    }

    pub fn requeue(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?
            .lock()
            .requeue(channel, id, delay, true)
    }

    pub fn commit_requeue(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?
            .lock()
            .requeue(channel, id, delay, false)
    }

    pub fn commit_requeue_at(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        available_at_ms: i64,
    ) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?.lock().requeue_at(
            channel,
            id,
            available_at_ms,
            false,
        )
    }

    pub fn touch(
        &self,
        topic: &str,
        channel: &str,
        id: u64,
        timeout: Option<Duration>,
    ) -> Result<(), BrokerError> {
        self.partition_for_message(topic, id)?.lock().touch(
            channel,
            id,
            timeout.unwrap_or(self.config.message_timeout),
        )
    }

    pub fn release(&self, topic: &str, channel: &str, ids: &[u64]) {
        if let Ok(topic) = self.topic(topic) {
            for id in ids {
                let slot = (id >> 48) as u16;
                if let Ok(partition) = topic.partition_by_slot(slot) {
                    let mut partition = partition.lock();
                    if let Some(state) = partition.channels.get_mut(channel) {
                        state.in_flight.remove(id);
                        state.delivery_blocked_until_ms = 0;
                        partition.signal_delivery();
                    }
                }
            }
        }
    }
}
