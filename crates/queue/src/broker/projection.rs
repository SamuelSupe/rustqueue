use super::*;

impl Broker {
    pub fn topic_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.topics.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn projection_only(&self) -> bool {
        self.config.projection_only
    }

    pub fn reset_partition_projection(
        &self,
        topic: &str,
        partition: u16,
    ) -> Result<(), BrokerError> {
        if !self.config.projection_only {
            return Err(BrokerError::InvalidRecord(
                "partition reset is only allowed for replicated projections".into(),
            ));
        }
        let partition = self.partition(topic, partition)?;
        let mut partition = partition.lock();
        partition.base_sequence = 1;
        partition.next_sequence = 1;
        partition.projection_index = 1;
        partition.messages.clear();
        partition.channels.clear();
        partition.dirty = false;
        Ok(())
    }

    pub fn export_partition_projection(
        &self,
        topic_name: &str,
        partition: u16,
    ) -> Result<PartitionProjection, BrokerError> {
        let partition = self.partition(topic_name, partition)?;
        let partition = partition.lock();
        let messages = partition
            .messages
            .iter()
            .map(|message| ProjectedMessage {
                id: message.id,
                timestamp_ns: message.timestamp_ns,
                available_at_ms: message.available_at_ms,
                log_index: message.log_index,
                batch_ordinal: message.batch_ordinal,
                payload: message.payload.clone(),
            })
            .collect();
        let channels = partition
            .channels
            .iter()
            .map(|(name, channel)| {
                (
                    name.clone(),
                    ProjectedChannel {
                        barrier: channel.barrier,
                        ack_floor: channel.ack_floor,
                        acknowledged: channel.acknowledged.iter().copied().collect(),
                        requeued_until: channel
                            .requeued_until
                            .iter()
                            .map(|(id, time)| (*id, *time))
                            .collect(),
                        paused: channel.paused,
                        ephemeral: channel.ephemeral,
                    },
                )
            })
            .collect();
        Ok(PartitionProjection {
            topic: topic_name.to_owned(),
            partition: partition.number,
            slot: partition.slot,
            cell_id: partition.cell_id,
            group_id: partition.group_id,
            wire_incarnation: partition.wire_incarnation,
            base_sequence: partition.base_sequence,
            next_sequence: partition.next_sequence,
            messages,
            channels,
        })
    }

    pub fn import_partition_projection(
        &self,
        projection: PartitionProjection,
    ) -> Result<(), BrokerError> {
        if !self.config.projection_only {
            return Err(BrokerError::InvalidRecord(
                "projection import requires replicated mode".into(),
            ));
        }
        let partition = self.partition(&projection.topic, projection.partition)?;
        let mut partition = partition.lock();
        if partition.slot != projection.slot
            || partition.cell_id != projection.cell_id
            || partition.group_id != projection.group_id
            || partition.wire_incarnation != projection.wire_incarnation
        {
            return Err(BrokerError::InvalidRecord(
                "snapshot partition v4 identity mismatch".into(),
            ));
        }
        let mut messages = Vec::with_capacity(projection.messages.len());
        let mut expected_sequence = projection.base_sequence;
        for message in projection.messages {
            if (message.id >> 48) as u16 != projection.slot {
                return Err(BrokerError::InvalidRecord(
                    "snapshot contains a message from another partition".into(),
                ));
            }
            if message.id & ((1u64 << 48) - 1) != expected_sequence {
                return Err(BrokerError::InvalidRecord(
                    "snapshot message sequence is not contiguous".into(),
                ));
            }
            expected_sequence = expected_sequence.saturating_add(1);
            messages.push(StoredMessage {
                id: message.id,
                timestamp_ns: message.timestamp_ns,
                available_at_ms: message.available_at_ms,
                log_index: message.log_index,
                batch_ordinal: message.batch_ordinal,
                payload: message.payload,
            });
        }
        let mut channels = HashMap::with_capacity(projection.channels.len());
        for (name, projected) in projection.channels {
            if projected.barrier > messages.len()
                || projected.ack_floor > messages.len()
                || projected.ack_floor < projected.barrier
            {
                return Err(BrokerError::InvalidRecord(
                    "snapshot channel cursor exceeds message boundary".into(),
                ));
            }
            let mut channel = ChannelState::new(
                projected.barrier,
                projected.ephemeral,
                partition.max_ack_gap,
            );
            channel.cursor = projected.ack_floor.max(projected.barrier);
            channel.retention_cursor = channel.cursor;
            channel.ack_floor = projected.ack_floor;
            channel.acknowledged = projected.acknowledged.into();
            channel.requeued_until = projected
                .requeued_until
                .into_iter()
                .collect::<HashMap<_, _>>()
                .into();
            channel.paused = projected.paused;
            channels.insert(name, channel);
        }
        if expected_sequence != projection.next_sequence {
            return Err(BrokerError::InvalidRecord(
                "snapshot next message sequence is inconsistent".into(),
            ));
        }
        partition.base_sequence = projection.base_sequence;
        partition.next_sequence = projection.next_sequence;
        partition.projection_index = messages
            .last()
            .map_or(1, |message| message.log_index.saturating_add(1));
        partition.messages = messages;
        partition.channels = channels;
        partition.dirty = false;
        Ok(())
    }

    pub fn compact_partition_projection(
        &self,
        topic: &str,
        partition: u16,
    ) -> Result<usize, BrokerError> {
        let partition = self.partition(topic, partition)?;
        let mut partition = partition.lock();
        let retain_from = partition
            .channels
            .values()
            .filter(|channel| !channel.ephemeral)
            .map(|channel| channel.ack_floor)
            .min()
            .unwrap_or(partition.messages.len())
            .min(partition.messages.len());
        if retain_from == 0 {
            return Ok(0);
        }
        Ok(partition.drop_prefix(retain_from))
    }

    pub fn retarget_partition_projection_files(
        &self,
        projection: &PartitionProjection,
        root: &Path,
    ) -> Result<(), BrokerError> {
        let partition = self.partition(&projection.topic, projection.partition)?;
        let mut partition = partition.lock();
        if partition.slot != projection.slot
            || partition.messages.len() != projection.messages.len()
        {
            return Err(BrokerError::InvalidRecord(
                "snapshot projection does not match live partition".into(),
            ));
        }
        for (live, snapshot) in partition.messages.iter_mut().zip(&projection.messages) {
            let relative = snapshot.payload.path.as_path();
            if live.id != snapshot.id
                || relative.as_os_str().is_empty()
                || relative.is_absolute()
                || relative
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(BrokerError::InvalidRecord(
                    "snapshot projection message identity is invalid".into(),
                ));
            }
            let mut payload = snapshot.payload.clone();
            payload.path = Arc::new(root.join(relative));
            live.payload = payload;
        }
        Ok(())
    }

    pub fn partition_payload_paths(
        &self,
        topic: &str,
        partition: u16,
    ) -> Result<BTreeSet<PathBuf>, BrokerError> {
        let partition = self.partition(topic, partition)?;
        let message_count = partition.lock().messages.len();
        let mut shared_paths = BTreeSet::new();
        for start in (0..message_count).step_by(4_096) {
            let state = partition.lock();
            if state.messages.len() != message_count {
                return Err(BrokerError::InvalidRecord(
                    "partition changed while payload paths were collected".into(),
                ));
            }
            let end = start.saturating_add(4_096).min(message_count);
            shared_paths.extend(
                state.messages[start..end]
                    .iter()
                    .map(|message| Arc::clone(&message.payload.path)),
            );
        }
        let mut paths: BTreeSet<_> = shared_paths
            .into_iter()
            .map(|path| path.as_ref().clone())
            .collect();
        paths.extend(self.payload_reader.retained_paths());
        Ok(paths)
    }

    pub fn channel_names(&self, topic: &str) -> Result<Vec<String>, BrokerError> {
        Ok(self.topic(topic)?.channel_names())
    }

    pub fn stats(&self) -> BrokerStats {
        let mut topics: Vec<_> = self
            .topics
            .read()
            .values()
            .map(|topic| topic.stats())
            .collect();
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        BrokerStats { topics }
    }

    pub fn partition_stats(
        &self,
        topic: &str,
        partition: u16,
    ) -> Result<PartitionStats, BrokerError> {
        Ok(self.partition(topic, partition)?.lock().stats())
    }

    pub fn scrub(&self) -> Result<usize, BrokerError> {
        let mut records = 0;
        for topic in self.topics.read().values() {
            for partition in topic.partitions() {
                records += partition.lock().log.scrub()?;
            }
        }
        Ok(records)
    }

    pub fn begin_replicated_batch(&self) {
        if self.config.projection_only {
            return;
        }
        for topic in self.topics.read().values() {
            for partition in topic.partitions() {
                partition.lock().durable_appends = false;
            }
        }
    }

    pub fn release_all_in_flight(&self) -> usize {
        let mut released = 0;
        for topic in self.topics.read().values() {
            for partition in topic.partitions() {
                let mut partition = partition.lock();
                for channel in partition.channels.values_mut() {
                    released += channel.in_flight.len();
                    channel.in_flight.clear();
                    channel.delivery_blocked_until_ms = 0;
                }
            }
        }
        released
    }

    pub fn finish_replicated_batch(&self) -> Result<(), BrokerError> {
        if self.config.projection_only {
            return Ok(());
        }
        let mut partitions = Vec::new();
        for topic in self.topics.read().values() {
            for partition in topic.partitions() {
                let mut state = partition.lock();
                if state.dirty {
                    partitions.push(Arc::clone(&partition));
                } else {
                    state.durable_appends = true;
                }
            }
        }
        std::thread::scope(|scope| {
            let handles: Vec<_> = partitions
                .into_iter()
                .map(|partition| {
                    scope.spawn(move || {
                        let mut partition = partition.lock();
                        let result = if partition.dirty {
                            partition.log.sync().map_err(BrokerError::from)
                        } else {
                            Ok(())
                        };
                        if result.is_ok() {
                            partition.dirty = false;
                        }
                        partition.durable_appends = true;
                        result
                    })
                })
                .collect();
            for handle in handles {
                handle.join().expect("partition sync worker panicked")?;
            }
            Ok(())
        })
    }

    pub(super) fn topic(&self, name: &str) -> Result<Arc<Topic>, BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        self.topics
            .read()
            .get(name)
            .cloned()
            .ok_or(BrokerError::TopicNotFound)
    }

    pub(super) fn partition_for_message(
        &self,
        topic_name: &str,
        id: u64,
    ) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        let slot = (id >> 48) as u16;
        self.topic(topic_name)?.partition_by_slot(slot)
    }

    pub(super) fn partition(
        &self,
        topic_name: &str,
        number: u16,
    ) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        self.topic(topic_name)?.partition_by_number(number)
    }
}
