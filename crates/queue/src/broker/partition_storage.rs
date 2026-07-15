use super::*;

impl Partition {
    pub(super) fn open(
        definition: &PartitionDefinition,
        topic: &str,
        config: &BrokerConfig,
    ) -> Result<Self, BrokerError> {
        let number = definition.number;
        let slot = definition.slot;
        let path = topic_path(&config.data_path, topic).join(format!("partition-{number:05}"));
        let log = SegmentLog::open(path, config.max_segment_bytes)?;
        let records = log.read_all_with_locations()?;
        let (delivery_wake, _) = tokio::sync::watch::channel(0);
        let mut partition = Self {
            number,
            slot,
            group_id: definition.group_id,
            cell_id: definition.cell_id,
            wire_incarnation: definition.wire_incarnation,
            base_sequence: 1,
            next_sequence: 1,
            log,
            messages: Vec::new(),
            channels: HashMap::new(),
            durable_appends: true,
            dirty: false,
            max_ack_gap: config.max_ack_gap,
            max_backlog_messages: config.max_backlog_messages_per_partition,
            persist_wal: !config.projection_only,
            projection_index: 1,
            next_delivery_token: 1,
            delivery_wake,
        };
        for (location, record) in records {
            partition.apply(record, &location)?;
        }
        partition.projection_index = partition.log.next_index();
        partition.base_sequence = partition
            .messages
            .first()
            .map_or(partition.next_sequence, |message| {
                message.id & ((1u64 << 48) - 1)
            });
        Ok(partition)
    }

    pub(super) fn publish_at<B>(
        &mut self,
        bodies: Vec<B>,
        timestamp_ns: i64,
        available_at_ms: i64,
        operation_id: Option<u64>,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: AsRef<[u8]>,
    {
        if bodies.is_empty() {
            return Ok(Vec::new());
        }
        if self
            .messages
            .len()
            .checked_add(bodies.len())
            .is_none_or(|messages| messages > self.max_backlog_messages)
        {
            return Err(BrokerError::BacklogLimit);
        }
        if bodies.len() as u64 > (1u64 << 48).saturating_sub(self.next_sequence) {
            return Err(BrokerError::SlotExhausted);
        }
        let mut headers = Vec::with_capacity(bodies.len());
        for _ in &bodies {
            let id = ((self.slot as u64) << 48) | self.next_sequence;
            self.next_sequence += 1;
            headers.push(batch::MessageHeader {
                id,
                timestamp_ns,
                available_at_ms,
            });
        }
        let (encoded_batch, body_ranges) = batch::encode(&headers, &bodies)?;
        let body_crcs: Vec<_> = bodies
            .iter()
            .map(|body| crc32c::crc32c(body.as_ref()))
            .collect();
        let payload = if let Some(operation_id) = operation_id {
            let mut payload = Vec::with_capacity(8 + encoded_batch.len());
            payload.extend_from_slice(&operation_id.to_be_bytes());
            payload.extend_from_slice(&encoded_batch);
            payload
        } else {
            encoded_batch
        };
        if payload.len() > MAX_RECORD_BYTES {
            return Err(BrokerError::BatchTooLarge);
        }
        let record = Record {
            kind: RecordKind::PublishBatch,
            flags: u16::from(operation_id.is_some()),
            term: 1,
            index: 0,
            timestamp_ns,
            message_id: headers[0].id,
            payload,
        };
        let location = self.log.append_at_with_location(
            Record {
                index: self.log.next_index(),
                ..record
            },
            self.durable_appends,
        )?;
        self.dirty |= !self.durable_appends;
        self.projection_index = location.index.saturating_add(1);
        let prefix = u64::from(operation_id.is_some()) * 8;
        let payload_base = location.offset + rustqueue_storage::HEADER_LEN as u64 + prefix;
        let ids: Vec<_> = headers.iter().map(|message| message.id).collect();
        let segment = Arc::clone(&location.segment);
        for (batch_ordinal, ((header, range), crc32c)) in headers
            .into_iter()
            .zip(body_ranges)
            .zip(body_crcs)
            .enumerate()
        {
            let message = StoredMessage {
                id: header.id,
                timestamp_ns: header.timestamp_ns,
                available_at_ms: header.available_at_ms,
                log_index: location.index,
                batch_ordinal: batch_ordinal as u32,
                payload: rustqueue_storage::PayloadRef {
                    path: Arc::clone(&segment),
                    offset: payload_base + range.start as u64,
                    len: (range.end - range.start) as u32,
                    crc32c,
                },
            };
            self.messages.push(message);
        }
        for channel in self.channels.values_mut() {
            channel.delivery_blocked_until_ms = 0;
        }
        self.signal_delivery();
        Ok(ids)
    }

    pub(super) fn publish_refs_at(
        &mut self,
        payloads: Vec<rustqueue_storage::PayloadRef>,
        timestamp_ns: i64,
        available_at_ms: i64,
        log_index: u64,
    ) -> Result<Vec<u64>, BrokerError> {
        if self
            .messages
            .len()
            .checked_add(payloads.len())
            .is_none_or(|messages| messages > self.max_backlog_messages)
        {
            return Err(BrokerError::BacklogLimit);
        }
        if payloads.len() as u64 > (1u64 << 48).saturating_sub(self.next_sequence) {
            return Err(BrokerError::SlotExhausted);
        }
        let mut ids = Vec::with_capacity(payloads.len());
        for (batch_ordinal, payload) in payloads.into_iter().enumerate() {
            let batch_ordinal = u32::try_from(batch_ordinal)
                .map_err(|_| BrokerError::InvalidRecord("message batch ordinal overflow".into()))?;
            let id = ((self.slot as u64) << 48) | self.next_sequence;
            self.next_sequence += 1;
            self.messages.push(StoredMessage {
                id,
                timestamp_ns,
                available_at_ms,
                log_index,
                batch_ordinal,
                payload,
            });
            ids.push(id);
        }
        for channel in self.channels.values_mut() {
            channel.delivery_blocked_until_ms = 0;
        }
        self.projection_index = self.projection_index.max(log_index.saturating_add(1));
        self.signal_delivery();
        Ok(ids)
    }
}
