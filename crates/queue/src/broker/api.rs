use super::*;

impl Broker {
    pub fn internal_message_id(
        &self,
        topic: &str,
        wire_id: u64,
    ) -> Result<rustqueue_protocol::InternalMessageId, BrokerError> {
        let partition = self.partition_for_message(topic, wire_id)?;
        let partition = partition.lock();
        let position = partition
            .message_position(wire_id)
            .ok_or(BrokerError::MessageNotInFlight)?;
        let message = &partition.messages[position];
        rustqueue_protocol::InternalMessageId::new(
            rustqueue_protocol::GlobalGroupId {
                cell: rustqueue_protocol::CellId(partition.cell_id),
                local: partition.group_id,
            },
            message.log_index,
            message.batch_ordinal,
            partition.wire_incarnation,
        )
        .map_err(|error| BrokerError::InvalidRecord(error.into()))
    }

    pub fn open(config: BrokerConfig) -> Result<Arc<Self>, BrokerError> {
        if config.default_partitions == 0 {
            return Err(BrokerError::InvalidRecord(
                "default_partitions must be greater than zero".into(),
            ));
        }
        if config.max_ack_gap == 0 {
            return Err(BrokerError::InvalidRecord(
                "max_ack_gap must be greater than zero".into(),
            ));
        }
        if config.dedup_max_entries == 0 || config.dedup_ttl.is_zero() {
            return Err(BrokerError::InvalidRecord(
                "dedup cache limits must be greater than zero".into(),
            ));
        }
        ensure_data_format(&config.data_path)?;
        let catalog_store = CatalogStore::new(&config.data_path)?;
        let catalog = catalog_store.load()?;
        let payload_reader = PayloadReader::new(
            config.entry_cache_bytes,
            config.payload_read_workers,
            config.payload_read_queue,
        );
        let mut topics = HashMap::new();
        for (name, definition) in &catalog.topics {
            let topic = Topic::open(name, definition, &config)?;
            topics.insert(name.clone(), Arc::new(topic));
        }
        let dedup = Mutex::new(DedupCache::new(config.dedup_max_entries, config.dedup_ttl));
        Ok(Arc::new(Self {
            config,
            catalog_store,
            catalog: Mutex::new(catalog),
            topics: RwLock::new(topics),
            payload_reader,
            dedup,
        }))
    }

    pub fn create_topic(&self, name: &str, partitions: Option<u16>) -> Result<(), BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        if self.topics.read().contains_key(name) {
            return Ok(());
        }
        let partitions = partitions.unwrap_or(self.config.default_partitions);
        if partitions == 0 {
            return Err(BrokerError::InvalidRecord(
                "partition count must be greater than zero".into(),
            ));
        }
        let mut catalog = self.catalog.lock();
        // Another publisher may have completed creation while this caller was
        // waiting for the catalog lock. Reopening the same partition here
        // would create two independent SegmentLog handles for one file.
        if self.topics.read().contains_key(name) {
            return Ok(());
        }
        if let Some(definition) = catalog.topics.get(name) {
            let topic = Topic::open(name, definition, &self.config)?;
            self.topics.write().insert(name.to_owned(), Arc::new(topic));
            return Ok(());
        }
        let first_group = catalog.next_slot;
        let next_group = first_group
            .checked_add(u32::from(partitions))
            .ok_or(BrokerError::SlotExhausted)?;
        let layouts: Vec<_> = (0..partitions)
            .map(|number| PartitionDefinition {
                number,
                slot: number + 1,
                cell_id: self.config.cell_id,
                group_id: u64::from(first_group) + u64::from(number),
                wire_incarnation: 1,
            })
            .collect();
        let definition = TopicDefinition {
            key_routing_slots: layouts.iter().map(|partition| partition.slot).collect(),
            partitions: layouts,
            paused: false,
        };
        let topic = Topic::open(name, &definition, &self.config)?;
        catalog.next_slot = next_group;
        catalog.topics.insert(name.to_owned(), definition);
        self.catalog_store.store(&catalog)?;
        self.topics.write().insert(name.to_owned(), Arc::new(topic));
        Ok(())
    }

    pub fn ensure_topic_layout(
        &self,
        name: &str,
        layouts: &[(u16, u16)],
        key_routing_slots: &[u16],
    ) -> Result<(), BrokerError> {
        let layouts = layouts
            .iter()
            .map(|(number, slot)| PartitionLayout {
                number: *number,
                slot: *slot,
                cell_id: self.config.cell_id,
                group_id: u64::from(*slot),
                wire_incarnation: 1,
            })
            .collect::<Vec<_>>();
        self.ensure_topic_layout_v4(name, &layouts, key_routing_slots)
    }

    pub fn ensure_topic_layout_v4(
        &self,
        name: &str,
        layouts: &[PartitionLayout],
        key_routing_slots: &[u16],
    ) -> Result<(), BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        if layouts.is_empty() || key_routing_slots.is_empty() {
            return Err(BrokerError::InvalidRecord(
                "topic layout and key routing slots cannot be empty".into(),
            ));
        }
        let desired: Vec<_> = layouts
            .iter()
            .map(|layout| PartitionDefinition {
                number: layout.number,
                slot: layout.slot,
                cell_id: layout.cell_id,
                group_id: layout.group_id,
                wire_incarnation: layout.wire_incarnation,
            })
            .collect();
        let max_slot = desired
            .iter()
            .map(|partition| partition.slot as u32)
            .max()
            .unwrap_or_default();
        if max_slot > MAX_PARTITION_SLOT {
            return Err(BrokerError::SlotExhausted);
        }
        let mut catalog = self.catalog.lock();
        let paused = catalog.topics.get(name).is_some_and(|topic| topic.paused);
        if let Some(existing) = catalog.topics.get(name) {
            for partition in &existing.partitions {
                if !desired.contains(partition) {
                    return Err(BrokerError::InvalidRecord(
                        "topic layout cannot remove or renumber a partition".into(),
                    ));
                }
            }
            if existing.key_routing_slots != key_routing_slots {
                return Err(BrokerError::InvalidRecord(
                    "topic key routing slots are immutable".into(),
                ));
            }
        }
        let definition = TopicDefinition {
            partitions: desired,
            key_routing_slots: key_routing_slots.to_vec(),
            paused,
        };
        catalog.next_slot = catalog.next_slot.max(max_slot.saturating_add(1));
        catalog.topics.insert(name.to_owned(), definition.clone());
        self.catalog_store.store(&catalog)?;
        drop(catalog);

        let runtime_topic = { self.topics.read().get(name).cloned() };
        if let Some(topic) = runtime_topic {
            let existing: HashMap<_, _> = topic
                .partitions
                .read()
                .ordered
                .iter()
                .map(|partition| {
                    let state = partition.lock();
                    (
                        state.number,
                        (
                            state.slot,
                            state.cell_id,
                            state.group_id,
                            state.wire_incarnation,
                            Arc::clone(partition),
                        ),
                    )
                })
                .collect();
            let mut partitions = Vec::with_capacity(definition.partitions.len());
            for layout in &definition.partitions {
                if let Some((slot, cell_id, group_id, incarnation, partition)) =
                    existing.get(&layout.number)
                {
                    if *slot != layout.slot
                        || *cell_id != layout.cell_id
                        || *group_id != layout.group_id
                        || *incarnation != layout.wire_incarnation
                    {
                        return Err(BrokerError::InvalidRecord(
                            "partition v4 identity cannot change".into(),
                        ));
                    }
                    partitions.push(Arc::clone(partition));
                } else {
                    partitions.push(Arc::new(Mutex::new(Partition::open(
                        layout,
                        name,
                        &self.config,
                    )?)));
                }
            }
            topic.replace_partitions(partitions);
        } else {
            let topic = Topic::open(name, &definition, &self.config)?;
            self.topics.write().insert(name.to_owned(), Arc::new(topic));
        }
        Ok(())
    }

    pub fn delete_topic(&self, name: &str) -> Result<(), BrokerError> {
        validate_name(name).map_err(|_| BrokerError::InvalidTopic)?;
        let mut catalog = self.catalog.lock();
        if catalog.topics.remove(name).is_none() {
            return Ok(());
        }
        self.catalog_store.store(&catalog)?;
        self.topics.write().remove(name);
        let path = topic_path(&self.config.data_path, name);
        if path.exists() {
            let trash = self
                .config
                .data_path
                .join(format!(".deleted-{name}-{}", now_ms()));
            fs::rename(path, trash)?;
        }
        Ok(())
    }

    pub fn publish<B>(
        &self,
        topic_name: &str,
        bodies: Vec<B>,
        delay: Duration,
        requested_partition: Option<u16>,
        routing_key: Option<&[u8]>,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: AsRef<[u8]>,
    {
        validate_name(topic_name).map_err(|_| BrokerError::InvalidTopic)?;
        if bodies
            .iter()
            .any(|body| body.as_ref().len() > self.config.max_message_bytes)
        {
            return Err(BrokerError::MessageTooLarge);
        }
        if !self.topics.read().contains_key(topic_name) {
            self.create_topic(topic_name, None)?;
        }
        let topic = self
            .topics
            .read()
            .get(topic_name)
            .cloned()
            .ok_or(BrokerError::TopicNotFound)?;
        let partition = topic.select_partition(requested_partition, routing_key)?;
        let mut partition = partition.lock();
        partition.publish_at(
            bodies,
            now_ns(),
            now_ms().saturating_add(duration_ms(delay)),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_replicated<B>(
        &self,
        operation_id: u64,
        topic_name: &str,
        bodies: Vec<B>,
        timestamp_ns: i64,
        available_at_ms: i64,
        requested_partition: Option<u16>,
        routing_key: Option<&[u8]>,
    ) -> Result<Vec<u64>, BrokerError>
    where
        B: AsRef<[u8]>,
    {
        validate_name(topic_name).map_err(|_| BrokerError::InvalidTopic)?;
        if bodies
            .iter()
            .any(|body| body.as_ref().len() > self.config.max_message_bytes)
        {
            return Err(BrokerError::MessageTooLarge);
        }
        if !self.topics.read().contains_key(topic_name) {
            self.create_topic(topic_name, None)?;
        }
        let topic = self.topic(topic_name)?;
        let partition = match (requested_partition, routing_key) {
            (Some(partition), _) => topic.select_partition(Some(partition), None)?,
            (None, Some(key)) => topic.select_partition(None, Some(key))?,
            (None, None) => topic.partition_at(operation_id as usize % topic.partition_count())?,
        };
        let partition_number = partition.lock().number;
        let key = DedupKey {
            operation_id,
            topic: topic_name.to_owned(),
            partition: partition_number,
        };
        if !self.config.projection_only {
            if let Some(message_ids) = self.dedup.lock().get(&key) {
                return Ok(message_ids);
            }
        }
        let result = partition
            .lock()
            .publish_at(bodies, timestamp_ns, available_at_ms, None)?;
        if !self.config.projection_only {
            self.dedup.lock().insert(key, result.clone());
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_replicated_refs(
        &self,
        _operation_id: u64,
        topic_name: &str,
        payloads: Vec<rustqueue_storage::PayloadRef>,
        timestamp_ns: i64,
        available_at_ms: i64,
        partition_number: u16,
        log_index: u64,
    ) -> Result<Vec<u64>, BrokerError> {
        validate_name(topic_name).map_err(|_| BrokerError::InvalidTopic)?;
        if payloads
            .iter()
            .any(|payload| payload.len as usize > self.config.max_message_bytes)
        {
            return Err(BrokerError::MessageTooLarge);
        }
        let partition = self.partition(topic_name, partition_number)?;
        let result =
            partition
                .lock()
                .publish_refs_at(payloads, timestamp_ns, available_at_ms, log_index)?;
        Ok(result)
    }

    pub fn cached_publish_result(
        &self,
        operation_id: u64,
        topic: &str,
        partition: u16,
    ) -> Option<Vec<u64>> {
        self.dedup.lock().get(&DedupKey {
            operation_id,
            topic: topic.to_owned(),
            partition,
        })
    }

    pub fn cache_publish_result(
        &self,
        operation_id: u64,
        topic: &str,
        partition: u16,
        message_ids: Vec<u64>,
    ) {
        self.dedup.lock().insert(
            DedupKey {
                operation_id,
                topic: topic.to_owned(),
                partition,
            },
            message_ids,
        );
    }
}
