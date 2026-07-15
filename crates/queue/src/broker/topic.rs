use super::*;

impl PartitionRoutes {
    fn new(ordered: Vec<Arc<Mutex<Partition>>>) -> Self {
        let mut by_number = HashMap::with_capacity(ordered.len());
        let mut by_slot = HashMap::with_capacity(ordered.len());
        for partition in &ordered {
            let state = partition.lock();
            by_number.insert(state.number, Arc::clone(partition));
            by_slot.insert(state.slot, Arc::clone(partition));
        }
        Self {
            ordered,
            by_number,
            by_slot,
        }
    }
}

impl Topic {
    pub(super) fn open(
        name: &str,
        definition: &TopicDefinition,
        config: &BrokerConfig,
    ) -> Result<Self, BrokerError> {
        let mut partitions = Vec::with_capacity(definition.partitions.len());
        for layout in &definition.partitions {
            partitions.push(Arc::new(Mutex::new(Partition::open(layout, name, config)?)));
        }
        Ok(Self {
            name: name.to_owned(),
            partitions: RwLock::new(PartitionRoutes::new(partitions)),
            key_routing_slots: definition.key_routing_slots.clone(),
            next_partition: AtomicUsize::new(0),
            paused: AtomicBool::new(definition.paused),
        })
    }

    pub(super) fn select_partition(
        &self,
        requested: Option<u16>,
        routing_key: Option<&[u8]>,
    ) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        let partitions = self.partitions.read();
        if let Some(number) = requested {
            return partitions
                .by_number
                .get(&number)
                .cloned()
                .ok_or(BrokerError::PartitionNotFound);
        }
        if let Some(key) = routing_key {
            let slot =
                self.key_routing_slots[crc32c::crc32c(key) as usize % self.key_routing_slots.len()];
            return partitions
                .by_slot
                .get(&slot)
                .cloned()
                .ok_or(BrokerError::PartitionNotFound);
        }
        let index = self.next_partition.fetch_add(1, Ordering::Relaxed) % partitions.ordered.len();
        Ok(Arc::clone(&partitions.ordered[index]))
    }

    pub(super) fn partitions(&self) -> Vec<Arc<Mutex<Partition>>> {
        self.partitions.read().ordered.clone()
    }

    pub(super) fn partition_count(&self) -> usize {
        self.partitions.read().ordered.len()
    }

    pub(super) fn partition_at(&self, index: usize) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        self.partitions
            .read()
            .ordered
            .get(index)
            .cloned()
            .ok_or(BrokerError::PartitionNotFound)
    }

    pub(super) fn partition_by_number(
        &self,
        number: u16,
    ) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        self.partitions
            .read()
            .by_number
            .get(&number)
            .cloned()
            .ok_or(BrokerError::PartitionNotFound)
    }

    pub(super) fn partition_by_slot(
        &self,
        slot: u16,
    ) -> Result<Arc<Mutex<Partition>>, BrokerError> {
        self.partitions
            .read()
            .by_slot
            .get(&slot)
            .cloned()
            .ok_or(BrokerError::PartitionNotFound)
    }

    pub(super) fn replace_partitions(&self, partitions: Vec<Arc<Mutex<Partition>>>) {
        *self.partitions.write() = PartitionRoutes::new(partitions);
    }

    pub(super) fn channel_names(&self) -> Vec<String> {
        let mut intersection: Option<BTreeSet<String>> = None;
        for partition in &self.partitions.read().ordered {
            let names: BTreeSet<_> = partition.lock().channels.keys().cloned().collect();
            intersection = Some(match intersection {
                Some(current) => current.intersection(&names).cloned().collect(),
                None => names,
            });
        }
        intersection.unwrap_or_default().into_iter().collect()
    }

    pub(super) fn stats(&self) -> TopicStats {
        let partitions: Vec<_> = self
            .partitions
            .read()
            .ordered
            .iter()
            .map(|partition| partition.lock().stats())
            .collect();
        TopicStats {
            name: self.name.clone(),
            paused: self.paused.load(Ordering::Acquire),
            message_count: partitions
                .iter()
                .map(|partition| partition.message_count)
                .sum(),
            channels: self.channel_names(),
            partitions,
        }
    }
}
