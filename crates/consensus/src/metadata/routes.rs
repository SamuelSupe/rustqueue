use super::*;

#[derive(Default)]
pub(super) struct MetadataRoutes {
    epoch: Option<u64>,
    topics: HashMap<String, Arc<TopicRoute>>,
    groups: HashMap<crate::GlobalGroupId, (String, Arc<PartitionDescriptor>)>,
}

pub struct TopicRoute {
    active: Arc<[Arc<PartitionDescriptor>]>,
    non_retired: Arc<[Arc<PartitionDescriptor>]>,
    by_number: HashMap<u16, Arc<PartitionDescriptor>>,
    by_group: HashMap<crate::GlobalGroupId, Arc<PartitionDescriptor>>,
    by_slot: HashMap<u16, Arc<PartitionDescriptor>>,
    key_routing_slots: Arc<[u16]>,
    key_bucket_ranges: Arc<[crate::BucketRange]>,
}

impl TopicRoute {
    fn new(topic: &TopicDescriptor, catalog: Option<&crate::CatalogTopic>) -> Self {
        let mut active = Vec::new();
        let mut non_retired = Vec::new();
        let mut by_group = HashMap::new();
        let mut by_number = HashMap::new();
        let mut by_slot = HashMap::new();
        for partition in &topic.partitions {
            let partition = Arc::new(partition.clone());
            if partition.lifecycle != PartitionLifecycle::Retired {
                non_retired.push(Arc::clone(&partition));
            }
            if topic.state == TopicState::Active
                && partition.lifecycle == PartitionLifecycle::Active
            {
                by_number.insert(partition.number, Arc::clone(&partition));
                by_slot.insert(partition.slot, Arc::clone(&partition));
                by_group.insert(partition.global_id(), Arc::clone(&partition));
                active.push(partition);
            }
        }
        Self {
            active: active.into(),
            non_retired: non_retired.into(),
            by_number,
            by_group,
            by_slot,
            key_routing_slots: topic.key_routing_slots.clone().into(),
            key_bucket_ranges: catalog
                .map(|topic| topic.bucket_ranges.clone())
                .unwrap_or_default()
                .into(),
        }
    }

    pub fn active_partitions(&self) -> Arc<[Arc<PartitionDescriptor>]> {
        Arc::clone(&self.active)
    }

    pub fn non_retired_partitions(&self) -> Arc<[Arc<PartitionDescriptor>]> {
        Arc::clone(&self.non_retired)
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    pub fn partition_by_group(
        &self,
        group_id: crate::GlobalGroupId,
    ) -> Option<Arc<PartitionDescriptor>> {
        self.by_group.get(&group_id).cloned()
    }

    pub fn partition_by_slot(&self, slot: u16) -> Option<Arc<PartitionDescriptor>> {
        self.by_slot.get(&slot).cloned()
    }

    pub fn partition_by_number(&self, number: u16) -> Option<Arc<PartitionDescriptor>> {
        self.by_number.get(&number).cloned()
    }

    pub fn select_partition(
        &self,
        operation_id: u64,
        requested: Option<u16>,
        routing_key: Option<&[u8]>,
    ) -> Result<Arc<PartitionDescriptor>, &'static str> {
        if self.active.is_empty() {
            return Err("topic has no active partitions");
        }
        if let Some(number) = requested {
            return self
                .by_number
                .get(&number)
                .cloned()
                .ok_or("partition not found or is not active");
        }
        if let Some(key) = routing_key {
            if !self.key_bucket_ranges.is_empty() {
                let bucket = (crc32c::crc32c(key) % crate::VIRTUAL_BUCKET_COUNT) as u16;
                let group_id = self
                    .key_bucket_ranges
                    .iter()
                    .find(|range| bucket >= range.start && bucket <= range.end)
                    .map(|range| range.partition)
                    .ok_or("topic has no route for virtual bucket")?;
                return self
                    .by_group
                    .get(&group_id)
                    .cloned()
                    .ok_or("key routing partition is not active");
            }
            if self.key_routing_slots.is_empty() {
                return Err("topic has no key routing slots");
            }
            let slot =
                self.key_routing_slots[crc32c::crc32c(key) as usize % self.key_routing_slots.len()];
            return self
                .by_slot
                .get(&slot)
                .cloned()
                .ok_or("key routing partition is not active");
        }
        Ok(Arc::clone(
            &self.active[operation_id as usize % self.active.len()],
        ))
    }
}

impl MetadataRoutes {
    fn rebuild(&mut self, state: &ClusterMetadata) {
        self.topics.clear();
        self.groups.clear();
        for topic in state.topics.values() {
            let route = Arc::new(TopicRoute::new(
                topic,
                state.catalog.topics.get(&topic.name),
            ));
            for partition in &topic.partitions {
                self.groups.insert(
                    partition.global_id(),
                    (topic.name.clone(), Arc::new(partition.clone())),
                );
            }
            self.topics.insert(topic.name.clone(), route);
        }
        self.epoch = Some(state.routing_epoch);
    }
}

impl MetadataCatalog {
    pub fn topic_route(&self, name: &str) -> Option<Arc<TopicRoute>> {
        let state = self.state.read().expect("metadata lock poisoned");
        let mut routes = self.routes.write().expect("metadata route lock poisoned");
        if routes.epoch != Some(state.routing_epoch) {
            routes.rebuild(&state);
        }
        routes.topics.get(name).cloned()
    }

    pub fn partition_route(
        &self,
        group_id: crate::GlobalGroupId,
    ) -> Option<(String, Arc<PartitionDescriptor>)> {
        let state = self.state.read().expect("metadata lock poisoned");
        let mut routes = self.routes.write().expect("metadata route lock poisoned");
        if routes.epoch != Some(state.routing_epoch) {
            routes.rebuild(&state);
        }
        routes.groups.get(&group_id).cloned()
    }

    pub(super) fn invalidate_routes(&self) {
        self.routes
            .write()
            .expect("metadata route lock poisoned")
            .epoch = None;
    }
}

pub(super) fn bump_routing_epoch(state: &mut ClusterMetadata) {
    state.routing_epoch = state.routing_epoch.saturating_add(1);
}
