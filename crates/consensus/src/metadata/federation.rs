use super::*;

impl MetadataCatalog {
    pub fn sync_catalog_topic(&self, descriptor: crate::TopicDescriptor) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let feature_level = state.active_feature_level;
        state
            .catalog
            .sync_topic(&descriptor, feature_level, self.max_home_cells_per_topic)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn remove_catalog_topic(&self, topic: &str) {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state.catalog.remove_topic(topic);
        state.epoch = state.epoch.saturating_add(1);
    }

    pub fn begin_catalog_topic_deletion(&self, topic: &str) -> Result<bool, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let changed = state.catalog.begin_topic_deletion(topic)?;
        if changed {
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(changed)
    }

    pub fn prepare_catalog_channel(&self, topic: &str, channel: &str) -> Result<u64, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let generation = state.catalog.prepare_channel(topic, channel)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(generation)
    }

    pub fn update_catalog_channel(
        &self,
        topic: &str,
        channel: &str,
        generation: u64,
        lifecycle: ChannelLifecycle,
        paused: bool,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state
            .catalog
            .update_channel(topic, channel, generation, lifecycle, paused)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn remove_catalog_channel(
        &self,
        topic: &str,
        channel: &str,
        generation: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state.catalog.remove_channel(topic, channel, generation)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn renew_catalog_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        expires_at_ms: i64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state
            .catalog
            .renew_ephemeral_lease(topic, channel, lease_id, expires_at_ms)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn release_catalog_ephemeral_lease(
        &self,
        topic: &str,
        channel: &str,
        lease_id: u64,
        now_ms: i64,
    ) -> Result<bool, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let deleting = state
            .catalog
            .release_ephemeral_lease(topic, channel, lease_id, now_ms)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(deleting)
    }

    pub fn expire_catalog_ephemeral_leases(&self, now_ms: i64) -> usize {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let expired = state.catalog.expire_ephemeral_leases(now_ms);
        if expired > 0 {
            state.epoch = state.epoch.saturating_add(1);
        }
        expired
    }

    pub fn register_federation_node(&self, node: crate::FederationNode) -> Result<bool, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let changed = state.federation_root.register_node(node)?;
        if changed {
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(changed)
    }

    pub fn apply_root_action(
        &self,
        action: crate::RootAction,
        now_ms: i64,
        policy: crate::CellFormationPolicy,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state
            .federation_root
            .apply_cell_action(action, now_ms, policy)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn begin_partition_migration(
        &self,
        topic: &str,
        partition: crate::GlobalGroupId,
        target: crate::CellId,
        now_ms: i64,
        max_home_cells: usize,
    ) -> Result<crate::PartitionMigration, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state.catalog.begin_partition_migration(
            topic,
            partition,
            target,
            now_ms,
            max_home_cells,
        )?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(operation)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn advance_partition_migration(
        &self,
        operation_id: u64,
        expected: crate::PartitionMigrationPhase,
        next: crate::PartitionMigrationPhase,
        observed_lag_entries: u64,
        now_ms: i64,
        max_home_cells: usize,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state.catalog.advance_partition_migration(
            operation_id,
            expected,
            next,
            observed_lag_entries,
            now_ms,
            max_home_cells,
        )?;
        state.epoch = state.epoch.saturating_add(1);
        super::routes::bump_routing_epoch(&mut state);
        Ok(())
    }

    pub fn mark_partition_migration_needs_operator(
        &self,
        operation_id: u64,
        error: String,
        now_ms: i64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        state
            .catalog
            .mark_partition_migration_needs_operator(operation_id, error, now_ms)?;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn activate_bucket_move(
        &self,
        topic: &str,
        start: u16,
        end: u16,
        target: crate::GlobalGroupId,
        expected_epoch: u64,
    ) -> Result<u64, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let epoch =
            state
                .catalog
                .activate_bucket_move(topic, start, end, target, expected_epoch)?;
        state.epoch = state.epoch.saturating_add(1);
        super::routes::bump_routing_epoch(&mut state);
        Ok(epoch)
    }

    pub fn activate_scoped_feature(
        &self,
        activation: crate::FeatureActivation,
        observed_protocol_floor: u32,
    ) -> Result<bool, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let changed = state
            .scoped_feature_levels
            .activate(activation, observed_protocol_floor)?;
        if changed {
            state.epoch = state.epoch.saturating_add(1);
        }
        Ok(changed)
    }
}
