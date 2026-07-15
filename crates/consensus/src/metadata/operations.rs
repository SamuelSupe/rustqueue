use super::*;

impl MetadataCatalog {
    pub fn create_operation(
        &self,
        kind: OperationKind,
        now_ms: i64,
        history_limit: usize,
    ) -> Result<MaintenanceOperation, String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if let Some(existing) = state.operations.values().find(|operation| {
            operation.kind == kind
                && !matches!(
                    operation.state,
                    OperationState::Completed | OperationState::Cancelled
                )
        }) {
            return Ok(existing.clone());
        }
        let id = state.next_operation_id;
        state.next_operation_id = state.next_operation_id.saturating_add(1);
        let operation = MaintenanceOperation {
            id,
            kind,
            state: OperationState::Running,
            phase: OperationPhase::Planned,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error: None,
            progress: OperationProgress::None,
        };
        state.operations.insert(id, operation.clone());
        let completed: Vec<_> = state
            .operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.state,
                    OperationState::Completed | OperationState::Cancelled
                )
            })
            .map(|operation| operation.id)
            .collect();
        for id in completed
            .into_iter()
            .take(state.operations.len().saturating_sub(history_limit))
        {
            state.operations.remove(&id);
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(operation)
    }

    pub fn update_operation(
        &self,
        operation_id: u64,
        phase: OperationPhase,
        operation_state: OperationState,
        now_ms: i64,
        error: Option<String>,
        progress: Option<OperationProgress>,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state
            .operations
            .get_mut(&operation_id)
            .ok_or_else(|| "operation not found".to_owned())?;
        if matches!(
            operation.state,
            OperationState::Completed | OperationState::Cancelled
        ) {
            return Ok(());
        }
        operation.phase = phase;
        operation.state = operation_state;
        operation.updated_at_ms = now_ms;
        operation.error = error;
        if let Some(progress) = progress {
            operation.progress = progress;
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn observe_node_health(
        &self,
        node_id: NodeId,
        healthy: bool,
        disk_used_percent: u8,
        disk_free_bytes: u64,
        storage_eligible: bool,
        now_ms: i64,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if !state.nodes.contains_key(&node_id) {
            return Err("health observation references an unknown node".into());
        }
        let in_maintenance = state
            .maintenance_nodes
            .get(&node_id)
            .is_some_and(|lease| lease.expires_at_ms > now_ms);
        let health = state.node_health.entry(node_id).or_default();
        health.last_observed_ms = now_ms;
        health.disk_used_percent = disk_used_percent;
        health.disk_free_bytes = disk_free_bytes;
        health.storage_eligible = storage_eligible;
        if in_maintenance {
            health.available = false;
            health.consecutive_failures = 0;
            health.unavailable_since_ms = None;
            health.stable_since_ms = None;
        } else if healthy {
            if !health.available || health.stable_since_ms.is_none() {
                health.stable_since_ms = Some(now_ms);
            }
            health.available = true;
            health.consecutive_failures = 0;
            health.unavailable_since_ms = None;
        } else {
            health.stable_since_ms = None;
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
            if health.consecutive_failures >= 3 {
                health.available = false;
                health.unavailable_since_ms.get_or_insert(now_ms);
            }
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn operation(&self, operation_id: u64) -> Option<MaintenanceOperation> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .operations
            .get(&operation_id)
            .cloned()
    }

    pub fn operations(&self) -> Vec<MaintenanceOperation> {
        self.state
            .read()
            .expect("metadata lock poisoned")
            .operations
            .values()
            .cloned()
            .collect()
    }

    pub fn set_operation_paused(&self, operation_id: u64, paused: bool) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        let operation = state
            .operations
            .get_mut(&operation_id)
            .ok_or_else(|| "operation not found".to_owned())?;
        if matches!(
            operation.state,
            OperationState::Completed | OperationState::Cancelled
        ) {
            return Err("completed operation cannot be paused or resumed".into());
        }
        if paused
            && matches!(
                operation.phase,
                OperationPhase::JointConsensus | OperationPhase::RemoveOld | OperationPhase::Retire
            )
        {
            return Err("operation has crossed the safe pause boundary".into());
        }
        operation.state = if paused {
            OperationState::Paused
        } else {
            OperationState::Running
        };
        operation.error = None;
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn set_automation_enabled(&self, enabled: bool) {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if state.automation_enabled != enabled {
            state.automation_enabled = enabled;
            state.epoch = state.epoch.saturating_add(1);
        }
    }

    pub fn set_maintenance(
        &self,
        node_id: NodeId,
        lease: Option<MaintenanceLease>,
    ) -> Result<(), String> {
        let mut state = self.state.write().expect("metadata lock poisoned");
        if !state.nodes.contains_key(&node_id) {
            return Err("maintenance node is not configured".into());
        }
        match lease {
            Some(lease) => {
                state.maintenance_nodes.insert(node_id, lease);
            }
            None => {
                state.maintenance_nodes.remove(&node_id);
            }
        }
        state.epoch = state.epoch.saturating_add(1);
        Ok(())
    }

    pub fn active_partitions(&self, topic: &str) -> Vec<PartitionDescriptor> {
        self.topic(topic)
            .map(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
                    .collect()
            })
            .unwrap_or_default()
    }
}
