use super::*;

impl StateMachineStore {
    pub fn open(directory: impl AsRef<Path>, broker: Arc<Broker>) -> io::Result<Arc<Self>> {
        Self::open_with_metadata(directory, broker, Arc::new(MetadataCatalog::standalone(1)))
    }

    pub fn open_with_metadata(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
    ) -> io::Result<Arc<Self>> {
        Self::open_for_group(directory, broker, metadata, StateMachineRole::All)
    }

    pub fn open_for_group(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        role: StateMachineRole,
    ) -> io::Result<Arc<Self>> {
        Self::open_for_group_with_entries(directory, broker, metadata, role, Vec::new())
    }

    pub fn open_for_group_with_entries(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        role: StateMachineRole,
        recovered_entries: Vec<Entry<TypeConfig>>,
    ) -> io::Result<Arc<Self>> {
        Self::open_for_group_with_source(
            directory,
            broker,
            metadata,
            role,
            recovered_entries,
            BTreeMap::new(),
            None,
            Arc::new(GroupLatencyMetrics::default()),
        )
    }

    pub fn open_for_group_with_log(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        role: StateMachineRole,
        recovered_entries: Vec<(Entry<TypeConfig>, Vec<PayloadRef>)>,
        payload_log: LogStore,
    ) -> io::Result<Arc<Self>> {
        Self::open_for_group_with_log_and_metrics(
            directory,
            broker,
            metadata,
            role,
            recovered_entries,
            payload_log,
            Arc::new(GroupLatencyMetrics::default()),
        )
    }

    pub(crate) fn open_for_group_with_log_and_metrics(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        role: StateMachineRole,
        recovered_entries: Vec<(Entry<TypeConfig>, Vec<PayloadRef>)>,
        payload_log: LogStore,
        latency: Arc<GroupLatencyMetrics>,
    ) -> io::Result<Arc<Self>> {
        let payloads = recovered_entries
            .iter()
            .map(|(entry, payloads)| (entry.log_id.index, payloads.clone()))
            .collect();
        let entries = recovered_entries
            .into_iter()
            .map(|(entry, _)| entry)
            .collect();
        Self::open_for_group_with_source(
            directory,
            broker,
            metadata,
            role,
            entries,
            payloads,
            Some(payload_log),
            latency,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_for_group_with_source(
        directory: impl AsRef<Path>,
        broker: Arc<Broker>,
        metadata: Arc<MetadataCatalog>,
        role: StateMachineRole,
        recovered_entries: Vec<Entry<TypeConfig>>,
        recovered_payloads: BTreeMap<u64, Vec<PayloadRef>>,
        payload_log: Option<LogStore>,
        latency: Arc<GroupLatencyMetrics>,
    ) -> io::Result<Arc<Self>> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let generations = GenerationStore::open(directory.join("snapshots"))?;
        let (current_snapshot, mut state): (Option<StoredSnapshot>, StateMachineData) =
            match generations.active()? {
                Some(active) => {
                    let meta: SnapshotMeta<NodeId, BasicNode> = read_json_optional(
                        &active.join("snapshot-meta.json"),
                    )?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "active snapshot generation has no metadata",
                        )
                    })?;
                    let state = snapshot_files::read_state(&active)?;
                    if state.last_applied != meta.last_log_id
                        || state.last_membership != meta.last_membership
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "snapshot metadata does not match bundle state",
                        ));
                    }
                    (
                        Some(StoredSnapshot {
                            meta,
                            directory: active,
                        }),
                        state,
                    )
                }
                None => (None, StateMachineData::default()),
            };
        let legacy_boundary: StateMachineData =
            read_json_optional(&directory.join("state.json"))?.unwrap_or_default();
        let persisted_boundary = read_applied_state(&directory.join("applied.boundary"))?
            .or(legacy_boundary.last_applied);
        let snapshot_index = state.last_applied.map(|log_id| log_id.index);
        let persisted_boundary_index = persisted_boundary.map(|log_id| log_id.index);
        let boundary_index = match (snapshot_index, persisted_boundary_index) {
            (Some(snapshot), Some(boundary)) => Some(snapshot.max(boundary)),
            (snapshot, boundary) => snapshot.or(boundary),
        };

        let tail = recovery_tail(&recovered_entries, snapshot_index, boundary_index)?;
        if role.carries_cell_metadata() {
            if let Some(snapshot) = state.metadata.clone() {
                metadata.replace(snapshot).map_err(io::Error::other)?;
            }
        } else if role.carries_root() {
            if let Some(root) = state.root.clone() {
                metadata.replace_root(root);
            }
        } else if role.carries_catalog() {
            if let Some(catalog) = state.catalog.clone() {
                metadata.replace_catalog(catalog);
            }
        }
        if role.carries_cell_metadata() || role.carries_root() || role.carries_catalog() {
            for entry in &tail {
                if let EntryPayload::Normal(envelope) = &entry.payload {
                    role.validate_envelope(envelope)?;
                    replay_metadata(&metadata, &envelope.command).map_err(io::Error::other)?;
                }
            }
            state.metadata = role.carries_cell_metadata().then(|| metadata.snapshot());
            state.root = role.carries_root().then(|| metadata.root_snapshot());
            state.catalog = role.carries_catalog().then(|| metadata.catalog_snapshot());
        }
        if role.carries_cell_metadata() || matches!(role, StateMachineRole::Partition { .. }) {
            reconcile_broker_topics(&broker, &metadata.snapshot()).map_err(io::Error::other)?;
        }
        if broker.projection_only() {
            if let Some(projection) = state.projection.take() {
                broker
                    .import_partition_projection(projection)
                    .map_err(io::Error::other)?;
            } else if let StateMachineRole::Partition { topic, partition } = &role {
                if let Some(snapshot) = &current_snapshot {
                    let projection = snapshot.directory.join("partition-projection.bin");
                    if !projection.exists() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "partition snapshot projection is missing",
                        ));
                    }
                    broker
                        .import_partition_snapshot(
                            topic,
                            *partition,
                            &projection,
                            &snapshot.directory,
                        )
                        .map_err(io::Error::other)?;
                } else {
                    broker
                        .reset_partition_projection(topic, *partition)
                        .map_err(io::Error::other)?;
                }
            }
            for entry in &tail {
                if let EntryPayload::Normal(envelope) = &entry.payload {
                    role.validate_envelope(envelope)?;
                    let command = &envelope.command;
                    if let Some(payloads) = recovered_payloads.get(&entry.log_id.index) {
                        rebuild_broker_projection_refs(
                            &broker,
                            &role,
                            command,
                            payloads,
                            entry.log_id.index,
                        )
                        .map_err(io::Error::other)?;
                    } else {
                        rebuild_broker_projection(&broker, &role, std::slice::from_ref(command))
                            .map_err(io::Error::other)?;
                    }
                }
            }
        }
        for entry in tail {
            state.last_applied = Some(entry.log_id);
            if let EntryPayload::Membership(membership) = entry.payload {
                state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
            }
        }
        if persisted_boundary_index >= snapshot_index {
            state.last_applied = persisted_boundary;
        }
        state.commands.clear();
        state.projection = None;
        Ok(Arc::new(Self {
            broker,
            metadata,
            directory,
            state: RwLock::new(state),
            operation_lock: tokio::sync::Mutex::new(()),
            generations,
            snapshot_index: AtomicU64::new(0),
            checkpoint_pending: AtomicU64::new(APPLIED_CHECKPOINT_ENTRIES.saturating_sub(1)),
            current_snapshot: RwLock::new(current_snapshot),
            role,
            payload_log,
            latency,
        }))
    }

    pub(super) fn apply_command(
        &self,
        command: &QueueCommand,
    ) -> Result<QueueResponse, BrokerError> {
        match &self.role {
            StateMachineRole::All => {}
            StateMachineRole::Root
            | StateMachineRole::Catalog { .. }
            | StateMachineRole::CellMetadata => return self.apply_metadata_command(command),
            StateMachineRole::Partition { topic, partition } => {
                return self.apply_partition_command(command, topic, *partition);
            }
        }
        let result = match command {
            QueueCommand::Batch { commands } => self.apply_batch(commands, true),
            QueueCommand::Publish {
                operation_id,
                topic,
                bodies,
                timestamp_ns,
                available_at_ms,
                partition,
                routing_key,
            } => {
                let descriptor = self
                    .metadata
                    .ensure_topic(topic, None, None)
                    .map_err(BrokerError::InvalidRecord)?;
                ensure_broker_topic(&self.broker, &descriptor)?;
                self.broker
                    .publish_replicated(
                        *operation_id,
                        topic,
                        bodies.clone(),
                        *timestamp_ns,
                        *available_at_ms,
                        *partition,
                        routing_key.as_deref(),
                    )
                    .map(|message_ids| QueueResponse {
                        message_ids,
                        error: None,
                        results: Vec::new(),
                    })
            }
            QueueCommand::CreateTopic {
                topic,
                partitions,
                replication_factor,
            } => {
                let descriptor = self
                    .metadata
                    .ensure_topic(topic, *partitions, *replication_factor)
                    .map_err(BrokerError::InvalidRecord)?;
                ensure_broker_topic(&self.broker, &descriptor)?;
                Ok(QueueResponse::default())
            }
            QueueCommand::DeleteTopic { topic } => {
                let result = self
                    .broker
                    .delete_topic(topic)
                    .map(|_| QueueResponse::default());
                if result.is_ok() {
                    self.metadata.delete_topic(topic);
                }
                result
            }
            QueueCommand::PrepareDeleteTopic { topic } => {
                self.metadata.prepare_delete_topic(topic);
                Ok(QueueResponse::default())
            }
            QueueCommand::CompleteDeleteTopic { topic } => {
                let result = self
                    .broker
                    .delete_topic(topic)
                    .map(|_| QueueResponse::default());
                if result.is_ok() {
                    self.metadata.delete_topic(topic);
                }
                result
            }
            QueueCommand::CreateChannel { topic, channel } => self
                .broker
                .create_channel(topic, channel)
                .map(|_| QueueResponse::default()),
            QueueCommand::DeleteChannel { topic, channel } => self
                .broker
                .delete_channel(topic, channel)
                .map(|_| QueueResponse::default()),
            QueueCommand::EmptyTopic { topic } => self
                .broker
                .empty_topic(topic)
                .map(|_| QueueResponse::default()),
            QueueCommand::EmptyChannel { topic, channel } => self
                .broker
                .empty_channel(topic, channel)
                .map(|_| QueueResponse::default()),
            QueueCommand::PauseChannel {
                topic,
                channel,
                paused,
            } => self
                .broker
                .set_channel_paused(topic, channel, *paused)
                .map(|_| QueueResponse::default()),
            QueueCommand::SetChannelMetadataPaused {
                topic,
                channel,
                paused,
            } => self
                .metadata
                .set_channel_metadata_paused(topic, channel, *paused)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::PauseTopic { topic, paused } => self
                .broker
                .set_topic_paused(topic, *paused)
                .and_then(|_| {
                    self.metadata
                        .set_topic_paused(topic, *paused)
                        .map_err(BrokerError::InvalidRecord)
                })
                .map(|_| QueueResponse::default()),
            QueueCommand::PrepareChannel { topic, channel } => match self
                .metadata
                .prepare_channel(topic, channel)
                .map_err(BrokerError::InvalidRecord)?
            {
                Some(_) => Ok(QueueResponse::default()),
                None => Ok(command_error("channel is being deleted")),
            },
            QueueCommand::ActivateChannel {
                topic,
                channel,
                generation,
            } => self
                .metadata
                .activate_channel(topic, channel, *generation)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::PrepareDeleteChannel { topic, channel } => match self
                .metadata
                .prepare_delete_channel(topic, channel)
                .map_err(BrokerError::InvalidRecord)?
            {
                Some(_) => Ok(QueueResponse::default()),
                None => Ok(command_error("channel not found")),
            },
            QueueCommand::CompleteDeleteChannel {
                topic,
                channel,
                generation,
            } => self
                .metadata
                .complete_delete_channel(topic, channel, *generation)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::UpdatePartitionReplicas { group_id, replicas } => self
                .metadata
                .update_partition_replicas(*group_id, replicas.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RegisterNode { descriptor } => self
                .metadata
                .register_node(descriptor.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SetNodeDrained { node_id, drained } => self
                .metadata
                .set_node_drained(*node_id, *drained)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ReservePartitionExpansion {
                topic,
                target_partitions,
                max_partitions,
                now_ms,
            } => self
                .metadata
                .reserve_partition_expansion(topic, *target_partitions, *max_partitions, *now_ms)
                .map(|operation| QueueResponse {
                    message_ids: vec![operation.id],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::AdvancePartitionExpansion {
                operation_id,
                phase,
                state,
                now_ms,
                error,
            } => self
                .metadata
                .advance_partition_expansion(*operation_id, *phase, *state, *now_ms, error.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ActivatePartitionExpansion {
                operation_id,
                expected_channel_revision,
                now_ms,
            } => self
                .metadata
                .activate_partition_expansion(*operation_id, *expected_channel_revision, *now_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::CancelPartitionExpansion {
                operation_id,
                now_ms,
            } => self
                .metadata
                .cancel_partition_expansion(*operation_id, *now_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SetOperationPaused {
                operation_id,
                paused,
            } => self
                .metadata
                .set_operation_paused(*operation_id, *paused)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::SetAutomationEnabled { enabled } => {
                self.metadata.set_automation_enabled(*enabled);
                Ok(QueueResponse::default())
            }
            QueueCommand::SetMaintenance { node_id, lease } => self
                .metadata
                .set_maintenance(*node_id, lease.clone())
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::CreateOperation {
                kind,
                now_ms,
                history_limit,
            } => self
                .metadata
                .create_operation(kind.clone(), *now_ms, *history_limit)
                .map(|operation| QueueResponse {
                    message_ids: vec![operation.id],
                    error: None,
                    results: Vec::new(),
                })
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::UpdateOperation {
                operation_id,
                phase,
                state,
                now_ms,
                error,
                progress,
            } => self
                .metadata
                .update_operation(
                    *operation_id,
                    *phase,
                    *state,
                    *now_ms,
                    error.clone(),
                    progress.clone(),
                )
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ObserveNodeHealth {
                node_id,
                healthy,
                disk_used_percent,
                disk_free_bytes,
                storage_eligible,
                now_ms,
            } => self
                .metadata
                .observe_node_health(
                    *node_id,
                    *healthy,
                    *disk_used_percent,
                    *disk_free_bytes,
                    *storage_eligible,
                    *now_ms,
                )
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RenewEphemeralLease {
                topic,
                channel,
                lease_id,
                expires_at_ms,
            } => self
                .metadata
                .renew_ephemeral_lease(topic, channel, *lease_id, *expires_at_ms)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::ReleaseEphemeralLease {
                topic,
                channel,
                lease_id,
            } => self
                .metadata
                .release_ephemeral_lease(topic, channel, *lease_id)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::Finish {
                topic,
                channel,
                message_id,
            } => self
                .broker
                .commit_finish(topic, channel, *message_id)
                .map(|_| QueueResponse::default()),
            QueueCommand::Requeue {
                topic,
                channel,
                message_id,
                available_at_ms,
            } => self
                .broker
                .commit_requeue_at(topic, channel, *message_id, *available_at_ms)
                .map(|_| QueueResponse::default()),
            QueueCommand::ActivateFeatureLevel { feature_level } => self
                .metadata
                .activate_feature_level(*feature_level)
                .map(|_| QueueResponse::default())
                .map_err(BrokerError::InvalidRecord),
            QueueCommand::RegisterFederationNode { .. }
            | QueueCommand::ApplyRootAction { .. }
            | QueueCommand::BeginPartitionMigration { .. }
            | QueueCommand::AdvancePartitionMigration { .. }
            | QueueCommand::MarkPartitionMigrationNeedsOperator { .. }
            | QueueCommand::ActivateBucketMove { .. }
            | QueueCommand::ActivateScopedFeature { .. }
            | QueueCommand::SyncCatalogTopic { .. }
            | QueueCommand::RemoveCatalogTopic { .. }
            | QueueCommand::PrepareCatalogChannel { .. }
            | QueueCommand::UpdateCatalogChannel { .. }
            | QueueCommand::RemoveCatalogChannel { .. }
            | QueueCommand::RenewCatalogEphemeralLease { .. }
            | QueueCommand::ReleaseCatalogEphemeralLease { .. }
            | QueueCommand::ExpireCatalogEphemeralLeases { .. } => {
                self.apply_metadata_command(command)
            }
            QueueCommand::BeginCatalogTopicDeletion { .. } => self.apply_metadata_command(command),
            QueueCommand::InstallChannelMetadata { .. } => self.apply_metadata_command(command),
            QueueCommand::UpsertFederatedPartition { .. } => self.apply_metadata_command(command),
            QueueCommand::ProtectiveEvict {
                topic,
                partition,
                through_message_id,
                ..
            } => self
                .broker
                .protective_evict_through(topic, *partition, *through_message_id)
                .map(|_| QueueResponse::default()),
        };
        result
    }
}
