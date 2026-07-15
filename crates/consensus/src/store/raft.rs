use super::*;

impl StateMachineStore {
    fn apply_entries_blocking(
        &self,
        mut state: StateMachineData,
        entries: Vec<Entry<TypeConfig>>,
        entry_payloads: Vec<Vec<PayloadRef>>,
    ) -> io::Result<(StateMachineData, Vec<QueueResponse>)> {
        let mut responses = Vec::with_capacity(entries.len());
        let applied_entries = entries.len() as u64;
        let mut broker_batch_dirty = false;
        for (entry, payloads) in entries.into_iter().zip(entry_payloads) {
            let response = match &entry.payload {
                EntryPayload::Blank => QueueResponse::default(),
                EntryPayload::Membership(membership) => {
                    state.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    QueueResponse::default()
                }
                EntryPayload::Normal(envelope) => {
                    self.role.validate_envelope(envelope)?;
                    let command = &envelope.command;
                    match command {
                        QueueCommand::Batch { commands } => match if self.payload_log.is_some() {
                            self.apply_command_with_payloads(command, &payloads, entry.log_id.index)
                        } else {
                            self.apply_batch(commands, false)
                        } {
                            Ok(response) => {
                                broker_batch_dirty = true;
                                response
                            }
                            Err(error) if is_fatal_queue_error(&error) => {
                                let _ = self.broker.finish_replicated_batch();
                                return Err(io::Error::other(error.to_string()));
                            }
                            Err(error) => QueueResponse {
                                message_ids: Vec::new(),
                                error: Some(error.to_string()),
                                results: Vec::new(),
                            },
                        },
                        _ => match if self.payload_log.is_some() {
                            self.apply_command_with_payloads(command, &payloads, entry.log_id.index)
                        } else {
                            self.apply_command(command)
                        } {
                            Ok(response) => response,
                            Err(error) if is_fatal_queue_error(&error) => {
                                return Err(io::Error::other(error.to_string()));
                            }
                            Err(error) => QueueResponse {
                                message_ids: Vec::new(),
                                error: Some(error.to_string()),
                                results: Vec::new(),
                            },
                        },
                    }
                }
            };
            state.last_applied = Some(entry.log_id);
            self.capture_control_state(&mut state);
            responses.push(response);
        }
        if broker_batch_dirty {
            self.broker
                .finish_replicated_batch()
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        let pending = self
            .checkpoint_pending
            .fetch_add(applied_entries, Ordering::AcqRel)
            .saturating_add(applied_entries);
        if pending >= APPLIED_CHECKPOINT_ENTRIES {
            write_applied_state(&self.directory.join("applied.boundary"), &state)?;
            self.checkpoint_pending.store(0, Ordering::Release);
        }
        Ok((state, responses))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let _timer = self.latency.snapshot_build.timer();
        let _operation = self.operation_lock.lock().await;
        let sequence = self.snapshot_index.fetch_add(1, Ordering::Relaxed) + 1;
        let mut state = self.state.read().await.clone();
        let mut files = Vec::new();
        let mut payload_targets = None;
        let mut projection_source = None;
        if let StateMachineRole::Partition { topic, partition } = &self.role {
            let log = self.payload_log.as_ref().ok_or_else(|| {
                StorageIOError::write_snapshot(
                    None,
                    &io::Error::other("partition snapshot has no payload log"),
                )
            })?;
            log.seal_payload_segments()
                .await
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            let broker = Arc::clone(&self.broker);
            let topic_name = topic.clone();
            let topic_for_paths = topic_name.clone();
            let partition = *partition;
            let paths = blocking_io::run(move || {
                broker
                    .compact_partition_projection(&topic_for_paths, partition)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                broker
                    .partition_payload_paths(&topic_for_paths, partition)
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            let prepared = snapshot_files::prepare_payload_files(paths, log, &self.generations)
                .await
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            let source = self
                .directory
                .join(format!(".snapshot-{sequence}.projection.bin"));
            let broker = Arc::clone(&self.broker);
            let targets = prepared.targets;
            let targets_for_write = targets.clone();
            let source_for_write = source.clone();
            blocking_io::run(move || {
                broker
                    .write_partition_snapshot(
                        &topic_name,
                        partition,
                        &source_for_write,
                        &targets_for_write,
                    )
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            files = prepared.files;
            payload_targets = Some(targets);
            projection_source = Some(source);
        }
        self.capture_control_state(&mut state);
        state.commands.clear();
        state.projection = None;
        let snapshot_id = state.last_applied.map_or_else(
            || format!("empty-{sequence}"),
            |log_id| format!("{}-{}-{sequence}", log_id.leader_id, log_id.index),
        );
        let generation = state.last_applied.map_or_else(
            || format!("snapshot-empty-{sequence}"),
            |log_id| {
                format!(
                    "snapshot-{}-{}-{sequence}",
                    log_id.leader_id.term, log_id.index
                )
            },
        );
        let meta = SnapshotMeta {
            last_log_id: state.last_applied,
            last_membership: state.last_membership.clone(),
            snapshot_id,
        };
        let state_source = self
            .directory
            .join(format!(".snapshot-{sequence}.state.bin"));
        let meta_source = self
            .directory
            .join(format!(".snapshot-{sequence}.meta.json"));
        let state_path = self.directory.join("applied.boundary");
        let generations = self.generations.clone();
        let state_for_disk = state;
        let meta_for_disk = meta.clone();
        let state_source_for_disk = state_source.clone();
        let meta_source_for_disk = meta_source.clone();
        let projection_source_for_disk = projection_source.clone();
        let (installed, state) = blocking_io::run(move || {
            write_binary_atomic(&state_source_for_disk, &state_for_disk)?;
            write_json_atomic(&meta_source_for_disk, &meta_for_disk)?;
            files.push(GenerationStore::describe_source(
                &state_source_for_disk,
                "snapshot-state.bin",
            )?);
            if let Some(source) = &projection_source_for_disk {
                files.push(GenerationStore::describe_source(
                    source,
                    "partition-projection.bin",
                )?);
            }
            files.push(GenerationStore::describe_source(
                &meta_source_for_disk,
                "snapshot-meta.json",
            )?);
            write_applied_state(&state_path, &state_for_disk)?;
            let installed = generations.install_linked(
                &generation,
                state_for_disk.last_applied.map_or(0, |log_id| log_id.index),
                &files,
            )?;
            Ok((installed, state_for_disk))
        })
        .await
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        if let (Some(targets), StateMachineRole::Partition { topic, partition }) =
            (payload_targets, &self.role)
        {
            let broker = Arc::clone(&self.broker);
            let installed_for_retarget = installed.clone();
            let topic = topic.clone();
            let partition = *partition;
            let retained = blocking_io::run(move || {
                broker
                    .retarget_partition_payload_files(
                        &topic,
                        partition,
                        &targets,
                        &installed_for_retarget,
                    )
                    .map_err(|error| io::Error::other(error.to_string()))?;
                broker
                    .partition_payload_paths(&topic, partition)
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
            if let Some(log) = &self.payload_log {
                log.gc_purged_segments(&retained).await.map_err(|error| {
                    StorageIOError::write_snapshot(Some(meta.signature()), &error)
                })?;
            }
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            directory: installed.clone(),
        };
        *self.current_snapshot.write().await = Some(stored);
        let mut live = self.state.write().await;
        live.projection = None;
        live.metadata = state.metadata.clone();
        live.commands.clear();
        drop(live);
        let generations = self.generations.clone();
        let installed_for_reader = installed.clone();
        let snapshot = blocking_io::run(move || {
            let _ = fs::remove_file(state_source);
            let _ = fs::remove_file(meta_source);
            if let Some(source) = projection_source {
                let _ = fs::remove_file(source);
            }
            generations.prune_old(2)?;
            crate::SnapshotData::reader(installed_for_reader)
        })
        .await
        .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state = self.state.read().await;
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<QueueResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let _operation = self.operation_lock.lock().await;
        let entries: Vec<_> = entries.into_iter().collect();
        let mut entry_payloads = Vec::with_capacity(entries.len());
        for entry in &entries {
            let payloads = if matches!(entry.payload, EntryPayload::Normal(_)) {
                if let Some(log) = &self.payload_log {
                    log.payload_refs(entry.log_id.index)
                        .await
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            entry_payloads.push(payloads);
        }
        let state = self.state.read().await.clone();
        let store = Arc::clone(self);
        let result =
            blocking_io::run(move || store.apply_entries_blocking(state, entries, entry_payloads))
                .await
                .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let (state, responses) = result;
        *self.state.write().await = state;
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        let sequence = self.snapshot_index.fetch_add(1, Ordering::Relaxed) + 1;
        let incoming = self.directory.join("incoming");
        let incoming_for_create = incoming.clone();
        blocking_io::run(move || fs::create_dir_all(&incoming_for_create))
            .await
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        let path = incoming.join(format!("snapshot-{sequence}.tmp"));
        let snapshot = crate::SnapshotData::receiver(path)
            .await
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        Ok(Box::new(snapshot))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let _timer = self.latency.snapshot_install.timer();
        let _operation = self.operation_lock.lock().await;
        let sequence = self.snapshot_index.fetch_add(1, Ordering::Relaxed) + 1;
        let input = (*snapshot)
            .finish_received()
            .await
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        let generation = meta.last_log_id.map_or_else(
            || format!("installed-empty-{sequence}"),
            |log_id| {
                format!(
                    "installed-{}-{}-{sequence}",
                    log_id.leader_id.term, log_id.index
                )
            },
        );
        let last_applied_index = meta.last_log_id.map_or(0, |log_id| log_id.index);
        let generations = self.generations.clone();
        let (installed, archive, stored_meta, state) = blocking_io::run(move || {
            let (installed, archive) = match input {
                crate::snapshot_data::SnapshotInput::Archive(path) => (
                    generations.install_archive(&generation, last_applied_index, &path)?,
                    Some(path),
                ),
                crate::snapshot_data::SnapshotInput::Generation(path) => (
                    generations.clone_generation(&generation, last_applied_index, path)?,
                    None,
                ),
            };
            let stored_meta: SnapshotMeta<NodeId, BasicNode> =
                read_json_optional(&installed.join("snapshot-meta.json"))?.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "snapshot metadata is missing")
                })?;
            let state = snapshot_files::read_state(&installed)?;
            Ok((installed, archive, stored_meta, state))
        })
        .await
        .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;
        let mut state: StateMachineData = state;
        if stored_meta != *meta
            || state.last_applied != meta.last_log_id
            || state.last_membership != meta.last_membership
        {
            return Err(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot metadata does not match snapshot state",
                ),
            )
            .into());
        }
        let store = Arc::clone(self);
        let installed_for_projection = installed.clone();
        state = blocking_io::run(move || {
            if store.role.carries_cell_metadata() {
                if let Some(metadata) = state.metadata.clone() {
                    store.metadata.replace(metadata).map_err(io::Error::other)?;
                    reconcile_broker_topics(&store.broker, &store.metadata.snapshot())
                        .map_err(|error| io::Error::other(error.to_string()))?;
                }
            } else if store.role.carries_root() {
                if let Some(root) = state.root.clone() {
                    store.metadata.replace_root(root);
                }
            } else if store.role.carries_catalog() {
                if let Some(catalog) = state.catalog.clone() {
                    store.metadata.replace_catalog(catalog);
                }
            }
            if store.broker.projection_only() {
                if let Some(projection) = state.projection.take() {
                    store
                        .broker
                        .import_partition_projection(projection)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                } else if let StateMachineRole::Partition { topic, partition } = &store.role {
                    let projection = installed_for_projection.join("partition-projection.bin");
                    if !projection.exists() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "partition snapshot projection is missing",
                        ));
                    }
                    store
                        .broker
                        .import_partition_snapshot(
                            topic,
                            *partition,
                            &projection,
                            &installed_for_projection,
                        )
                        .map_err(|error| io::Error::other(error.to_string()))?;
                }
            }
            for command in &state.commands {
                store
                    .apply_command(command)
                    .map_err(|error| io::Error::other(error.to_string()))?;
            }
            state.commands.clear();
            state.projection = None;
            write_applied_state(&store.directory.join("applied.boundary"), &state)?;
            Ok(state)
        })
        .await
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            directory: installed,
        };
        *self.state.write().await = state;
        *self.current_snapshot.write().await = Some(stored);
        let generations = self.generations.clone();
        blocking_io::run(move || {
            if let Some(archive) = archive {
                let _ = fs::remove_file(archive);
            }
            generations.prune_old(2).map(|_| ())
        })
        .await
        .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.current_snapshot.read().await.clone();
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let directory = snapshot.directory.clone();
        let signature = snapshot.meta.signature();
        let file = blocking_io::run(move || crate::SnapshotData::reader(directory))
            .await
            .map_err(|error| StorageIOError::read_snapshot(Some(signature), &error))?;
        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(file),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
