use super::*;
use std::ops::Bound;

impl LogStore {
    pub fn open(directory: impl AsRef<Path>, max_segment_bytes: u64) -> io::Result<Self> {
        Self::open_with_metrics(
            directory,
            max_segment_bytes,
            Arc::new(GroupLatencyMetrics::default()),
        )
    }

    pub(crate) fn open_with_metrics(
        directory: impl AsRef<Path>,
        max_segment_bytes: u64,
        latency: Arc<GroupLatencyMetrics>,
    ) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let vote = read_json_optional(&directory.join("vote.json"))?;
        let last_purged: Option<LogId<NodeId>> =
            read_json_optional(&directory.join("last-purged.json"))?;
        let segments =
            SegmentLog::open_with_start_index(directory.join("log"), max_segment_bytes, 0)
                .map_err(io::Error::other)?;
        let mut entries = BTreeMap::new();
        for location in segments.locations().to_vec() {
            if last_purged.is_some_and(|purged| location.index <= purged.index) {
                continue;
            }
            let record = segments
                .read_location(&location)
                .map_err(io::Error::other)?;
            if record.flags & 1 == 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unpurged Raft log contains a placeholder record",
                ));
            }
            let log_id = entry_codec::decode_log_id(&record.payload)?;
            if log_id.index != record.index || log_id.leader_id.term != record.term {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Raft entry identity does not match segment header",
                ));
            }
            entries.insert(record.index, LogEntryPointer { log_id, location });
        }
        Ok(Self {
            directory: Arc::new(directory),
            inner: Arc::new(BlockingMutex::new(LogStateData {
                vote,
                last_purged,
                entries,
                segments,
                pending_flush: Vec::new(),
                flush_scheduled: false,
                latency,
            })),
        })
    }

    pub async fn recovered_entries(&self) -> io::Result<Vec<Entry<TypeConfig>>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            read_entries(&inner.lock(), false)
                .map(|items| items.into_iter().map(|(entry, _)| entry).collect())
        })
        .await
    }

    pub async fn recovered_entries_with_payloads(
        &self,
    ) -> io::Result<Vec<(Entry<TypeConfig>, Vec<PayloadRef>)>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || read_entries(&inner.lock(), true)).await
    }

    pub async fn payload_refs(&self, index: u64) -> io::Result<Vec<PayloadRef>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let inner = inner.lock();
            let pointer = inner
                .entries
                .get(&index)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Raft entry was purged"))?;
            let record = inner
                .segments
                .read_location(&pointer.location)
                .map_err(io::Error::other)?;
            entry_codec::payload_refs(&record.payload, &pointer.location)
        })
        .await
    }

    pub async fn scrub(&self) -> io::Result<usize> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || inner.lock().segments.scrub().map_err(io::Error::other)).await
    }

    pub async fn seal_payload_segments(&self) -> io::Result<()> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let mut inner = inner.lock();
            flush_locked(&mut inner)?;
            inner.segments.seal().map_err(io::Error::other)
        })
        .await
    }

    pub async fn immutable_segment_descriptor(
        &self,
        path: &Path,
    ) -> io::Result<Option<(u64, u32)>> {
        let inner = Arc::clone(&self.inner);
        let path = path.to_path_buf();
        blocking_io::run(move || Ok(inner.lock().segments.immutable_file(&path))).await
    }

    pub async fn gc_purged_segments(
        &self,
        retained: &std::collections::BTreeSet<PathBuf>,
    ) -> io::Result<usize> {
        let inner = Arc::clone(&self.inner);
        let retained = retained.clone();
        blocking_io::run(move || {
            let mut inner = inner.lock();
            let latency = Arc::clone(&inner.latency);
            let _timer = latency.gc.timer();
            let Some(last_purged) = inner.last_purged else {
                return Ok(0);
            };
            inner
                .segments
                .purge_prefix_retaining(last_purged.index, &retained)
                .map_err(io::Error::other)
        })
        .await
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = owned_bound(range.start_bound());
        let end = owned_bound(range.end_bound());
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let inner = inner.lock();
            inner
                .entries
                .range((start, end))
                .map(|(_, pointer)| {
                    let record = inner
                        .segments
                        .read_location(&pointer.location)
                        .map_err(io::Error::other)?;
                    entry_codec::decode(&record.payload)
                })
                .collect()
        })
        .await
        .map_err(|error| StorageIOError::read_logs(&error).into())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let inner = inner.lock();
            let last_log_id = inner
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.log_id)
                .or(inner.last_purged);
            Ok(LogState {
                last_purged_log_id: inner.last_purged,
                last_log_id,
            })
        })
        .await
        .map_err(|error| StorageIOError::read_logs(&error).into())
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let path = self.directory.join("vote.json");
        let vote = *vote;
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            write_json_atomic(&path, &vote)?;
            inner.lock().vote = Some(vote);
            Ok(())
        })
        .await
        .map_err(|error| StorageIOError::write_vote(&error).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || Ok(inner.lock().vote))
            .await
            .map_err(|error| StorageIOError::read_vote(&error).into())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let inner = Arc::clone(&self.inner);
        let schedule = blocking_io::run(move || append_locked(&inner, &entries, callback))
            .await
            .map_err(|error| StorageIOError::write_logs(&error))?;
        if schedule {
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                let _ = flush_pending_logs(inner).await;
            });
        }
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let mut inner = inner.lock();
            flush_locked(&mut inner)?;
            inner
                .segments
                .truncate_suffix(log_id.index)
                .map_err(io::Error::other)?;
            inner.entries.retain(|index, _| *index < log_id.index);
            Ok(())
        })
        .await
        .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let path = self.directory.join("last-purged.json");
        let inner = Arc::clone(&self.inner);
        blocking_io::run(move || {
            let mut inner = inner.lock();
            flush_locked(&mut inner)?;
            write_json_atomic(&path, &log_id)?;
            inner.last_purged = Some(log_id);
            inner.entries = inner.entries.split_off(&(log_id.index + 1));
            Ok(())
        })
        .await
        .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

fn read_entries(
    inner: &LogStateData,
    with_payloads: bool,
) -> io::Result<Vec<(Entry<TypeConfig>, Vec<PayloadRef>)>> {
    inner
        .entries
        .values()
        .map(|pointer| {
            let record = inner
                .segments
                .read_location(&pointer.location)
                .map_err(io::Error::other)?;
            let entry = if with_payloads {
                entry_codec::decode_without_bodies(&record.payload)?
            } else {
                entry_codec::decode(&record.payload)?
            };
            let payloads = if with_payloads {
                entry_codec::payload_refs(&record.payload, &pointer.location)?
            } else {
                Vec::new()
            };
            Ok((entry, payloads))
        })
        .collect()
}

fn append_locked(
    shared: &BlockingMutex<LogStateData>,
    entries: &[Entry<TypeConfig>],
    callback: LogFlushed<TypeConfig>,
) -> io::Result<bool> {
    let mut inner = shared.lock();
    if let Some(first) = entries.first() {
        let expected = inner.segments.next_index();
        if first.log_id.index > expected
            && inner
                .last_purged
                .is_some_and(|purged| purged.index >= first.log_id.index - 1)
        {
            for index in expected..first.log_id.index {
                inner
                    .segments
                    .append_at(
                        Record {
                            kind: RecordKind::Noop,
                            flags: 1,
                            term: 0,
                            index,
                            timestamp_ns: 0,
                            message_id: 0,
                            payload: Vec::new(),
                        },
                        false,
                    )
                    .map_err(io::Error::other)?;
            }
        }
    }
    for entry in entries {
        let encoded = entry_codec::encode(entry)?;
        let kind = match &entry.payload {
            EntryPayload::Blank => RecordKind::Noop,
            EntryPayload::Membership(_) => RecordKind::Membership,
            EntryPayload::Normal(_) => RecordKind::PublishBatch,
        };
        let location = inner
            .segments
            .append_at_with_location(
                Record {
                    kind,
                    flags: 0,
                    term: entry.log_id.leader_id.term,
                    index: entry.log_id.index,
                    timestamp_ns: 0,
                    message_id: 0,
                    payload: encoded.bytes,
                },
                false,
            )
            .map_err(io::Error::other)?;
        inner.entries.insert(
            entry.log_id.index,
            LogEntryPointer {
                log_id: entry.log_id,
                location,
            },
        );
    }
    inner.pending_flush.push(callback);
    let schedule = !inner.flush_scheduled;
    inner.flush_scheduled = true;
    Ok(schedule)
}

async fn flush_pending_logs(inner: Arc<BlockingMutex<LogStateData>>) -> io::Result<()> {
    blocking_io::run(move || flush_locked(&mut inner.lock())).await
}

fn flush_locked(inner: &mut LogStateData) -> io::Result<()> {
    if inner.pending_flush.is_empty() {
        inner.flush_scheduled = false;
        return Ok(());
    }
    let result = {
        let _timer = inner.latency.fsync.timer();
        inner.segments.sync().map_err(io::Error::other)
    };
    let callbacks = std::mem::take(&mut inner.pending_flush);
    inner.flush_scheduled = false;
    match result {
        Ok(()) => {
            for callback in callbacks {
                callback.log_io_completed(Ok(()));
            }
            Ok(())
        }
        Err(error) => {
            let kind = error.kind();
            let message = error.to_string();
            for callback in callbacks {
                callback.log_io_completed(Err(io::Error::new(kind, message.clone())));
            }
            Err(error)
        }
    }
}

fn owned_bound(bound: Bound<&u64>) -> Bound<u64> {
    match bound {
        Bound::Included(value) => Bound::Included(*value),
        Bound::Excluded(value) => Bound::Excluded(*value),
        Bound::Unbounded => Bound::Unbounded,
    }
}
