use super::*;
use bincode::Options;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

const SNAPSHOT_MAGIC: &[u8; 8] = b"RQPROJ06";
const SNAPSHOT_CHUNK_MESSAGES: usize = 4_096;
const MAX_SNAPSHOT_VALUE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
struct SnapshotHeader {
    version: u32,
    topic: String,
    partition: u16,
    slot: u16,
    cell_id: u64,
    group_id: u64,
    wire_incarnation: u32,
    base_sequence: u64,
    next_sequence: u64,
    message_count: u64,
    segments: Vec<SnapshotSegment>,
    channels: BTreeMap<String, SnapshotChannel>,
}

#[derive(Deserialize, Serialize)]
struct SnapshotSegment {
    path: String,
    bytes: u64,
}

#[derive(Deserialize, Serialize)]
struct SnapshotChannel {
    barrier: u64,
    ack_floor: u64,
    acknowledged: Vec<u64>,
    requeued_until: Vec<(u64, i64)>,
    paused: bool,
    ephemeral: bool,
}

#[derive(Deserialize, Serialize)]
struct SnapshotMessage {
    id: u64,
    timestamp_ns: i64,
    available_at_ms: i64,
    log_index: u64,
    batch_ordinal: u32,
    segment: u32,
    offset: u64,
    len: u32,
    crc32c: u32,
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_SNAPSHOT_VALUE_BYTES)
}

impl Broker {
    pub fn write_partition_snapshot(
        &self,
        topic: &str,
        partition_number: u16,
        path: &Path,
        targets: &BTreeMap<PathBuf, (PathBuf, u64)>,
    ) -> Result<(), BrokerError> {
        let partition = self.partition(topic, partition_number)?;
        let (
            slot,
            cell_id,
            group_id,
            wire_incarnation,
            base_sequence,
            next_sequence,
            message_count,
            channels,
        ) = {
            let state = partition.lock();
            let channels = state
                .channels
                .iter()
                .map(|(name, channel)| {
                    (
                        name.clone(),
                        SnapshotChannel {
                            barrier: channel.barrier as u64,
                            ack_floor: channel.ack_floor as u64,
                            acknowledged: channel.acknowledged.iter().copied().collect(),
                            requeued_until: channel
                                .requeued_until
                                .iter()
                                .map(|(id, deadline)| (*id, *deadline))
                                .collect(),
                            paused: channel.paused,
                            ephemeral: channel.ephemeral,
                        },
                    )
                })
                .collect();
            (
                state.slot,
                state.cell_id,
                state.group_id,
                state.wire_incarnation,
                state.base_sequence,
                state.next_sequence,
                state.messages.len(),
                channels,
            )
        };

        let mut source_to_segment = BTreeMap::new();
        let mut segment_names = BTreeSet::new();
        let mut segments = Vec::with_capacity(targets.len());
        for (source, (relative, bytes)) in targets {
            validate_snapshot_path(relative)?;
            let name = relative
                .to_str()
                .ok_or_else(|| invalid_snapshot("snapshot segment path is not UTF-8"))?
                .to_owned();
            if !segment_names.insert(name.clone()) {
                return Err(invalid_snapshot("duplicate snapshot segment target"));
            }
            let index = u32::try_from(segments.len())
                .map_err(|_| invalid_snapshot("too many snapshot segments"))?;
            source_to_segment.insert(source.clone(), index);
            segments.push(SnapshotSegment {
                path: name,
                bytes: *bytes,
            });
        }

        let header = SnapshotHeader {
            version: 6,
            topic: topic.to_owned(),
            partition: partition_number,
            slot,
            cell_id,
            group_id,
            wire_incarnation,
            base_sequence,
            next_sequence,
            message_count: message_count as u64,
            segments,
            channels,
        };
        let mut writer = BufWriter::with_capacity(256 * 1024, File::create(path)?);
        writer.write_all(SNAPSHOT_MAGIC)?;
        codec()
            .serialize_into(&mut writer, &header)
            .map_err(|error| invalid_snapshot(error.to_string()))?;

        for start in (0..message_count).step_by(SNAPSHOT_CHUNK_MESSAGES) {
            let end = start
                .saturating_add(SNAPSHOT_CHUNK_MESSAGES)
                .min(message_count);
            let chunk: Vec<_> = {
                let state = partition.lock();
                ensure_snapshot_boundary(
                    &state,
                    slot,
                    base_sequence,
                    next_sequence,
                    message_count,
                )?;
                state.messages[start..end]
                    .iter()
                    .map(|message| {
                        let segment = source_to_segment
                            .get(message.payload.path.as_ref())
                            .copied()
                            .ok_or_else(|| {
                                invalid_snapshot("message references an unsealed segment")
                            })?;
                        let segment_bytes = header.segments[segment as usize].bytes;
                        let payload_end = message
                            .payload
                            .offset
                            .checked_add(message.payload.len as u64)
                            .ok_or_else(|| invalid_snapshot("payload offset overflow"))?;
                        if payload_end > segment_bytes {
                            return Err(invalid_snapshot(
                                "message reference exceeds snapshot segment",
                            ));
                        }
                        Ok(SnapshotMessage {
                            id: message.id,
                            timestamp_ns: message.timestamp_ns,
                            available_at_ms: message.available_at_ms,
                            log_index: message.log_index,
                            batch_ordinal: message.batch_ordinal,
                            segment,
                            offset: message.payload.offset,
                            len: message.payload.len,
                            crc32c: message.payload.crc32c,
                        })
                    })
                    .collect::<Result<_, BrokerError>>()?
            };
            for message in chunk {
                codec()
                    .serialize_into(&mut writer, &message)
                    .map_err(|error| invalid_snapshot(error.to_string()))?;
            }
        }
        ensure_snapshot_boundary(
            &partition.lock(),
            slot,
            base_sequence,
            next_sequence,
            message_count,
        )?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn import_partition_snapshot(
        &self,
        topic: &str,
        partition_number: u16,
        path: &Path,
        root: &Path,
    ) -> Result<(), BrokerError> {
        let partition = self.partition(topic, partition_number)?;
        let (
            expected_slot,
            expected_cell,
            expected_group,
            expected_incarnation,
            max_messages,
            max_ack_gap,
        ) = {
            let state = partition.lock();
            (
                state.slot,
                state.cell_id,
                state.group_id,
                state.wire_incarnation,
                state.max_backlog_messages,
                state.max_ack_gap,
            )
        };
        let mut reader = BufReader::with_capacity(256 * 1024, File::open(path)?);
        let mut magic = [0; SNAPSHOT_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(invalid_snapshot("invalid partition snapshot magic"));
        }
        let header: SnapshotHeader = codec()
            .deserialize_from(&mut reader)
            .map_err(|error| invalid_snapshot(error.to_string()))?;
        if header.version != 6
            || header.topic != topic
            || header.partition != partition_number
            || header.slot != expected_slot
            || header.cell_id != expected_cell
            || header.group_id != expected_group
            || header.wire_incarnation != expected_incarnation
        {
            return Err(invalid_snapshot("partition snapshot identity mismatch"));
        }
        let message_count = usize::try_from(header.message_count)
            .map_err(|_| invalid_snapshot("snapshot message count is too large"))?;
        if message_count > max_messages
            || header.base_sequence == 0
            || header.next_sequence
                != header
                    .base_sequence
                    .checked_add(header.message_count)
                    .ok_or_else(|| invalid_snapshot("snapshot sequence overflow"))?
            || header.next_sequence > (1u64 << 48)
        {
            return Err(invalid_snapshot("invalid partition snapshot boundary"));
        }

        let mut segment_paths = Vec::with_capacity(header.segments.len());
        let mut segment_names = BTreeSet::new();
        for segment in &header.segments {
            let relative = Path::new(&segment.path);
            validate_snapshot_path(relative)?;
            if !segment_names.insert(segment.path.clone()) {
                return Err(invalid_snapshot("duplicate partition snapshot segment"));
            }
            let resolved = root.join(relative);
            if fs::metadata(&resolved)?.len() != segment.bytes {
                return Err(invalid_snapshot(
                    "partition snapshot segment length mismatch",
                ));
            }
            segment_paths.push((Arc::new(resolved), segment.bytes));
        }

        let mut messages = Vec::new();
        messages
            .try_reserve_exact(message_count)
            .map_err(|_| invalid_snapshot("cannot reserve partition snapshot messages"))?;
        for offset in 0..message_count {
            let message: SnapshotMessage = codec()
                .deserialize_from(&mut reader)
                .map_err(|error| invalid_snapshot(error.to_string()))?;
            let expected_sequence = header.base_sequence + offset as u64;
            if (message.id >> 48) as u16 != header.slot
                || message.id & ((1u64 << 48) - 1) != expected_sequence
            {
                return Err(invalid_snapshot(
                    "snapshot message sequence is not contiguous",
                ));
            }
            let (segment, bytes) = segment_paths
                .get(message.segment as usize)
                .ok_or_else(|| invalid_snapshot("snapshot message segment is missing"))?;
            let payload_end = message
                .offset
                .checked_add(message.len as u64)
                .ok_or_else(|| invalid_snapshot("snapshot payload offset overflow"))?;
            if payload_end > *bytes {
                return Err(invalid_snapshot(
                    "snapshot message exceeds its payload segment",
                ));
            }
            messages.push(StoredMessage {
                id: message.id,
                timestamp_ns: message.timestamp_ns,
                available_at_ms: message.available_at_ms,
                log_index: message.log_index,
                batch_ordinal: message.batch_ordinal,
                payload: rustqueue_storage::PayloadRef {
                    path: Arc::clone(segment),
                    offset: message.offset,
                    len: message.len,
                    crc32c: message.crc32c,
                },
            });
        }
        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(invalid_snapshot("partition snapshot has trailing bytes"));
        }
        let channels = restore_channels(
            header.channels,
            &messages,
            header.slot,
            header.base_sequence,
            max_ack_gap,
        )?;

        let mut state = partition.lock();
        if state.slot != expected_slot {
            return Err(invalid_snapshot(
                "partition slot changed during snapshot import",
            ));
        }
        state.base_sequence = header.base_sequence;
        state.next_sequence = header.next_sequence;
        state.projection_index = messages
            .last()
            .map_or(1, |message| message.log_index.saturating_add(1));
        state.messages = messages;
        state.channels = channels;
        state.dirty = false;
        state.signal_delivery();
        Ok(())
    }

    pub fn retarget_partition_payload_files(
        &self,
        topic: &str,
        partition_number: u16,
        targets: &BTreeMap<PathBuf, (PathBuf, u64)>,
        root: &Path,
    ) -> Result<(), BrokerError> {
        let partition = self.partition(topic, partition_number)?;
        let resolved: BTreeMap<_, _> = targets
            .iter()
            .map(|(source, (relative, bytes))| {
                validate_snapshot_path(relative)?;
                Ok((source.clone(), (Arc::new(root.join(relative)), *bytes)))
            })
            .collect::<Result<_, BrokerError>>()?;
        let message_count = partition.lock().messages.len();
        for start in (0..message_count).step_by(SNAPSHOT_CHUNK_MESSAGES) {
            let end = start
                .saturating_add(SNAPSHOT_CHUNK_MESSAGES)
                .min(message_count);
            let mut state = partition.lock();
            if state.messages.len() != message_count {
                return Err(invalid_snapshot(
                    "partition changed while snapshot paths were retargeted",
                ));
            }
            for message in &mut state.messages[start..end] {
                let (target, bytes) = resolved
                    .get(message.payload.path.as_ref())
                    .ok_or_else(|| invalid_snapshot("snapshot payload target is missing"))?;
                let payload_end = message
                    .payload
                    .offset
                    .checked_add(message.payload.len as u64)
                    .ok_or_else(|| invalid_snapshot("payload offset overflow"))?;
                if payload_end > *bytes {
                    return Err(invalid_snapshot("payload exceeds snapshot target"));
                }
                message.payload.path = Arc::clone(target);
            }
        }
        Ok(())
    }
}

fn restore_channels(
    projected: BTreeMap<String, SnapshotChannel>,
    messages: &[StoredMessage],
    slot: u16,
    base_sequence: u64,
    max_ack_gap: usize,
) -> Result<HashMap<String, ChannelState>, BrokerError> {
    let mut channels = HashMap::with_capacity(projected.len());
    for (name, channel) in projected {
        validate_name(&name).map_err(|_| BrokerError::InvalidChannel)?;
        let barrier = usize::try_from(channel.barrier)
            .map_err(|_| invalid_snapshot("channel barrier is too large"))?;
        let ack_floor = usize::try_from(channel.ack_floor)
            .map_err(|_| invalid_snapshot("channel ACK cursor is too large"))?;
        if barrier > messages.len()
            || ack_floor > messages.len()
            || ack_floor < barrier
            || channel.acknowledged.len() > max_ack_gap
            || channel.requeued_until.len() > max_ack_gap
        {
            return Err(invalid_snapshot("invalid snapshot channel boundary"));
        }
        let acknowledged: BTreeSet<_> = channel.acknowledged.into_iter().collect();
        if acknowledged.len() > max_ack_gap
            || acknowledged.iter().any(|id| {
                snapshot_message_position(*id, slot, messages.len(), base_sequence).is_none_or(
                    |position| {
                        position < ack_floor || position >= ack_floor.saturating_add(max_ack_gap)
                    },
                )
            })
            || channel.requeued_until.iter().any(|(id, _)| {
                snapshot_message_position(*id, slot, messages.len(), base_sequence).is_none()
            })
        {
            return Err(invalid_snapshot(
                "snapshot channel references an unknown message",
            ));
        }
        let mut state = ChannelState::new(barrier, channel.ephemeral, max_ack_gap);
        state.cursor = ack_floor;
        state.retention_cursor = ack_floor;
        state.ack_floor = ack_floor;
        state.acknowledged = acknowledged.into();
        state.requeued_until = channel
            .requeued_until
            .into_iter()
            .collect::<HashMap<_, _>>()
            .into();
        state.paused = channel.paused;
        channels.insert(name, state);
    }
    Ok(channels)
}

fn snapshot_message_position(
    id: u64,
    slot: u16,
    message_count: usize,
    base_sequence: u64,
) -> Option<usize> {
    if (id >> 48) as u16 != slot {
        return None;
    }
    let sequence = id & ((1u64 << 48) - 1);
    let position = usize::try_from(sequence.checked_sub(base_sequence)?).ok()?;
    (position < message_count).then_some(position)
}

fn ensure_snapshot_boundary(
    state: &Partition,
    slot: u16,
    base_sequence: u64,
    next_sequence: u64,
    message_count: usize,
) -> Result<(), BrokerError> {
    if state.slot != slot
        || state.base_sequence != base_sequence
        || state.next_sequence != next_sequence
        || state.messages.len() != message_count
    {
        return Err(invalid_snapshot(
            "partition changed while its snapshot was being streamed",
        ));
    }
    Ok(())
}

fn validate_snapshot_path(path: &Path) -> Result<(), BrokerError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(invalid_snapshot("unsafe snapshot payload path"));
    }
    Ok(())
}

fn invalid_snapshot(message: impl Into<String>) -> BrokerError {
    BrokerError::InvalidRecord(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn streams_projection_without_embedding_payload_or_message_vector() {
        let root = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: root.path().join("queue"),
            projection_only: true,
            max_backlog_messages_per_partition: 10_000,
            ..BrokerConfig::default()
        })
        .unwrap();
        broker.create_topic("events", Some(1)).unwrap();
        broker.create_channel("events", "workers").unwrap();

        let source = root.path().join("source.rqseg");
        fs::write(&source, b"one-two-three").unwrap();
        let payloads = [
            (0, b"one".as_slice()),
            (4, b"two".as_slice()),
            (8, b"three".as_slice()),
        ]
        .into_iter()
        .map(|(offset, body)| rustqueue_storage::PayloadRef {
            path: Arc::new(source.clone()),
            offset,
            len: body.len() as u32,
            crc32c: crc32c::crc32c(body),
        })
        .collect();
        broker
            .publish_replicated_refs(1, "events", payloads, 10, 0, 0, 7)
            .unwrap();

        let snapshot_root = root.path().join("snapshot");
        fs::create_dir_all(snapshot_root.join("payloads")).unwrap();
        fs::copy(&source, snapshot_root.join("payloads/000000.rqseg")).unwrap();
        let targets = BTreeMap::from([(
            source,
            (
                PathBuf::from("payloads/000000.rqseg"),
                b"one-two-three".len() as u64,
            ),
        )]);
        let projection = snapshot_root.join("partition-projection.bin");
        broker
            .write_partition_snapshot("events", 0, &projection, &targets)
            .unwrap();
        broker.reset_partition_projection("events", 0).unwrap();
        broker
            .import_partition_snapshot("events", 0, &projection, &snapshot_root)
            .unwrap();

        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            3
        );
        let mut cursor = 0;
        let delivery = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.body.as_ref(), b"one");
    }
}
