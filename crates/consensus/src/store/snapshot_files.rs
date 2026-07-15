use super::*;
use rustqueue_storage::LinkedGenerationFile;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct PreparedPayloadFiles {
    pub files: Vec<LinkedGenerationFile>,
    pub targets: BTreeMap<PathBuf, (PathBuf, u64)>,
}

pub(super) async fn prepare_payload_files(
    paths: BTreeSet<PathBuf>,
    log: &LogStore,
    generations: &GenerationStore,
) -> io::Result<PreparedPayloadFiles> {
    let mut targets = BTreeMap::<PathBuf, (PathBuf, u64)>::new();
    let mut files = Vec::with_capacity(paths.len());
    for (file_index, source) in paths.into_iter().enumerate() {
        let relative = PathBuf::from(format!("payloads/{file_index:06}.rqseg"));
        let linked =
            if let Some((bytes, crc32c)) = log.immutable_segment_descriptor(&source).await? {
                LinkedGenerationFile {
                    source: source.clone(),
                    file: rustqueue_storage::GenerationFile {
                        name: relative.to_string_lossy().into_owned(),
                        bytes,
                        crc32c,
                    },
                }
            } else {
                let generations = generations.clone();
                let source_for_lookup = source.clone();
                let relative_for_lookup = relative.clone();
                blocking_io::run(move || {
                    generations.trusted_generation_file(&source_for_lookup, &relative_for_lookup)
                })
                .await?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "snapshot payload source is not immutable: {}",
                            source.display()
                        ),
                    )
                })?
            };
        targets.insert(source, (relative, linked.file.bytes));
        files.push(linked);
    }
    Ok(PreparedPayloadFiles { files, targets })
}

pub(super) fn read_state(directory: &Path) -> io::Result<StateMachineData> {
    let mut state: StateMachineData =
        match read_binary_optional(&directory.join("snapshot-state.bin"))? {
            Some(state) => state,
            None => {
                read_json_optional(&directory.join("snapshot-state.json"))?.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "snapshot state is missing")
                })?
            }
        };
    resolve_payload_paths(&mut state, directory)?;
    Ok(state)
}

fn resolve_payload_paths(state: &mut StateMachineData, directory: &Path) -> io::Result<()> {
    let Some(projection) = &mut state.projection else {
        return Ok(());
    };
    for message in &mut projection.messages {
        let relative = message.payload.path.as_path();
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot payload path is unsafe",
            ));
        }
        let path = directory.join(relative);
        let end = message
            .payload
            .offset
            .checked_add(message.payload.len as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "payload overflow"))?;
        if fs::metadata(&path)?.len() < end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "snapshot payload reference exceeds the linked segment",
            ));
        }
        message.payload.path = Arc::new(path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_storage::{Record, RecordKind, SegmentLog};
    use tempfile::tempdir;

    #[tokio::test]
    async fn snapshot_reuses_immutable_segment_without_reading_payloads() {
        let root = tempdir().unwrap();
        let mut segments = SegmentLog::open(root.path().join("log/log"), 1024 * 1024).unwrap();
        let entry = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::Publish {
                operation_id: 1,
                topic: "events".into(),
                bodies: vec![bytes::Bytes::from_static(b"encoded-body")],
                timestamp_ns: 0,
                available_at_ms: 0,
                partition: Some(0),
                routing_key: None,
            })),
        };
        let encoded = entry_codec::encode(&entry).unwrap();
        let location = segments
            .append_at_with_location(
                Record {
                    kind: RecordKind::PublishBatch,
                    flags: 0,
                    term: 1,
                    index: 1,
                    timestamp_ns: 0,
                    message_id: 0,
                    payload: encoded.bytes.clone(),
                },
                true,
            )
            .unwrap();
        let source = location.segment.as_ref().clone();
        segments.seal().unwrap();
        let log = LogStore::open(root.path().join("log"), 1024 * 1024).unwrap();
        let generations = GenerationStore::open(root.path().join("snapshots")).unwrap();
        let prepared = prepare_payload_files(BTreeSet::from([source.clone()]), &log, &generations)
            .await
            .unwrap();
        assert_eq!(prepared.files.len(), 1);
        assert_eq!(prepared.files[0].source, source);
        assert_eq!(
            prepared.targets.get(&source).unwrap().0,
            PathBuf::from("payloads/000000.rqseg")
        );
        let installed = generations
            .install_linked("one", 1, &prepared.files)
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(&source).unwrap().ino(),
                fs::metadata(installed.join("payloads/000000.rqseg"))
                    .unwrap()
                    .ino()
            );
        }
    }
}
