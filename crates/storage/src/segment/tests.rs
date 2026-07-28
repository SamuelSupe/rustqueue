use super::*;
use crate::RecordKind;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

fn record(index: u64, payload: &[u8]) -> Record {
    Record {
        kind: RecordKind::PublishBatch,
        flags: 0,
        index,
        timestamp_ns: 1,
        message_id: index,
        available_at_ms: 0,
        payload: payload.to_vec(),
    }
}

#[test]
fn appends_rotates_and_recovers() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    assert_eq!(log.segment_paths().unwrap().len(), 2);
    drop(log);

    let log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!(log.last_index(), Some(2));
    assert_eq!(log.read(2).unwrap().unwrap().payload, vec![2; 20]);
}

#[test]
fn multipart_append_preserves_the_record_wire_format() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 4096).unwrap();
    let header = RecordHeader {
        kind: RecordKind::PublishBatch,
        flags: 0,
        index: 1,
        timestamp_ns: 7,
        message_id: 9,
        available_at_ms: 11,
    };
    let location = log
        .append_parts_at_with_location(header, &[b"one", b"two"], true)
        .unwrap();
    let record = log.read_location(&location).unwrap();
    assert_eq!(record.payload, b"onetwo");
    assert_eq!(record.timestamp_ns, 7);
    assert_eq!(record.message_id, 9);
}

#[test]
fn sealed_segments_recover_from_the_sidecar_index() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    let sealed = log.segment_paths().unwrap()[0].clone();
    log.persist_recovery_index(&sealed, b"queue-metadata".to_vec())
        .unwrap();
    drop(log);

    let log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!(log.recovery_report().indexed_records, 1);
    assert_eq!(log.recovery_report().scanned_records, 1);
    assert_eq!(
        log.load_recovery_metadata(&sealed).unwrap(),
        Some(b"queue-metadata".to_vec())
    );
    assert_eq!(log.read(1).unwrap().unwrap().payload, vec![1; 20]);
}

#[test]
fn storage_usage_includes_recovery_sidecars() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    let sealed = log.segment_paths().unwrap()[0].clone();
    let before = log.storage_usage().1;
    log.persist_recovery_index(&sealed, vec![0x5a; 4096])
        .unwrap();
    assert!(log.storage_usage().1 >= before.saturating_add(4096));
}

#[test]
fn corrupt_sidecar_falls_back_to_a_full_segment_scan() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    let sealed = log.segment_paths().unwrap()[0].clone();
    log.persist_recovery_index(&sealed, Vec::new()).unwrap();
    drop(log);
    std::fs::write(sealed.with_extension("rqidx"), b"corrupt").unwrap();

    let log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!(log.recovery_report().indexed_records, 0);
    assert_eq!(log.recovery_report().scanned_records, 2);
    assert_eq!(log.read(1).unwrap().unwrap().payload, vec![1; 20]);
}

#[test]
fn sidecar_body_is_verified_by_background_scrub_not_startup() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    let sealed = log.segment_paths().unwrap()[0].clone();
    log.persist_recovery_index(&sealed, vec![0x5a; 4096])
        .unwrap();
    drop(log);

    let sidecar = sealed.with_extension("rqidx");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&sidecar)
        .unwrap();
    file.seek(SeekFrom::Start(recovery_index::HEADER_LEN + 24 + 2048))
        .unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);

    let log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!(log.recovery_report().indexed_records, 1);
    assert!(matches!(log.scrub(), Err(StorageError::Corrupt { path, .. }) if path == sidecar));
}

#[test]
fn indexed_cold_corruption_is_isolated_by_scrub() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    log.append(record(0, &[1; 20]), true).unwrap();
    log.append(record(0, &[2; 20]), true).unwrap();
    let sealed = log.segment_paths().unwrap()[0].clone();
    log.persist_recovery_index(&sealed, Vec::new()).unwrap();
    drop(log);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&sealed)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);

    let log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!(log.recovery_report().indexed_records, 1);
    assert!(matches!(log.scrub(), Err(StorageError::Corrupt { .. })));
    let mut log = log;
    assert!(matches!(
        log.append(record(0, b"blocked"), true),
        Err(StorageError::Isolated)
    ));
}

#[test]
fn repairs_partial_tail_only() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 1024).unwrap();
    log.append(record(0, b"complete"), true).unwrap();
    let path = log.current_path.clone();
    drop(log);
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(b"RQW1partial").unwrap();
    drop(file);

    let log = SegmentLog::open(directory.path(), 1024).unwrap();
    assert_eq!(log.recovery_report().records, 1);
    assert!(log.recovery_report().truncated_bytes > 0);
}

#[test]
fn truncates_uncommitted_suffix() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 1024).unwrap();
    for _ in 0..3 {
        log.append(record(0, b"entry"), true).unwrap();
    }
    log.truncate_suffix(2).unwrap();
    assert_eq!(log.last_index(), Some(1));
    log.append(record(0, b"replacement"), true).unwrap();
    assert_eq!(log.last_index(), Some(2));
}

#[test]
fn multi_segment_suffix_truncation_reopens_and_accepts_replacement() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    for value in 1..=5 {
        log.append(record(0, &[value; 20]), true).unwrap();
    }
    assert_eq!(log.segment_paths().unwrap().len(), 5);

    log.truncate_suffix(3).unwrap();
    drop(log);

    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    assert_eq!((log.first_index(), log.last_index()), (Some(1), Some(2)));
    assert_eq!(log.read(2).unwrap().unwrap().payload, vec![2; 20]);
    assert!(log.read(3).unwrap().is_none());
    log.append(record(0, b"replacement"), true).unwrap();
    assert_eq!(log.last_index(), Some(3));
}

#[test]
fn refuses_middle_corruption() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 4096).unwrap();
    log.append(record(0, b"first"), true).unwrap();
    log.append(record(0, b"second"), true).unwrap();
    let path = log.current_path.clone();
    drop(log);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);

    assert!(matches!(
        SegmentLog::open(directory.path(), 4096),
        Err(StorageError::Corrupt { offset: 0, .. })
    ));
}

#[test]
fn refuses_a_checksum_corrupt_complete_tail() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 4096).unwrap();
    log.append(record(0, b"confirmed"), true).unwrap();
    let path = log.current_path.clone();
    drop(log);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);

    assert!(matches!(
        SegmentLog::open(directory.path(), 4096),
        Err(StorageError::Corrupt { offset: 0, .. })
    ));
}

#[test]
fn purges_only_complete_inactive_segments() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    for value in 1..=4 {
        log.append(record(0, &[value; 20]), true).unwrap();
    }
    assert_eq!(log.segment_paths().unwrap().len(), 4);
    let bytes_before = log.storage_usage().1;
    assert_eq!(log.purge_prefix(2).unwrap(), 2);
    assert_eq!(log.first_index(), Some(3));
    assert_eq!(log.segment_paths().unwrap().len(), 2);
    assert!(log.storage_usage().1 < bytes_before);
    assert!(log.read(2).unwrap().is_none());
    assert_eq!(log.read(3).unwrap().unwrap().payload, vec![3; 20]);
}

#[test]
fn cached_log_bounds_survive_empty_reopen_and_truncation() {
    let directory = tempdir().unwrap();
    let log = SegmentLog::open_with_start_index(directory.path(), 1024, 17).unwrap();
    assert_eq!(log.first_index(), None);
    assert_eq!(log.last_index(), None);
    assert_eq!(log.next_index(), 17);
    drop(log);

    let mut log = SegmentLog::open_with_start_index(directory.path(), 1024, 17).unwrap();
    assert_eq!(log.last_index(), None);
    log.append(record(0, b"one"), true).unwrap();
    log.append(record(0, b"two"), true).unwrap();
    assert_eq!((log.first_index(), log.last_index()), (Some(17), Some(18)));
    log.truncate_suffix(18).unwrap();
    assert_eq!((log.first_index(), log.last_index()), (Some(17), Some(17)));
    assert_eq!(log.next_index(), 18);
}

#[test]
fn retained_oldest_segment_blocks_non_contiguous_gc() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    for value in 1..=4 {
        log.append(record(0, &[value; 20]), true).unwrap();
    }
    let paths = log.segment_paths().unwrap();
    let retained = [paths[0].clone()].into_iter().collect();
    assert!(log
        .oldest_inactive_boundary_retaining(&retained)
        .unwrap()
        .is_none());
    assert_eq!(log.purge_prefix_retaining(3, &retained).unwrap(), 0);
    assert_eq!(log.segment_paths().unwrap().len(), 4);
    assert_eq!(log.purge_prefix(3).unwrap(), 3);
    assert_eq!(log.first_index(), Some(4));
}

#[test]
fn malformed_segment_filename_is_rejected() {
    let directory = tempdir().unwrap();
    drop(SegmentLog::open(directory.path(), 1024).unwrap());
    std::fs::write(directory.path().join("segment-invalid.rqlog"), b"").unwrap();

    assert!(matches!(
        SegmentLog::open(directory.path(), 1024),
        Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
}

#[cfg(unix)]
#[test]
fn isolates_log_after_a_mutating_io_failure() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 1024).unwrap();
    log.append(record(0, b"entry"), true).unwrap();
    std::fs::remove_file(&log.current_path).unwrap();

    assert!(matches!(log.truncate_suffix(1), Err(StorageError::Io(_))));
    assert!(matches!(
        log.append(record(0, b"must-not-write"), true),
        Err(StorageError::Isolated)
    ));
}
