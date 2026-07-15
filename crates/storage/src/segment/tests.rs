use super::*;
use crate::RecordKind;
use tempfile::tempdir;

fn record(index: u64, payload: &[u8]) -> Record {
    Record {
        kind: RecordKind::PublishBatch,
        flags: 0,
        term: 1,
        index,
        timestamp_ns: 1,
        message_id: index,
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
fn purges_only_complete_inactive_segments() {
    let directory = tempdir().unwrap();
    let mut log = SegmentLog::open(directory.path(), 100).unwrap();
    for value in 1..=4 {
        log.append(record(0, &[value; 20]), true).unwrap();
    }
    assert_eq!(log.segment_paths().unwrap().len(), 4);
    assert_eq!(log.purge_prefix(2).unwrap(), 2);
    assert_eq!(log.first_index(), Some(3));
    assert_eq!(log.segment_paths().unwrap().len(), 2);
    assert!(log.read(2).unwrap().is_none());
    assert_eq!(log.read(3).unwrap().unwrap().payload, vec![3; 20]);
}
