use parking_lot::Mutex;
use rustqueue_storage::PayloadRef;
use rustqueue_telemetry::LatencyHistogram;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use tokio::sync::oneshot;

type ReadFailure = (io::ErrorKind, String);
type ReadResult = Result<Vec<Vec<u8>>, ReadFailure>;

struct ReadJob {
    payloads: Vec<PayloadRef>,
    _lease: PayloadLease,
    response: oneshot::Sender<ReadResult>,
}

pub(crate) struct PayloadLease {
    active_paths: Arc<Mutex<HashMap<PathBuf, usize>>>,
    payloads: Vec<PayloadRef>,
}

const MAX_COALESCED_READ: u64 = 2 * 1024 * 1024;
const MAX_COALESCE_GAP: u64 = 64 * 1024;

struct PayloadCache {
    values: HashMap<PayloadRef, Arc<[u8]>>,
    order: VecDeque<PayloadRef>,
    bytes: usize,
    max_bytes: usize,
}

struct FileCache {
    values: HashMap<PathBuf, Arc<File>>,
    order: VecDeque<PathBuf>,
    max_files: usize,
}

pub(crate) struct PayloadReader {
    cache: Mutex<PayloadCache>,
    sender: SyncSender<ReadJob>,
    active_paths: Arc<Mutex<HashMap<std::path::PathBuf, usize>>>,
    latency: Arc<LatencyHistogram>,
}

impl PayloadReader {
    pub fn new(
        cache_bytes: usize,
        workers: usize,
        queue_depth: usize,
        latency: Arc<LatencyHistogram>,
    ) -> Arc<Self> {
        let (sender, receiver) = sync_channel(queue_depth.max(1));
        let receiver = Arc::new(Mutex::new(receiver));
        let active_paths = Arc::new(Mutex::new(HashMap::new()));
        let files = Arc::new(Mutex::new(FileCache {
            values: HashMap::new(),
            order: VecDeque::new(),
            max_files: 256,
        }));
        let workers = if workers == 0 {
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(16)
        } else {
            workers.min(16)
        }
        .max(1);
        for index in 0..workers {
            let receiver = Arc::clone(&receiver);
            let files = Arc::clone(&files);
            std::thread::Builder::new()
                .name(format!("rustqueue-payload-{index}"))
                .spawn(move || worker(receiver, files))
                .expect("payload reader worker must start");
        }
        Arc::new(Self {
            cache: Mutex::new(PayloadCache {
                values: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                max_bytes: cache_bytes.max(1),
            }),
            sender,
            active_paths,
            latency,
        })
    }

    #[cfg(test)]
    pub async fn read_many(&self, payloads: &[PayloadRef]) -> io::Result<Vec<Arc<[u8]>>> {
        let lease = self.retain(payloads.to_vec());
        self.read_retained(lease).await
    }

    pub fn retain(&self, payloads: Vec<PayloadRef>) -> PayloadLease {
        {
            let mut active = self.active_paths.lock();
            for payload in &payloads {
                *active.entry(payload.path.as_ref().clone()).or_default() += 1;
            }
        }
        PayloadLease {
            active_paths: Arc::clone(&self.active_paths),
            payloads,
        }
    }

    pub async fn read_retained(&self, lease: PayloadLease) -> io::Result<Vec<Arc<[u8]>>> {
        let _timer = self.latency.timer();
        let payloads = lease.payloads.clone();
        let mut output = vec![None; payloads.len()];
        let mut missing = Vec::new();
        {
            let cache = self.cache.lock();
            for (index, payload) in payloads.iter().enumerate() {
                if let Some(body) = cache.values.get(payload).cloned() {
                    output[index] = Some(body);
                } else {
                    missing.push(index);
                }
            }
        }
        if missing.is_empty() {
            return Ok(output.into_iter().flatten().collect());
        }
        let (sender, receiver) = oneshot::channel();
        let job = ReadJob {
            payloads: missing
                .iter()
                .map(|index| payloads[*index].clone())
                .collect(),
            _lease: lease,
            response: sender,
        };
        match self.sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(_job)) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "payload read queue is full",
                ));
            }
            Err(TrySendError::Disconnected(_job)) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "payload readers stopped",
                ));
            }
        }
        let bodies = receiver
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "payload reader stopped"))?
            .map_err(|(kind, message)| io::Error::new(kind, message))?;
        let mut cache = self.cache.lock();
        for ((index, payload), body) in missing
            .into_iter()
            .map(|index| (index, &payloads[index]))
            .zip(bodies)
        {
            let body: Arc<[u8]> = Arc::from(body);
            cache.insert(payload, Arc::clone(&body));
            output[index] = Some(body);
        }
        Ok(output
            .into_iter()
            .map(|body| body.expect("every payload read produced a body"))
            .collect())
    }

    pub fn retained_paths(&self) -> BTreeSet<std::path::PathBuf> {
        self.active_paths.lock().keys().cloned().collect()
    }

    pub fn has_active_under(&self, directory: &Path) -> bool {
        self.active_paths
            .lock()
            .keys()
            .any(|path| path.starts_with(directory))
    }
}

impl Drop for PayloadLease {
    fn drop(&mut self) {
        release_paths(&self.active_paths, &self.payloads);
    }
}

impl PayloadCache {
    fn insert(&mut self, payload: &PayloadRef, body: Arc<[u8]>) {
        if body.len() > self.max_bytes || self.values.contains_key(payload) {
            return;
        }
        while self.bytes.saturating_add(body.len()) > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(removed) = self.values.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
        self.bytes = self.bytes.saturating_add(body.len());
        self.order.push_back(payload.clone());
        self.values.insert(payload.clone(), body);
    }
}

fn worker(receiver: Arc<Mutex<Receiver<ReadJob>>>, files: Arc<Mutex<FileCache>>) {
    loop {
        let job = receiver.lock().recv();
        let Ok(job) = job else { return };
        let result =
            read_payloads(&files, &job.payloads).map_err(|error| (error.kind(), error.to_string()));
        let _ = job.response.send(result);
    }
}

fn read_payloads(files: &Mutex<FileCache>, payloads: &[PayloadRef]) -> io::Result<Vec<Vec<u8>>> {
    let mut groups = HashMap::<Arc<PathBuf>, Vec<usize>>::new();
    for (index, payload) in payloads.iter().enumerate() {
        groups
            .entry(Arc::clone(&payload.path))
            .or_default()
            .push(index);
    }
    let mut output: Vec<Option<Vec<u8>>> = (0..payloads.len()).map(|_| None).collect();
    for (path, mut indices) in groups {
        indices.sort_by_key(|index| payloads[*index].offset);
        let file = cached_file(files, &path)?;
        read_file_group(&file, payloads, &indices, &mut output)?;
    }
    output
        .into_iter()
        .map(|body| body.ok_or_else(|| io::Error::other("payload batch result missing")))
        .collect()
}

#[cfg(unix)]
fn read_file_group(
    file: &File,
    payloads: &[PayloadRef],
    indices: &[usize],
    output: &mut [Option<Vec<u8>>],
) -> io::Result<()> {
    let mut cursor = 0;
    while cursor < indices.len() {
        let start_cursor = cursor;
        let start = payloads[indices[cursor]].offset;
        let mut end = payload_end(&payloads[indices[cursor]])?;
        cursor += 1;
        while cursor < indices.len() {
            let payload = &payloads[indices[cursor]];
            let candidate_end = payload_end(payload)?;
            if payload.offset > end.saturating_add(MAX_COALESCE_GAP)
                || candidate_end.saturating_sub(start) > MAX_COALESCED_READ
            {
                break;
            }
            end = end.max(candidate_end);
            cursor += 1;
        }
        let mut bytes = vec![0; usize::try_from(end - start).map_err(io::Error::other)?];
        file.read_exact_at(&mut bytes, start)?;
        for index in &indices[start_cursor..cursor] {
            let payload = &payloads[*index];
            let from = usize::try_from(payload.offset - start).map_err(io::Error::other)?;
            let to = from + payload.len as usize;
            let body = bytes[from..to].to_vec();
            verify_payload(payload, &body)?;
            output[*index] = Some(body);
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_file_group(
    file: &File,
    payloads: &[PayloadRef],
    indices: &[usize],
    output: &mut [Option<Vec<u8>>],
) -> io::Result<()> {
    for index in indices {
        output[*index] = Some(payloads[*index].read_verified_from(file)?);
    }
    Ok(())
}

fn payload_end(payload: &PayloadRef) -> io::Result<u64> {
    payload
        .offset
        .checked_add(payload.len as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "payload offset overflow"))
}

fn verify_payload(payload: &PayloadRef, body: &[u8]) -> io::Result<()> {
    if crc32c::crc32c(body) == payload.crc32c {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("payload checksum mismatch at {}", payload.path.display()),
    ))
}

fn cached_file(files: &Mutex<FileCache>, path: &Path) -> io::Result<Arc<File>> {
    if let Some(file) = files.lock().values.get(path).cloned() {
        return Ok(file);
    }
    let opened = Arc::new(File::open(path)?);
    let mut files = files.lock();
    if let Some(file) = files.values.get(path).cloned() {
        return Ok(file);
    }
    while files.values.len() >= files.max_files {
        let Some(oldest) = files.order.pop_front() else {
            break;
        };
        files.values.remove(&oldest);
    }
    let path = path.to_path_buf();
    files.order.push_back(path.clone());
    files.values.insert(path, Arc::clone(&opened));
    Ok(opened)
}

fn release_path(active_paths: &Mutex<HashMap<std::path::PathBuf, usize>>, path: &std::path::Path) {
    let mut active = active_paths.lock();
    if let Some(count) = active.get_mut(path) {
        *count -= 1;
        if *count == 0 {
            active.remove(path);
        }
    }
}

fn release_paths(
    active_paths: &Mutex<HashMap<std::path::PathBuf, usize>>,
    payloads: &[PayloadRef],
) {
    for payload in payloads {
        release_path(active_paths, payload.path.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_telemetry::LatencyHistogram;
    use tempfile::tempdir;

    #[test]
    fn payload_lease_keeps_segment_visible_to_gc_until_drop() {
        let root = tempdir().unwrap();
        let path = root.path().join("payload");
        let payload = PayloadRef {
            path: Arc::new(path.clone()),
            offset: 0,
            len: 1,
            crc32c: 0,
        };
        let reader = PayloadReader::new(1, 1, 1, Arc::new(LatencyHistogram::default()));
        let lease = reader.retain(vec![payload]);
        assert!(reader.retained_paths().contains(&path));
        assert!(reader.has_active_under(root.path()));
        drop(lease);
        assert!(!reader.retained_paths().contains(&path));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn segment_fd_cache_avoids_opening_each_payload() {
        let root = tempdir().unwrap();
        let path = root.path().join("payload");
        std::fs::write(&path, b"payload").unwrap();
        let payload = PayloadRef {
            path: Arc::new(path.clone()),
            offset: 0,
            len: 7,
            crc32c: crc32c::crc32c(b"payload"),
        };
        let reader = PayloadReader::new(1, 1, 4, Arc::new(LatencyHistogram::default()));
        assert_eq!(
            &*reader
                .read_many(std::slice::from_ref(&payload))
                .await
                .unwrap()[0],
            b"payload"
        );
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            &*reader
                .read_many(std::slice::from_ref(&payload))
                .await
                .unwrap()[0],
            b"payload"
        );
    }
}
