use super::recovery;
use crate::model::MessageMeta;
use crate::BrokerError;
use parking_lot::{Condvar, Mutex};
use rustqueue_storage::RecoveryMetadataRef;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use tokio::sync::oneshot;

const MIN_CACHE_BYTES: usize = 1024 * std::mem::size_of::<MessageMeta>();

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct PageKey {
    pub(super) segment: PathBuf,
    pub(super) page: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PageRequest {
    pub(super) key: PageKey,
    pub(super) metadata: RecoveryMetadataRef,
    pub(super) first_ordinal: u64,
    pub(super) count: usize,
}

impl PageRequest {
    pub(crate) fn segment_path(&self) -> &Path {
        self.metadata.segment_path()
    }
}

struct ReadJob {
    request: PageRequest,
    response: oneshot::Sender<Result<Vec<MessageMeta>, BrokerError>>,
    _guard: Box<dyn Send>,
}

struct CacheState {
    pages: HashMap<PageKey, Arc<Vec<MessageMeta>>>,
    order: VecDeque<PageKey>,
    bytes: usize,
    active_bytes: usize,
    reserved_bytes: usize,
    max_bytes: usize,
    change_epoch: u64,
}

pub(crate) struct MessageIndexCache {
    state: Mutex<CacheState>,
    changed: Condvar,
    sender: SyncSender<ReadJob>,
}

pub(crate) struct MetadataReservation {
    cache: Arc<MessageIndexCache>,
    remaining_bytes: usize,
}

impl MessageIndexCache {
    pub(crate) fn new(
        cache_bytes: usize,
        workers: usize,
        queue_depth: usize,
        storage_healthy: Arc<AtomicBool>,
    ) -> Arc<Self> {
        let (sender, receiver) = sync_channel(queue_depth.max(1));
        let receiver = Arc::new(Mutex::new(receiver));
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
            let storage_healthy = Arc::clone(&storage_healthy);
            std::thread::Builder::new()
                .name(format!("rustqueue-index-{index}"))
                .spawn(move || index_worker(receiver, storage_healthy))
                .expect("message index reader worker must start");
        }
        Arc::new(Self {
            state: Mutex::new(CacheState {
                pages: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                active_bytes: 0,
                reserved_bytes: 0,
                max_bytes: cache_bytes.max(MIN_CACHE_BYTES),
                change_epoch: 0,
            }),
            changed: Condvar::new(),
            sender,
        })
    }

    pub(crate) async fn load(
        &self,
        request: PageRequest,
        guard: impl Send + 'static,
    ) -> Result<(), BrokerError> {
        if self.state.lock().pages.contains_key(&request.key) {
            return Ok(());
        }
        let (sender, receiver) = oneshot::channel();
        match self.sender.try_send(ReadJob {
            request: request.clone(),
            response: sender,
            _guard: Box::new(guard),
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(BrokerError::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "message index read queue is full",
                )))
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(BrokerError::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "message index readers stopped",
                )))
            }
        }
        let page = receiver.await.map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "message index reader stopped",
            ))
        })??;
        self.insert(request.key, page);
        Ok(())
    }

    pub(super) fn get(&self, key: &PageKey, offset: usize) -> Option<MessageMeta> {
        self.state
            .lock()
            .pages
            .get(key)
            .and_then(|page| page.get(offset))
            .cloned()
    }

    pub(super) fn load_blocking(&self, request: PageRequest) -> Result<(), BrokerError> {
        if self.state.lock().pages.contains_key(&request.key) {
            return Ok(());
        }
        let page = recovery::read_page(&request.metadata, request.first_ordinal, request.count)?;
        self.insert(request.key, page);
        Ok(())
    }

    pub(super) fn invalidate(&self, segment: &Path) {
        let mut state = self.state.lock();
        let keys: Vec<_> = state
            .pages
            .keys()
            .filter(|key| key.segment == segment)
            .cloned()
            .collect();
        for key in keys {
            if let Some(page) = state.pages.remove(&key) {
                state.bytes = state.bytes.saturating_sub(page_bytes(&page));
            }
        }
        state.order.retain(|key| key.segment != segment);
    }

    pub(crate) fn try_reserve(self: &Arc<Self>, messages: usize) -> Option<MetadataReservation> {
        let bytes = messages.saturating_mul(std::mem::size_of::<MessageMeta>());
        let mut state = self.state.lock();
        evict_until_available(&mut state, bytes);
        let used = state
            .bytes
            .saturating_add(state.active_bytes)
            .saturating_add(state.reserved_bytes);
        let active = state.active_bytes.saturating_add(state.reserved_bytes);
        let active_limit = state.max_bytes.saturating_sub(MIN_CACHE_BYTES);
        let oversized_empty = state.active_bytes == 0 && state.reserved_bytes == 0;
        if (used.saturating_add(bytes) > state.max_bytes
            || active.saturating_add(bytes) > active_limit)
            && !oversized_empty
        {
            return None;
        }
        state.reserved_bytes = state.reserved_bytes.saturating_add(bytes);
        Some(MetadataReservation {
            cache: Arc::clone(self),
            remaining_bytes: bytes,
        })
    }

    pub(super) fn add_recovered_active(&self, messages: usize) {
        let bytes = messages.saturating_mul(std::mem::size_of::<MessageMeta>());
        let mut state = self.state.lock();
        evict_until_available(&mut state, bytes);
        state.active_bytes = state.active_bytes.saturating_add(bytes);
    }

    pub(super) fn release_active(&self, messages: usize) {
        let bytes = messages.saturating_mul(std::mem::size_of::<MessageMeta>());
        if bytes == 0 {
            return;
        }
        let mut state = self.state.lock();
        state.active_bytes = state.active_bytes.saturating_sub(bytes);
        mark_changed(&mut state, &self.changed);
    }

    pub(crate) fn change_epoch(&self) -> u64 {
        self.state.lock().change_epoch
    }

    pub(crate) fn wait_for_change(&self, observed: u64) {
        let mut state = self.state.lock();
        while state.change_epoch == observed {
            self.changed.wait(&mut state);
        }
    }

    pub(crate) fn over_budget(&self) -> bool {
        let state = self.state.lock();
        let active = state.active_bytes.saturating_add(state.reserved_bytes);
        active > state.max_bytes.saturating_sub(MIN_CACHE_BYTES)
            || state
                .bytes
                .saturating_add(state.active_bytes)
                .saturating_add(state.reserved_bytes)
                > state.max_bytes
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        let state = self.state.lock();
        state.bytes.saturating_add(state.active_bytes)
    }

    fn insert(&self, key: PageKey, page: Vec<MessageMeta>) {
        let page = Arc::new(page);
        let bytes = page_bytes(&page);
        let mut state = self.state.lock();
        if bytes > state.max_bytes || state.pages.contains_key(&key) {
            return;
        }
        evict_until_available(&mut state, bytes);
        if state
            .bytes
            .saturating_add(state.active_bytes)
            .saturating_add(state.reserved_bytes)
            .saturating_add(bytes)
            > state.max_bytes
        {
            return;
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.order.push_back(key.clone());
        state.pages.insert(key, page);
    }
}

impl MetadataReservation {
    pub(crate) fn consume(&mut self, messages: usize) {
        let bytes = messages.saturating_mul(std::mem::size_of::<MessageMeta>());
        debug_assert!(bytes <= self.remaining_bytes);
        let bytes = bytes.min(self.remaining_bytes);
        let mut state = self.cache.state.lock();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
        state.active_bytes = state.active_bytes.saturating_add(bytes);
        mark_changed(&mut state, &self.cache.changed);
        self.remaining_bytes -= bytes;
    }
}

impl Drop for MetadataReservation {
    fn drop(&mut self) {
        if self.remaining_bytes > 0 {
            let mut state = self.cache.state.lock();
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.remaining_bytes);
            mark_changed(&mut state, &self.cache.changed);
        }
    }
}

fn page_bytes(page: &[MessageMeta]) -> usize {
    page.len()
        .saturating_mul(std::mem::size_of::<MessageMeta>())
}

fn evict_until_available(state: &mut CacheState, additional: usize) {
    while state
        .bytes
        .saturating_add(state.active_bytes)
        .saturating_add(state.reserved_bytes)
        .saturating_add(additional)
        > state.max_bytes
    {
        let Some(oldest) = state.order.pop_front() else {
            break;
        };
        if let Some(removed) = state.pages.remove(&oldest) {
            state.bytes = state.bytes.saturating_sub(page_bytes(&removed));
        }
    }
}

fn mark_changed(state: &mut CacheState, changed: &Condvar) {
    state.change_epoch = state.change_epoch.wrapping_add(1);
    changed.notify_all();
}

fn index_worker(receiver: Arc<Mutex<Receiver<ReadJob>>>, storage_healthy: Arc<AtomicBool>) {
    loop {
        let job = receiver.lock().recv();
        let Ok(job) = job else { return };
        let result = recovery::read_page(
            &job.request.metadata,
            job.request.first_ordinal,
            job.request.count,
        );
        if result.as_ref().is_err_and(|error| {
            matches!(
                error,
                BrokerError::StorageUnavailable
                    | BrokerError::Storage(_)
                    | BrokerError::Io(_)
                    | BrokerError::InvalidRecord(_)
            )
        }) {
            storage_healthy.store(false, Ordering::Release);
        }
        let _ = job.response.send(result);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use rustqueue_storage::{Record, RecordKind, SegmentLog, HEADER_LEN};
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use tempfile::tempdir;

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn cancelled_index_load_keeps_its_path_guard_until_the_worker_finishes() {
        let root = tempdir().unwrap();
        let mut log = SegmentLog::open(root.path(), HEADER_LEN as u64 + 1).unwrap();
        let record = || Record {
            kind: RecordKind::Noop,
            flags: 0,
            index: 0,
            timestamp_ns: 0,
            message_id: 0,
            available_at_ms: 0,
            payload: vec![0],
        };
        log.append(record(), true).unwrap();
        let sealed = log.current_segment_path().to_path_buf();
        log.append(record(), true).unwrap();
        log.persist_recovery_index(
            &sealed,
            vec![0; recovery::HEADER_LEN + recovery::MESSAGE_LEN],
        )
        .unwrap();
        let metadata = log.recovery_metadata_ref(&sealed).unwrap();
        let index_path = sealed.with_extension("rqidx");
        drop(log);
        std::fs::remove_file(&index_path).unwrap();
        let path = CString::new(index_path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);

        let healthy = Arc::new(AtomicBool::new(true));
        let cache = MessageIndexCache::new(MIN_CACHE_BYTES * 2, 1, 1, healthy);
        let request = PageRequest {
            key: PageKey {
                segment: sealed,
                page: 0,
            },
            metadata,
            first_ordinal: 0,
            count: 1,
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let mut loading = Box::pin(cache.load(request, DropProbe(Arc::clone(&dropped))));
        std::future::poll_fn(|context| {
            assert!(
                std::future::Future::poll(loading.as_mut(), context).is_pending(),
                "the FIFO-backed index read must remain pending"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(loading);
        assert!(!dropped.load(Ordering::Acquire));

        drop(
            std::fs::OpenOptions::new()
                .write(true)
                .open(index_path)
                .unwrap(),
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
