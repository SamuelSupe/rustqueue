use super::*;
use futures::future::join_all;

const MAX_READ_OPERATIONS: usize = 64;

pub(super) fn worker(
    receiver: Arc<Mutex<Receiver<ReadJob>>>,
    files: Arc<Mutex<FileCache>>,
    storage_healthy: Arc<AtomicBool>,
    ready: SyncSender<io::Result<()>>,
) {
    let runtime = match crate::io_backend::runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    if ready.send(Ok(())).is_err() {
        return;
    }
    runtime.block_on(async {
        loop {
            let job = receiver.lock().recv();
            let Ok(job) = job else { break };
            let result = read_payloads(&files, &job.payloads).await;
            complete_job(job, result, &storage_healthy);
        }
    });
}

async fn read_payloads(
    files: &Mutex<FileCache>,
    payloads: &[PayloadRef],
) -> io::Result<Vec<Vec<u8>>> {
    let mut groups = HashMap::<Arc<PathBuf>, Vec<usize>>::new();
    for (index, payload) in payloads.iter().enumerate() {
        groups
            .entry(Arc::clone(&payload.path))
            .or_default()
            .push(index);
    }
    let mut reads = Vec::new();
    for (path, mut indices) in groups {
        indices.sort_by_key(|index| payloads[*index].offset);
        for range in read_ranges(payloads, &indices)? {
            reads.push((Arc::clone(&path), range));
        }
    }
    let mut output: Vec<Option<Vec<u8>>> = (0..payloads.len()).map(|_| None).collect();
    let mut cursor = 0;
    while cursor < reads.len() {
        let start = cursor;
        let mut bytes = 0;
        while cursor < reads.len() && cursor - start < MAX_READ_OPERATIONS {
            let len = reads[cursor].1.len;
            if cursor > start && bytes + len > MAX_COALESCED_READ as usize {
                break;
            }
            bytes += len;
            cursor += 1;
        }
        // Keep the original workspace bound: at most 2 MiB of coalesced
        // buffers, or one oversized message, in addition to the output bodies.
        read_batch(files, &reads[start..cursor], payloads, &mut output).await?;
    }
    output
        .into_iter()
        .map(|body| body.ok_or_else(|| io::Error::other("payload batch result missing")))
        .collect()
}

async fn read_batch(
    files: &Mutex<FileCache>,
    reads: &[(Arc<PathBuf>, ReadRange)],
    payloads: &[PayloadRef],
    output: &mut [Option<Vec<u8>>],
) -> io::Result<()> {
    if let [(path, range)] = reads {
        // A contiguous cached read already costs one syscall. Avoid duplicating
        // and closing a descriptor plus a ring round trip for this common case.
        let file = cached_file(files, path)?;
        let mut bytes = vec![0; range.len];
        file.read_exact_at(&mut bytes, range.offset)?;
        return collect_payloads(range, bytes, payloads, output);
    }
    let mut handles = Vec::new();
    let result = async {
        let mut file_indices = HashMap::new();
        let mut plan = Vec::new();
        for (path, range) in reads {
            let file_index = match file_indices.get(path) {
                Some(index) => *index,
                None => {
                    let source = cached_file(files, path)?;
                    let index = handles.len();
                    handles.push(tokio_uring::fs::File::from_std(source.try_clone()?));
                    file_indices.insert(Arc::clone(path), index);
                    index
                }
            };
            plan.push((file_index, range));
        }
        let pending = plan.iter().map(|(file, range)| {
            let file = &handles[*file];
            async move {
                let (result, bytes) = file.read_exact_at(vec![0; range.len], range.offset).await;
                (*range, result, bytes)
            }
        });
        // Drain all reads before releasing this job's leases/budget on error.
        for (range, result, bytes) in join_all(pending).await {
            result?;
            collect_payloads(range, bytes, payloads, output)?;
        }
        Ok(())
    }
    .await;
    // At most 64 duplicates are open. Explicit close also lets shared-cache
    // invalidation release deleted segment space, even when readers go idle.
    let mut close_result = Ok(());
    for closed in join_all(handles.into_iter().map(|file| file.close())).await {
        if close_result.is_ok() {
            close_result = closed;
        }
    }
    result.and(close_result)
}
