use futures::stream::{self, FuturesUnordered, StreamExt};
use std::fs::File;
use std::io;
use std::sync::{mpsc, Arc};
use tokio::sync::{mpsc as async_mpsc, Semaphore};

const MAX_SYNC_JOBS: usize = 64;
const MAX_SYNC_OPERATIONS: usize = 64;

struct SyncJob {
    files: Vec<File>,
    reply: mpsc::SyncSender<io::Result<()>>,
}

pub(super) struct SyncWorker {
    sender: async_mpsc::Sender<SyncJob>,
}

pub(crate) fn runtime() -> io::Result<tokio_uring::Runtime> {
    tokio_uring::Runtime::new(&tokio_uring::builder()).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot enable io_uring: {error}; check Linux kernel/container io_uring permissions, or set storage.io_uring_enabled = false"
            ),
        )
    })
}

impl SyncWorker {
    pub fn new() -> io::Result<Self> {
        let (sender, mut receiver) = async_mpsc::channel::<SyncJob>(MAX_SYNC_JOBS);
        let (ready, started) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("rustqueue-uring-sync".into())
            .spawn(move || {
                let runtime = match runtime() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    let permits = Arc::new(Semaphore::new(MAX_SYNC_OPERATIONS));
                    let mut pending = FuturesUnordered::new();
                    let mut closed = false;
                    loop {
                        tokio::select! {
                            job = receiver.recv(), if !closed && pending.len() < MAX_SYNC_JOBS => {
                                match job {
                                    Some(job) => pending.push(sync_job(job, Arc::clone(&permits))),
                                    None => closed = true,
                                }
                            }
                            _ = pending.next(), if !pending.is_empty() => {}
                            else => break,
                        }
                    }
                });
            })?;
        started.recv().map_err(|_| stopped())??;
        Ok(Self { sender })
    }

    pub fn sync_data(&self, files: Vec<File>) -> io::Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.sender
            .blocking_send(SyncJob { files, reply })
            .map_err(|_| stopped())?;
        response.recv().map_err(|_| stopped())?
    }
}

async fn sync_job(job: SyncJob, permits: Arc<Semaphore>) {
    let mut syncs = stream::iter(job.files)
        .map(|file| {
            let permits = Arc::clone(&permits);
            async move {
                let _permit = permits.acquire().await.expect("sync semaphore stays open");
                let file = tokio_uring::fs::File::from_std(file);
                let result = file.sync_data().await;
                let close = file.close().await;
                result.and(close)
            }
        })
        .buffer_unordered(MAX_SYNC_OPERATIONS);
    let mut result = Ok(());
    // Drain even after an error: other files may still be in flight, and the
    // caller must not release its commit gate or report success prematurely.
    while let Some(completed) = syncs.next().await {
        if result.is_ok() {
            result = completed;
        }
    }
    let _ = job.reply.send(result);
}

fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "io_uring sync worker stopped")
}
