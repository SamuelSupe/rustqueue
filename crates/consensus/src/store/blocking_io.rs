use std::io;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

fn permits() -> Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(PERMITS.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(2, usize::from)
            .clamp(2, 16);
        Arc::new(Semaphore::new(workers))
    }))
}

pub(crate) async fn run<T, F>(job: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let permit = permits()
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("storage executor stopped"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        job()
    })
    .await
    .map_err(|error| io::Error::other(format!("storage task failed: {error}")))?
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn runs_blocking_io_off_the_runtime_thread() {
        let runtime_thread = std::thread::current().id();
        let storage_thread = super::run(|| Ok(std::thread::current().id()))
            .await
            .unwrap();
        assert_ne!(runtime_thread, storage_thread);
    }
}
