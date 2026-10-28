use std::fs::File;
use std::io;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::runtime;

pub(crate) struct SyncBackend {
    #[cfg(target_os = "linux")]
    uring: Option<linux::SyncWorker>,
}

impl SyncBackend {
    pub fn new(enabled: bool) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self {
                uring: enabled.then(linux::SyncWorker::new).transpose()?,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            if enabled {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "io_uring requires Linux; set storage.io_uring_enabled = false",
                ));
            }
            Ok(Self {})
        }
    }

    // Callers finish their appends before submitting and retain the commit gate
    // until every sync completes. No ACK or durable-tail advance may precede it.
    pub fn sync_data(&self, files: Vec<File>) -> io::Result<()> {
        // A lone fdatasync has no parallelism to exploit; dispatching it to
        // another worker only adds a round trip to the durable ACK path.
        if files.len() <= 1 {
            return files.into_iter().try_for_each(|file| file.sync_data());
        }
        #[cfg(target_os = "linux")]
        if let Some(worker) = &self.uring {
            return worker.sync_data(files);
        }
        files.into_iter().try_for_each(|file| file.sync_data())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::Write;

    #[cfg(unix)]
    #[test]
    fn failed_sync_batch_returns_an_error_and_drains_other_files() {
        let backend =
            SyncBackend::new(std::env::var_os("RUSTQUEUE_TEST_IO_URING").is_some()).unwrap();
        let mut files = Vec::new();
        for _ in 0..80 {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"durable").unwrap();
            files.push(file);
        }
        files.insert(1, File::open("/dev/null").unwrap());
        assert!(backend.sync_data(files).is_err());
        let mut next_batch = Vec::new();
        for _ in 0..2 {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"next batch").unwrap();
            next_batch.push(file);
        }
        backend.sync_data(next_batch).unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn enabling_io_uring_on_other_platforms_is_an_explicit_error() {
        assert_eq!(
            SyncBackend::new(true).err().unwrap().kind(),
            io::ErrorKind::Unsupported
        );
    }
}
