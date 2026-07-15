use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

pub struct DataDirectoryLock {
    file: File,
}

impl DataDirectoryLock {
    pub fn acquire(directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(".rustqueue.lock"))?;
        // SAFETY: flock only reads the valid file descriptor and does not keep
        // a pointer into Rust-owned memory.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "RustQueue data directory is in use; stop the broker first",
            ));
        }
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        file.sync_all()?;
        Ok(Self { file })
    }
}

impl Drop for DataDirectoryLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains valid until the File field is dropped.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rejects_a_second_owner() {
        let directory = tempdir().unwrap();
        let _first = DataDirectoryLock::acquire(directory.path()).unwrap();
        assert_eq!(
            DataDirectoryLock::acquire(directory.path())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
