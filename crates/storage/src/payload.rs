use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct PayloadRef {
    pub path: Arc<PathBuf>,
    pub offset: u64,
    pub len: u32,
    pub crc32c: u32,
}

impl PayloadRef {
    pub fn read_verified(&self) -> io::Result<Vec<u8>> {
        let file = File::open(self.path.as_ref())?;
        self.read_verified_from(&file)
    }

    pub fn read_verified_from(&self, file: &File) -> io::Result<Vec<u8>> {
        let mut body = vec![0; self.len as usize];
        #[cfg(unix)]
        file.read_exact_at(&mut body, self.offset)?;
        #[cfg(not(unix))]
        {
            let mut file = file.try_clone()?;
            file.seek(SeekFrom::Start(self.offset))?;
            file.read_exact(&mut body)?;
        }
        if crc32c::crc32c(&body) != self.crc32c {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("payload checksum mismatch at {}", self.path.display()),
            ));
        }
        Ok(body)
    }
}
