use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct DiskStatus {
    pub used_percent: u8,
    pub free_bytes: u64,
    pub eligible: bool,
}

pub(super) fn probe(
    path: &Path,
    high_watermark: u8,
    low_watermark: u8,
    min_free_bytes: u64,
    was_eligible: bool,
) -> io::Result<DiskStatus> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "data path contains NUL"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stats` points to
    // writable storage for exactly one libc statvfs value.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success and initialized the output value.
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize.max(1);
    let total = stats.f_blocks.saturating_mul(block_size);
    let free = stats.f_bavail.saturating_mul(block_size);
    let used_percent = if total == 0 {
        100
    } else {
        total
            .saturating_sub(free)
            .saturating_mul(100)
            .div_ceil(total)
            .min(100) as u8
    };
    let eligible = if free < min_free_bytes || used_percent >= high_watermark {
        false
    } else if used_percent < low_watermark {
        true
    } else {
        was_eligible
    };
    Ok(DiskStatus {
        used_percent,
        free_bytes: free,
        eligible,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probes_a_real_filesystem() {
        let status = probe(Path::new("."), 100, 99, 0, true).unwrap();
        assert!(status.free_bytes > 0);
        assert!(status.used_percent <= 100);
    }
}
