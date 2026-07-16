use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskSpace {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: u8,
}

pub fn disk_space(path: &Path) -> io::Result<DiskSpace> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "disk path contains NUL"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stats` points to
    // writable storage for one statvfs result.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success and initialized the output structure.
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize as u128;
    let total = (stats.f_blocks as u128).saturating_mul(block_size);
    let available = (stats.f_bavail as u128).saturating_mul(block_size);
    let used = total.saturating_sub(available);
    let used_percent = if total == 0 {
        100
    } else {
        ((used.saturating_mul(100) / total).min(100)) as u8
    };
    Ok(DiskSpace {
        total_bytes: total.min(u64::MAX as u128) as u64,
        available_bytes: available.min(u64::MAX as u128) as u64,
        used_percent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_sane_space_for_a_real_directory() {
        let root = tempfile::tempdir().unwrap();
        let space = disk_space(root.path()).unwrap();
        assert!(space.total_bytes > 0);
        assert!(space.available_bytes <= space.total_bytes);
        assert!(space.used_percent <= 100);
    }
}
