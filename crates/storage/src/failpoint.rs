#[cfg(feature = "crash-injection")]
use std::fs::{self, File};
#[cfg(feature = "crash-injection")]
use std::io::Write;
#[cfg(feature = "crash-injection")]
use std::path::Path;
#[cfg(feature = "crash-injection")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "crash-injection")]
static TRIGGERED: AtomicBool = AtomicBool::new(false);

/// Stops a dedicated crash-test child at an exact persistence boundary.
/// It is inert unless both test-only environment variables are present.
#[doc(hidden)]
#[cfg(feature = "crash-injection")]
pub fn crash_failpoint(name: &str) {
    if std::env::var("RUSTQUEUE_CRASH_FAILPOINT").ok().as_deref() != Some(name) {
        return;
    }
    let Some(marker) = std::env::var_os("RUSTQUEUE_CRASH_MARKER") else {
        return;
    };
    if TRIGGERED.swap(true, Ordering::AcqRel) {
        return;
    }
    if let Err(error) = write_marker(Path::new(&marker), name) {
        panic!("failed to arm crash failpoint {name}: {error}");
    }
    loop {
        std::thread::park();
    }
}

#[doc(hidden)]
#[cfg(not(feature = "crash-injection"))]
#[inline(always)]
pub fn crash_failpoint(_name: &str) {}

#[cfg(feature = "crash-injection")]
fn write_marker(path: &Path, name: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(name.as_bytes())?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
