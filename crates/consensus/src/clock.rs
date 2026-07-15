use serde::Serialize;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_OFFSET: Duration = Duration::from_millis(500);
const RECOVERY_WINDOW: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize)]
pub struct ClockStatus {
    pub healthy: bool,
    pub offset_ms: u64,
    pub reason: Option<String>,
}

pub struct ClockGuard {
    state: Mutex<ClockState>,
}

struct ClockState {
    healthy: bool,
    offset_ms: u64,
    reason: Option<String>,
    last_bad: Option<Instant>,
    last_wall_ms: i128,
    last_mono: Instant,
}

impl Default for ClockGuard {
    fn default() -> Self {
        Self {
            state: Mutex::new(ClockState {
                healthy: true,
                offset_ms: 0,
                reason: None,
                last_bad: None,
                last_wall_ms: wall_time_ms() as i128,
                last_mono: Instant::now(),
            }),
        }
    }
}

impl ClockGuard {
    pub fn observe_local(&self) {
        let now = Instant::now();
        let wall = wall_time_ms() as i128;
        let mut state = self.state.lock().expect("clock guard lock poisoned");
        let expected = state.last_wall_ms + now.duration_since(state.last_mono).as_millis() as i128;
        let offset = wall.abs_diff(expected).min(u64::MAX as u128) as u64;
        state.last_wall_ms = wall;
        state.last_mono = now;
        observe(&mut state, offset, "local wall/monotonic clock jump");
    }

    pub fn observe_peer(&self, peer_wall_ms: u64, before_ms: u64, after_ms: u64) {
        let midpoint = before_ms.saturating_add(after_ms.saturating_sub(before_ms) / 2);
        let offset = peer_wall_ms.abs_diff(midpoint);
        let mut state = self.state.lock().expect("clock guard lock poisoned");
        observe(&mut state, offset, "peer clock offset exceeds limit");
    }

    pub fn status(&self) -> ClockStatus {
        let state = self.state.lock().expect("clock guard lock poisoned");
        ClockStatus {
            healthy: state.healthy,
            offset_ms: state.offset_ms,
            reason: state.reason.clone(),
        }
    }

    pub fn ensure_safe(&self) -> Result<(), String> {
        let status = self.status();
        status.healthy.then_some(()).ok_or_else(|| {
            status
                .reason
                .unwrap_or_else(|| "node clock is unsafe".into())
        })
    }
}

fn observe(state: &mut ClockState, offset_ms: u64, reason: &str) {
    state.offset_ms = offset_ms;
    if offset_ms > MAX_OFFSET.as_millis() as u64 {
        state.healthy = false;
        state.last_bad = Some(Instant::now());
        state.reason = Some(reason.to_owned());
        return;
    }
    if !state.healthy
        && state
            .last_bad
            .is_some_and(|last_bad| last_bad.elapsed() >= RECOVERY_WINDOW)
    {
        state.healthy = true;
        state.reason = None;
    }
}

pub fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_drift_fails_closed() {
        let guard = ClockGuard::default();
        let now = wall_time_ms();
        guard.observe_peer(now + 501, now, now);
        assert!(!guard.status().healthy);
        assert!(guard.ensure_safe().is_err());
    }
}
