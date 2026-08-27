use std::time::Duration;

pub(super) fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

pub(super) fn remaining_until_ns(deadline_ns: i64) -> Duration {
    let remaining = deadline_ns.saturating_sub(now_ns()).max(0) as u64;
    Duration::from_nanos(remaining)
}
