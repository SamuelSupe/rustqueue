use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::api::{Api, PostParams};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

const LEASE_NAME: &str = "rustqueue-operator-leader";
const LEASE_SECONDS: u64 = 20;
const RENEW_INTERVAL: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn start(
    client: kube::Client,
    namespace: String,
    leader: Arc<AtomicBool>,
) -> (tokio::task::JoinHandle<()>, watch::Receiver<bool>) {
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("operator-{}", std::process::id()));
    let (updates, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _reset = LeaderReset {
            leader: Arc::clone(&leader),
            updates: updates.clone(),
        };
        let mut observer = LeaseObserver::default();
        loop {
            let acquired = match tokio::time::timeout(
                REQUEST_TIMEOUT,
                acquire_or_renew(&client, &namespace, &identity, &mut observer),
            )
            .await
            {
                Ok(Ok(acquired)) => acquired,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "operator leader lease renewal failed");
                    false
                }
                Err(_) => {
                    tracing::warn!("operator leader lease renewal timed out");
                    false
                }
            };
            set_leader(&leader, &updates, acquired);
            tokio::time::sleep(RENEW_INTERVAL).await;
        }
    });
    (task, receiver)
}

fn set_leader(leader: &AtomicBool, updates: &watch::Sender<bool>, value: bool) {
    leader.store(value, Ordering::Release);
    updates.send_replace(value);
}

struct LeaderReset {
    leader: Arc<AtomicBool>,
    updates: watch::Sender<bool>,
}

impl Drop for LeaderReset {
    fn drop(&mut self) {
        set_leader(&self.leader, &self.updates, false);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LeaseFingerprint {
    holder: Option<String>,
    resource_version: Option<String>,
    renew_time: Option<String>,
}

#[derive(Default)]
struct LeaseObserver {
    observed: Option<(LeaseFingerprint, Instant)>,
}

impl LeaseObserver {
    fn takeover_ready(&mut self, lease: &Lease, now: Instant) -> bool {
        let holder = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref());
        if holder.is_none_or(str::is_empty) {
            return true;
        }
        let fingerprint = LeaseFingerprint {
            holder: holder.map(str::to_owned),
            resource_version: lease.metadata.resource_version.clone(),
            renew_time: lease
                .spec
                .as_ref()
                .and_then(|spec| spec.renew_time.as_ref())
                .and_then(|time| serde_json::to_string(time).ok()),
        };
        let duration = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.lease_duration_seconds)
            .unwrap_or(LEASE_SECONDS as i32)
            .max(1) as u64;
        match self.observed.as_mut() {
            Some((observed, since)) if observed == &fingerprint => {
                now.saturating_duration_since(*since) >= Duration::from_secs(duration)
            }
            _ => {
                self.observed = Some((fingerprint, now));
                false
            }
        }
    }
}

async fn acquire_or_renew(
    client: &kube::Client,
    namespace: &str,
    identity: &str,
    observer: &mut LeaseObserver,
) -> anyhow::Result<bool> {
    let api = Api::<Lease>::namespaced(client.clone(), namespace);
    let mut lease = match api.get_opt(LEASE_NAME).await? {
        Some(lease) => lease,
        None => {
            let lease = desired_lease(namespace, identity);
            return match api.create(&PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(error)) if error.code == 409 => Ok(false),
                Err(error) => Err(error.into()),
            };
        }
    };
    let holder = lease
        .spec
        .as_ref()
        .and_then(|spec| spec.holder_identity.as_deref());
    let renewing = holder == Some(identity);
    if !renewing && !observer.takeover_ready(&lease, Instant::now()) {
        return Ok(false);
    }

    let now = current_micro_time();
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    if !renewing {
        spec.acquire_time = Some(now.clone());
        spec.lease_transitions = Some(spec.lease_transitions.unwrap_or_default().saturating_add(1));
    }
    spec.holder_identity = Some(identity.into());
    spec.lease_duration_seconds = Some(LEASE_SECONDS as i32);
    spec.renew_time = Some(now);
    match api
        .replace(LEASE_NAME, &PostParams::default(), &lease)
        .await
    {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(error)) if error.code == 409 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn desired_lease(namespace: &str, identity: &str) -> Lease {
    let now = current_micro_time();
    Lease {
        metadata: kube::api::ObjectMeta {
            name: Some(LEASE_NAME.into()),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            acquire_time: Some(now.clone()),
            holder_identity: Some(identity.into()),
            lease_duration_seconds: Some(LEASE_SECONDS as i32),
            lease_transitions: Some(0),
            renew_time: Some(now),
            ..Default::default()
        }),
    }
}

fn current_micro_time() -> MicroTime {
    MicroTime(k8s_openapi::jiff::Timestamp::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takeover_uses_local_monotonic_observation_time() {
        let mut lease = desired_lease("queue", "operator-1");
        lease.metadata.resource_version = Some("7".into());
        let start = Instant::now();
        let mut observer = LeaseObserver::default();
        assert!(!observer.takeover_ready(&lease, start));
        assert!(!observer.takeover_ready(&lease, start + Duration::from_secs(19)));
        assert!(observer.takeover_ready(&lease, start + Duration::from_secs(20)));

        lease.metadata.resource_version = Some("8".into());
        assert!(!observer.takeover_ready(&lease, start + Duration::from_secs(21)));
    }

    #[test]
    fn desired_lease_uses_standard_holder_duration_and_timestamps() {
        let lease = desired_lease("queue", "operator-1");
        assert!(lease.metadata.annotations.is_none());
        let spec = lease.spec.unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("operator-1"));
        assert_eq!(spec.lease_duration_seconds, Some(20));
        assert_eq!(spec.lease_transitions, Some(0));
        assert!(spec.acquire_time.is_some());
        assert!(spec.renew_time.is_some());
    }
}
