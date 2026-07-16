use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use kube::api::{Api, PostParams};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const LEASE_NAME: &str = "rustqueue-operator-leader";
const LEASE_SECONDS: u64 = 20;
const RENEW_INTERVAL: Duration = Duration::from_secs(5);
const RENEWED_AT: &str = "rustqueue.io/renewed-at-unix";

pub(super) fn start(client: kube::Client, namespace: String, leader: Arc<AtomicBool>) {
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| format!("operator-{}", std::process::id()));
    tokio::spawn(async move {
        loop {
            match acquire_or_renew(&client, &namespace, &identity).await {
                Ok(acquired) => leader.store(acquired, Ordering::Release),
                Err(error) => {
                    leader.store(false, Ordering::Release);
                    tracing::warn!(%error, "operator leader lease renewal failed");
                }
            }
            tokio::time::sleep(RENEW_INTERVAL).await;
        }
    });
}

async fn acquire_or_renew(
    client: &kube::Client,
    namespace: &str,
    identity: &str,
) -> anyhow::Result<bool> {
    let api = Api::<Lease>::namespaced(client.clone(), namespace);
    let now = unix_seconds();
    let mut lease = match api.get_opt(LEASE_NAME).await? {
        Some(lease) => lease,
        None => {
            let lease = desired_lease(namespace, identity, now);
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
    let renewed = lease
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(RENEWED_AT))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if holder != Some(identity) && !expired(renewed, now) {
        return Ok(false);
    }
    lease
        .spec
        .get_or_insert_with(LeaseSpec::default)
        .holder_identity = Some(identity.into());
    lease
        .spec
        .as_mut()
        .expect("inserted lease spec")
        .lease_duration_seconds = Some(LEASE_SECONDS as i32);
    lease
        .metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(RENEWED_AT.into(), now.to_string());
    match api
        .replace(LEASE_NAME, &PostParams::default(), &lease)
        .await
    {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(error)) if error.code == 409 => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn desired_lease(namespace: &str, identity: &str, now: u64) -> Lease {
    Lease {
        metadata: kube::api::ObjectMeta {
            name: Some(LEASE_NAME.into()),
            namespace: Some(namespace.into()),
            annotations: Some(BTreeMap::from([(RENEWED_AT.into(), now.to_string())])),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(identity.into()),
            lease_duration_seconds: Some(LEASE_SECONDS as i32),
            ..Default::default()
        }),
    }
}

fn expired(renewed: u64, now: u64) -> bool {
    now.saturating_sub(renewed) >= LEASE_SECONDS
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_expiry_is_monotonic_under_clock_underflow() {
        assert!(!expired(100, 90));
        assert!(!expired(100, 119));
        assert!(expired(100, 120));
    }

    #[test]
    fn desired_lease_has_a_holder_and_duration() {
        let lease = desired_lease("queue", "operator-1", 42);
        let spec = lease.spec.unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("operator-1"));
        assert_eq!(spec.lease_duration_seconds, Some(20));
    }
}
