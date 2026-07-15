use super::kube_resources;
use crate::crd::RustQueueCluster;
use crate::layout::ClusterLayout;
use crate::pki::{generate_ca, issue_leaf, load_ca, random_hex, CaMaterial};
use crate::resources::{
    self, ANNOTATION_CERT_NOT_AFTER, ANNOTATION_TLS_REVISION, LABEL_CLUSTER, LABEL_COMPONENT,
};
use anyhow::Context;
use k8s_openapi::api::core::v1::Secret;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use std::collections::BTreeMap;

const ANNOTATION_CA_REVISION: &str = "rustqueue.io/ca-revision";

pub struct SecurityState {
    pub ca_revision: u64,
    pub admin_token: String,
    pub tls_revisions: BTreeMap<u64, u64>,
}

pub async fn ensure(
    client: Client,
    namespace: &str,
    cluster: &RustQueueCluster,
    layout: &ClusterLayout,
) -> anyhow::Result<SecurityState> {
    let api = Api::<Secret>::namespaced(client, namespace);
    let ca = ensure_ca(&api, namespace, cluster).await?;
    let (admin_token, discovery_token) = ensure_shared(&api, namespace, cluster, &ca).await?;
    let selector = format!(
        "{LABEL_CLUSTER}={},{}=tls",
        cluster.name_any(),
        LABEL_COMPONENT
    );
    let existing = api
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
        .into_iter()
        .map(|secret| (secret.name_any(), secret))
        .collect::<BTreeMap<_, _>>();
    let mut tls_revisions = BTreeMap::new();
    for broker in layout.brokers() {
        let revision = ensure_leaf(
            &api,
            namespace,
            cluster,
            broker,
            &ca,
            existing.get(&broker.tls_secret),
        )
        .await?;
        tls_revisions.insert(broker.node_id, revision);
    }
    tracing::trace!(
        admin_token_bytes = admin_token.len(),
        discovery_token_bytes = discovery_token.len(),
        "cluster security material is ready"
    );
    Ok(SecurityState {
        ca_revision: ca.revision,
        admin_token,
        tls_revisions,
    })
}

async fn ensure_ca(
    api: &Api<Secret>,
    namespace: &str,
    cluster: &RustQueueCluster,
) -> anyhow::Result<CaMaterial> {
    let name = format!("{}-ca", cluster.name_any());
    if let Some(secret) = api.get_opt(&name).await? {
        let certificate = secret_string(&secret, "ca.crt")?;
        let key = secret_string(&secret, "ca.key")?;
        let ca = load_ca(&certificate, &key)?;
        anyhow::ensure!(
            ca.not_after_unix > crate::status::unix_now(),
            "managed cluster CA has expired"
        );
        return Ok(ca);
    }

    let ca = generate_ca(
        &format!("{} RustQueue CA", cluster.name_any()),
        cluster.spec.security.ca_validity_days,
    )?;
    let secret = resources::secret(
        cluster,
        namespace,
        &name,
        "pki",
        BTreeMap::from([
            ("ca.crt".into(), ca.certificate_pem.clone()),
            ("ca.key".into(), ca.private_key_pem.clone()),
        ]),
        BTreeMap::from([
            (ANNOTATION_CA_REVISION.into(), ca.revision.to_string()),
            (
                ANNOTATION_CERT_NOT_AFTER.into(),
                ca.not_after_unix.to_string(),
            ),
        ]),
    )?;
    kube_resources::apply(api, &secret).await?;
    tracing::info!(revision = ca.revision, "generated managed cluster CA");
    Ok(ca)
}

async fn ensure_shared(
    api: &Api<Secret>,
    namespace: &str,
    cluster: &RustQueueCluster,
    ca: &CaMaterial,
) -> anyhow::Result<(String, String)> {
    let name = format!("{}-shared", cluster.name_any());
    let existing = api.get_opt(&name).await?;
    let admin = existing
        .as_ref()
        .and_then(|secret| secret_string(secret, "admin.token").ok())
        .unwrap_or_else(|| random_hex(32));
    let discovery = existing
        .as_ref()
        .and_then(|secret| secret_string(secret, "discovery.token").ok())
        .unwrap_or_else(|| random_hex(32));
    let current_ca = existing
        .as_ref()
        .and_then(|secret| annotation_u64(secret, ANNOTATION_CA_REVISION));
    if current_ca != Some(ca.revision) {
        let secret = resources::secret(
            cluster,
            namespace,
            &name,
            "pki",
            BTreeMap::from([
                ("ca.crt".into(), ca.certificate_pem.clone()),
                ("admin.token".into(), admin.clone()),
                ("discovery.token".into(), discovery.clone()),
            ]),
            BTreeMap::from([(ANNOTATION_CA_REVISION.into(), ca.revision.to_string())]),
        )?;
        kube_resources::apply(api, &secret).await?;
    }
    Ok((admin, discovery))
}

async fn ensure_leaf(
    api: &Api<Secret>,
    namespace: &str,
    cluster: &RustQueueCluster,
    broker: &crate::layout::BrokerPlan,
    ca: &CaMaterial,
    existing: Option<&Secret>,
) -> anyhow::Result<u64> {
    let renew_at = crate::status::unix_now()
        + i64::from(cluster.spec.security.renew_before_days) * 24 * 60 * 60;
    if let Some(secret) = existing {
        let ca_revision = annotation_u64(secret, ANNOTATION_CA_REVISION);
        let not_after = annotation_i64(secret, ANNOTATION_CERT_NOT_AFTER);
        let tls_revision = annotation_u64(secret, ANNOTATION_TLS_REVISION);
        if ca_revision == Some(ca.revision) && not_after.is_some_and(|value| value > renew_at) {
            if let Some(tls_revision) = tls_revision {
                return Ok(tls_revision);
            }
        }
    }

    let pod = &broker.pod_name;
    let headless = &broker.headless_service;
    let dns_names = vec![
        pod.clone(),
        format!("{pod}.{headless}"),
        format!("{pod}.{headless}.{namespace}.svc"),
        format!("{pod}.{headless}.{namespace}.svc.cluster.local"),
    ];
    let leaf = issue_leaf(
        ca,
        pod,
        &dns_names,
        cluster.spec.security.certificate_validity_days,
    )?;
    let secret = resources::secret(
        cluster,
        namespace,
        &broker.tls_secret,
        "tls",
        BTreeMap::from([
            ("tls.crt".into(), leaf.certificate_pem),
            ("tls.key".into(), leaf.private_key_pem),
        ]),
        BTreeMap::from([
            (ANNOTATION_CA_REVISION.into(), ca.revision.to_string()),
            (ANNOTATION_TLS_REVISION.into(), leaf.revision.to_string()),
            (
                ANNOTATION_CERT_NOT_AFTER.into(),
                leaf.not_after_unix.to_string(),
            ),
        ]),
    )?;
    kube_resources::apply(api, &secret).await?;
    Ok(leaf.revision)
}

fn secret_string(secret: &Secret, key: &str) -> anyhow::Result<String> {
    let bytes = secret
        .data
        .as_ref()
        .and_then(|data| data.get(key))
        .with_context(|| format!("Secret {} lacks {key}", secret.name_any()))?;
    String::from_utf8(bytes.0.clone()).context("Secret data is not UTF-8")
}

fn annotation_u64(secret: &Secret, key: &str) -> Option<u64> {
    secret.metadata.annotations.as_ref()?.get(key)?.parse().ok()
}

fn annotation_i64(secret: &Secret, key: &str) -> Option<i64> {
    secret.metadata.annotations.as_ref()?.get(key)?.parse().ok()
}
