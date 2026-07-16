use crate::RustQueue;
use anyhow::{bail, Context};
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, PostParams};
use kube::{Resource, ResourceExt};
use rand::distributions::{Alphanumeric, DistString};
use std::collections::BTreeMap;

pub(super) struct AuthSecret {
    pub name: String,
    pub admin_token: String,
    pub registry_token: String,
    pub revision: String,
}

pub(super) async fn ensure(
    client: &kube::Client,
    cluster: &RustQueue,
    namespace: &str,
) -> anyhow::Result<AuthSecret> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let generated = cluster.spec.registry_secret_name.is_none();
    let name = cluster
        .spec
        .registry_secret_name
        .clone()
        .unwrap_or_else(|| format!("{}-auth", cluster.name_any()));
    let secret = match api.get_opt(&name).await? {
        Some(secret) => secret,
        None if generated => {
            let owner = cluster
                .controller_owner_ref(&())
                .context("auth Secret owner")?;
            let mut string_data = BTreeMap::new();
            string_data.insert(
                "admin-token".into(),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
            );
            string_data.insert(
                "registry-token".into(),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
            );
            api.create(
                &PostParams::default(),
                &Secret {
                    metadata: kube::api::ObjectMeta {
                        name: Some(name.clone()),
                        namespace: Some(namespace.into()),
                        owner_references: Some(vec![owner]),
                        ..Default::default()
                    },
                    string_data: Some(string_data),
                    type_: Some("Opaque".into()),
                    ..Default::default()
                },
            )
            .await?
        }
        None => bail!("configured registry Secret {name} does not exist"),
    };
    let data = secret.data.as_ref().context("auth Secret has no data")?;
    let read = |key: &str| -> anyhow::Result<String> {
        let bytes = data
            .get(key)
            .with_context(|| format!("auth Secret is missing {key}"))?;
        let value =
            String::from_utf8(bytes.0.clone()).with_context(|| format!("{key} is not UTF-8"))?;
        if value.trim().is_empty() {
            bail!("auth Secret {key} is empty");
        }
        Ok(value.trim().into())
    };
    Ok(AuthSecret {
        name,
        admin_token: read("admin-token")?,
        registry_token: read("registry-token")?,
        revision: secret.resource_version().unwrap_or_default(),
    })
}
