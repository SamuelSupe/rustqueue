use crate::RustQueue;
use anyhow::{bail, Context};
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::{Resource, ResourceExt};
use rand::distributions::{Alphanumeric, DistString};
use serde_json::json;
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
    let mut secret = match api.get_opt(&name).await? {
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
            string_data.insert(
                "console-token".into(),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
            );
            if cluster.spec.kodo_compatibility.effective_cleanup_enabled() {
                string_data.insert(
                    "kodo-cleanup-token".into(),
                    Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
                );
            }
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
    let missing_console = secret
        .data
        .as_ref()
        .is_none_or(|data| !data.contains_key("console-token"));
    let has_observer = secret
        .data
        .as_ref()
        .is_some_and(|data| data.contains_key("observer-token"));
    let missing_cleanup = secret
        .data
        .as_ref()
        .is_none_or(|data| !data.contains_key("kodo-cleanup-token"));
    let needs_cleanup =
        cluster.spec.kodo_compatibility.effective_cleanup_enabled() && missing_cleanup;
    if generated && (missing_console || needs_cleanup || has_observer) {
        let mut additions = BTreeMap::new();
        if missing_console {
            additions.insert(
                "console-token",
                Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
            );
        }
        if needs_cleanup {
            additions.insert(
                "kodo-cleanup-token",
                Alphanumeric.sample_string(&mut rand::thread_rng(), 48),
            );
        }
        let patch = json!({
            "stringData": additions,
            "data": {"observer-token": null},
        });
        secret = api
            .patch(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await?;
    }
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
    let admin_token = read("admin-token")?;
    let registry_token = read("registry-token")?;
    let console_token = read("console-token")?;
    let mut tokens = vec![
        ("admin-token", admin_token.as_str()),
        ("registry-token", registry_token.as_str()),
        ("console-token", console_token.as_str()),
    ];
    let cleanup_token = if cluster.spec.kodo_compatibility.effective_cleanup_enabled() {
        Some(read("kodo-cleanup-token")?)
    } else {
        None
    };
    if let Some(cleanup_token) = cleanup_token.as_deref() {
        tokens.push(("kodo-cleanup-token", cleanup_token));
    }
    validate_distinct_tokens(&tokens)?;
    Ok(AuthSecret {
        name,
        admin_token,
        registry_token,
        revision: secret.resource_version().unwrap_or_default(),
    })
}

fn validate_distinct_tokens(tokens: &[(&str, &str)]) -> anyhow::Result<()> {
    for (index, (left_name, left)) in tokens.iter().enumerate() {
        if let Some((right_name, _)) = tokens[index + 1..].iter().find(|(_, right)| left == right) {
            bail!("auth Secret {left_name} and {right_name} must be distinct");
        }
    }
    Ok(())
}

pub(super) async fn mounted_secret_revision(
    client: &kube::Client,
    cluster: &RustQueue,
    namespace: &str,
    auth: &AuthSecret,
) -> anyhow::Result<String> {
    let Some(name) = cluster.spec.client_tls_secret_name.as_deref() else {
        return Ok(auth.revision.clone());
    };
    let secret = Api::<Secret>::namespaced(client.clone(), namespace)
        .get(name)
        .await
        .with_context(|| format!("read client TLS Secret {name}"))?;
    let data = secret
        .data
        .as_ref()
        .context("client TLS Secret has no data")?;
    for key in ["tls.crt", "tls.key"] {
        if data.get(key).is_none_or(|value| value.0.is_empty()) {
            bail!("client TLS Secret {name} is missing {key}");
        }
    }
    Ok(format!(
        "{}:{}",
        auth.revision,
        secret.resource_version().unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::validate_distinct_tokens;

    #[test]
    fn auth_roles_require_pairwise_distinct_tokens() {
        assert!(validate_distinct_tokens(&[
            ("admin-token", "admin"),
            ("registry-token", "registry"),
            ("console-token", "console"),
            ("kodo-cleanup-token", "cleanup"),
        ])
        .is_ok());
        let error = validate_distinct_tokens(&[
            ("admin-token", "shared"),
            ("registry-token", "registry"),
            ("console-token", "shared"),
        ])
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("admin-token and console-token must be distinct"));
    }
}
