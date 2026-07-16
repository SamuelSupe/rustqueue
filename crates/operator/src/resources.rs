use crate::RustQueue;
use anyhow::{bail, Context};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{ConfigMap, Service, ServiceAccount};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::{Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const MANAGER: &str = "rustqueue-operator";

pub struct ResourceSet {
    pub revision: String,
    pub config: ConfigMap,
    pub service_account: ServiceAccount,
    pub role: Role,
    pub role_binding: RoleBinding,
    pub broker_service: Service,
    pub brokers: StatefulSet,
    pub discovery_service: Service,
    pub discovery: Deployment,
    pub proxy_service: Service,
    pub proxy: DaemonSet,
    pub network_policy: NetworkPolicy,
}

pub struct BuildInput<'a> {
    pub cluster: &'a RustQueue,
    pub replicas: i32,
    pub secret_name: &'a str,
    pub secret_revision: &'a str,
}

pub fn build(input: BuildInput<'_>) -> anyhow::Result<ResourceSet> {
    let cluster = input.cluster;
    let name = cluster.name_any();
    let namespace = cluster
        .namespace()
        .context("RustQueue must be namespaced")?;
    let owner = cluster
        .controller_owner_ref(&())
        .context("RustQueue owner reference")?;
    let labels = base_labels(&name);
    let metadata = |resource_name: &str, component: &str| {
        json!({
            "name": resource_name, "namespace": namespace,
            "labels": labels_for(&labels, component),
            "ownerReferences": [owner],
        })
    };
    let broker_service_name = format!("{name}-brokers");
    let discovery_name = format!("{name}-discovery");
    let proxy_name = format!("{name}-proxy");
    let service_account_name = format!("{name}-runtime");
    let config_name = format!("{name}-config");
    let config_text = broker_config(cluster, input.secret_name);
    let revision = format!(
        "{:08x}",
        crc32c::crc32c(
            format!(
                "{}\0{}\0{}",
                cluster.spec.image, config_text, input.secret_revision
            )
            .as_bytes(),
        )
    );
    let node_selector = pod_node_selector(&cluster.spec.eligible_node_selector)?;

    let config = typed(json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": metadata(&config_name, "broker"),
        "data": {"rustqueue.toml": config_text},
    }))?;
    let service_account = typed(json!({
        "apiVersion": "v1", "kind": "ServiceAccount",
        "metadata": metadata(&service_account_name, "runtime"),
    }))?;
    let role = typed(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
        "metadata": metadata(&service_account_name, "runtime"),
        "rules": [{
            "apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"],
            "verbs": ["get", "list", "watch"]
        }],
    }))?;
    let role_binding = typed(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": metadata(&service_account_name, "runtime"),
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": service_account_name},
        "subjects": [{"kind": "ServiceAccount", "name": service_account_name, "namespace": namespace}],
    }))?;
    let broker_service = typed(json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": metadata(&broker_service_name, "broker"),
        "spec": {
            "clusterIP": "None", "publishNotReadyAddresses": true,
            "selector": labels_for(&labels, "broker"),
            "ports": [
                {"name": "tcp", "port": 4150, "targetPort": "tcp"},
                {"name": "http", "port": 4151, "targetPort": "http"}
            ]
        }
    }))?;

    let mut broker_volumes = vec![
        json!({"name": "config", "configMap": {"name": config_name}}),
        json!({"name": "auth", "secret": {"secretName": input.secret_name}}),
    ];
    let mut broker_mounts = vec![
        json!({"name": "data", "mountPath": "/data"}),
        json!({"name": "config", "mountPath": "/etc/rustqueue", "readOnly": true}),
        json!({"name": "auth", "mountPath": "/run/secrets/rustqueue", "readOnly": true}),
    ];
    if let Some(tls_secret) = &cluster.spec.client_tls_secret_name {
        broker_volumes.push(json!({"name": "client-tls", "secret": {"secretName": tls_secret}}));
        broker_mounts.push(
            json!({"name": "client-tls", "mountPath": "/run/tls/rustqueue", "readOnly": true}),
        );
    }
    let brokers = typed(json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": metadata(&name, "broker"),
        "spec": {
            "serviceName": broker_service_name,
            "replicas": input.replicas,
            "podManagementPolicy": "Parallel",
            "updateStrategy": {"type": "OnDelete"},
            "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain", "whenScaled": "Retain"},
            "selector": {"matchLabels": labels_for(&labels, "broker")},
            "template": {
                "metadata": {
                    "labels": labels_for(&labels, "broker"),
                    "annotations": {"rustqueue.io/revision": revision}
                },
                "spec": {
                    "serviceAccountName": service_account_name,
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "nodeSelector": node_selector,
                    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": labels_for(&labels, "broker")},
                        "topologyKey": "kubernetes.io/hostname"
                    }]}},
                    "containers": [{
                        "name": "broker", "image": cluster.spec.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueued"], "args": ["--config", "/etc/rustqueue/rustqueue.toml"],
                        "ports": [{"name": "tcp", "containerPort": 4150}, {"name": "http", "containerPort": 4151}],
                        "env": [
                            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                            {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                            {"name": "RUSTQUEUE_BROADCAST_ADDRESS", "value": format!("$(POD_NAME).{broker_service_name}.$(POD_NAMESPACE).svc")},
                            {"name": "RUSTQUEUE_DATA_PATH", "value": "/data"}
                        ],
                        "volumeMounts": broker_mounts,
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2, "failureThreshold": 2},
                        "livenessProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 10, "failureThreshold": 3},
                        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}}
                    }],
                    "volumes": broker_volumes
                }
            },
            "volumeClaimTemplates": [{
                "metadata": {"name": "data", "labels": labels_for(&labels, "broker")},
                "spec": {
                    "accessModes": ["ReadWriteOnce"], "storageClassName": cluster.spec.storage_class_name,
                    "resources": {"requests": {"storage": cluster.spec.storage_size}}
                }
            }]
        }
    }))?;

    let discovery_service = typed(service_json(
        metadata(&discovery_name, "discovery"),
        labels_for(&labels, "discovery"),
        vec![json!({"name": "http", "port": 4161, "targetPort": "http"})],
    ))?;
    let discovery = typed(json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": metadata(&discovery_name, "discovery"),
        "spec": {
            "replicas": cluster.spec.discovery_replicas.max(2),
            "selector": {"matchLabels": labels_for(&labels, "discovery")},
            "template": {
                "metadata": {"labels": labels_for(&labels, "discovery"), "annotations": {"rustqueue.io/revision": revision}},
                "spec": {
                    "serviceAccountName": service_account_name,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "discovery", "image": cluster.spec.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueue-discovery"],
                        "ports": [{"name": "http", "containerPort": 4161}],
                        "env": [
                            {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                            {"name": "RUSTQUEUE_BROKER_SERVICE", "value": broker_service_name},
                            {"name": "RUSTQUEUE_REGISTRY_TOKEN_FILE", "value": "/run/secrets/rustqueue/registry-token"}
                        ],
                        "volumeMounts": [{"name": "auth", "mountPath": "/run/secrets/rustqueue", "readOnly": true}],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}
                    }],
                    "volumes": [{"name": "auth", "secret": {"secretName": input.secret_name}}]
                }
            }
        }
    }))?;

    let proxy_service = typed(service_json(
        metadata(&proxy_name, "proxy"),
        labels_for(&labels, "proxy"),
        vec![
            json!({"name": "tcp", "port": 4150, "targetPort": "tcp"}),
            json!({"name": "http", "port": 4151, "targetPort": "http"}),
        ],
    ))?;
    let proxy = typed(json!({
        "apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": metadata(&proxy_name, "proxy"),
        "spec": {
            "selector": {"matchLabels": labels_for(&labels, "proxy")},
            "template": {
                "metadata": {"labels": labels_for(&labels, "proxy"), "annotations": {"rustqueue.io/revision": revision}},
                "spec": {
                    "nodeSelector": cluster.spec.proxy_node_selector,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "proxy", "image": cluster.spec.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueue-proxy"],
                        "ports": [{"name": "tcp", "containerPort": 4150}, {"name": "http", "containerPort": 4151}],
                        "env": [{"name": "RUSTQUEUE_DISCOVERY_URLS", "value": format!("http://{discovery_name}:4161")}],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}
                    }]
                }
            }
        }
    }))?;
    let network_policy = typed(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": metadata(&format!("{name}-internal"), "runtime"),
        "spec": {
            "podSelector": {"matchLabels": {"app.kubernetes.io/instance": name}},
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [{"namespaceSelector": {}}]}]
        }
    }))?;

    Ok(ResourceSet {
        revision,
        config,
        service_account,
        role,
        role_binding,
        broker_service,
        brokers,
        discovery_service,
        discovery,
        proxy_service,
        proxy,
        network_policy,
    })
}

fn broker_config(cluster: &RustQueue, secret_name: &str) -> String {
    let mut output = format!(
        "[storage]\ndata_path = \"/data\"\nfeature_level = {}\nmin_free_bytes = {}\ndisk_high_watermark_percent = {}\ndisk_low_watermark_percent = {}\nprotective_eviction_enabled = {}\ndisk_pressure_grace_seconds = {}\n\n[queue]\nbootstrap_retention_seconds = {}\nmax_message_bytes = {}\nmax_backlog_messages = {}\n\n[security]\nadmin_token_file = \"/run/secrets/rustqueue/admin-token\"\nregistry_token_file = \"/run/secrets/rustqueue/registry-token\"\n# secret: {secret_name}\n",
        cluster.spec.storage_feature_level,
        cluster.spec.min_free_bytes,
        cluster.spec.disk_high_watermark_percent,
        cluster.spec.disk_low_watermark_percent,
        cluster.spec.protective_eviction_enabled,
        cluster.spec.disk_pressure_grace_seconds,
        cluster.spec.bootstrap_retention_seconds,
        cluster.spec.max_message_bytes,
        cluster.spec.max_backlog_messages,
    );
    if cluster.spec.client_tls_secret_name.is_some() {
        output.push_str("\n[security.tls]\ncertificate_file = \"/run/tls/rustqueue/tls.crt\"\nprivate_key_file = \"/run/tls/rustqueue/tls.key\"\nclient_ca_file = \"/run/tls/rustqueue/ca.crt\"\nrequire_client_certificate = false\nrequired = false\n");
    }
    output
}

fn base_labels(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), "rustqueue".into()),
        ("app.kubernetes.io/instance".into(), name.into()),
        ("app.kubernetes.io/managed-by".into(), MANAGER.into()),
    ])
}

fn labels_for(base: &BTreeMap<String, String>, component: &str) -> BTreeMap<String, String> {
    let mut labels = base.clone();
    labels.insert("app.kubernetes.io/component".into(), component.into());
    labels
}

fn pod_node_selector(selector: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let Some((key, value)) = selector.split_once('=') else {
        bail!("eligibleNodeSelector must be one key=value selector");
    };
    if key.trim().is_empty() || value.trim().is_empty() || key.contains(',') || value.contains(',')
    {
        bail!("eligibleNodeSelector must be one key=value selector");
    }
    Ok(BTreeMap::from([(key.trim().into(), value.trim().into())]))
}

fn service_json(metadata: Value, selector: BTreeMap<String, String>, ports: Vec<Value>) -> Value {
    json!({"apiVersion": "v1", "kind": "Service", "metadata": metadata, "spec": {"selector": selector, "ports": ports}})
}

fn typed<T: DeserializeOwned>(value: Value) -> anyhow::Result<T> {
    serde_json::from_value(value).context("build Kubernetes resource")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::RustQueueSpec;
    use kube::api::ObjectMeta;

    fn cluster() -> RustQueue {
        RustQueue {
            metadata: ObjectMeta {
                name: Some("queue".into()),
                namespace: Some("test".into()),
                uid: Some("uid".into()),
                ..Default::default()
            },
            spec: RustQueueSpec {
                image: "rustqueue:test".into(),
                image_pull_policy: "Never".into(),
                min_brokers: 1,
                max_brokers: 500,
                eligible_node_selector: "rustqueue.io/eligible=true".into(),
                storage_class_name: "ssd".into(),
                storage_size: "100Gi".into(),
                storage_feature_level: 1,
                min_free_bytes: 1024,
                disk_high_watermark_percent: 85,
                disk_low_watermark_percent: 75,
                protective_eviction_enabled: true,
                disk_pressure_grace_seconds: 60,
                bootstrap_retention_seconds: 30,
                max_message_bytes: 20 * 1024 * 1024,
                max_backlog_messages: 10_000_000,
                registry_secret_name: None,
                client_tls_secret_name: None,
                proxy_node_selector: BTreeMap::new(),
                discovery_replicas: 2,
            },
            status: None,
        }
    }

    #[test]
    fn renders_one_share_nothing_statefulset_and_retained_pvcs() {
        let resources = build(BuildInput {
            cluster: &cluster(),
            replicas: 3,
            secret_name: "queue-auth",
            secret_revision: "1",
        })
        .unwrap();
        let spec = resources.brokers.spec.unwrap();
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(spec.service_name.as_deref(), Some("queue-brokers"));
        assert_eq!(spec.update_strategy.unwrap().type_, Some("OnDelete".into()));
        assert_eq!(spec.volume_claim_templates.unwrap().len(), 1);
        assert_eq!(
            spec.template
                .spec
                .unwrap()
                .security_context
                .unwrap()
                .fs_group,
            Some(65532)
        );
        assert_eq!(resources.discovery.spec.unwrap().replicas, Some(2));
    }
}
