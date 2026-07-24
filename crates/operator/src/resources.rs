#[path = "resources/kodo.rs"]
mod kodo;

use crate::RustQueue;
use anyhow::{bail, Context};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{ConfigMap, Service, ServiceAccount};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::{Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MANAGER: &str = "rustqueue-operator";
pub const DISCOVERY_MODE_LABEL: &str = "rustqueue.io/discovery-publisher-mode";

pub struct ResourceSet {
    pub revision: String,
    pub retain_existing_kodo_resources: bool,
    pub config: ConfigMap,
    pub service_account: ServiceAccount,
    pub role: Role,
    pub role_binding: RoleBinding,
    pub broker_service: Service,
    pub brokers: StatefulSet,
    pub broker_pdb: PodDisruptionBudget,
    pub broker_network_policy: NetworkPolicy,
    pub discovery_service: Service,
    pub discovery: Deployment,
    pub discovery_pdb: PodDisruptionBudget,
    pub proxy_service: Service,
    pub proxy: DaemonSet,
    pub kodo_gateway_service: Option<Service>,
    pub kodo_gateway_headless_service: Option<Service>,
    pub kodo_gateway: Option<StatefulSet>,
    pub kodo_gateway_pdb: Option<PodDisruptionBudget>,
    pub kodo_gateway_network_policy: Option<NetworkPolicy>,
    pub network_policy: NetworkPolicy,
}

pub struct BuildInput<'a> {
    pub cluster: &'a RustQueue,
    pub replicas: i32,
    pub kodo_gateway_replicas: i32,
    pub advertise_kodo_gateways: bool,
    pub discovery_service_kodo: Option<bool>,
    pub activate_kodo_cleanup: bool,
    pub retain_existing_kodo_resources: bool,
    pub image: &'a str,
    pub claim_template_size: &'a str,
    pub secret_name: &'a str,
    pub mounted_secret_revision: &'a str,
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
    let kodo_gateway_publish_service_name = format!("{name}-kodo-publish");
    let service_account_name = format!("{name}-runtime");
    let config_name = format!("{name}-config");
    let config_text = broker_config(cluster, input.secret_name);
    let revision_contract = serde_json::to_vec(&json!({
        "image": input.image,
        "imagePullPolicy": cluster.spec.image_pull_policy,
        "config": &config_text,
        "secretName": input.secret_name,
        "mountedSecretRevision": input.mounted_secret_revision,
        "clientTlsSecretName": cluster.spec.client_tls_secret_name,
        "messageIndexCacheBytes": cluster.spec.message_index_cache_bytes,
        "eligibleNodeSelector": cluster.spec.eligible_node_selector,
        "brokerScheduling": &cluster.spec.broker_scheduling,
        "brokerResources": &cluster.spec.broker_resources,
    }))?;
    let revision_digest = Sha256::digest(revision_contract);
    let revision = hex::encode(&revision_digest[..16]);
    let node_selector = pod_node_selector(&cluster.spec.eligible_node_selector)?;
    let broker_resources = workload_resources(&cluster.spec.broker_resources);
    let tolerations: Vec<_> = cluster
        .spec
        .broker_scheduling
        .tolerations
        .iter()
        .map(|item| serde_json::to_value(item).expect("serializable toleration"))
        .collect();

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
                    "terminationGracePeriodSeconds": 45,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "nodeSelector": node_selector,
                    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": labels_for(&labels, "broker")},
                        "topologyKey": cluster.spec.broker_scheduling.topology_key
                    }]}},
                    "priorityClassName": cluster.spec.broker_scheduling.priority_class_name,
                    "tolerations": tolerations,
                    "containers": [{
                        "name": "broker", "image": input.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueued"], "args": ["--config", "/etc/rustqueue/rustqueue.toml"],
                        "ports": [
                            {"name": "tcp", "containerPort": 4150},
                            {"name": "http", "containerPort": 4151},
                            {"name": "kodo-http", "containerPort": 4152}
                        ],
                        "env": [
                            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                            {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                            {"name": "RUSTQUEUE_BROADCAST_ADDRESS", "value": format!("$(POD_NAME).{broker_service_name}.$(POD_NAMESPACE).svc")},
                            {"name": "RUSTQUEUE_DATA_PATH", "value": "/data"},
                            {"name": "RUSTQUEUE_MESSAGE_INDEX_CACHE_BYTES", "value": cluster.spec.message_index_cache_bytes.to_string()}
                        ],
                        "volumeMounts": broker_mounts,
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2, "failureThreshold": 2},
                        "livenessProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 10, "failureThreshold": 3},
                        "resources": broker_resources
                    }],
                    "volumes": broker_volumes
                }
            },
            "volumeClaimTemplates": [{
                "metadata": {"name": "data", "labels": labels_for(&labels, "broker")},
                "spec": {
                    "accessModes": ["ReadWriteOnce"], "storageClassName": cluster.spec.storage_class_name,
                    "resources": {"requests": {"storage": input.claim_template_size}}
                }
            }]
        }
    }))?;
    let broker_pdb = typed(json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": metadata(&format!("{name}-brokers"), "broker"),
        "spec": {
            "minAvailable": (input.replicas - 1).max(1),
            "selector": {"matchLabels": labels_for(&labels, "broker")}
        }
    }))?;

    let discovery_mode = if input.advertise_kodo_gateways {
        "kodo"
    } else {
        "direct"
    };
    let mut discovery_pod_labels = labels_for(&labels, "discovery");
    discovery_pod_labels.insert(DISCOVERY_MODE_LABEL.into(), discovery_mode.into());
    let mut discovery_service_selector = labels_for(&labels, "discovery");
    if let Some(kodo) = input.discovery_service_kodo {
        discovery_service_selector.insert(
            DISCOVERY_MODE_LABEL.into(),
            if kodo { "kodo" } else { "direct" }.into(),
        );
    }
    let discovery_service = typed(service_json(
        metadata(&discovery_name, "discovery"),
        discovery_service_selector,
        vec![json!({"name": "http", "port": 4161, "targetPort": "http"})],
    ))?;
    let mut discovery_env = vec![
        json!({"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}),
        json!({"name": "RUSTQUEUE_BROKER_SERVICE", "value": broker_service_name}),
        json!({"name": "RUSTQUEUE_REGISTRY_TOKEN_FILE", "value": "/run/secrets/rustqueue/registry-token"}),
    ];
    if input.advertise_kodo_gateways {
        let cleanup_enabled = input.activate_kodo_cleanup
            && cluster.spec.kodo_compatibility.effective_cleanup_enabled();
        discovery_env.extend([
            json!({"name": "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED", "value": "true"}),
            json!({"name": "RUSTQUEUE_KODO_CLEANUP_ENABLED", "value": cleanup_enabled.to_string()}),
        ]);
        let address = format!("{kodo_gateway_publish_service_name}.{namespace}.svc");
        discovery_env.push(json!({"name": "RUSTQUEUE_KODO_GATEWAY_ADDRESS", "value": address}));
    }
    let discovery_strategy = json!({
        "type": "RollingUpdate",
        "rollingUpdate": {"maxUnavailable": 0, "maxSurge": "100%"}
    });
    let discovery_min_ready_seconds =
        if cluster.spec.kodo_compatibility.enabled || input.retain_existing_kodo_resources {
            i32::try_from(
                cluster
                    .spec
                    .kodo_compatibility
                    .cutover_grace_seconds
                    .saturating_add(30),
            )
            .context("Kodo cutover grace exceeds the Deployment minReadySeconds range")?
        } else {
            30
        };
    let discovery = typed(json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": metadata(&discovery_name, "discovery"),
        "spec": {
            "replicas": cluster.spec.discovery_replicas.max(2),
            "minReadySeconds": discovery_min_ready_seconds,
            "strategy": discovery_strategy,
            "selector": {"matchLabels": labels_for(&labels, "discovery")},
            "template": {
                "metadata": {
                    "labels": discovery_pod_labels,
                    "annotations": {"rustqueue.io/revision": revision}
                },
                "spec": {
                    "serviceAccountName": service_account_name,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "discovery", "image": input.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueue-discovery"],
                        "ports": [{"name": "http", "containerPort": 4161}],
                        "env": discovery_env,
                        "volumeMounts": [{"name": "auth", "mountPath": "/run/secrets/rustqueue", "readOnly": true}],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2},
                        "livenessProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 10, "failureThreshold": 3},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}
                    }],
                    "volumes": [{
                        "name": "auth",
                        "secret": {
                            "secretName": input.secret_name,
                            "items": [{"key": "registry-token", "path": "registry-token"}]
                        }
                    }]
                }
            }
        }
    }))?;
    let discovery_pdb = typed(json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": metadata(&format!("{discovery_name}-pdb"), "discovery"),
        "spec": {
            "minAvailable": 1,
            "selector": {"matchLabels": labels_for(&labels, "discovery")}
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
                    "terminationGracePeriodSeconds": 45,
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "proxy", "image": input.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["rustqueue-proxy"],
                        "ports": [{"name": "tcp", "containerPort": 4150}, {"name": "http", "containerPort": 4151}],
                        "env": [
                            {"name": "RUSTQUEUE_DISCOVERY_URLS", "value": format!("http://{discovery_name}:4161")},
                            {"name": "RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS", "value": cluster.spec.proxy_tcp_max_connection_age_seconds.to_string()},
                            {"name": "RUSTQUEUE_PROXY_SHUTDOWN_GRACE_SECONDS", "value": "30"}
                        ],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2},
                        "livenessProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 10, "failureThreshold": 3},
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}}
                    }]
                }
            }
        }
    }))?;
    let kodo_resources = (cluster.spec.kodo_compatibility.enabled
        || input.retain_existing_kodo_resources)
        .then(|| {
            kodo::build(kodo::BuildInput {
                cluster,
                replicas: input.kodo_gateway_replicas,
                image: input.image,
                secret_name: input.secret_name,
                revision: &revision,
                name: &name,
                namespace: &namespace,
                owner: owner.clone(),
                labels: &labels,
                discovery_name: &discovery_name,
                service_account_name: &service_account_name,
            })
        })
        .transpose()?;
    let (
        kodo_gateway_service,
        kodo_gateway_headless_service,
        kodo_gateway,
        kodo_gateway_pdb,
        kodo_gateway_network_policy,
    ) = kodo_resources.map_or((None, None, None, None, None), |resources| {
        (
            Some(resources.service),
            Some(resources.headless_service),
            Some(resources.gateway),
            Some(resources.pdb),
            Some(resources.network_policy),
        )
    });
    let mut broker_ingress = if cluster.spec.kodo_compatibility.enabled {
        vec![
            json!({
                "from": [
                    {"podSelector": {"matchLabels": {"app.kubernetes.io/instance": name}}},
                    kodo::allowed_peer(cluster)
                ],
                "ports": [{"protocol": "TCP", "port": 4150}]
            }),
            json!({
                "from": [{"namespaceSelector": {}}],
                "ports": [{"protocol": "TCP", "port": 4151}]
            }),
        ]
    } else {
        vec![json!({
            "from": [{"namespaceSelector": {}}],
            "ports": [
                {"protocol": "TCP", "port": 4150},
                {"protocol": "TCP", "port": 4151}
            ]
        })]
    };
    if cluster.spec.kodo_compatibility.effective_cleanup_enabled() {
        broker_ingress.push(json!({
            "from": [kodo::allowed_peer(cluster)],
            "ports": [{"protocol": "TCP", "port": 4152}]
        }));
    }
    let broker_network_policy = typed(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": metadata(&format!("{name}-broker-ingress"), "broker"),
        "spec": {
            "podSelector": {"matchLabels": labels_for(&labels, "broker")},
            "policyTypes": ["Ingress"],
            "ingress": broker_ingress
        }
    }))?;
    let network_policy = typed(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": metadata(&format!("{name}-internal"), "runtime"),
        "spec": {
            "podSelector": {
                "matchLabels": {"app.kubernetes.io/instance": name},
                "matchExpressions": [{
                    "key": "app.kubernetes.io/component",
                    "operator": "NotIn",
                    "values": ["broker", "kodo-gateway"]
                }]
            },
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [{"namespaceSelector": {}}]}]
        }
    }))?;
    Ok(ResourceSet {
        revision,
        retain_existing_kodo_resources: input.retain_existing_kodo_resources,
        config,
        service_account,
        role,
        role_binding,
        broker_service,
        brokers,
        broker_pdb,
        broker_network_policy,
        discovery_service,
        discovery,
        discovery_pdb,
        proxy_service,
        proxy,
        kodo_gateway_service,
        kodo_gateway_headless_service,
        kodo_gateway,
        kodo_gateway_pdb,
        kodo_gateway_network_policy,
        network_policy,
    })
}

fn broker_config(cluster: &RustQueue, secret_name: &str) -> String {
    let kodo = cluster.spec.kodo_compatibility.enabled;
    let large_messages = cluster.spec.max_message_bytes > 64 * 1024 * 1024;
    let max_segment_bytes = if kodo || large_messages {
        256 * 1024 * 1024
    } else {
        100 * 1024 * 1024
    };
    let max_body_bytes = if kodo {
        128 * 1024 * 1024
    } else {
        (64 * 1024 * 1024).max(cluster.spec.max_message_bytes)
    };
    let connection_publish_inflight_bytes = if kodo || large_messages {
        160 * 1024 * 1024
    } else {
        80 * 1024 * 1024
    };
    let node_publish_inflight_bytes = if kodo || large_messages {
        1024 * 1024 * 1024
    } else {
        512 * 1024 * 1024
    };
    let cleanup_enabled = cluster.spec.kodo_compatibility.effective_cleanup_enabled();
    let kodo_cleanup_token = if cleanup_enabled {
        "kodo_cleanup_token_file = \"/run/secrets/rustqueue/kodo-cleanup-token\"\n"
    } else {
        ""
    };
    let publish_token = if kodo {
        "publish_token_file = \"/run/secrets/rustqueue/admin-token\"\n"
    } else {
        ""
    };
    let kodo_network = if cleanup_enabled {
        "[network]\nkodo_http_address = \"0.0.0.0:4152\"\n\n"
    } else {
        ""
    };
    let mut output = format!(
        "{kodo_network}[storage]\ndata_path = \"/data\"\nfeature_level = {}\nmax_segment_bytes = {max_segment_bytes}\nmin_free_bytes = {}\ndisk_high_watermark_percent = {}\ndisk_low_watermark_percent = {}\nprotective_eviction_enabled = {}\ndisk_pressure_grace_seconds = {}\nmaintenance_startup_delay_seconds = {}\n\n[queue]\nbootstrap_retention_seconds = {}\nmax_message_bytes = {}\nmax_topics = {}\nmax_publish_workers = {}\npublish_worker_idle_seconds = {}\n\n[limits]\nmax_body_bytes = {max_body_bytes}\nnode_publish_inflight_bytes = {node_publish_inflight_bytes}\nconnection_publish_inflight_bytes = {connection_publish_inflight_bytes}\nnode_delivery_inflight_bytes = {}\nconnection_delivery_inflight_bytes = {}\ndisconnect_on_retriable_publish_error = false\n\n[metrics]\ndetailed_queue_metrics = {}\nmax_detailed_series = {}\n\n[security]\nadmin_token_file = \"/run/secrets/rustqueue/admin-token\"\n{publish_token}registry_token_file = \"/run/secrets/rustqueue/registry-token\"\nconsole_token_file = \"/run/secrets/rustqueue/console-token\"\n{kodo_cleanup_token}console_management_enabled = {}\nkodo_cleanup_enabled = {}\n# secret: {secret_name}\n",
        cluster.spec.storage_feature_level,
        cluster.spec.min_free_bytes,
        cluster.spec.disk_high_watermark_percent,
        cluster.spec.disk_low_watermark_percent,
        cluster.spec.protective_eviction_enabled,
        cluster.spec.disk_pressure_grace_seconds,
        cluster.spec.maintenance_startup_delay_seconds,
        cluster.spec.bootstrap_retention_seconds,
        cluster.spec.max_message_bytes,
        cluster.spec.max_topics,
        cluster.spec.max_publish_workers,
        cluster.spec.publish_worker_idle_seconds,
        cluster.spec.node_delivery_inflight_bytes,
        cluster.spec.connection_delivery_inflight_bytes,
        cluster.spec.detailed_queue_metrics,
        cluster.spec.max_detailed_metric_series,
        cluster.spec.console_management_enabled,
        cleanup_enabled,
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

fn workload_resources(resources: &crate::crd::WorkloadResources) -> Value {
    let mut limits = serde_json::Map::new();
    if let Some(cpu) = &resources.cpu_limit {
        limits.insert("cpu".into(), json!(cpu));
    }
    if let Some(memory) = &resources.memory_limit {
        limits.insert("memory".into(), json!(memory));
    }
    let mut value = json!({
        "requests": {
            "cpu": resources.cpu_request,
            "memory": resources.memory_request,
        }
    });
    if !limits.is_empty() {
        value["limits"] = Value::Object(limits);
    }
    value
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
                message_index_cache_bytes: 64 * 1024 * 1024,
                maintenance_startup_delay_seconds: 30,
                node_delivery_inflight_bytes: 512 * 1024 * 1024,
                connection_delivery_inflight_bytes: 32 * 1024 * 1024,
                min_free_bytes: 1024,
                disk_high_watermark_percent: 85,
                disk_low_watermark_percent: 75,
                protective_eviction_enabled: false,
                disk_pressure_grace_seconds: 60,
                bootstrap_retention_seconds: 90,
                max_message_bytes: 20 * 1024 * 1024,
                max_topics: 10_000,
                max_publish_workers: 1_024,
                publish_worker_idle_seconds: 60,
                detailed_queue_metrics: false,
                max_detailed_metric_series: 1_000,
                registry_secret_name: None,
                console_management_enabled: false,
                client_tls_secret_name: None,
                proxy_node_selector: BTreeMap::new(),
                proxy_tcp_max_connection_age_seconds: 300,
                discovery_replicas: 2,
                kodo_compatibility: crate::crd::KodoCompatibility::default(),
                maintenance: None,
                rollout: crate::crd::RolloutPolicy::default(),
                broker_scheduling: crate::crd::BrokerScheduling::default(),
                broker_resources: crate::crd::WorkloadResources::default(),
            },
            status: None,
        }
    }

    fn enable_kodo(cluster: &mut RustQueue, cleanup_enabled: bool) {
        cluster.spec.min_brokers = 3;
        cluster.spec.max_brokers = 3;
        cluster.spec.storage_feature_level = 2;
        cluster.spec.bootstrap_retention_seconds = 180;
        cluster.spec.max_message_bytes = 100 * 1024 * 1024;
        cluster.spec.connection_delivery_inflight_bytes = 128 * 1024 * 1024;
        cluster.spec.node_delivery_inflight_bytes = 512 * 1024 * 1024;
        cluster.spec.kodo_compatibility.enabled = true;
        cluster.spec.kodo_compatibility.cleanup_enabled = cleanup_enabled;
        cluster
            .spec
            .kodo_compatibility
            .allowed_pod_selector
            .insert("app.kubernetes.io/name".into(), "kodo".into());
    }

    #[test]
    fn renders_one_share_nothing_statefulset_and_retained_pvcs() {
        let resources = build(BuildInput {
            cluster: &cluster(),
            replicas: 3,
            kodo_gateway_replicas: 0,
            advertise_kodo_gateways: false,
            discovery_service_kodo: Some(false),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: false,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        assert!(resources
            .config
            .data
            .as_ref()
            .and_then(|data| data.get("rustqueue.toml"))
            .is_some_and(|config| {
                config.contains("console_token_file")
                    && config.contains("detailed_queue_metrics = false")
                    && config.contains("max_detailed_series = 1000")
                    && config.contains("maintenance_startup_delay_seconds = 30")
                    && config.contains("node_delivery_inflight_bytes = 536870912")
                    && config.contains("connection_delivery_inflight_bytes = 33554432")
                    && !config.contains("message_index_cache_bytes")
            }));
        assert!(resources.kodo_gateway.is_none());
        assert!(resources.kodo_gateway_service.is_none());
        assert!(resources.kodo_gateway_headless_service.is_none());
        assert!(resources.kodo_gateway_network_policy.is_none());
        let policy = serde_json::to_value(&resources.network_policy).unwrap();
        assert_eq!(
            policy["spec"]["podSelector"]["matchExpressions"][0]["values"],
            json!(["broker", "kodo-gateway"])
        );
        let broker_policy = serde_json::to_value(&resources.broker_network_policy).unwrap();
        assert_eq!(
            broker_policy["spec"]["ingress"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            broker_policy["spec"]["ingress"][0]["from"],
            json!([{"namespaceSelector": {}}])
        );
        let spec = resources.brokers.spec.unwrap();
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(spec.service_name.as_deref(), Some("queue-brokers"));
        assert_eq!(spec.update_strategy.unwrap().type_, Some("OnDelete".into()));
        assert_eq!(spec.volume_claim_templates.unwrap().len(), 1);
        assert_eq!(
            resources.broker_pdb.spec.unwrap().min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
        );
        assert_eq!(
            resources
                .discovery
                .spec
                .as_ref()
                .unwrap()
                .strategy
                .as_ref()
                .unwrap()
                .type_
                .as_deref(),
            Some("RollingUpdate")
        );
        let discovery_value = serde_json::to_value(&resources.discovery).unwrap();
        assert_eq!(discovery_value["spec"]["minReadySeconds"], 30);
        assert_eq!(
            discovery_value["spec"]["template"]["spec"]["containers"][0]["livenessProbe"]
                ["httpGet"]["path"],
            "/ping"
        );
        assert_eq!(
            discovery_value["spec"]["strategy"]["rollingUpdate"]["maxSurge"],
            "100%"
        );
        assert_eq!(
            discovery_value["spec"]["template"]["metadata"]["labels"][DISCOVERY_MODE_LABEL],
            "direct"
        );
        let discovery_service = serde_json::to_value(&resources.discovery_service).unwrap();
        assert_eq!(
            discovery_service["spec"]["selector"][DISCOVERY_MODE_LABEL],
            "direct"
        );
        assert_eq!(
            spec.template
                .spec
                .as_ref()
                .unwrap()
                .security_context
                .as_ref()
                .unwrap()
                .fs_group,
            Some(65532)
        );
        assert!(spec
            .template
            .spec
            .unwrap()
            .containers
            .first()
            .and_then(|container| container.env.as_ref())
            .is_some_and(|env| env.iter().any(|variable| {
                variable.name == "RUSTQUEUE_MESSAGE_INDEX_CACHE_BYTES"
                    && variable.value.as_deref() == Some("67108864")
            })));
        assert_eq!(resources.discovery.spec.unwrap().replicas, Some(2));
        assert!(resources
            .proxy
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .first()
            .and_then(|container| container.env.as_ref())
            .is_some_and(|env| env.iter().any(|variable| {
                variable.name == "RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS"
                    && variable.value.as_deref() == Some("300")
            })));
        let proxy_value = serde_json::to_value(&resources.proxy).unwrap();
        assert_eq!(
            proxy_value["spec"]["template"]["spec"]["containers"][0]["livenessProbe"]["httpGet"]
                ["path"],
            "/ping"
        );
        assert_eq!(
            resources
                .proxy
                .spec
                .as_ref()
                .unwrap()
                .template
                .spec
                .as_ref()
                .unwrap()
                .termination_grace_period_seconds,
            Some(45)
        );
    }

    #[test]
    fn kodo_profile_renders_three_stable_gateways_and_large_message_limits() {
        let mut cluster = cluster();
        enable_kodo(&mut cluster, false);
        let mut resources = build(BuildInput {
            cluster: &cluster,
            replicas: 3,
            kodo_gateway_replicas: 3,
            advertise_kodo_gateways: true,
            discovery_service_kodo: Some(true),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: false,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        let config = resources
            .config
            .data
            .as_ref()
            .unwrap()
            .get("rustqueue.toml")
            .unwrap();
        assert!(config.contains("feature_level = 2"));
        assert!(config.contains("bootstrap_retention_seconds = 180"));
        assert!(config.contains("max_message_bytes = 104857600"));
        assert!(config.contains("max_segment_bytes = 268435456"));
        assert!(config.contains("connection_publish_inflight_bytes = 167772160"));
        assert!(config.contains("node_publish_inflight_bytes = 1073741824"));
        assert!(config.contains("disconnect_on_retriable_publish_error = false"));
        assert!(config.contains("publish_token_file = \"/run/secrets/rustqueue/admin-token\""));
        assert!(config.contains("kodo_cleanup_enabled = false"));
        assert!(!config.contains("kodo_cleanup_token_file"));
        assert!(!config.contains("kodo_http_address"));
        assert_eq!(
            resources
                .discovery
                .spec
                .as_ref()
                .unwrap()
                .strategy
                .as_ref()
                .unwrap()
                .type_
                .as_deref(),
            Some("RollingUpdate")
        );
        assert_eq!(
            resources.discovery.spec.as_ref().unwrap().min_ready_seconds,
            Some(660)
        );
        assert_eq!(
            resources
                .discovery
                .spec
                .as_ref()
                .unwrap()
                .template
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap()
                .get(DISCOVERY_MODE_LABEL)
                .map(String::as_str),
            Some("kodo")
        );
        assert_eq!(
            resources
                .discovery_service
                .spec
                .as_ref()
                .unwrap()
                .selector
                .as_ref()
                .unwrap()
                .get(DISCOVERY_MODE_LABEL)
                .map(String::as_str),
            Some("kodo")
        );

        let gateway = resources.kodo_gateway.take().unwrap();
        let gateway_policy = resources.kodo_gateway_network_policy.take().unwrap();
        let gateway_policy = serde_json::to_value(gateway_policy).unwrap();
        assert_eq!(
            gateway_policy["spec"]["ingress"][0]["from"][0],
            json!({"podSelector": {
                "matchLabels": {"app.kubernetes.io/name": "kodo"}
            }})
        );
        assert_eq!(
            gateway_policy["spec"]["ingress"][0]["ports"]
                .as_array()
                .unwrap()
                .iter()
                .map(|port| port["port"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![4150, 4152, 4153, 4151, 4154, 4155]
        );
        assert_eq!(
            gateway_policy["spec"]["ingress"][1],
            json!({
                "from": [{"namespaceSelector": {}}],
                "ports": [{"protocol": "TCP", "port": 4160}]
            })
        );
        let broker_policy = serde_json::to_value(&resources.broker_network_policy).unwrap();
        assert_eq!(
            broker_policy["spec"]["ingress"].as_array().unwrap().len(),
            2
        );
        assert_eq!(
            broker_policy["spec"]["ingress"][0]["from"],
            json!([
                {"podSelector": {
                    "matchLabels": {"app.kubernetes.io/instance": "queue"}
                }},
                {"podSelector": {
                    "matchLabels": {"app.kubernetes.io/name": "kodo"}
                }}
            ])
        );
        assert_eq!(
            broker_policy["spec"]["ingress"][0]["ports"],
            json!([{"protocol": "TCP", "port": 4150}])
        );
        assert_eq!(
            broker_policy["spec"]["ingress"][1],
            json!({
                "from": [{"namespaceSelector": {}}],
                "ports": [{"protocol": "TCP", "port": 4151}]
            })
        );
        assert_eq!(gateway.spec.as_ref().unwrap().replicas, Some(3));
        assert_eq!(
            gateway.spec.as_ref().unwrap().service_name.as_deref(),
            Some("queue-kodo-gateways")
        );
        let gateway_service = resources.kodo_gateway_service.take().unwrap();
        assert_eq!(
            gateway_service.metadata.name.as_deref(),
            Some("queue-kodo-publish")
        );
        assert_eq!(
            gateway_service
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get("rustqueue.io/metrics"))
                .map(String::as_str),
            Some("true")
        );
        let gateway_service = gateway_service.spec.unwrap();
        assert_ne!(gateway_service.cluster_ip.as_deref(), Some("None"));
        assert_eq!(
            gateway_service
                .ports
                .unwrap()
                .into_iter()
                .map(|port| (port.name.unwrap(), port.port, port.target_port.unwrap()))
                .collect::<Vec<_>>(),
            vec![
                (
                    "tcp-0".into(),
                    4150,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String("tcp".into())
                ),
                (
                    "tcp-1".into(),
                    4152,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String("tcp".into())
                ),
                (
                    "tcp-2".into(),
                    4153,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String("tcp".into())
                ),
                (
                    "http-0".into(),
                    4151,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "http-0".into()
                    )
                ),
                (
                    "http-1".into(),
                    4154,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "http-1".into()
                    )
                ),
                (
                    "http-2".into(),
                    4155,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "http-2".into()
                    )
                ),
                (
                    "metrics".into(),
                    4160,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "metrics".into()
                    )
                ),
            ]
        );
        let headless_service = resources.kodo_gateway_headless_service.take().unwrap();
        assert_eq!(
            headless_service.metadata.name.as_deref(),
            Some("queue-kodo-gateways")
        );
        let headless_service = headless_service.spec.unwrap();
        assert_eq!(headless_service.cluster_ip.as_deref(), Some("None"));
        assert_eq!(headless_service.publish_not_ready_addresses, Some(true));
        let gateway_value = serde_json::to_value(&gateway).unwrap();
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["terminationGracePeriodSeconds"],
            75
        );
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["livenessProbe"]["httpGet"]
                ["path"],
            "/ping"
        );
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]
                ["memory"],
            "1Gi"
        );
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]
                ["memory"],
            "768Mi"
        );
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]
                ["cpu"],
            "1"
        );
        assert_eq!(
            gateway_value["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]
                ["requiredDuringSchedulingIgnoredDuringExecution"][0]["topologyKey"],
            "kubernetes.io/hostname"
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|variable| {
                    variable["name"] == "RUSTQUEUE_PROXY_MAX_MESSAGE_BYTES"
                        && variable["value"] == "104857600"
                })
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|variable| {
                    variable["name"] == "RUSTQUEUE_PROXY_SHUTDOWN_GRACE_SECONDS"
                        && variable["value"] == "60"
                })
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|variable| {
                    variable["name"] == "RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES"
                        && variable["value"] == "536870912"
                })
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|variable| {
                    variable["name"] == "RUSTQUEUE_PROXY_TCP_COMMAND_TIMEOUT_MS"
                        && variable["value"] == "120000"
                })
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .any(|variable| {
                    variable["name"] == "RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS"
                        && variable["value"] == "0"
                })
        );
        assert!(
            gateway_value["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array()
                .unwrap()
                .iter()
                .all(|variable| {
                    variable["name"] != "RUSTQUEUE_KODO_CLEANUP_TOKEN_FILE"
                        && variable["name"] != "RUSTQUEUE_REGISTRY_TOKEN_FILE"
                })
        );
        assert_eq!(
            resources
                .kodo_gateway_pdb
                .take()
                .unwrap()
                .spec
                .unwrap()
                .min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
        );
        let discovery_env = resources
            .discovery
            .spec
            .take()
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0)
            .env
            .unwrap();
        assert!(discovery_env.iter().any(|item| {
            item.name == "RUSTQUEUE_KODO_GATEWAY_ADDRESS"
                && item.value.as_deref() == Some("queue-kodo-publish.test.svc")
        }));
        assert!(discovery_env.iter().any(|item| {
            item.name == "RUSTQUEUE_KODO_CLEANUP_ENABLED" && item.value.as_deref() == Some("false")
        }));
    }

    #[test]
    fn staged_kodo_gateways_fail_closed_without_mounting_cleanup_credentials() {
        let mut cluster = cluster();
        enable_kodo(&mut cluster, false);
        let mut resources = build(BuildInput {
            cluster: &cluster,
            replicas: 3,
            kodo_gateway_replicas: 0,
            advertise_kodo_gateways: false,
            discovery_service_kodo: Some(false),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: false,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        let discovery = resources.discovery.spec.as_ref().unwrap();
        assert_eq!(
            discovery
                .template
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap()
                .get(DISCOVERY_MODE_LABEL)
                .map(String::as_str),
            Some("direct")
        );
        assert_eq!(
            resources
                .discovery_service
                .spec
                .as_ref()
                .unwrap()
                .selector
                .as_ref()
                .unwrap()
                .get(DISCOVERY_MODE_LABEL)
                .map(String::as_str),
            Some("direct")
        );
        let discovery_env = discovery
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .first()
            .unwrap()
            .env
            .as_ref()
            .unwrap();
        assert!(discovery_env
            .iter()
            .all(|variable| variable.name != "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED"));
        assert!(discovery_env
            .iter()
            .all(|variable| variable.name != "RUSTQUEUE_KODO_GATEWAY_ADDRESS"));
        let gateway = resources.kodo_gateway.take().unwrap();
        assert_eq!(gateway.spec.as_ref().unwrap().replicas, Some(0));
        let pod = gateway.spec.unwrap().template.spec.unwrap();
        assert!(pod.volumes.unwrap().is_empty());
        let container = pod.containers.first().unwrap();
        assert!(container.volume_mounts.as_ref().unwrap().is_empty());
        assert!(container.env.as_ref().unwrap().iter().all(|variable| {
            variable.name != "RUSTQUEUE_KODO_CLEANUP_TOKEN_FILE"
                && variable.name != "RUSTQUEUE_REGISTRY_TOKEN_FILE"
        }));
        assert_eq!(
            resources
                .kodo_gateway_pdb
                .take()
                .unwrap()
                .spec
                .unwrap()
                .min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(0))
        );
    }

    #[test]
    fn requested_kodo_cleanup_remains_disabled_in_rendered_resources() {
        let mut cluster = cluster();
        enable_kodo(&mut cluster, true);
        let resources = build(BuildInput {
            cluster: &cluster,
            replicas: 3,
            kodo_gateway_replicas: 3,
            advertise_kodo_gateways: true,
            discovery_service_kodo: Some(true),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: false,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        let config = resources
            .config
            .data
            .as_ref()
            .unwrap()
            .get("rustqueue.toml")
            .unwrap();
        assert!(config.contains("kodo_cleanup_enabled = false"));
        assert!(!config.contains("kodo_http_address"));
        let discovery_env = resources
            .discovery
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap();
        assert!(discovery_env.iter().any(|variable| {
            variable.name == "RUSTQUEUE_KODO_CLEANUP_ENABLED"
                && variable.value.as_deref() == Some("false")
        }));
    }

    #[test]
    fn disabling_kodo_keeps_the_gateway_recoverable_without_cleanup() {
        let mut cluster = cluster();
        cluster.spec.kodo_compatibility.cleanup_enabled = true;
        let resources = build(BuildInput {
            cluster: &cluster,
            replicas: 3,
            kodo_gateway_replicas: 3,
            advertise_kodo_gateways: false,
            discovery_service_kodo: Some(false),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: true,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        let config = resources
            .config
            .data
            .as_ref()
            .unwrap()
            .get("rustqueue.toml")
            .unwrap();
        assert!(config.contains("kodo_cleanup_enabled = false"));
        assert!(!config.contains("kodo_cleanup_token_file"));
        assert!(!config.contains("kodo_http_address"));
        assert_eq!(
            resources
                .kodo_gateway
                .as_ref()
                .and_then(|gateway| gateway.spec.as_ref())
                .and_then(|spec| spec.replicas),
            Some(3)
        );
        let gateway_env = resources
            .kodo_gateway
            .as_ref()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap();
        assert!(gateway_env.iter().all(|variable| {
            variable.name != "RUSTQUEUE_KODO_CLEANUP_TOKEN_FILE"
                && variable.name != "RUSTQUEUE_REGISTRY_TOKEN_FILE"
        }));
        assert!(resources.retain_existing_kodo_resources);
        assert_eq!(
            resources.discovery.spec.as_ref().unwrap().min_ready_seconds,
            Some(660)
        );
    }

    #[test]
    fn standalone_large_message_profile_has_matching_publish_limits() {
        let mut cluster = cluster();
        cluster.spec.storage_feature_level = 2;
        cluster.spec.max_message_bytes = 100 * 1024 * 1024;
        cluster.spec.connection_delivery_inflight_bytes = 100 * 1024 * 1024;
        cluster.spec.node_delivery_inflight_bytes = 200 * 1024 * 1024;
        let resources = build(BuildInput {
            cluster: &cluster,
            replicas: 3,
            kodo_gateway_replicas: 0,
            advertise_kodo_gateways: false,
            discovery_service_kodo: Some(false),
            activate_kodo_cleanup: false,
            retain_existing_kodo_resources: false,
            image: "rustqueue:test",
            claim_template_size: "100Gi",
            secret_name: "queue-auth",
            mounted_secret_revision: "1",
        })
        .unwrap();
        let mut data = resources.config.data.unwrap();
        let config = data.remove("rustqueue.toml").unwrap();
        assert!(config.contains("max_body_bytes = 104857600"));
        assert!(config.contains("connection_publish_inflight_bytes = 167772160"));
        assert!(config.contains("max_segment_bytes = 268435456"));
        assert!(resources.kodo_gateway.is_none());
        assert!(resources.kodo_gateway_service.is_none());
        assert!(resources.kodo_gateway_headless_service.is_none());
    }

    #[test]
    fn broker_pod_template_changes_advance_the_rollout_revision() {
        let render = |cluster: &RustQueue, secret_name: &str| {
            build(BuildInput {
                cluster,
                replicas: 3,
                kodo_gateway_replicas: 0,
                advertise_kodo_gateways: false,
                discovery_service_kodo: Some(false),
                activate_kodo_cleanup: false,
                retain_existing_kodo_resources: false,
                image: "rustqueue:test",
                claim_template_size: "100Gi",
                secret_name,
                mounted_secret_revision: "same-resource-version",
            })
            .unwrap()
            .revision
        };
        let mut cluster = cluster();
        let original = render(&cluster, "queue-auth");
        cluster.spec.broker_resources.cpu_request = "2".into();
        let resources_changed = render(&cluster, "queue-auth");
        assert_ne!(original, resources_changed);
        assert_ne!(resources_changed, render(&cluster, "replacement-auth"));
        assert_eq!(original.len(), 32);
    }
}
