use super::{labels_for, typed};
use crate::RustQueue;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(super) struct KodoResources {
    pub service: Service,
    pub headless_service: Service,
    pub gateway: StatefulSet,
    pub pdb: PodDisruptionBudget,
    pub network_policy: NetworkPolicy,
}

pub(super) struct BuildInput<'a> {
    pub cluster: &'a RustQueue,
    pub replicas: i32,
    pub image: &'a str,
    pub secret_name: &'a str,
    pub revision: &'a str,
    pub name: &'a str,
    pub namespace: &'a str,
    pub owner: OwnerReference,
    pub labels: &'a BTreeMap<String, String>,
    pub discovery_name: &'a str,
    pub service_account_name: &'a str,
}

pub(super) fn build(input: BuildInput<'_>) -> anyhow::Result<KodoResources> {
    let gateway_name = format!("{}-kodo-gateway", input.name);
    let publish_service_name = format!("{}-kodo-publish", input.name);
    let headless_service_name = format!("{}-kodo-gateways", input.name);
    let gateway_labels = labels_for(input.labels, "kodo-gateway");
    let allowed_peer = allowed_peer(input.cluster);
    let metadata = |name: &str| {
        json!({
            "name": name,
            "namespace": input.namespace,
            "labels": gateway_labels,
            "ownerReferences": [input.owner],
        })
    };
    let mut publish_metadata = metadata(&publish_service_name);
    publish_metadata["labels"]["rustqueue.io/metrics"] = json!("true");
    let service = typed(json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": publish_metadata,
        "spec": {
            "selector": gateway_labels,
            "ports": [
                {"name": "tcp-0", "port": 4150, "targetPort": "tcp"},
                {"name": "tcp-1", "port": 4152, "targetPort": "tcp"},
                {"name": "tcp-2", "port": 4153, "targetPort": "tcp"},
                {"name": "http-0", "port": 4151, "targetPort": "http-0"},
                {"name": "http-1", "port": 4154, "targetPort": "http-1"},
                {"name": "http-2", "port": 4155, "targetPort": "http-2"},
                {"name": "metrics", "port": 4160, "targetPort": "metrics"}
            ]
        }
    }))?;
    let headless_service = typed(json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": metadata(&headless_service_name),
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": gateway_labels,
            "ports": [
                {"name": "tcp", "port": 4150, "targetPort": "tcp"},
                {"name": "http-0", "port": 4151, "targetPort": "http-0"}
            ]
        }
    }))?;
    let mut env = vec![
        json!({"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}),
        json!({"name": "RUSTQUEUE_DISCOVERY_URLS", "value": format!("http://{}:4161", input.discovery_name)}),
        json!({"name": "RUSTQUEUE_KODO_COMPATIBILITY_ENABLED", "value": "true"}),
        json!({"name": "RUSTQUEUE_KODO_CLEANUP_ENABLED", "value": input.cluster.spec.kodo_compatibility.effective_cleanup_enabled().to_string()}),
        json!({"name": "RUSTQUEUE_PROXY_MAX_MESSAGE_BYTES", "value": (100 * 1024 * 1024).to_string()}),
        json!({"name": "RUSTQUEUE_PROXY_MAX_BODY_BYTES", "value": (128 * 1024 * 1024).to_string()}),
        json!({"name": "RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES", "value": (512 * 1024 * 1024).to_string()}),
        json!({"name": "RUSTQUEUE_PROXY_TCP_COMMAND_TIMEOUT_MS", "value": "120000"}),
        json!({"name": "RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS", "value": "0"}),
        json!({"name": "RUSTQUEUE_PROXY_SHUTDOWN_GRACE_SECONDS", "value": "60"}),
    ];
    let (volume_mounts, volumes) = if input
        .cluster
        .spec
        .kodo_compatibility
        .effective_cleanup_enabled()
    {
        env.extend([
            json!({"name": "RUSTQUEUE_KODO_CLEANUP_TOKEN_FILE", "value": "/run/secrets/rustqueue/kodo-cleanup-token"}),
            json!({"name": "RUSTQUEUE_REGISTRY_TOKEN_FILE", "value": "/run/secrets/rustqueue/registry-token"}),
        ]);
        (
            vec![json!({
                "name": "auth",
                "mountPath": "/run/secrets/rustqueue",
                "readOnly": true
            })],
            vec![json!({
                "name": "auth",
                "secret": {
                    "secretName": input.secret_name,
                    "items": [
                        {"key": "kodo-cleanup-token", "path": "kodo-cleanup-token"},
                        {"key": "registry-token", "path": "registry-token"}
                    ]
                }
            })],
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let gateway = typed(json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": metadata(&gateway_name),
        "spec": {
            "serviceName": headless_service_name,
            "replicas": input.replicas,
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": gateway_labels},
            "template": {
                "metadata": {
                    "labels": gateway_labels,
                    "annotations": {"rustqueue.io/revision": input.revision}
                },
                "spec": {
                    "serviceAccountName": input.service_account_name,
                    "terminationGracePeriodSeconds": 75,
                    "nodeSelector": input.cluster.spec.proxy_node_selector,
                    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                            "labelSelector": {"matchLabels": gateway_labels},
                            "topologyKey": "kubernetes.io/hostname"
                    }]}},
                    "securityContext": {
                        "runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"}
                    },
                    "containers": [{
                        "name": "gateway", "image": input.image,
                        "imagePullPolicy": input.cluster.spec.image_pull_policy,
                        "command": ["rustqueue-proxy"],
                        "ports": [
                            {"name": "tcp", "containerPort": 4150},
                            {"name": "http-0", "containerPort": 4151},
                            {"name": "http-1", "containerPort": 4154},
                            {"name": "http-2", "containerPort": 4155},
                            {"name": "metrics", "containerPort": 4160}
                        ],
                        "env": env,
                        "volumeMounts": volume_mounts,
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {"drop": ["ALL"]}
                        },
                        "readinessProbe": {
                            "httpGet": {"path": "/v1/health", "port": "http-0"},
                            "periodSeconds": 2,
                            "failureThreshold": 2
                        },
                        "livenessProbe": {
                            "httpGet": {"path": "/ping", "port": "http-0"},
                            "periodSeconds": 10,
                            "failureThreshold": 3
                        },
                        "resources": {
                            "requests": {"cpu": "1", "memory": "768Mi"},
                            "limits": {"memory": "1Gi"}
                        }
                    }],
                    "volumes": volumes
                }
            }
        }
    }))?;
    let pdb = typed(json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": metadata(&format!("{gateway_name}-pdb")),
        "spec": {
            "minAvailable": if input.replicas == 0 { 0 } else { 2 },
            "selector": {"matchLabels": gateway_labels}
        }
    }))?;
    let network_policy = typed(json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": metadata(&format!("{}-kodo-gateway-ingress", input.name)),
        "spec": {
            "podSelector": {"matchLabels": gateway_labels},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [allowed_peer],
                    "ports": [
                        {"protocol": "TCP", "port": 4150},
                        {"protocol": "TCP", "port": 4152},
                        {"protocol": "TCP", "port": 4153},
                        {"protocol": "TCP", "port": 4151},
                        {"protocol": "TCP", "port": 4154},
                        {"protocol": "TCP", "port": 4155}
                    ]
                },
                {
                    "from": [{"namespaceSelector": {}}],
                    "ports": [{"protocol": "TCP", "port": 4160}]
                }
            ]
        }
    }))?;
    Ok(KodoResources {
        service,
        headless_service,
        gateway,
        pdb,
        network_policy,
    })
}

pub(super) fn allowed_peer(cluster: &RustQueue) -> Value {
    let allowed_pod_selector = cluster
        .spec
        .kodo_compatibility
        .effective_allowed_pod_selector();
    let mut peer = json!({
        "podSelector": {
            "matchLabels": allowed_pod_selector
        }
    });
    if !cluster
        .spec
        .kodo_compatibility
        .allowed_namespace_selector
        .is_empty()
    {
        peer["namespaceSelector"] = json!({
            "matchLabels": cluster.spec.kodo_compatibility.allowed_namespace_selector
        });
    }
    peer
}
