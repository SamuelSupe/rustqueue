use crate::crd::RustQueueCluster;
use crate::layout::{BrokerPlan, CellPlan};
use anyhow::Context;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service, ServiceAccount};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::{Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const MANAGER: &str = "rustqueue-operator";
pub const LABEL_CLUSTER: &str = "rustqueue.io/cluster";
pub const LABEL_CELL: &str = "rustqueue.io/cell";
pub const LABEL_NODE_ID: &str = "rustqueue.io/node-id";
pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
pub const ANNOTATION_TLS_REVISION: &str = "rustqueue.io/tls-revision";
pub const ANNOTATION_CONFIG_REVISION: &str = "rustqueue.io/config-revision";
pub const ANNOTATION_TARGET_NODE: &str = "rustqueue.io/target-node";
pub const ANNOTATION_ROLLOUT_REVISION: &str = "rustqueue.io/rollout-revision";
pub const ANNOTATION_CERT_NOT_AFTER: &str = "rustqueue.io/certificate-not-after";

pub fn service_account(
    cluster: &RustQueueCluster,
    namespace: &str,
) -> anyhow::Result<ServiceAccount> {
    from_value(json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": metadata(cluster, namespace, &format!("{}-broker", cluster.name_any()), labels(cluster, "broker"))?,
        "automountServiceAccountToken": false
    }))
}

pub fn client_service(cluster: &RustQueueCluster, namespace: &str) -> anyhow::Result<Service> {
    from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": metadata(cluster, namespace, &cluster.name_any(), labels(cluster, "gateway"))?,
        "spec": {
            "type": "ClusterIP",
            "selector": { LABEL_CLUSTER: cluster.name_any(), LABEL_COMPONENT: "broker" },
            "ports": [
                {"name": "tcp", "port": 4150, "targetPort": "tcp"},
                {"name": "http", "port": 4151, "targetPort": "http"}
            ]
        }
    }))
}

pub fn headless_service(
    cluster: &RustQueueCluster,
    namespace: &str,
    cell: &CellPlan,
) -> anyhow::Result<Service> {
    let mut resource_labels = labels(cluster, "broker");
    resource_labels.insert(LABEL_CELL.into(), cell.id.to_string());
    from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": metadata(cluster, namespace, &cell.brokers[0].headless_service, resource_labels)?,
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": { LABEL_CLUSTER: cluster.name_any(), LABEL_COMPONENT: "broker", LABEL_CELL: cell.id.to_string() },
            "ports": [
                {"name": "tcp", "port": 4150, "targetPort": "tcp"},
                {"name": "http", "port": 4151, "targetPort": "http"},
                {"name": "raft", "port": 4250, "targetPort": "raft"},
                {"name": "p2p", "port": 4350, "targetPort": "p2p"}
            ]
        }
    }))
}

pub fn disruption_budget(
    cluster: &RustQueueCluster,
    namespace: &str,
    cell: &CellPlan,
) -> anyhow::Result<PodDisruptionBudget> {
    let name = format!("{}-cell-{}", cluster.name_any(), cell.id);
    from_value(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": metadata(cluster, namespace, &name, labels(cluster, "broker"))?,
        "spec": {
            "maxUnavailable": 1,
            "selector": {"matchLabels": {LABEL_CLUSTER: cluster.name_any(), LABEL_COMPONENT: "broker", LABEL_CELL: cell.id.to_string()}},
            "unhealthyPodEvictionPolicy": "IfHealthyBudget"
        }
    }))
}

pub fn pending_config_map(
    cluster: &RustQueueCluster,
    namespace: &str,
    broker: &BrokerPlan,
) -> anyhow::Result<ConfigMap> {
    from_value(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata(cluster, namespace, &broker.config_map, broker_labels(cluster, broker))?,
        "data": {"node-name": "", "rustqueue.toml": ""}
    }))
}

pub fn configured_config_map(
    cluster: &RustQueueCluster,
    namespace: &str,
    broker: &BrokerPlan,
    node_name: &str,
    contents: &str,
) -> anyhow::Result<ConfigMap> {
    from_value(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata(cluster, namespace, &broker.config_map, broker_labels(cluster, broker))?,
        "data": {"node-name": node_name, "rustqueue.toml": contents}
    }))
}

pub fn secret(
    cluster: &RustQueueCluster,
    namespace: &str,
    name: &str,
    component: &str,
    string_data: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> anyhow::Result<Secret> {
    let mut metadata = metadata(cluster, namespace, name, labels(cluster, component))?;
    metadata["annotations"] = serde_json::to_value(annotations)?;
    from_value(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": metadata,
        "type": "Opaque",
        "stringData": string_data
    }))
}

pub fn stateful_set(
    cluster: &RustQueueCluster,
    namespace: &str,
    broker: &BrokerPlan,
    tls_revision: u64,
    config_revision: u64,
    target_node: &str,
) -> anyhow::Result<StatefulSet> {
    let name = &broker.stateful_set;
    let pod_labels = broker_labels(cluster, broker);
    let anti_affinity = if cluster.spec.development.allow_single_node {
        Value::Null
    } else {
        json!({
            "requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": {"matchLabels": {LABEL_CLUSTER: cluster.name_any(), LABEL_COMPONENT: "broker"}},
                "topologyKey": "kubernetes.io/hostname"
            }]
        })
    };
    let tolerations = if cluster.spec.nodes.dedicated {
        json!([{
            "key": cluster.spec.nodes.taint_key,
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule"
        }])
    } else {
        json!([])
    };
    let retention = if cluster.spec.storage.retain_on_delete {
        "Retain"
    } else {
        "Delete"
    };
    let pre_stop = format!(
        "token=$(cat /etc/rustqueue/shared/admin.token); curl -fsS -X POST -H \"authorization: Bearer $token\" -H 'content-type: application/json' -d '{{\"enabled\":true,\"ttl_seconds\":1800,\"reason\":\"kubernetes termination\"}}' http://127.0.0.1:4151/v1/cluster/nodes/{}/maintenance >/dev/null || true; sleep 5",
        broker.node_id
    );
    let post_start = format!(
        "token=$(cat /etc/rustqueue/shared/admin.token); for attempt in $(seq 1 120); do curl -fsS -X POST -H \"authorization: Bearer $token\" -H 'content-type: application/json' -d '{{\"enabled\":false,\"reason\":\"kubernetes pod ready\"}}' http://127.0.0.1:4151/v1/cluster/nodes/{}/maintenance >/dev/null && exit 0; sleep 1; done; exit 0",
        broker.node_id
    );
    let wait_config = "until [ -s /config-src/rustqueue.toml ] && [ \"$(cat /config-src/node-name 2>/dev/null)\" = \"$NODE_NAME\" ]; do sleep 1; done; cp /config-src/rustqueue.toml /config-runtime/rustqueue.toml";
    let mut template_annotations = BTreeMap::new();
    template_annotations.insert(ANNOTATION_TLS_REVISION, tls_revision.to_string());
    template_annotations.insert(ANNOTATION_CONFIG_REVISION, config_revision.to_string());
    template_annotations.insert(ANNOTATION_TARGET_NODE, target_node.to_owned());
    template_annotations.insert(
        ANNOTATION_ROLLOUT_REVISION,
        cluster.spec.upgrade.retry_generation.to_string(),
    );
    template_annotations.insert("prometheus.io/scrape", "true".into());
    template_annotations.insert("prometheus.io/port", "4151".into());
    template_annotations.insert("prometheus.io/path", "/metrics".into());
    from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": metadata(cluster, namespace, name, pod_labels.clone())?,
        "spec": {
            "replicas": 1,
            "serviceName": broker.headless_service,
            "podManagementPolicy": "Parallel",
            "updateStrategy": {"type": "OnDelete"},
            "persistentVolumeClaimRetentionPolicy": {"whenDeleted": retention, "whenScaled": "Retain"},
            "selector": {"matchLabels": {LABEL_CLUSTER: cluster.name_any(), LABEL_NODE_ID: broker.node_id.to_string()}},
            "template": {
                "metadata": {"labels": pod_labels, "annotations": template_annotations},
                "spec": {
                    "serviceAccountName": format!("{}-broker", cluster.name_any()),
                    "automountServiceAccountToken": false,
                    "terminationGracePeriodSeconds": 90,
                    "securityContext": {"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532, "seccompProfile": {"type": "RuntimeDefault"}},
                    "affinity": {
                        "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
                            "matchExpressions": selector_expressions(&cluster.spec.nodes.selector),
                            "matchFields": [{"key": "metadata.name", "operator": "In", "values": [target_node]}]
                        }]}},
                        "podAntiAffinity": anti_affinity
                    },
                    "topologySpreadConstraints": [{
                        "maxSkew": 1,
                        "topologyKey": cluster.spec.nodes.failure_domain_label,
                        "whenUnsatisfiable": if cluster.spec.development.allow_single_node { "ScheduleAnyway" } else { "DoNotSchedule" },
                        "labelSelector": {"matchLabels": {LABEL_CLUSTER: cluster.name_any(), LABEL_COMPONENT: "broker"}}
                    }],
                    "tolerations": tolerations,
                    "initContainers": [{
                        "name": "wait-for-config",
                        "image": cluster.spec.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "command": ["/bin/sh", "-ec", wait_config],
                        "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                        "securityContext": container_security(),
                        "volumeMounts": [
                            {"name": "config-source", "mountPath": "/config-src", "readOnly": true},
                            {"name": "config-runtime", "mountPath": "/config-runtime"}
                        ]
                    }],
                    "containers": [{
                        "name": "rustqueue",
                        "image": cluster.spec.image,
                        "imagePullPolicy": cluster.spec.image_pull_policy,
                        "args": ["--config", "/etc/rustqueue/runtime/rustqueue.toml"],
                        "ports": [
                            {"name": "tcp", "containerPort": 4150},
                            {"name": "http", "containerPort": 4151},
                            {"name": "raft", "containerPort": 4250},
                            {"name": "p2p", "containerPort": 4350}
                        ],
                        "resources": {
                            "requests": {"cpu": cluster.spec.resources.cpu_request, "memory": cluster.spec.resources.memory_request},
                            "limits": {"cpu": cluster.spec.resources.cpu_limit, "memory": cluster.spec.resources.memory_limit}
                        },
                        "startupProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 2, "failureThreshold": 90},
                        "readinessProbe": {"httpGet": {"path": "/v1/health", "port": "http"}, "periodSeconds": 2, "failureThreshold": 3},
                        "livenessProbe": {"httpGet": {"path": "/ping", "port": "http"}, "periodSeconds": 10, "failureThreshold": 6},
                        "lifecycle": {
                            "postStart": {"exec": {"command": ["/bin/sh", "-ec", post_start]}},
                            "preStop": {"exec": {"command": ["/bin/sh", "-ec", pre_stop]}}
                        },
                        "securityContext": container_security(),
                        "volumeMounts": [
                            {"name": "data", "mountPath": "/data"},
                            {"name": "config-runtime", "mountPath": "/etc/rustqueue/runtime", "readOnly": true},
                            {"name": "tls", "mountPath": "/etc/rustqueue/tls", "readOnly": true},
                            {"name": "shared", "mountPath": "/etc/rustqueue/shared", "readOnly": true},
                            {"name": "tmp", "mountPath": "/tmp"}
                        ]
                    }],
                    "volumes": [
                        {"name": "config-source", "configMap": {"name": broker.config_map}},
                        {"name": "config-runtime", "emptyDir": {}},
                        {"name": "tls", "secret": {"secretName": broker.tls_secret}},
                        {"name": "shared", "secret": {"secretName": format!("{}-shared", cluster.name_any())}},
                        {"name": "tmp", "emptyDir": {}}
                    ]
                }
            },
            "volumeClaimTemplates": [{
                "metadata": {"name": "data", "labels": {LABEL_CLUSTER: cluster.name_any(), LABEL_NODE_ID: broker.node_id.to_string()}},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": cluster.spec.storage.class_name,
                    "resources": {"requests": {"storage": cluster.spec.storage.size}}
                }
            }]
        }
    }))
}

fn labels(cluster: &RustQueueCluster, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), "rustqueue".into()),
        ("app.kubernetes.io/instance".into(), cluster.name_any()),
        ("app.kubernetes.io/managed-by".into(), MANAGER.into()),
        (LABEL_CLUSTER.into(), cluster.name_any()),
        (LABEL_COMPONENT.into(), component.into()),
    ])
}

fn broker_labels(cluster: &RustQueueCluster, broker: &BrokerPlan) -> BTreeMap<String, String> {
    let mut result = labels(cluster, "broker");
    result.insert(LABEL_CELL.into(), broker.cell_id.to_string());
    result.insert(LABEL_NODE_ID.into(), broker.node_id.to_string());
    result
}

fn metadata(
    cluster: &RustQueueCluster,
    namespace: &str,
    name: &str,
    labels: BTreeMap<String, String>,
) -> anyhow::Result<Value> {
    let owner = cluster
        .controller_owner_ref(&())
        .context("RustQueueCluster has no UID for owner reference")?;
    Ok(json!({
        "name": name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner]
    }))
}

fn selector_expressions(selector: &BTreeMap<String, String>) -> Vec<Value> {
    selector
        .iter()
        .map(|(key, value)| json!({"key": key, "operator": "In", "values": [value]}))
        .collect()
}

fn container_security() -> Value {
    json!({
        "allowPrivilegeEscalation": false,
        "readOnlyRootFilesystem": true,
        "runAsNonRoot": true,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "capabilities": {"drop": ["ALL"]}
    })
}

fn from_value<T: DeserializeOwned>(value: Value) -> anyhow::Result<T> {
    serde_json::from_value(value).context("deserialize generated Kubernetes resource")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::RustQueueClusterSpec;
    use crate::layout;
    use kube::api::ObjectMeta;

    fn cluster() -> RustQueueCluster {
        let mut spec = RustQueueClusterSpec::default();
        spec.storage.class_name = "fast-ssd".into();
        RustQueueCluster {
            metadata: ObjectMeta {
                name: Some("queue".into()),
                namespace: Some("messaging".into()),
                uid: Some("test-uid".into()),
                ..ObjectMeta::default()
            },
            spec,
            status: None,
        }
    }

    #[test]
    fn stateful_set_retains_pvc_and_uses_on_delete_rollout() {
        let cluster = cluster();
        let layout = layout::plan("queue", 3, &cluster.spec.cells);
        let sts = stateful_set(
            &cluster,
            "messaging",
            layout.broker(1).unwrap(),
            7,
            9,
            "worker-a",
        )
        .unwrap();
        assert_eq!(
            sts.spec
                .as_ref()
                .unwrap()
                .update_strategy
                .as_ref()
                .unwrap()
                .type_
                .as_deref(),
            Some("OnDelete")
        );
        assert_eq!(
            sts.spec
                .as_ref()
                .unwrap()
                .persistent_volume_claim_retention_policy
                .as_ref()
                .unwrap()
                .when_deleted
                .as_deref(),
            Some("Retain")
        );
        assert_eq!(
            sts.spec
                .as_ref()
                .unwrap()
                .template
                .spec
                .as_ref()
                .unwrap()
                .automount_service_account_token,
            Some(false)
        );
        assert_eq!(
            sts.spec
                .as_ref()
                .unwrap()
                .template
                .metadata
                .as_ref()
                .unwrap()
                .annotations
                .as_ref()
                .unwrap()[ANNOTATION_TARGET_NODE],
            "worker-a"
        );
    }

    #[test]
    fn peer_service_publishes_dns_before_readiness() {
        let cluster = cluster();
        let layout = layout::plan("queue", 3, &cluster.spec.cells);
        let service = headless_service(&cluster, "messaging", &layout.cells[0]).unwrap();
        assert_eq!(
            service.spec.unwrap().publish_not_ready_addresses,
            Some(true)
        );
    }
}
