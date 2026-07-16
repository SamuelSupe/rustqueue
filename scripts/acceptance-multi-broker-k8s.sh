#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-rustqueue-multi-e2e}"
RELEASE="${RELEASE:-rustqueue-multi-e2e}"
QUEUE="${QUEUE:-queue}"
BROKER_IMAGE_A="${BROKER_IMAGE_A:-rustqueue:multi-e2e-a}"
BROKER_IMAGE_B="${BROKER_IMAGE_B:-rustqueue:multi-e2e-b}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-rustqueue-operator:multi-e2e}"
COMPAT_IMAGE="${COMPAT_IMAGE:-rustqueue-go-compat:multi-e2e}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
LEDGER_SECONDS="${LEDGER_SECONDS:-60}"
CHART="deploy/helm/rustqueue"
FAKE_NODES=(rustqueue-capacity-e2e-1 rustqueue-capacity-e2e-2)
LABELED_NODE=0
NODE_KEEPER=""

require() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 1; }
}

diagnostics() {
  kubectl -n "$NAMESPACE" get rustqueue,pods,pvc,statefulset,deployment,daemonset -o wide || true
  kubectl -n "$NAMESPACE" describe rustqueue "$QUEUE" || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=operator --tail=300 || true
  kubectl -n "$NAMESPACE" logs operational-ledger --tail=300 || true
}

cleanup() {
  code=$?
  if [[ -n "$NODE_KEEPER" ]]; then
    kill "$NODE_KEEPER" >/dev/null 2>&1 || true
    wait "$NODE_KEEPER" 2>/dev/null || true
  fi
  if [[ $code -ne 0 ]]; then diagnostics; fi
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 || true
    kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
    kubectl delete node "${FAKE_NODES[@]}" --ignore-not-found >/dev/null 2>&1 || true
    if [[ "$LABELED_NODE" == "1" ]]; then
      kubectl label node "$NODE_NAME" rustqueue.io/eligible- >/dev/null 2>&1 || true
    fi
  else
    echo "KEEP_CLUSTER=1: preserving namespace $NAMESPACE and capacity-only Nodes"
  fi
  exit "$code"
}
trap cleanup EXIT

wait_namespace_deleted() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || return 0
    sleep 2
  done
  return 1
}

wait_queue_ready() {
  local brokers=$1 deadline=$((SECONDS + ${2:-360}))
  while (( SECONDS < deadline )); do
    local phase desired ready observed generation
    phase=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    desired=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.desiredBrokers}' 2>/dev/null || true)
    ready=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.readyBrokers}' 2>/dev/null || true)
    observed=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
    generation=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
    if [[ "$phase" == "Ready" && "$desired" == "$brokers" && "$ready" == "$brokers" && "$observed" == "$generation" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "queue did not become Ready with $brokers brokers" >&2
  return 1
}

wait_queue_phase() {
  local expected=$1 deadline=$((SECONDS + ${2:-120})) phase
  while (( SECONDS < deadline )); do
    phase=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "$expected" ]] && return 0
    sleep 2
  done
  echo "queue did not enter phase $expected" >&2
  return 1
}

wait_pod_succeeded() {
  local pod=$1 deadline=$((SECONDS + ${2:-240})) phase
  while (( SECONDS < deadline )); do
    phase=$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Succeeded" ]] && return 0
    [[ "$phase" == "Failed" ]] && return 1
    sleep 2
  done
  return 1
}

mark_fake_nodes_ready() {
  local now node
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  for node in "${FAKE_NODES[@]}"; do
    kubectl patch node "$node" --subresource=status --type=merge \
      -p "{\"status\":{\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\",\"reason\":\"RustQueueAcceptance\",\"message\":\"capacity-only acceptance node\",\"lastHeartbeatTime\":\"$now\",\"lastTransitionTime\":\"$now\"}]}}" >/dev/null
  done
}

create_fake_nodes() {
  local node
  kubectl delete node "${FAKE_NODES[@]}" --ignore-not-found >/dev/null
  for node in "${FAKE_NODES[@]}"; do
    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Node
metadata:
  name: $node
  labels:
    rustqueue.io/eligible: "true"
spec:
  taints:
    - key: rustqueue.io/capacity-only
      effect: NoSchedule
EOF
  done
  mark_fake_nodes_ready
  (
    while true; do
      mark_fake_nodes_ready || true
      sleep 2
    done
  ) &
  NODE_KEEPER=$!
}

bind_acceptance_pvcs() {
  local ordinal deadline=$((SECONDS + 60))
  for ordinal in 1 2; do
    while ! kubectl -n "$NAMESPACE" get pvc "data-$QUEUE-$ordinal" >/dev/null 2>&1; do
      (( SECONDS < deadline )) || return 1
      sleep 1
    done
    kubectl -n "$NAMESPACE" annotate pvc "data-$QUEUE-$ordinal" \
      "volume.kubernetes.io/selected-node=$NODE_NAME" --overwrite >/dev/null
  done
}

start_operational_ledger() {
  local overrides
  overrides=$(jq -cn --arg image "$COMPAT_IMAGE" --arg secret "$QUEUE-auth" --arg duration "$LEDGER_SECONDS" \
    '{apiVersion:"v1",spec:{containers:[{name:"operational-ledger",image:$image,imagePullPolicy:"Never",args:["operational-ledger","queue-proxy:4151","queue-discovery:4161",$duration,"3"],env:[{name:"RUSTQUEUE_ADMIN_TOKEN",valueFrom:{secretKeyRef:{name:$secret,key:"admin-token"}}}]}]}}')
  kubectl -n "$NAMESPACE" delete pod operational-ledger --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run operational-ledger --restart=Never --image="$COMPAT_IMAGE" \
    --image-pull-policy=Never --overrides="$overrides" >/dev/null
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    local logs
    logs=$(kubectl -n "$NAMESPACE" logs operational-ledger 2>/dev/null || true)
    if [[ "$logs" == *operational-ledger-ready* ]]; then
      return 0
    fi
    if [[ "$(kubectl -n "$NAMESPACE" get pod operational-ledger -o jsonpath='{.status.phase}' 2>/dev/null || true)" == "Failed" ]]; then
      return 1
    fi
    sleep 1
  done
  return 1
}

probe_metric() {
  local name=$1 url=$2 metric=$3 output
  kubectl -n "$NAMESPACE" delete pod "$name" --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run "$name" --restart=Never --image="$BROKER_IMAGE_B" \
    --image-pull-policy=Never --command -- curl -fsS "$url" >/dev/null
  wait_pod_succeeded "$name" 120
  output=$(kubectl -n "$NAMESPACE" logs "$name")
  [[ "$output" == *"$metric"* ]]
}

require kubectl
require helm
require docker
require jq
[[ "$LEDGER_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  echo "LEDGER_SECONDS must be a positive integer" >&2
  exit 2
}
[[ "$(kubectl config current-context)" == "orbstack" ]] || {
  echo "multi-broker acceptance only runs against OrbStack Kubernetes" >&2
  exit 1
}

if [[ "$BUILD_IMAGES" == "1" ]]; then
  make image
  docker tag rustqueue:dev "$BROKER_IMAGE_A"
  docker tag rustqueue:dev "$BROKER_IMAGE_B"
  make operator-image
  docker tag rustqueue-operator:dev "$OPERATOR_IMAGE"
  docker build -t "$COMPAT_IMAGE" tests/compat/go
fi

NODE_NAME=$(kubectl get nodes -o json | jq -r '.items[] | select(.metadata.name | startswith("rustqueue-capacity-e2e-") | not) | .metadata.name' | head -1)
[[ -n "$NODE_NAME" ]] || { echo "OrbStack has no real Kubernetes Node" >&2; exit 1; }
if [[ "$(kubectl get node "$NODE_NAME" -o jsonpath='{.metadata.labels.rustqueue\.io/eligible}')" != "true" ]]; then
  kubectl label node "$NODE_NAME" rustqueue.io/eligible=true >/dev/null
  LABELED_NODE=1
fi
STORAGE_CLASS=$(kubectl get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}')
[[ -n "$STORAGE_CLASS" ]] || { echo "OrbStack has no default StorageClass" >&2; exit 1; }

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
wait_namespace_deleted
kubectl create namespace "$NAMESPACE" >/dev/null
kubectl apply -f "$CHART/crds/rustqueue.io_rustqueues.yaml" >/dev/null
kubectl wait --for=condition=Established crd/rustqueues.rustqueue.io --timeout=60s
helm upgrade --install "$RELEASE" "$CHART" \
  --namespace "$NAMESPACE" \
  --set-string operator.image.repository="${OPERATOR_IMAGE%:*}" \
  --set-string operator.image.tag="${OPERATOR_IMAGE##*:}" \
  --set operator.image.pullPolicy=Never \
  --set-string queue.name="$QUEUE" \
  --set-string queue.image="$BROKER_IMAGE_A" \
  --set queue.imagePullPolicy=Never \
  --set queue.minBrokers=1 --set queue.maxBrokers=3 \
  --set-string queue.storageClassName="$STORAGE_CLASS" --set-string queue.storageSize=1Gi \
  --set queue.minFreeBytes=0 --set queue.protectiveEvictionEnabled=false \
  --set queue.bootstrapRetentionSeconds=1 \
  --wait --timeout 5m

wait_queue_ready 1 300
kubectl -n "$NAMESPACE" patch statefulset "$QUEUE" --type=merge \
  -p "{\"spec\":{\"template\":{\"spec\":{\"nodeName\":\"$NODE_NAME\"}}}}" >/dev/null
create_fake_nodes
bind_acceptance_pvcs
wait_queue_ready 3 360
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$QUEUE-0" pod/"$QUEUE-1" pod/"$QUEUE-2" --timeout=180s
[[ "$(kubectl -n "$NAMESPACE" get pvc -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o json | jq '.items | length')" == "3" ]] || {
  echo "three broker PVCs were not created" >&2
  exit 1
}

uids_before=$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
  -o json | jq -r '.items | sort_by(.metadata.name) | map(.metadata.uid) | join(",")')
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"storageFeatureLevel":2}}' >/dev/null
wait_queue_phase PreflightBlocked 120
uids_after=$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
  -o json | jq -r '.items | sort_by(.metadata.name) | map(.metadata.uid) | join(",")')
[[ "$uids_before" == "$uids_after" ]] || {
  echo "capability preflight replaced a broker before rejecting the feature" >&2
  exit 1
}
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"storageFeatureLevel":1}}' >/dev/null
wait_queue_ready 3 120

start_operational_ledger
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"image\":\"$BROKER_IMAGE_B\"}}" >/dev/null

saw_rolling=0
deadline=$((SECONDS + 420))
while (( SECONDS < deadline )); do
  phase=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  observed=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
  generation=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
  [[ "$phase" == "Rolling" ]] && saw_rolling=1
  if [[ "$phase" == "Ready" && "$observed" == "$generation" ]]; then break; fi
  sleep 2
done
[[ "$saw_rolling" == "1" && "$phase" == "Ready" ]] || {
  echo "operator did not complete a visible multi-broker rolling replacement" >&2
  exit 1
}
if ! wait_pod_succeeded operational-ledger 180; then
  kubectl -n "$NAMESPACE" logs operational-ledger || true
  exit 1
fi
kubectl -n "$NAMESPACE" logs operational-ledger
kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o json | \
  jq -e --arg image "$BROKER_IMAGE_B" \
    '.items | (length == 3) and all(.[]; all(.spec.containers[]; .image == $image))' >/dev/null

probe_metric broker-metrics "http://$QUEUE-0.$QUEUE-brokers:4151/metrics" rustqueue_storage_fsync_duration_seconds
probe_metric proxy-metrics "http://$QUEUE-proxy:4151/metrics" rustqueue_proxy_backend_duration_seconds
probe_metric discovery-metrics "http://$QUEUE-discovery:4161/metrics" rustqueue_discovery_registry_poll_duration_seconds

echo "OrbStack Kubernetes 3-broker rolling operations acceptance passed"
