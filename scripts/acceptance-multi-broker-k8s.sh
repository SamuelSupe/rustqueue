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
LEDGER_SECONDS="${LEDGER_SECONDS:-90}"
CHART="deploy/helm/rustqueue"
FAKE_NODES=(rustqueue-capacity-e2e-1 rustqueue-capacity-e2e-2)
LABELED_NODE=0
NODE_KEEPER=""
STORAGE_CLASS=""
STORAGE_CLASS_CREATED=0
CONSOLE_FORWARD_PID=""

source "$(dirname "$0")/lib/console-multi-owner.sh"
source "$(dirname "$0")/lib/token-rotation.sh"

require() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 1; }
}

diagnostics() {
  kubectl -n "$NAMESPACE" get rustqueue,pods,pvc,statefulset,deployment,daemonset -o wide || true
  kubectl -n "$NAMESPACE" describe rustqueue "$QUEUE" || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=operator --tail=300 || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=console --tail=300 || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=discovery --tail=300 || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=broker --tail=300 --prefix || true
  kubectl -n "$NAMESPACE" logs default-lookup-bootstrap --tail=300 || true
  kubectl -n "$NAMESPACE" logs operational-ledger --tail=300 || true
}

cleanup() {
    code=$?
    if [[ -n "$CONSOLE_FORWARD_PID" ]]; then
      kill "$CONSOLE_FORWARD_PID" >/dev/null 2>&1 || true
      wait "$CONSOLE_FORWARD_PID" 2>/dev/null || true
    fi
  if [[ -n "$NODE_KEEPER" ]]; then
    kill "$NODE_KEEPER" >/dev/null 2>&1 || true
    wait "$NODE_KEEPER" 2>/dev/null || true
  fi
  if [[ $code -ne 0 ]]; then diagnostics; fi
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 || true
    kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
    kubectl delete node "${FAKE_NODES[@]}" --ignore-not-found >/dev/null 2>&1 || true
    if [[ "$STORAGE_CLASS_CREATED" == "1" ]]; then
      kubectl delete storageclass "$STORAGE_CLASS" --ignore-not-found >/dev/null 2>&1 || true
    fi
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
  local brokers=$1 deadline=$((SECONDS + ${2:-360})) minimum_generation=${3:-0}
  local active_feature_level=${4:-}
  while (( SECONDS < deadline )); do
    local queue
    queue=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o json 2>/dev/null || true)
    if jq -e \
      --argjson brokers "$brokers" \
      --argjson minimum_generation "$minimum_generation" \
      --arg active_feature_level "$active_feature_level" \
      '(.metadata.generation // 0) >= $minimum_generation
        and .status.phase == "Ready"
        and .status.desiredBrokers == $brokers
        and .status.readyBrokers == $brokers
        and .status.observedGeneration == .metadata.generation
        and ($active_feature_level == ""
          or (.status.activeStorageFeatureLevel | tostring) == $active_feature_level)' \
      <<<"$queue" >/dev/null 2>&1; then
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

wait_operator_failover() {
  local previous=$1 deadline=$((SECONDS + ${2:-60})) holder
  while (( SECONDS < deadline )); do
    holder=$(kubectl -n "$NAMESPACE" get lease rustqueue-operator-leader \
      -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)
    if [[ -n "$holder" && "$holder" != "$previous" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "operator leader did not fail over from $previous" >&2
  return 1
}

wait_storage_ready() {
  local size=$1 deadline=$((SECONDS + ${2:-180})) ready requests
  while (( SECONDS < deadline )); do
    ready=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" \
      -o json | jq -r '.status.conditions[]? | select(.type == "StorageReady") | .status' | tail -1)
    requests=$(kubectl -n "$NAMESPACE" get pvc \
      -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
      -o json | jq -r --arg size "$size" '[.items[].spec.resources.requests.storage == $size] | all')
    if [[ "$ready" == "True" && "$requests" == "true" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "PVCs did not expand to $size" >&2
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

run_default_lookup_bootstrap() {
  local overrides
  overrides=$(jq -cn --arg image "$COMPAT_IMAGE" --arg secret "$QUEUE-auth" \
    '{apiVersion:"v1",spec:{containers:[{name:"default-lookup-bootstrap",image:$image,imagePullPolicy:"Never",args:["lookup-default-bootstrap","queue-discovery:4161","queue-0.queue-brokers:4151","queue-2.queue-brokers:4151"],env:[{name:"RUSTQUEUE_ADMIN_TOKEN",valueFrom:{secretKeyRef:{name:$secret,key:"admin-token"}}}]}]}}')
  kubectl -n "$NAMESPACE" delete pod default-lookup-bootstrap --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run default-lookup-bootstrap --restart=Never --image="$COMPAT_IMAGE" \
    --image-pull-policy=Never --overrides="$overrides" >/dev/null
  if ! wait_pod_succeeded default-lookup-bootstrap 150; then
    kubectl -n "$NAMESPACE" logs default-lookup-bootstrap --all-containers || true
    return 1
  fi
  kubectl -n "$NAMESPACE" logs default-lookup-bootstrap
}

run_proxy_rotation() {
  local overrides
  overrides=$(jq -cn --arg image "$COMPAT_IMAGE" \
    '{apiVersion:"v1",spec:{containers:[{name:"proxy-rotation",image:$image,imagePullPolicy:"Never",args:["proxy-rotation","queue-proxy:4150","queue-discovery:4161"]}]}}')
  kubectl -n "$NAMESPACE" delete pod proxy-rotation --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run proxy-rotation --restart=Never --image="$COMPAT_IMAGE" \
    --image-pull-policy=Never --overrides="$overrides" >/dev/null
  if ! wait_pod_succeeded proxy-rotation 60; then
    kubectl -n "$NAMESPACE" logs proxy-rotation --all-containers || true
    return 1
  fi
  kubectl -n "$NAMESPACE" logs proxy-rotation
}

select_expandable_storage_class() {
  local default_class provisioner reclaim_policy binding_mode
  default_class=$(kubectl get storageclass -o json | jq -r \
    '.items[] | select(.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true") | .metadata.name' | head -1)
  [[ -n "$default_class" ]] || {
    echo "OrbStack has no default StorageClass" >&2
    return 1
  }
  if [[ "$(kubectl get storageclass "$default_class" -o jsonpath='{.allowVolumeExpansion}')" == "true" ]]; then
    STORAGE_CLASS="$default_class"
    return 0
  fi

  provisioner=$(kubectl get storageclass "$default_class" -o jsonpath='{.provisioner}')
  [[ "$provisioner" == "rancher.io/local-path" ]] || {
    echo "default StorageClass $default_class cannot be cloned safely for expansion acceptance" >&2
    return 1
  }
  reclaim_policy=$(kubectl get storageclass "$default_class" -o jsonpath='{.reclaimPolicy}')
  binding_mode=$(kubectl get storageclass "$default_class" -o jsonpath='{.volumeBindingMode}')
  STORAGE_CLASS="${NAMESPACE}-expandable"
  kubectl apply -f - >/dev/null <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $STORAGE_CLASS
  labels:
    rustqueue.io/acceptance-storage-class: "true"
provisioner: $provisioner
reclaimPolicy: ${reclaim_policy:-Delete}
volumeBindingMode: ${binding_mode:-WaitForFirstConsumer}
allowVolumeExpansion: true
EOF
  STORAGE_CLASS_CREATED=1
}

complete_orbstack_local_path_resize() {
  local size=$1 deadline=$((SECONDS + 120)) requests reason claim volume
  [[ "$STORAGE_CLASS_CREATED" == "1" ]] || return 0
  while (( SECONDS < deadline )); do
    requests=$(kubectl -n "$NAMESPACE" get pvc \
      -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
      -o json | jq -r --arg size "$size" \
      '(.items | length) > 0 and ([.items[].spec.resources.requests.storage == $size] | all)')
    reason=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o json | jq -r \
      '.status.conditions[]? | select(.type == "StorageReady") | .reason' | tail -1)
    [[ "$requests" == "true" && "$reason" == "StorageResizing" ]] && break
    sleep 2
  done
  [[ "$requests" == "true" && "$reason" == "StorageResizing" ]] || {
    echo "operator did not request all local-path PVC expansions before provider acknowledgement" >&2
    return 1
  }

  # OrbStack local-path volumes are host directories without a quota, but the
  # provisioner has no CSI resizer. Emulate only its capacity acknowledgement
  # after the operator has issued and observed every PVC expansion request.
  for claim in $(kubectl -n "$NAMESPACE" get pvc \
    -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
    -o json | jq -r '.items[].metadata.name'); do
    volume=$(kubectl -n "$NAMESPACE" get pvc "$claim" -o jsonpath='{.spec.volumeName}')
    kubectl patch pv "$volume" --type=merge \
      -p "{\"spec\":{\"capacity\":{\"storage\":\"$size\"}}}" >/dev/null
    kubectl -n "$NAMESPACE" patch pvc "$claim" --subresource=status --type=merge \
      -p "{\"status\":{\"capacity\":{\"storage\":\"$size\"}}}" >/dev/null
  done
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
  BUILD_VERSION=0.8.0-e2e-a MAX_STORAGE_FEATURE_LEVEL=1 make image
  docker tag rustqueue:dev "$BROKER_IMAGE_A"
  BUILD_VERSION=0.8.0-e2e-b MAX_STORAGE_FEATURE_LEVEL=2 make image-from-dist
  docker tag rustqueue:dev "$BROKER_IMAGE_B"
  [[ "$(docker image inspect "$BROKER_IMAGE_A" -f '{{.Id}}')" != \
     "$(docker image inspect "$BROKER_IMAGE_B" -f '{{.Id}}')" ]] || {
    echo "acceptance images A and B are not distinct binaries" >&2
    exit 1
  }
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
select_expandable_storage_class

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
wait_namespace_deleted
kubectl create namespace "$NAMESPACE" >/dev/null
  kubectl apply -f "$CHART/crds" >/dev/null
  kubectl wait --for=condition=Established crd/rustqueues.rustqueue.io --timeout=60s
  kubectl wait --for=condition=Established crd/rustqueuetopics.rustqueue.io --timeout=60s
  kubectl wait --for=condition=Established crd/rustqueuechannels.rustqueue.io --timeout=60s
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
  --set queue.proxyTcpMaxConnectionAgeSeconds=2 \
  --set console.management.enabled=true --set console.pollIntervalSeconds=5 \
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
[[ "$(kubectl -n "$NAMESPACE" get pdb "$QUEUE-brokers" -o jsonpath='{.spec.minAvailable}')" == "2" ]] || {
  echo "broker disruption budget does not preserve two available Pods" >&2
  exit 1
}
[[ "$(kubectl -n "$NAMESPACE" get pdb -l app.kubernetes.io/component=operator -o json | jq '.items | length')" == "1" ]] || {
  echo "operator disruption budget was not installed" >&2
  exit 1
}

kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"storageSize":"2Gi"}}' >/dev/null
complete_orbstack_local_path_resize 2Gi
wait_storage_ready 2Gi 240

run_console_multi_owner_crash_acceptance

CONSOLE_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" \
  -o go-template='{{index .data "console-token" | base64decode}}')
head_observation=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS -H "Authorization: Bearer $CONSOLE_TOKEN" \
  http://127.0.0.1:4151/v1/observe/head)
[[ "$(jq -r 'has("queue") | not' <<<"$head_observation")" == "true" \
  && "$(jq -r '.delivery_budget.in_flight_bytes == 0' <<<"$head_observation")" == "true" ]] || {
  echo "lightweight Console observation unexpectedly included the queue catalog" >&2
  exit 1
}

ADMIN_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" \
  -o go-template='{{index .data "admin-token" | base64decode}}')
frozen=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"enabled":true,"freeze_deliveries":true}' \
  http://127.0.0.1:4151/v1/drain)
[[ "$(jq -r '.draining and .delivery_frozen and .quiesced' <<<"$frozen")" == "true" ]] || {
  echo "Broker did not reach a stable frozen rollout barrier" >&2
  exit 1
}
kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"enabled":false}' \
  http://127.0.0.1:4151/v1/drain >/dev/null
wait_queue_ready 3 120

ADMIN_TOKEN=$(accept_admin_token_rotation "$NAMESPACE" "$QUEUE" 3)

kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"maintenance\":{\"broker\":\"$QUEUE-2\",\"enabled\":true}}}" >/dev/null
wait_queue_phase Maintenance 120
[[ "$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.currentOperation.target}')" == "$QUEUE-2" ]] || {
  echo "targeted Broker maintenance was not persisted in status" >&2
  exit 1
}
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"maintenance\":{\"broker\":\"$QUEUE-2\",\"enabled\":false}}}" >/dev/null
wait_queue_ready 3 120
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"maintenance":null}}' >/dev/null
run_default_lookup_bootstrap
run_proxy_rotation

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
REGISTRY_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" \
  -o go-template='{{index .data "registry-token" | base64decode}}')
kubectl -n "$NAMESPACE" exec "$QUEUE-2" -c broker -- \
  curl -fsS -X POST --data-binary preserve-through-rollout \
  "http://127.0.0.1:4151/pub?topic=rollout_backlog" >/dev/null
[[ "$(kubectl -n "$NAMESPACE" exec "$QUEUE-2" -c broker -- \
  curl -fsS -H "Authorization: Bearer $REGISTRY_TOKEN" http://127.0.0.1:4151/v1/drain | jq -r '.stored_messages > 0')" == "true" ]] || {
  echo "failed to establish durable backlog before rollout" >&2
  exit 1
}
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"image\":\"$BROKER_IMAGE_B\",\"rollout\":{\"requireCanaryApproval\":true,\"approvedRevision\":null}}}" >/dev/null

wait_queue_phase RolloutAwaitingApproval 60
[[ "$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o json | jq --arg image "$BROKER_IMAGE_B" '[.items[].spec.containers[] | select(.name == "broker" and .image == $image)] | length')" == "1" ]] || {
  echo "rollout did not stop after exactly one canary Broker" >&2
  exit 1
}
[[ "$(kubectl -n "$NAMESPACE" exec "$QUEUE-2" -c broker -- \
  curl -fsS 'http://127.0.0.1:4151/v1/stats?topic=rollout_backlog' | jq -r '.topics[0].message_count > 0')" == "true" ]] || {
  echo "canary replacement did not preserve its durable backlog" >&2
  exit 1
}
leader=$(kubectl -n "$NAMESPACE" get lease rustqueue-operator-leader -o jsonpath='{.spec.holderIdentity}')
[[ -n "$leader" ]] || { echo "operator leader lease has no holder" >&2; exit 1; }
kubectl -n "$NAMESPACE" delete pod "$leader" --wait=false >/dev/null
wait_operator_failover "$leader" 90
wait_queue_phase RolloutAwaitingApproval 30
approved_revision=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.currentOperation.revision}')
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"rollout\":{\"approvedRevision\":\"$approved_revision\"}}}" >/dev/null

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

feature_generation=$(kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"storageFeatureLevel":2,"rollout":{"requireCanaryApproval":false,"approvedRevision":null}}}' \
  -o json | jq -r '.metadata.generation')
wait_queue_ready 3 420 "$feature_generation" 2

uids_before=$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
  -o json | jq -r '.items | sort_by(.metadata.name) | map(.metadata.uid) | join(",")')
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"rollout\":{\"rollbackToImage\":\"$BROKER_IMAGE_A\"}}}" >/dev/null
wait_queue_phase PreflightBlocked 120
uids_after=$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker \
  -o json | jq -r '.items | sort_by(.metadata.name) | map(.metadata.uid) | join(",")')
[[ "$uids_before" == "$uids_after" ]] || {
  echo "rollback fence replaced a Broker with an incompatible binary" >&2
  exit 1
}
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p '{"spec":{"rollout":{"rollbackToImage":null}}}' >/dev/null
wait_queue_ready 3 120

[[ "$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o json | jq '.status.operationHistory | length')" -ge 1 ]] || {
  echo "completed operations were not retained in bounded status history" >&2
  exit 1
}

probe_metric broker-metrics "http://$QUEUE-0.$QUEUE-brokers:4151/metrics" rustqueue_storage_fsync_duration_seconds
probe_metric broker-delivery-metrics "http://$QUEUE-0.$QUEUE-brokers:4151/metrics" rustqueue_delivery_inflight_bytes
probe_metric proxy-metrics "http://$QUEUE-proxy:4151/metrics" rustqueue_proxy_backend_duration_seconds
probe_metric proxy-rotation-metrics "http://$QUEUE-proxy:4151/metrics" rustqueue_proxy_tcp_connection_rotations_total
probe_metric discovery-metrics "http://$QUEUE-discovery:4161/metrics" rustqueue_discovery_registry_poll_duration_seconds
probe_metric discovery-timeout-metrics "http://$QUEUE-discovery:4161/metrics" rustqueue_discovery_endpoint_slice_timeouts_total

echo "OrbStack Kubernetes 3-broker rolling operations acceptance passed"
