#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-rustqueue-k8s-e2e}"
RELEASE="${RELEASE:-rustqueue-e2e}"
QUEUE="${QUEUE:-queue}"
BROKER_IMAGE_A="${BROKER_IMAGE_A:-rustqueue:k8s-e2e-a}"
BROKER_IMAGE_B="${BROKER_IMAGE_B:-rustqueue:k8s-e2e-b}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-rustqueue-operator:k8s-e2e}"
COMPAT_IMAGE="${COMPAT_IMAGE:-rustqueue-go-compat:k8s-e2e}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
CHART="deploy/helm/rustqueue"
LABELED_NODE=0
NODE_NAME=""
ADMIN_TOKEN=""

require() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 1; }
}

diagnostics() {
  kubectl -n "$NAMESPACE" get rustqueue,pods,pvc,statefulset,deployment,daemonset,service -o wide || true
  kubectl -n "$NAMESPACE" describe rustqueue "$QUEUE" || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/component=operator --tail=300 || true
  for pod in $(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o name 2>/dev/null); do
    kubectl -n "$NAMESPACE" logs "$pod" -c broker --tail=160 || true
  done
  for pod in $(kubectl -n "$NAMESPACE" get pods -o name 2>/dev/null); do
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers --tail=160 || true
  done
}

cleanup() {
  code=$?
  if [[ $code -ne 0 ]]; then diagnostics; fi
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 || true
    kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
    if [[ "$LABELED_NODE" == "1" && -n "$NODE_NAME" ]]; then
      kubectl label node "$NODE_NAME" rustqueue.io/eligible- >/dev/null 2>&1 || true
    fi
  else
    echo "KEEP_CLUSTER=1: preserving namespace $NAMESPACE"
  fi
  exit "$code"
}
trap cleanup EXIT

wait_namespace_deleted() {
  local deadline=$((SECONDS + ${1:-180}))
  while (( SECONDS < deadline )); do
    kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || return 0
    sleep 2
  done
  echo "namespace $NAMESPACE did not finish terminating" >&2
  return 1
}

wait_queue_ready() {
  local deadline=$((SECONDS + ${1:-300}))
  while (( SECONDS < deadline )); do
    local phase desired ready observed generation
    phase=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    desired=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.desiredBrokers}' 2>/dev/null || true)
    ready=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.readyBrokers}' 2>/dev/null || true)
    observed=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
    generation=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
    if [[ "$phase" == "Ready" && "$desired" == "1" && "$ready" == "1" && "$observed" == "$generation" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "RustQueue did not become Ready" >&2
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

run_compat() {
  local pod=$1
  shift
  local overrides
  overrides=$(jq -cn --arg name "$pod" --arg image "$COMPAT_IMAGE" \
    --arg secret "$QUEUE-auth" --args \
    '{apiVersion:"v1",spec:{containers:[{name:$name,image:$image,imagePullPolicy:"Never",args:$ARGS.positional,env:[{name:"RUSTQUEUE_ADMIN_TOKEN",valueFrom:{secretKeyRef:{name:$secret,key:"admin-token"}}}]}]}}' \
    "$@")
  kubectl -n "$NAMESPACE" delete pod "$pod" --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run "$pod" --restart=Never --image="$COMPAT_IMAGE" \
    --image-pull-policy=Never --overrides="$overrides" >/dev/null
  if ! wait_pod_succeeded "$pod" 300; then
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers || true
    return 1
  fi
  kubectl -n "$NAMESPACE" logs "$pod"
}

run_curl() {
  local pod=$1
  shift
  kubectl -n "$NAMESPACE" delete pod "$pod" --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" run "$pod" --restart=Never --image="$BROKER_IMAGE_A" \
    --image-pull-policy=Never --command -- curl "$@" >/dev/null
  if ! wait_pod_succeeded "$pod" 120; then
    kubectl -n "$NAMESPACE" logs "$pod" --all-containers || true
    return 1
  fi
  kubectl -n "$NAMESPACE" logs "$pod"
}

require kubectl
require helm
require docker
require jq
[[ "$(kubectl config current-context)" == "orbstack" ]] || {
  echo "acceptance-k8s.sh only runs against the OrbStack Kubernetes context" >&2
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

NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
if [[ "$(kubectl get node "$NODE_NAME" -o jsonpath='{.metadata.labels.rustqueue\.io/eligible}')" != "true" ]]; then
  kubectl label node "$NODE_NAME" rustqueue.io/eligible=true >/dev/null
  LABELED_NODE=1
fi
STORAGE_CLASS=$(kubectl get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}')
[[ -n "$STORAGE_CLASS" ]] || { echo "OrbStack has no default StorageClass" >&2; exit 1; }

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
wait_namespace_deleted 180
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
  --set queue.minBrokers=1 \
  --set queue.maxBrokers=1 \
  --set-string queue.storageClassName="$STORAGE_CLASS" \
  --set-string queue.storageSize=1Gi \
  --set queue.minFreeBytes=0 \
  --set queue.protectiveEvictionEnabled=false \
  --wait --timeout 5m

wait_queue_ready 300
kubectl -n "$NAMESPACE" rollout status deployment/"$QUEUE-discovery" --timeout=180s
kubectl -n "$NAMESPACE" rollout status daemonset/"$QUEUE-proxy" --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$QUEUE-0" --timeout=180s
PVC_BEFORE=$(kubectl -n "$NAMESPACE" get pvc -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}')
[[ -n "$PVC_BEFORE" ]] || { echo "operator did not create the broker PVC" >&2; exit 1; }

run_curl discovery-health -fsS "http://$QUEUE-discovery:4161/v1/health"
run_curl proxy-health -fsS "http://$QUEUE-proxy:4151/v1/health"

DIRECT_TCP="$QUEUE-0.$QUEUE-brokers:4150"
DIRECT_HTTP="$QUEUE-0.$QUEUE-brokers:4151"
PROXY_TCP="$QUEUE-proxy:4150"
PROXY_HTTP="$QUEUE-proxy:4151"
LOOKUP_HTTP="$QUEUE-discovery:4161"
run_compat go-direct core "$DIRECT_TCP" "$DIRECT_HTTP"
run_compat go-lookup lookup "$PROXY_TCP" "$PROXY_HTTP" "$LOOKUP_HTTP"

ADMIN_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" -o go-template='{{index .data "admin-token" | base64decode}}')
RECOVERY_TOPIC="pvc_recovery_$(date +%s)"
run_curl create-recovery-channel -fsS -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://$QUEUE-proxy:4151/channel/create?topic=$RECOVERY_TOPIC&channel=workers"
run_curl publish-recovery-message -fsS -X POST --data-binary survive-restart \
  "http://$QUEUE-proxy:4151/pub?topic=$RECOVERY_TOPIC"

POD_UID_BEFORE=$(kubectl -n "$NAMESPACE" get pod "$QUEUE-0" -o jsonpath='{.metadata.uid}')
kubectl -n "$NAMESPACE" delete pod "$QUEUE-0" --wait=false >/dev/null
kubectl -n "$NAMESPACE" wait --for=delete pod/"$QUEUE-0" --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$QUEUE-0" --timeout=300s
wait_queue_ready 300
POD_UID_AFTER=$(kubectl -n "$NAMESPACE" get pod "$QUEUE-0" -o jsonpath='{.metadata.uid}')
PVC_AFTER=$(kubectl -n "$NAMESPACE" get pvc -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}')
[[ "$POD_UID_BEFORE" != "$POD_UID_AFTER" ]] || { echo "broker Pod was not recreated" >&2; exit 1; }
[[ "$PVC_BEFORE" == "$PVC_AFTER" ]] || { echo "PVC identity changed after Pod recreation" >&2; exit 1; }
run_compat go-pvc-recovery consume-one "$DIRECT_TCP" "$RECOVERY_TOPIC" workers survive-restart

kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"image\":\"$BROKER_IMAGE_B\"}}" >/dev/null
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  phase=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  message=$(kubectl -n "$NAMESPACE" get rustqueue "$QUEUE" -o jsonpath='{.status.message}' 2>/dev/null || true)
  if [[ "$phase" == "Rolling" && "$message" == "rolling replacement needs at least two brokers" ]]; then break; fi
  sleep 2
done
[[ "$phase" == "Rolling" && "$message" == "rolling replacement needs at least two brokers" ]] || {
  echo "single-broker rolling safety gate was not enforced" >&2
  exit 1
}
[[ "$(kubectl -n "$NAMESPACE" get pod "$QUEUE-0" -o jsonpath='{.metadata.uid}')" == "$POD_UID_AFTER" ]] || {
  echo "operator replaced the only broker during an unsafe rollout" >&2
  exit 1
}
kubectl -n "$NAMESPACE" patch rustqueue "$QUEUE" --type=merge \
  -p "{\"spec\":{\"image\":\"$BROKER_IMAGE_A\"}}" >/dev/null
wait_queue_ready 300
run_curl proxy-health-after-recovery -fsS "http://$QUEUE-proxy:4151/v1/health"

echo "OrbStack Kubernetes share-nothing v7 acceptance passed"
