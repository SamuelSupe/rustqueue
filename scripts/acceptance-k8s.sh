#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-rustqueue-k8s-e2e}"
RELEASE="${RELEASE:-rustqueue-e2e}"
CLUSTER="${CLUSTER:-queue}"
BROKER_IMAGE_A="${BROKER_IMAGE_A:-rustqueue:k8s-e2e-a}"
BROKER_IMAGE_B="${BROKER_IMAGE_B:-rustqueue:k8s-e2e-b}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-rustqueue-operator:k8s-e2e}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
CHART="deploy/helm/rustqueue"
LABELED_NODE=0
NODE_NAME=""

require() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 1; }
}

diagnostics() {
  kubectl -n "$NAMESPACE" get rustqueuecluster,pods,pvc,statefulset,service,pdb -o wide || true
  kubectl -n "$NAMESPACE" describe rustqueuecluster "$CLUSTER" || true
  kubectl -n "$NAMESPACE" logs deployment/"${RELEASE}-rustqueue-operator" --tail=300 || true
  for pod in $(kubectl -n "$NAMESPACE" get pods -l rustqueue.io/cluster="$CLUSTER" -o name 2>/dev/null); do
    kubectl -n "$NAMESPACE" logs "$pod" -c rustqueue --tail=120 || true
  done
}

cleanup() {
  code=$?
  if [[ $code -ne 0 ]]; then diagnostics; fi
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    helm uninstall "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1 || true
    kubectl delete namespace "$NAMESPACE" --wait=false >/dev/null 2>&1 || true
    if [[ "$LABELED_NODE" == "1" && -n "$NODE_NAME" ]]; then
      kubectl label node "$NODE_NAME" rustqueue.io/dedicated- >/dev/null 2>&1 || true
    fi
  else
    echo "KEEP_CLUSTER=1: preserving namespace $NAMESPACE"
  fi
  exit "$code"
}
trap cleanup EXIT

wait_cluster_ready() {
  local deadline=$((SECONDS + ${1:-600}))
  while (( SECONDS < deadline )); do
    local phase desired ready observed generation
    phase=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    desired=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || true)
    ready=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    observed=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
    generation=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
    if [[ "$phase" == "Ready" && "$desired" == "3" && "$ready" == "3" && "$observed" == "$generation" ]]; then
      return 0
    fi
    sleep 3
  done
  echo "RustQueueCluster did not become Ready" >&2
  return 1
}

wait_pod_succeeded() {
  local pod=$1 deadline=$((SECONDS + ${2:-180}))
  while (( SECONDS < deadline )); do
    phase=$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Succeeded" ]] && return 0
    [[ "$phase" == "Failed" ]] && return 1
    sleep 2
  done
  return 1
}

wait_pod_exists() {
  local pod=$1 deadline=$((SECONDS + ${2:-120}))
  while (( SECONDS < deadline )); do
    kubectl -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

wait_namespace_deleted() {
  local deadline=$((SECONDS + ${1:-180}))
  while (( SECONDS < deadline )); do
    kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || return 0
    sleep 2
  done
  echo "namespace $NAMESPACE did not finish terminating" >&2
  return 1
}

require kubectl
require helm
require docker
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
fi

NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
if [[ "$(kubectl get node "$NODE_NAME" -o jsonpath='{.metadata.labels.rustqueue\.io/dedicated}')" != "true" ]]; then
  kubectl label node "$NODE_NAME" rustqueue.io/dedicated=true >/dev/null
  LABELED_NODE=1
fi
STORAGE_CLASS=$(kubectl get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}')
[[ -n "$STORAGE_CLASS" ]] || { echo "OrbStack has no default StorageClass" >&2; exit 1; }

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null
wait_namespace_deleted 180
kubectl create namespace "$NAMESPACE" >/dev/null
kubectl apply -f "$CHART/crds/rustqueue.io_rustqueueclusters.yaml" >/dev/null
helm upgrade --install "$RELEASE" "$CHART" \
  --namespace "$NAMESPACE" \
  --set-string operator.image.repository="${OPERATOR_IMAGE%:*}" \
  --set-string operator.image.tag="${OPERATOR_IMAGE##*:}" \
  --set operator.image.pullPolicy=Never \
  --set-string cluster.name="$CLUSTER" \
  --set-string cluster.image="$BROKER_IMAGE_A" \
  --set cluster.imagePullPolicy=Never \
  --set-string cluster.storage.size=1Gi \
  --set cluster.storage.minFreeBytes=0 \
  --set cluster.nodes.dedicated=false \
  --set cluster.nodes.autoScaleFromNodes=true \
  --set-string "cluster.nodes.selector.kubernetes\\.io/hostname=$NODE_NAME" \
  --set cluster.development.allowSingleNode=true \
  --set cluster.development.virtualReplicas=3 \
  --set-string cluster.resources.cpuRequest=50m \
  --set-string cluster.resources.memoryRequest=128Mi \
  --set-string cluster.resources.cpuLimit=2 \
  --set-string cluster.resources.memoryLimit=1Gi \
  --wait --timeout 5m

wait_cluster_ready 600
PVC_CLASSES=$(kubectl -n "$NAMESPACE" get pvc -l rustqueue.io/cluster="$CLUSTER" -o jsonpath='{range .items[*]}{.spec.storageClassName}{"\n"}{end}' | sort -u)
[[ "$PVC_CLASSES" == "$STORAGE_CLASS" ]] || {
  echo "Operator did not select the single default StorageClass" >&2
  exit 1
}
echo "initial three-Broker virtual Cell is Ready"

kubectl -n "$NAMESPACE" delete pod rustqueue-client --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" run rustqueue-client \
  --restart=Never \
  --image="$BROKER_IMAGE_A" \
  --image-pull-policy=Never \
  --command -- /usr/local/bin/rustqueue-bench \
  --address "$CLUSTER.$NAMESPACE.svc:4150" \
  --topic k8s_acceptance --messages 200 --message-bytes 1024 \
  --producers 2 --consumers 2 --batch-size 8 --json
wait_pod_succeeded rustqueue-client 180
kubectl -n "$NAMESPACE" logs rustqueue-client

PVC_BEFORE=$(kubectl -n "$NAMESPACE" get pvc -l rustqueue.io/cluster="$CLUSTER" -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}' | sort)
kubectl -n "$NAMESPACE" delete pod "$CLUSTER-c1-n1-0" --wait=false
kubectl -n "$NAMESPACE" wait --for=delete pod/"$CLUSTER-c1-n1-0" --timeout=180s
wait_pod_exists "$CLUSTER-c1-n1-0" 120
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$CLUSTER-c1-n1-0" --timeout=300s
wait_cluster_ready 300
PVC_AFTER=$(kubectl -n "$NAMESPACE" get pvc -l rustqueue.io/cluster="$CLUSTER" -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}' | sort)
[[ "$PVC_BEFORE" == "$PVC_AFTER" ]] || { echo "PVC identity changed after Pod recreation" >&2; exit 1; }
echo "Pod recreation retained every PVC identity"

kubectl -n "$NAMESPACE" patch rustqueuecluster "$CLUSTER" --type=merge \
  -p "{\"spec\":{\"image\":\"$BROKER_IMAGE_B\"}}" >/dev/null
deadline=$((SECONDS + 600))
while (( SECONDS < deadline )); do
  desired=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || echo 0)
  ready=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  if [[ "$desired" == "3" && "$ready" =~ ^[0-9]+$ && "$ready" -lt 2 ]]; then
    echo "rolling upgrade violated maxUnavailablePerCell=1" >&2
    exit 1
  fi
  images=$(kubectl -n "$NAMESPACE" get pods -l rustqueue.io/cluster="$CLUSTER" -o jsonpath='{range .items[*]}{.spec.containers[?(@.name=="rustqueue")].image}{"\n"}{end}' 2>/dev/null | sort -u)
  phase=$(kubectl -n "$NAMESPACE" get rustqueuecluster "$CLUSTER" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  count=$(kubectl -n "$NAMESPACE" get pods -l rustqueue.io/cluster="$CLUSTER" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$images" == "$BROKER_IMAGE_B" && "$phase" == "Ready" && "$count" == "3" ]]; then
    break
  fi
  sleep 3
done
[[ "$images" == "$BROKER_IMAGE_B" && "$phase" == "Ready" && "$count" == "3" ]] || {
  echo "automatic rolling upgrade did not converge" >&2
  exit 1
}
echo "automatic one-at-a-time image rollout succeeded"

echo "OrbStack Kubernetes acceptance passed"
