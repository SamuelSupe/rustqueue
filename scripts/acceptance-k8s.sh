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
CONSOLE_MANAGEMENT_ENABLED="${CONSOLE_MANAGEMENT_ENABLED:-false}"
CONSOLE_PORT="${CONSOLE_PORT:-14180}"
CHART="deploy/helm/rustqueue"
LABELED_NODE=0
NODE_NAME=""
ADMIN_TOKEN=""
CONSOLE_TOKEN=""
PORT_FORWARD_PID=""

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
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
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

wait_replacement_pod_ready() {
  local pod=$1 previous_uid=$2 deadline=$((SECONDS + ${3:-300})) document uid ready
  while (( SECONDS < deadline )); do
    document=$(kubectl -n "$NAMESPACE" get pod "$pod" -o json 2>/dev/null || true)
    if [[ -n "$document" ]]; then
      uid=$(jq -r '.metadata.uid // ""' <<<"$document")
      ready=$(jq -r 'any(.status.conditions[]?; .type == "Ready" and .status == "True")' <<<"$document")
      if [[ -n "$uid" && "$uid" != "$previous_uid" && "$ready" == "true" ]]; then
        return 0
      fi
    fi
    sleep 2
  done
  echo "replacement Pod $pod did not become Ready with a new UID" >&2
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

wait_managed_resource() {
  local resource=$1 topic=$2 channel=${3:-} phase=$4 deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if kubectl -n "$NAMESPACE" get "$resource" -o json | jq -e \
      --arg topic "$topic" --arg channel "$channel" --arg phase "$phase" \
      'any(.items[]; .spec.topic == $topic and (.spec.channel // "") == $channel and .spec.phase == $phase)' \
      >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "$resource $topic/$channel did not reach $phase" >&2
  return 1
}

wait_console_topic() {
  # The catalog refresh defaults to 30 seconds; allow scheduler/network slack.
  local base=$1 topic=$2 phase=$3 deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if curl -fsS "$base/api/v1/snapshot" | jq -e --arg topic "$topic" --arg phase "$phase" \
      'any(.topics[]; .name == $topic and .managed_phase == $phase)' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Console snapshot did not observe topic $topic in $phase" >&2
  return 1
}

wait_managed_topic_state() {
  local topic=$1 phase=$2 paused=$3 deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if kubectl -n "$NAMESPACE" get rustqueuetopics -o json | jq -e \
      --arg topic "$topic" --arg phase "$phase" --argjson paused "$paused" \
      'any(.items[]; .spec.topic == $topic and .spec.phase == $phase and .spec.paused == $paused)' \
      >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "managed topic $topic did not reach $phase paused=$paused" >&2
  return 1
}

wait_console_channel() {
  # Queue depth is refreshed on the bounded full-catalog interval, not the head poll.
  local base=$1 topic=$2 channel=$3 paused=$4 depth=${5:-} deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if curl -fsS "$base/api/v1/snapshot" | jq -e \
      --arg topic "$topic" --arg channel "$channel" --argjson paused "$paused" --arg depth "$depth" \
      'any(.topics[]; .name == $topic and any(.channels[]; .name == $channel and .managed_phase == "ACTIVE" and .paused == $paused and ($depth == "" or .depth == ($depth | tonumber))))' \
      >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Console snapshot did not observe channel $topic/$channel in the expected state" >&2
  return 1
}

run_console_management_acceptance() {
  local base="http://127.0.0.1:$CONSOLE_PORT" origin="http://127.0.0.1:$CONSOLE_PORT"
  local cookie="/tmp/${NAMESPACE}-console-cookie" log="/tmp/${NAMESPACE}-console-forward.log"
  local topic="console_topic_$(date +%s)" channel="workers"
  kubectl -n "$NAMESPACE" port-forward "svc/$QUEUE-console" "$CONSOLE_PORT:4180" >"$log" 2>&1 &
  PORT_FORWARD_PID=$!
  for _ in $(seq 1 40); do
    curl -fsS "$base/healthz" >/dev/null 2>&1 && break
    sleep 0.25
  done
  curl -fsS "$base/healthz" >/dev/null

  local bad_origin
  bad_origin=$(curl -sS -o /dev/null -w '%{http_code}' \
    -H 'Origin: http://attacker.invalid' -H 'Content-Type: application/json' \
    --data "{\"confirmation\":\"$NAMESPACE/$QUEUE\"}" \
    "$base/api/v1/management/unlock")
  [[ "$bad_origin" == "400" ]] || {
    echo "cross-origin management unlock returned HTTP $bad_origin" >&2
    return 1
  }

  local unlocked csrf
  unlocked=$(curl -fsS -c "$cookie" -H "Origin: $origin" \
    -H 'Content-Type: application/json' \
    --data "{\"confirmation\":\"$NAMESPACE/$QUEUE\"}" \
    "$base/api/v1/management/unlock")
  csrf=$(jq -er '.csrf_token' <<<"$unlocked")

  local no_csrf preview token apply_body replay_code
  no_csrf=$(curl -sS -b "$cookie" -o /dev/null -w '%{http_code}' \
    -H "Origin: $origin" -H 'Content-Type: application/json' \
    --data "$(jq -cn --arg topic "$topic" '{kind:"topic",action:"create",topic:$topic,channel:null}')" \
    "$base/api/v1/management/preview")
  [[ "$no_csrf" == "401" ]] || {
    echo "management preview without CSRF returned HTTP $no_csrf" >&2
    return 1
  }

  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" topic create "$topic" "")
  token=$(jq -er '.action_token' <<<"$preview")
  apply_body=$(console_apply_body topic create "$topic" "" "$token" "")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" --data "$apply_body" \
    "$base/api/v1/management/apply" >/dev/null
  replay_code=$(curl -sS -b "$cookie" -o /dev/null -w '%{http_code}' \
    -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" --data "$apply_body" \
    "$base/api/v1/management/apply")
  [[ "$replay_code" == "409" ]] || {
    echo "reused action token returned HTTP $replay_code" >&2
    return 1
  }
  wait_managed_resource rustqueuetopics "$topic" "" ACTIVE
  wait_console_topic "$base" "$topic" ACTIVE

  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" channel create "$topic" "$channel")
  token=$(jq -er '.action_token' <<<"$preview")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body channel create "$topic" "$channel" "$token" "")" \
    "$base/api/v1/management/apply" >/dev/null
  wait_managed_resource rustqueuechannels "$topic" "$channel" ACTIVE
  wait_console_channel "$base" "$topic" "$channel" false

  for action in pause unpause; do
    preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" channel "$action" "$topic" "$channel")
    token=$(jq -er '.action_token' <<<"$preview")
    curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
      -H "X-RustQueue-CSRF: $csrf" \
      --data "$(console_apply_body channel "$action" "$topic" "$channel" "$token" "")" \
      "$base/api/v1/management/apply" >/dev/null
    if [[ "$action" == "pause" ]]; then
      wait_console_channel "$base" "$topic" "$channel" true
    else
      wait_console_channel "$base" "$topic" "$channel" false
    fi
  done

  run_curl console-publish -fsS -X POST --data-binary managed-message \
    "http://$QUEUE-proxy:4151/pub?topic=$topic" >/dev/null
  wait_console_channel "$base" "$topic" "$channel" false 1
  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" channel empty "$topic" "$channel")
  token=$(jq -er '.action_token' <<<"$preview")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body channel empty "$topic" "$channel" "$token" "$channel")" \
    "$base/api/v1/management/apply" >/dev/null
  wait_console_channel "$base" "$topic" "$channel" false 0

  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" topic delete "$topic" "")
  token=$(jq -er '.action_token' <<<"$preview")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body topic delete "$topic" "" "$token" "$topic")" \
    "$base/api/v1/management/apply" >/dev/null
  wait_managed_resource rustqueuetopics "$topic" "" TOMBSTONED
  wait_console_topic "$base" "$topic" TOMBSTONED
  local tombstone_code
  tombstone_code=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
    curl -sS -o /dev/null -w '%{http_code}' -X POST --data-binary blocked \
    "http://127.0.0.1:4151/pub?topic=$topic")
  [[ "$tombstone_code" == "409" ]] || {
    echo "tombstoned topic publish returned HTTP $tombstone_code" >&2
    return 1
  }

  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" topic create "$topic" "")
  token=$(jq -er '.action_token' <<<"$preview")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body topic create "$topic" "" "$token" "")" \
    "$base/api/v1/management/apply" >/dev/null
  wait_managed_resource rustqueuetopics "$topic" "" ACTIVE
  wait_console_topic "$base" "$topic" ACTIVE
  run_curl console-publish-after-recreate -fsS -X POST --data-binary recreated \
    "http://$QUEUE-proxy:4151/pub?topic=$topic" >/dev/null

  # Simulate a Console crash after persisting the operation but before calling a broker.
  local operation_now resource
  operation_now=$(date +%s000)
  resource=$(kubectl -n "$NAMESPACE" get rustqueuetopics -o json | jq -er \
    --arg topic "$topic" '.items[] | select(.spec.topic == $topic) | .metadata.name')
  kubectl -n "$NAMESPACE" patch rustqueuetopic "$resource" --type merge \
    -p "$(jq -cn --arg id "acceptance-resume-$operation_now" --argjson now "$operation_now" \
      '{spec:{phase:"APPLYING",lastError:null,operation:{id:$id,action:"PAUSE",completedOwners:[],attempt:1,createdAtMs:$now,updatedAtMs:$now}}}')" \
    >/dev/null
  wait_managed_topic_state "$topic" ACTIVE true

  # A non-retryable failure keeps its operation ID and can be explicitly resumed.
  operation_now=$((operation_now + 1))
  kubectl -n "$NAMESPACE" patch rustqueuetopic "$resource" --type merge \
    -p "$(jq -cn --arg id "acceptance-retry-$operation_now" --argjson now "$operation_now" \
      '{spec:{phase:"FAILED",lastError:"acceptance injected failure",operation:{id:$id,action:"UNPAUSE",completedOwners:[],attempt:1,createdAtMs:$now,updatedAtMs:$now}}}')" \
    >/dev/null
  wait_console_topic "$base" "$topic" FAILED
  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" topic retry "$topic" "")
  token=$(jq -er '.action_token' <<<"$preview")
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body topic retry "$topic" "" "$token" "$topic")" \
    "$base/api/v1/management/apply" >/dev/null
  wait_managed_topic_state "$topic" ACTIVE false
  # The CRD can become ACTIVE just before the Console collector publishes its
  # next merged snapshot. Wait for the same state the preview API consumes so
  # the following step tests resource-version drift, not observation lag.
  wait_console_topic "$base" "$topic" ACTIVE

  preview=$(console_preview "$base" "$origin" "$cookie" "$csrf" topic pause "$topic" "")
  token=$(jq -er '.action_token' <<<"$preview")
  local drift_code
  resource=$(kubectl -n "$NAMESPACE" get rustqueuetopics -o json | jq -er \
    --arg topic "$topic" '.items[] | select(.spec.topic == $topic) | .metadata.name')
  kubectl -n "$NAMESPACE" annotate rustqueuetopic "$resource" \
    rustqueue.io/acceptance-drift="$(date +%s)" --overwrite >/dev/null
  drift_code=$(curl -sS -b "$cookie" -o /dev/null -w '%{http_code}' \
    -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(console_apply_body topic pause "$topic" "" "$token" "")" \
    "$base/api/v1/management/apply")
  [[ "$drift_code" == "409" ]] || {
    echo "resource-version drift returned HTTP $drift_code" >&2
    return 1
  }
  [[ -n "$(kubectl -n "$NAMESPACE" get events --field-selector reason=ConsoleManagementSucceeded -o name)" ]] || {
    echo "Console management did not emit an audit Event" >&2
    return 1
  }
}

console_preview() {
  local base=$1 origin=$2 cookie=$3 csrf=$4 kind=$5 action=$6 topic=$7 channel=$8
  curl -fsS -b "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json' \
    -H "X-RustQueue-CSRF: $csrf" \
    --data "$(jq -cn --arg kind "$kind" --arg action "$action" --arg topic "$topic" \
      --arg channel "$channel" '{kind:$kind,action:$action,topic:$topic,channel:(if $channel == "" then null else $channel end)}')" \
    "$base/api/v1/management/preview"
}

console_apply_body() {
  local kind=$1 action=$2 topic=$3 channel=$4 token=$5 confirmation=$6
  jq -cn --arg kind "$kind" --arg action "$action" --arg topic "$topic" \
    --arg channel "$channel" --arg token "$token" --arg confirmation "$confirmation" \
    '{kind:$kind,action:$action,topic:$topic,channel:(if $channel == "" then null else $channel end),action_token:$token,confirmation:$confirmation}'
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
  --set queue.minBrokers=1 \
  --set queue.maxBrokers=1 \
  --set-string queue.storageClassName="$STORAGE_CLASS" \
  --set-string queue.storageSize=1Gi \
  --set queue.minFreeBytes=0 \
  --set queue.protectiveEvictionEnabled=false \
  --set console.management.enabled="$CONSOLE_MANAGEMENT_ENABLED" \
  --wait --timeout 5m

wait_queue_ready 300
kubectl -n "$NAMESPACE" rollout status deployment/"$QUEUE-discovery" --timeout=180s
kubectl -n "$NAMESPACE" rollout status deployment/"$QUEUE-console" --timeout=180s
kubectl -n "$NAMESPACE" rollout status daemonset/"$QUEUE-proxy" --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$QUEUE-0" --timeout=180s
PVC_BEFORE=$(kubectl -n "$NAMESPACE" get pvc -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=broker -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}')
[[ -n "$PVC_BEFORE" ]] || { echo "operator did not create the broker PVC" >&2; exit 1; }

run_curl discovery-health -fsS "http://$QUEUE-discovery:4161/v1/health"
run_curl proxy-health -fsS "http://$QUEUE-proxy:4151/v1/health"
CONSOLE_SNAPSHOT=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS "http://$QUEUE-console:4180/api/v1/snapshot")
[[ "$(jq -r '.complete' <<<"$CONSOLE_SNAPSHOT")" == "true" ]] || {
  echo "Console returned an incomplete snapshot" >&2
  exit 1
}
[[ "$(jq -r '.brokers | length' <<<"$CONSOLE_SNAPSHOT")" == "1" ]] || {
  echo "Console did not observe the broker" >&2
  exit 1
}
[[ "$(jq '[paths | map(tostring) | join(".") | select(endswith(".body"))] | length' <<<"$CONSOLE_SNAPSHOT")" == "0" ]] || {
  echo "Console snapshot exposed a message body field" >&2
  exit 1
}

DIRECT_TCP="$QUEUE-0.$QUEUE-brokers:4150"
DIRECT_HTTP="$QUEUE-0.$QUEUE-brokers:4151"
PROXY_TCP="$QUEUE-proxy:4150"
PROXY_HTTP="$QUEUE-proxy:4151"
LOOKUP_HTTP="$QUEUE-discovery:4161"
run_compat go-direct core "$DIRECT_TCP" "$DIRECT_HTTP"
run_compat go-lookup lookup "$PROXY_TCP" "$PROXY_HTTP" "$LOOKUP_HTTP"

ADMIN_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" -o go-template='{{index .data "admin-token" | base64decode}}')
CONSOLE_TOKEN=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" -o go-template='{{index .data "console-token" | base64decode}}')
kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS -H "Authorization: Bearer $CONSOLE_TOKEN" \
  "http://127.0.0.1:4151/v1/observe" >/dev/null
CONSOLE_WRITE_CODE=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -sS -o /dev/null -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $CONSOLE_TOKEN" -H 'Content-Type: application/json' \
  --data '{"enabled":true}' "http://127.0.0.1:4151/v1/drain")
[[ "$CONSOLE_WRITE_CODE" == "401" ]] || {
  echo "console token unexpectedly authorized a drain write: HTTP $CONSOLE_WRITE_CODE" >&2
  exit 1
}
MANAGEMENT_STATUS=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
  curl -fsS "http://$QUEUE-console:4180/api/v1/management")
if [[ "$CONSOLE_MANAGEMENT_ENABLED" == "true" ]]; then
  [[ "$(jq -r '.enabled' <<<"$MANAGEMENT_STATUS")" == "true" ]] || {
    echo "Console management was requested but is disabled" >&2
    exit 1
  }
  run_console_management_acceptance
else
  [[ "$(jq -r '.enabled' <<<"$MANAGEMENT_STATUS")" == "false" ]] || {
    echo "Console management must be disabled by default" >&2
    exit 1
  }
  MANAGEMENT_ROUTE_CODE=$(kubectl -n "$NAMESPACE" exec "$QUEUE-0" -c broker -- \
    curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $CONSOLE_TOKEN" -H 'Content-Type: application/json' \
    --data '{"topic":"disabled","expected_revision":1}' \
    "http://127.0.0.1:4151/v1/manage/topics/create")
  [[ "$MANAGEMENT_ROUTE_CODE" == "404" ]] || {
    echo "disabled broker management route returned HTTP $MANAGEMENT_ROUTE_CODE" >&2
    exit 1
  }
fi
RECOVERY_TOPIC="pvc_recovery_$(date +%s)"
run_curl create-recovery-channel -fsS -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://$QUEUE-proxy:4151/channel/create?topic=$RECOVERY_TOPIC&channel=workers"
run_curl publish-recovery-message -fsS -X POST --data-binary survive-restart \
  "http://$QUEUE-proxy:4151/pub?topic=$RECOVERY_TOPIC"

POD_UID_BEFORE=$(kubectl -n "$NAMESPACE" get pod "$QUEUE-0" -o jsonpath='{.metadata.uid}')
kubectl -n "$NAMESPACE" delete pod "$QUEUE-0" --wait=false >/dev/null
wait_replacement_pod_ready "$QUEUE-0" "$POD_UID_BEFORE" 300
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
  if [[ "$phase" == "RolloutBlocked" && "$message" == "rolling replacement needs at least two brokers" ]]; then break; fi
  sleep 2
done
[[ "$phase" == "RolloutBlocked" && "$message" == "rolling replacement needs at least two brokers" ]] || {
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
