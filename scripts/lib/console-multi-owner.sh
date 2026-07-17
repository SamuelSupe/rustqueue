#!/usr/bin/env bash

console_request() {
  local method=$1 url=$2 cookie=$3 origin=$4 csrf=${5:-} body=${6:-}
  local args=(-fsS -X "$method" -b "$cookie" -c "$cookie" -H "Origin: $origin" -H 'Content-Type: application/json')
  [[ -n "$csrf" ]] && args+=(-H "X-RustQueue-CSRF: $csrf")
  [[ -n "$body" ]] && args+=(--data "$body")
  curl "${args[@]}" "$url"
}

wait_managed_topic() {
  local topic=$1 phase=$2 paused=$3 deadline=$((SECONDS + ${4:-120}))
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

wait_topic_owners() {
  local topic=$1 expected=$2 deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if kubectl -n "$NAMESPACE" get rustqueuetopics -o json | jq -e \
      --arg topic "$topic" --argjson expected "$expected" \
      'any(.items[]; .spec.topic == $topic and (.spec.owners | length) == $expected)' \
      >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "managed topic $topic did not expose $expected owners" >&2
  return 1
}

wait_one_completed_owner() {
  local resource=$1 deadline=$((SECONDS + 60)) count
  while (( SECONDS < deadline )); do
    count=$(kubectl -n "$NAMESPACE" get rustqueuetopic "$resource" -o json 2>/dev/null | \
      jq -r '.spec.operation.completedOwners | length' 2>/dev/null || true)
    [[ "$count" == "1" ]] && return 0
    [[ "$count" =~ ^[2-9] ]] && {
      echo "Console advanced past the single-owner crash boundary" >&2
      return 1
    }
    sleep 0.1
  done
  echo "Console did not persist the first completed owner" >&2
  return 1
}

run_console_multi_owner_crash_acceptance() {
  local port=${CONSOLE_PORT:-14182}
  local base="http://127.0.0.1:$port" origin="http://127.0.0.1:$port"
  local cookie="/tmp/${NAMESPACE}-multi-console-cookie"
  local log="/tmp/${NAMESPACE}-multi-console-forward.log"
  local topic="console_multi_$(date +%s)"

  kubectl -n "$NAMESPACE" port-forward "svc/$QUEUE-console" "$port:4180" >"$log" 2>&1 &
  CONSOLE_FORWARD_PID=$!
  for _ in $(seq 1 80); do
    curl -fsS "$base/healthz" >/dev/null 2>&1 && break
    sleep 0.25
  done
  curl -fsS "$base/healthz" >/dev/null

  local unlocked csrf preview token apply resource
  unlocked=$(console_request POST "$base/api/v1/management/unlock" "$cookie" "$origin" "" \
    "$(jq -cn --arg confirmation "$NAMESPACE/$QUEUE" '{confirmation:$confirmation}')")
  csrf=$(jq -er '.csrf_token' <<<"$unlocked")
  preview=$(console_request POST "$base/api/v1/management/preview" "$cookie" "$origin" "$csrf" \
    "$(jq -cn --arg topic "$topic" '{kind:"topic",action:"create",topic:$topic,channel:null}')")
  token=$(jq -er '.action_token' <<<"$preview")
  apply=$(jq -cn --arg topic "$topic" --arg token "$token" \
    '{kind:"topic",action:"create",topic:$topic,channel:null,action_token:$token,confirmation:""}')
  console_request POST "$base/api/v1/management/apply" "$cookie" "$origin" "$csrf" "$apply" >/dev/null
  wait_managed_topic "$topic" ACTIVE false

  local admin_token ordinal
  admin_token=$(kubectl -n "$NAMESPACE" get secret "$QUEUE-auth" \
    -o go-template='{{index .data "admin-token" | base64decode}}')
  for ordinal in 0 1 2; do
    kubectl -n "$NAMESPACE" exec "$QUEUE-$ordinal" -c broker -- \
      curl -fsS -X POST -H "Authorization: Bearer $admin_token" \
      "http://127.0.0.1:4151/topic/create?topic=$topic" >/dev/null
  done
  wait_topic_owners "$topic" 3
  local owners_ready=0
  for _ in $(seq 1 60); do
    if curl -fsS "$base/api/v1/snapshot" | jq -e --arg topic "$topic" \
      'any(.topics[]; .name == $topic and .managed_phase == "ACTIVE" and (.owners | length) == 3)' \
      >/dev/null; then
      owners_ready=1
      break
    fi
    sleep 1
  done
  [[ "$owners_ready" == "1" ]] || {
    echo "Console snapshot did not converge on all three topic owners" >&2
    return 1
  }

  preview=$(console_request POST "$base/api/v1/management/preview" "$cookie" "$origin" "$csrf" \
    "$(jq -cn --arg topic "$topic" '{kind:"topic",action:"pause",topic:$topic,channel:null}')")
  [[ "$(jq -r '.impact.owners | length' <<<"$preview")" == "3" ]] || {
    echo "Console pause preview did not target all three owners" >&2
    return 1
  }
  token=$(jq -er '.action_token' <<<"$preview")
  apply=$(jq -cn --arg topic "$topic" --arg token "$token" \
    '{kind:"topic",action:"pause",topic:$topic,channel:null,action_token:$token,confirmation:""}')
  console_request POST "$base/api/v1/management/apply" "$cookie" "$origin" "$csrf" "$apply" >/dev/null
  resource=$(kubectl -n "$NAMESPACE" get rustqueuetopics -o json | jq -er \
    --arg topic "$topic" '.items[] | select(.spec.topic == $topic) | .metadata.name')
  wait_one_completed_owner "$resource"

  local console_pod old_uid
  console_pod=$(kubectl -n "$NAMESPACE" get pod \
    -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=console \
    -o jsonpath='{.items[0].metadata.name}')
  old_uid=$(kubectl -n "$NAMESPACE" get pod "$console_pod" -o jsonpath='{.metadata.uid}')
  kubectl -n "$NAMESPACE" delete pod "$console_pod" --grace-period=0 --force --wait=false >/dev/null
  kill "$CONSOLE_FORWARD_PID" >/dev/null 2>&1 || true
  wait "$CONSOLE_FORWARD_PID" 2>/dev/null || true
  CONSOLE_FORWARD_PID=""

  local deadline=$((SECONDS + 180)) new_uid=""
  while (( SECONDS < deadline )); do
    new_uid=$(kubectl -n "$NAMESPACE" get pod \
      -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=console \
      -o jsonpath='{.items[0].metadata.uid}' 2>/dev/null || true)
    [[ -n "$new_uid" && "$new_uid" != "$old_uid" ]] && break
    sleep 1
  done
  [[ -n "$new_uid" && "$new_uid" != "$old_uid" ]] || {
    echo "Console Pod was not recreated after forced deletion" >&2
    return 1
  }
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod \
    -l app.kubernetes.io/instance="$QUEUE",app.kubernetes.io/component=console --timeout=180s
  wait_managed_topic "$topic" ACTIVE true 180

  for ordinal in 0 1 2; do
    kubectl -n "$NAMESPACE" exec "$QUEUE-$ordinal" -c broker -- \
      curl -fsS "http://127.0.0.1:4151/stats?format=json&topic=$topic" | \
      jq -e --arg topic "$topic" 'any(.topics[]; .name == $topic and .paused == true)' >/dev/null
  done
  echo "Console multi-owner forced-crash recovery acceptance passed"
}
