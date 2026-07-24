#!/usr/bin/env bash

accept_admin_token_rotation() {
  local namespace=$1 queue=$2 brokers=$3
  local deployment replicas old_token new_token old_revision deadline code
  deployment=$(kubectl -n "$namespace" get deployment \
    -l app.kubernetes.io/component=operator \
    -o jsonpath='{.items[0].metadata.name}')
  replicas=$(kubectl -n "$namespace" get deployment "$deployment" -o jsonpath='{.spec.replicas}')
  old_token=$(kubectl -n "$namespace" get secret "$queue-auth" \
    -o go-template='{{index .data "admin-token" | base64decode}}')
  old_revision=$(kubectl -n "$namespace" get statefulset "$queue" \
    -o jsonpath='{.spec.template.metadata.annotations.rustqueue\.io/revision}')
  new_token="admin-rotation-$(date +%s)-$RANDOM"

  kubectl -n "$namespace" scale deployment "$deployment" --replicas=0 >/dev/null
  kubectl -n "$namespace" wait --for=delete pod \
    -l app.kubernetes.io/component=operator --timeout=90s >/dev/null
  kubectl -n "$namespace" patch secret "$queue-auth" --type=merge \
    -p "$(jq -cn --arg token "$new_token" '{stringData:{"admin-token":$token}}')" >/dev/null

  deadline=$((SECONDS + 120))
  code=""
  while (( SECONDS < deadline )); do
    code=$(kubectl -n "$namespace" exec "$queue-0" -c broker -- \
      curl -sS -o /dev/null -w '%{http_code}' \
      -X POST -H 'Content-Type: application/json' -d '{"enabled":false}' \
      -H "Authorization: Bearer $new_token" \
      http://127.0.0.1:4151/v1/drain 2>/dev/null || true)
    [[ "$code" == "200" ]] && break
    sleep 2
  done
  if [[ "$code" != "200" ]]; then
    kubectl -n "$namespace" scale deployment "$deployment" --replicas="$replicas" >/dev/null
    echo "running Broker did not hot-reload the rotated admin token" >&2
    return 1
  fi

  code=$(kubectl -n "$namespace" exec "$queue-0" -c broker -- \
    curl -sS -o /dev/null -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' -d '{"enabled":false}' \
    -H "Authorization: Bearer $old_token" \
    http://127.0.0.1:4151/v1/drain)
  if [[ "$code" != "401" ]]; then
    kubectl -n "$namespace" scale deployment "$deployment" --replicas="$replicas" >/dev/null
    echo "running Broker still accepted the old admin token: HTTP $code" >&2
    return 1
  fi

  kubectl -n "$namespace" scale deployment "$deployment" --replicas="$replicas" >/dev/null
  deadline=$((SECONDS + 420))
  while (( SECONDS < deadline )); do
    local phase desired ready observed generation target_revision converged
    phase=$(kubectl -n "$namespace" get rustqueue "$queue" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    desired=$(kubectl -n "$namespace" get rustqueue "$queue" -o jsonpath='{.status.desiredBrokers}' 2>/dev/null || true)
    ready=$(kubectl -n "$namespace" get rustqueue "$queue" -o jsonpath='{.status.readyBrokers}' 2>/dev/null || true)
    observed=$(kubectl -n "$namespace" get rustqueue "$queue" -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)
    generation=$(kubectl -n "$namespace" get rustqueue "$queue" -o jsonpath='{.metadata.generation}' 2>/dev/null || true)
    target_revision=$(kubectl -n "$namespace" get statefulset "$queue" \
      -o jsonpath='{.spec.template.metadata.annotations.rustqueue\.io/revision}' 2>/dev/null || true)
    converged=$(kubectl -n "$namespace" get pods \
      -l app.kubernetes.io/instance="$queue",app.kubernetes.io/component=broker \
      -o json 2>/dev/null | jq -r --arg revision "$target_revision" --argjson brokers "$brokers" \
      '(.items | length) == $brokers and all(.items[];
        .metadata.annotations["rustqueue.io/revision"] == $revision
        and any(.status.conditions[]?; .type == "Ready" and .status == "True"))' || true)
    if [[ -n "$target_revision" && "$target_revision" != "$old_revision" \
      && "$converged" == "true" \
      && "$phase" == "Ready" && "$desired" == "$brokers" && "$ready" == "$brokers" \
      && "$observed" == "$generation" ]]; then
      printf '%s\n' "$new_token"
      return 0
    fi
    sleep 2
  done
  echo "cluster did not recover after admin token rotation" >&2
  return 1
}
