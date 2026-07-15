#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue4 -f $root/deploy/multinode-compose.yml"

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

wait_operation() {
  base=$1
  operation_id=$2
  for _ in $(seq 1 180); do
    state=$(curl -fsS "$base/v1/cluster/operations/$operation_id" | jq -r '.state')
    [ "$state" = completed ] && return 0
    if [ "$state" = needs_operator ] || [ "$state" = cancelled ]; then
      curl -fsS "$base/v1/cluster/operations/$operation_id" >&2
      return 1
    fi
    sleep 1
  done
  return 1
}

wait_ready() {
  for _ in $(seq 1 60); do
    ready=0
    for port in 4151 5151 6151 7151; do
      curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
    done
    [ "$ready" -eq 4 ] && return 0
    sleep 1
  done
  return 1
}

CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster4-up
wait_ready
curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"enabled":false}' \
  http://127.0.0.1:5151/v1/cluster/automation >/dev/null

curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"id":4,"raft_address":"https://rustqueue-4:4250","broadcast_address":"host.docker.internal","tcp_port":7150,"http_port":7151,"tls_server_name":"rustqueue-4","failure_domain":"zone-4"}' \
  http://127.0.0.1:5151/v1/cluster/node/add >/dev/null

metadata_leader=$(curl -fsS http://127.0.0.1:5151/v1/cluster | jq -r '.current_leader')
leadership_target=2
[ "$metadata_leader" -eq 2 ] && leadership_target=3
COMPOSE_PROFILES=plus $compose stop "rustqueue-$leadership_target" >/dev/null
transfer=$(curl -fsS -X POST \
  "http://127.0.0.1:4151/v1/cluster/transfer-leader?group_id=0&node_id=$leadership_target")
transfer_id=$(printf '%s' "$transfer" | jq -r '.operation_id')
retry_observed=0
for _ in $(seq 1 20); do
  operation=$(curl -fsS "http://127.0.0.1:4151/v1/cluster/operations/$transfer_id")
  state=$(printf '%s' "$operation" | jq -r '.state')
  [ "$state" = needs_operator ] && { printf '%s\n' "$operation" >&2; exit 1; }
  if [ "$(printf '%s' "$operation" | jq -r '.error != null')" = true ]; then
    [ "$state" = running ]
    retry_observed=1
    break
  fi
  sleep 1
done
[ "$retry_observed" -eq 1 ]
COMPOSE_PROFILES=plus $compose start "rustqueue-$leadership_target" >/dev/null
wait_operation http://127.0.0.1:4151 "$transfer_id"
[ "$(curl -fsS http://127.0.0.1:4151/v1/cluster | jq -r '.current_leader')" -eq "$leadership_target" ]
wait_ready

docker run --rm -v "$root:/work:ro" -w /work python:3.12-alpine \
  python tests/acceptance/ephemeral.py

topic="accept4-$(date +%s)"
curl -fsS -X POST \
  "http://127.0.0.1:4151/topic/create?topic=$topic&partitions=8&replication_factor=3" >/dev/null
for partition in $(seq 0 7); do
  curl -fsS -X POST --data-binary "before-drain-$partition" \
    "http://127.0.0.1:4151/pub?topic=$topic&partition=$partition" >/dev/null
done

phase_partition=$(curl -fsS "http://127.0.0.1:5151/v1/partitions?topic=$topic" | jq -c '.partitions[0]')
phase_group=$(printf '%s' "$phase_partition" | jq -r '.group_id')
phase_replicas=$(printf '%s' "$phase_partition" | jq -c '.replicas')
phase_number=$(printf '%s' "$phase_partition" | jq -r '.partition')
for index in $(seq 1 50); do
  curl -fsS -X POST --data-binary "snapshot-payload-$index" \
    "http://127.0.0.1:5151/pub?topic=$topic&partition=$phase_number" >/dev/null
done
snapshot_built=0
for node_id in $(printf '%s' "$phase_replicas" | jq -r '.[]'); do
  port=$((4151 + (node_id - 1) * 1000))
  if curl -fsS -X POST \
    "http://127.0.0.1:$port/v1/cluster/snapshot?group_id=$phase_group" >/dev/null 2>&1; then
    snapshot_built=1
    break
  fi
done
[ "$snapshot_built" -eq 1 ]
phase_removed=$(printf '%s' "$phase_replicas" | jq -r 'if index(1) then 1 else .[0] end')
phase_replacement=$(printf '%s' "$phase_replicas" | jq -r '[1,2,3,4] - . | .[0]')
phase_voters=$(printf '%s' "$phase_replicas" | jq -c \
  --argjson removed "$phase_removed" --argjson replacement "$phase_replacement" \
  'map(select(. != $removed)) + [$replacement] | sort')
phase_operation=$(curl -fsS -X POST -H 'content-type: application/json' \
  -d "{\"group_id\":$phase_group,\"voters\":$phase_voters,\"retain_removed_as_learners\":true}" \
  http://127.0.0.1:4151/v1/cluster/rebalance)
phase_operation_id=$(printf '%s' "$phase_operation" | jq -r '.operation_id')
seen_transfer=0
seen_add=0
seen_catchup=0
seen_joint=0
seen_remove=0
seen_retire=0
for _ in $(seq 1 180); do
  operation=$(curl -fsS "http://127.0.0.1:5151/v1/cluster/operations/$phase_operation_id")
  state=$(printf '%s' "$operation" | jq -r '.state')
  phase=$(printf '%s' "$operation" | jq -r '.phase')
  case "$phase" in
    transfer_leader) seen_transfer=1 ;;
    add_learner) seen_add=1 ;;
    catch_up) seen_catchup=1 ;;
    joint_consensus) seen_joint=1 ;;
    remove_old) seen_remove=1 ;;
    retire) seen_retire=1 ;;
  esac
  [ "$state" = completed ] && break
  if [ "$state" = needs_operator ] || [ "$state" = cancelled ]; then
    printf '%s\n' "$operation" >&2
    exit 1
  fi
  sleep 1
done
[ "$seen_transfer$seen_add$seen_catchup$seen_joint$seen_remove$seen_retire" = 111111 ]
[ "$state" = completed ]
planned=$(curl -fsS -X POST http://127.0.0.1:5151/v1/cluster/rebalance/run)
[ "$(printf '%s' "$planned" | jq '.operation_ids | length')" -ge 1 ]
operation_ids=$(printf '%s' "$planned" | jq -r '.operation_ids[]')
for operation_id in $operation_ids; do
  wait_operation http://127.0.0.1:5151 "$operation_id"
done

drain=$(curl -fsS -X POST "http://127.0.0.1:4151/v1/cluster/drain?node_id=1")
drain_id=$(printf '%s' "$drain" | jq -r '.operation_id')
drain_interrupted=0
for _ in $(seq 1 180); do
  operation=$(curl -fsS "http://127.0.0.1:5151/v1/cluster/operations/$drain_id")
  state=$(printf '%s' "$operation" | jq -r '.state')
  current=$(printf '%s' "$operation" | jq -r \
    '.progress | if type == "object" and has("drain") then .drain.current else -1 end')
  group_count=$(printf '%s' "$operation" | jq -r \
    '.progress | if type == "object" and has("drain") then (.drain.groups | length) else 0 end')
  if [ "$current" -ge 1 ] && [ "$current" -lt "$group_count" ]; then
    interrupted_current=$current
    interrupted_phase=$(printf '%s' "$operation" | jq -r '.phase')
    drain_interrupted=1
    break
  fi
  [ "$state" = needs_operator ] && { printf '%s\n' "$operation" >&2; exit 1; }
  sleep 1
done
[ "$drain_interrupted" -eq 1 ]
metadata_leader=$(curl -fsS http://127.0.0.1:5151/v1/cluster | jq -r '.current_leader')
resume_node=$((metadata_leader % 4 + 1))
resume_port=$((4151 + (resume_node - 1) * 1000))
COMPOSE_PROFILES=plus $compose kill -s KILL "rustqueue-$metadata_leader" >/dev/null
sleep 4
COMPOSE_PROFILES=plus $compose start "rustqueue-$metadata_leader" >/dev/null
drain_resumed=0
for _ in $(seq 1 90); do
  operation=$(curl -fsS "http://127.0.0.1:$resume_port/v1/cluster/operations/$drain_id" 2>/dev/null || true)
  [ -n "$operation" ] || { sleep 1; continue; }
  state=$(printf '%s' "$operation" | jq -r '.state')
  current=$(printf '%s' "$operation" | jq -r \
    '.progress | if type == "object" and has("drain") then .drain.current else -1 end')
  phase=$(printf '%s' "$operation" | jq -r '.phase')
  [ "$current" -ge "$interrupted_current" ]
  if [ "$state" = completed ] || [ "$current" -gt "$interrupted_current" ] || [ "$phase" != "$interrupted_phase" ]; then
    drain_resumed=1
    break
  fi
  [ "$state" = needs_operator ] && { printf '%s\n' "$operation" >&2; exit 1; }
  sleep 1
done
[ "$drain_resumed" -eq 1 ]
wait_operation "http://127.0.0.1:$resume_port" "$drain_id"
operation=$(curl -fsS "http://127.0.0.1:$resume_port/v1/cluster/operations/$drain_id")
[ "$(printf '%s' "$operation" | jq -r '.progress.drain.current')" -eq "$(printf '%s' "$operation" | jq -r '.progress.drain.groups | length')" ]
[ "$(printf '%s' "$operation" | jq -r '.progress.drain.metadata_completed')" = true ]
partitions=$(curl -fsS "http://127.0.0.1:5151/v1/partitions?topic=$topic")
[ "$(printf '%s' "$partitions" | jq '[.partitions[] | select(.replicas | index(1))] | length')" -eq 0 ]
[ "$(curl -fsS http://127.0.0.1:5151/v1/cluster | jq '.drained_nodes == [1]')" = true ]

after="after-drain-$(date +%s)"
curl -fsS -X POST \
  "http://127.0.0.1:5151/topic/create?topic=$after&partitions=8&replication_factor=3" >/dev/null
new_partitions=$(curl -fsS "http://127.0.0.1:5151/v1/partitions?topic=$after")
[ "$(printf '%s' "$new_partitions" | jq '[.partitions[] | select(.replicas | index(1))] | length')" -eq 0 ]

sleep 6
[ "$(curl -fsS http://127.0.0.1:5151/nodes | jq '.producers | length')" -eq 3 ]
curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"enabled":true,"ttl_seconds":120,"reason":"acceptance restart"}' \
  http://127.0.0.1:5151/v1/cluster/nodes/4/maintenance >/dev/null
COMPOSE_PROFILES=plus $compose stop rustqueue-4 >/dev/null
sleep 6
[ "$(curl -fsS http://127.0.0.1:5151/nodes | jq '.producers | length')" -eq 2 ]
curl -fsS -X POST --data-binary during-outage \
  "http://127.0.0.1:6151/pub?topic=$after&partition=0" >/dev/null
COMPOSE_PROFILES=plus $compose start rustqueue-4 >/dev/null

for _ in $(seq 1 60); do
  curl -fsS http://127.0.0.1:7151/v1/health >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"enabled":false,"reason":"acceptance complete"}' \
  http://127.0.0.1:5151/v1/cluster/nodes/4/maintenance >/dev/null
group_id=$(printf '%s' "$new_partitions" | jq -r '.partitions[0].group_id')
repair=$(curl -fsS -X POST "http://127.0.0.1:5151/v1/replicas/$group_id/3/repair")
wait_operation http://127.0.0.1:5151 "$(printf '%s' "$repair" | jq -r '.operation_id')"
scrub=$(curl -fsS -X POST http://127.0.0.1:5151/v1/storage/scrub)
[ "$(printf '%s' "$scrub" | jq -r '.status')" = ok ]
metrics=$(curl -fsS http://127.0.0.1:5151/metrics)
for metric in fsync group_commit forward snapshot_build snapshot_install gc repair; do
  case "$metrics" in
    *"rustqueue_${metric}_duration_seconds"*) ;;
    *) echo "missing latency metric: $metric" >&2; exit 1 ;;
  esac
done

printf '%s\n' '{"cluster":4,"rf":3,"node_add":true,"leader_transfer_retry":true,"membership_phases":true,"rebalance_plan":true,"snapshot_linked_segments":true,"drain":true,"drain_resume_after_leader_kill":true,"outage":true,"repair":true,"scrub":true,"ephemeral":true,"latency_metrics":true}'
