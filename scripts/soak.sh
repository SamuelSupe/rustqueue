#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
duration=${DURATION_SECONDS:-86400}
grace=${GRACE_SECONDS:-120}
rate=${RATE:-1000}
restart_interval=${RESTART_INTERVAL_SECONDS:-900}
restart_mode=${RESTART_MODE:-kill}
minimum_restarts=${MIN_RESTARTS:-0}
node_recovery=${NODE_RECOVERY_SECONDS:-60}
cluster_size=${CLUSTER_SIZE:-5}
project=${COMPOSE_PROJECT:-rustqueue5}
profiles=${COMPOSE_PROFILES:-plus,five}
name="rustqueue-soak-$$"

case "$restart_mode" in
  kill | graceful) ;;
  *)
    echo "RESTART_MODE must be kill or graceful" >&2
    exit 2
    ;;
esac
case "$minimum_restarts" in
  '' | *[!0-9]*)
    echo "MIN_RESTARTS must be a non-negative integer" >&2
    exit 2
    ;;
esac

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_node() {
  node_id=$1
  port=$((4151 + (node_id - 1) * 1000))
  for _ in $(seq 1 "$node_recovery"); do
    if curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "rustqueue-$node_id did not become healthy" >&2
  return 1
}

node=1
while [ "$node" -le "$cluster_size" ]; do
  wait_node "$node"
  node=$((node + 1))
done

docker run -d --name "$name" \
  -e GOTOOLCHAIN=local \
  -v "$root/tests/soak:/work" \
  -w /work \
  golang:1.26-alpine \
  /bin/sh -c '/usr/local/go/bin/go mod download && /usr/local/go/bin/go run . --lookup "$1" --tcp "$2" --duration "$3s" --grace "$4s" --rate "$5" --ready-file /tmp/rustqueue-soak-ready' sh \
  host.docker.internal:4151 \
  host.docker.internal:4150,host.docker.internal:5150,host.docker.internal:6150,host.docker.internal:7150,host.docker.internal:8150 \
  "$duration" "$grace" "$rate" >/dev/null

ready=0
for _ in $(seq 1 90); do
  if docker exec "$name" test -f /tmp/rustqueue-soak-ready >/dev/null 2>&1; then
    ready=1
    break
  fi
  [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)" = true ] || break
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  docker logs "$name"
  echo "soak harness did not become ready" >&2
  exit 1
fi

node=1
restart_count=0
if [ "$restart_interval" -gt 0 ]; then
  restart_deadline=$(($(date +%s) + duration))
  while [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)" = true ] \
    && [ "$(date +%s)" -lt "$restart_deadline" ]; do
    sleep "$restart_interval"
    [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)" = true ] || break
    [ "$(date +%s)" -lt "$restart_deadline" ] || break
    if [ "$restart_mode" = kill ]; then
      CLUSTER_SIZE="$cluster_size" COMPOSE_PROFILES="$profiles" \
        docker compose -p "$project" -f "$root/deploy/multinode-compose.yml" \
        kill -s KILL "rustqueue-$node"
      CLUSTER_SIZE="$cluster_size" COMPOSE_PROFILES="$profiles" \
        docker compose -p "$project" -f "$root/deploy/multinode-compose.yml" \
        start "rustqueue-$node"
    else
      CLUSTER_SIZE="$cluster_size" COMPOSE_PROFILES="$profiles" \
        docker compose -p "$project" -f "$root/deploy/multinode-compose.yml" \
        restart "rustqueue-$node"
    fi
    wait_node "$node"
    restart_count=$((restart_count + 1))
    node=$((node % cluster_size + 1))
  done
fi

if [ "$restart_count" -lt "$minimum_restarts" ]; then
  echo "soak completed only $restart_count restarts; expected at least $minimum_restarts" >&2
  exit 1
fi
echo "soak completed $restart_count $restart_mode restarts" >&2

status=$(docker wait "$name")
docker logs "$name"
[ "$status" -eq 0 ]
