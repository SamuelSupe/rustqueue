#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue-discovery -f $root/deploy/multinode-compose.yml"

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" dev-certs image
DISCOVERY_DYNAMIC=1 "$root/scripts/generate-cluster-configs.sh" 4

if grep -q '\[cluster.nodes.4\]' "$root/deploy/generated/4/node-1.toml"; then
  printf 'bootstrap node unexpectedly contains node 4 in its static config\n' >&2
  exit 1
fi

CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose up -d --no-build
for _ in $(seq 1 120); do
  ready=0
  for port in 4151 5151 6151 7151; do
    curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
  done
  if [ "$ready" -eq 4 ]; then
    break
  fi
  sleep 1
done
[ "${ready:-0}" -eq 4 ]

nodes=$(curl -fsS http://127.0.0.1:4151/v1/cluster/nodes)
[ "$(printf '%s' "$nodes" | jq '.nodes | length')" -eq 4 ]
peer_id=$(printf '%s' "$nodes" | jq -r '.nodes[] | select(.node.id == 4) | .node.peer_id')
[ -n "$peer_id" ] && [ "$peer_id" != null ]

for _ in $(seq 1 60); do
  nodes=$(curl -fsS http://127.0.0.1:4151/v1/cluster/nodes)
  if printf '%s' "$nodes" | jq -e '.nodes | length == 4 and all(.healthy)' >/dev/null; then
    break
  fi
  sleep 1
done
printf '%s' "$nodes" | jq -e '.nodes | length == 4 and all(.healthy)' >/dev/null

topic="discovery-$(date +%s)"
curl -fsS -X POST \
  "http://127.0.0.1:4151/topic/create?topic=$topic&partitions=8&replication_factor=3" >/dev/null
layout=$(curl -fsS "http://127.0.0.1:4151/v1/partitions?topic=$topic")
[ "$(printf '%s' "$layout" | jq '[.partitions[].replicas[]] | unique | length')" -eq 4 ]

COMPOSE_PROFILES=plus $compose restart rustqueue-4 >/dev/null
for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:7151/v1/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://127.0.0.1:7151/v1/health >/dev/null
after=$(curl -fsS http://127.0.0.1:4151/v1/cluster/nodes |
  jq -r '.nodes[] | select(.node.id == 4) | .node.peer_id')
[ "$after" = "$peer_id" ]

printf '%s\n' "{\"automatic_discovery\":true,\"nodes\":4,\"stable_peer_id\":\"$peer_id\",\"manual_add\":false}"
