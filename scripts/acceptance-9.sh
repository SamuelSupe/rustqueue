#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue9 -f $root/deploy/multinode-compose.yml"
profiles=plus,five,nine

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=9 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

publish_with_retry() {
  partition=$1
  for _ in $(seq 1 30); do
    if curl -fsS -X POST --data-binary "node9-down-$partition" \
      "http://127.0.0.1:5151/pub?topic=$topic&partition=$partition" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

CLUSTER_SIZE=9 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster9-up

for _ in $(seq 1 120); do
  ready=0
  for port in 4151 5151 6151 7151 8151 9151 10151 11151 12151; do
    curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
  done
  [ "$ready" -eq 9 ] && break
  sleep 1
done
[ "${ready:-0}" -eq 9 ]

topic="accept9-$(date +%s)"
curl -fsS -X POST \
  "http://127.0.0.1:4151/topic/create?topic=$topic&partitions=18&replication_factor=3" >/dev/null
layout=$(curl -fsS "http://127.0.0.1:4151/v1/partitions?topic=$topic")
[ "$(printf '%s' "$layout" | jq '[.partitions[] | select(.lifecycle == "active")] | length')" -eq 18 ]
[ "$(printf '%s' "$layout" | jq '[.partitions[] | select((.replicas | length) != 3)] | length')" -eq 0 ]
[ "$(printf '%s' "$layout" | jq '[.partitions[].replicas[]] | unique | length')" -eq 9 ]
spread=$(printf '%s' "$layout" | jq '[.partitions[].replicas[]] | group_by(.) | map(length) | (max - min)')
[ "$spread" -le 1 ]

COMPOSE_PROFILES=$profiles $compose stop rustqueue-9 >/dev/null
sleep 4
for partition in 0 5 10 15; do
  publish_with_retry "$partition"
done
COMPOSE_PROFILES=$profiles $compose start rustqueue-9 >/dev/null

for _ in $(seq 1 90); do
  curl -fsS http://127.0.0.1:12151/v1/health >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS http://127.0.0.1:12151/v1/health >/dev/null
printf '%s\n' "{\"cluster\":9,\"rf\":3,\"partitions\":18,\"placement_spread\":$spread,\"single_node_outage\":true}"
