#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue5rf5 -f $root/deploy/multinode-compose.yml"
ports="4151 5151 6151 7151 8151"

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  [ -z "${ledger_dir:-}" ] || rm -rf "$ledger_dir"
}
trap cleanup EXIT INT TERM

wait_all() {
  for _ in $(seq 1 90); do
    ready=0
    for port in $ports; do
      curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
    done
    [ "$ready" -eq 5 ] && return 0
    sleep 1
  done
  return 1
}

publish_acked() {
  topic=$1
  partition=$2
  body=$3
  for _ in $(seq 1 60); do
    for port in $ports; do
      if curl -fsS --connect-timeout 1 --max-time 5 -X POST --data-binary "$body" \
        "http://127.0.0.1:$port/pub?topic=$topic&partition=$partition" \
        >/dev/null 2>&1; then
        return 0
      fi
    done
    sleep 1
  done
  echo "no live gateway acknowledged topic=$topic partition=$partition" >&2
  return 1
}

CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster5-rf5-up
wait_all
curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"enabled":false}' \
  http://127.0.0.1:4151/v1/cluster/automation >/dev/null

rf3="rf3-$(date +%s)"
rf5="rf5-$(date +%s)"
channel=ledger
ledger_dir=$(mktemp -d)
rf3_ledger="$ledger_dir/rf3.txt"
rf5_ledger="$ledger_dir/rf5.txt"
: > "$rf3_ledger"
: > "$rf5_ledger"
curl -fsS -X POST "http://127.0.0.1:4151/topic/create?topic=$rf3&partitions=10&replication_factor=3" >/dev/null
curl -fsS -X POST "http://127.0.0.1:4151/topic/create?topic=$rf5&partitions=5&replication_factor=5" >/dev/null
curl -fsS -X POST "http://127.0.0.1:4151/channel/create?topic=$rf3&channel=$channel" >/dev/null
curl -fsS -X POST "http://127.0.0.1:4151/channel/create?topic=$rf5&channel=$channel" >/dev/null

node=1
while [ "$node" -le 5 ]; do
  COMPOSE_PROFILES=plus,five $compose stop "rustqueue-$node" >/dev/null
  sleep 3
  partition=0
  while [ "$partition" -lt 10 ]; do
    body="rf3-loss-$node-$partition"
    publish_acked "$rf3" "$partition" "$body"
    printf '%s\n' "$body" >> "$rf3_ledger"
    partition=$((partition + 1))
  done
  COMPOSE_PROFILES=plus,five $compose start "rustqueue-$node" >/dev/null
  wait_all
  node=$((node + 1))
done

pair=0
first=1
while [ "$first" -le 4 ]; do
  second=$((first + 1))
  while [ "$second" -le 5 ]; do
    COMPOSE_PROFILES=plus,five $compose stop "rustqueue-$first" "rustqueue-$second" >/dev/null
    sleep 4
    partition=0
    while [ "$partition" -lt 5 ]; do
      body="rf5-loss-$first-$second-$partition"
      publish_acked "$rf5" "$partition" "$body"
      printf '%s\n' "$body" >> "$rf5_ledger"
      partition=$((partition + 1))
    done
    COMPOSE_PROFILES=plus,five $compose start "rustqueue-$first" "rustqueue-$second" >/dev/null
    wait_all
    pair=$((pair + 1))
    second=$((second + 1))
  done
  first=$((first + 1))
done

[ "$pair" -eq 10 ]
[ "$(wc -l < "$rf3_ledger" | tr -d ' ')" -eq 50 ]
[ "$(wc -l < "$rf5_ledger" | tr -d ' ')" -eq 50 ]
run_ledger() {
  topic=$1
  expected=$2
  docker run --rm \
    -e GOTOOLCHAIN=local \
    -v "$root/tests/compat/go:/work:ro" \
    -v "$ledger_dir:/ledger:ro" \
    -w /tmp \
    golang:1.26-alpine \
    /bin/sh -c 'cp -R /work /tmp/compat && cd /tmp/compat && /usr/local/go/bin/go mod download && /usr/local/go/bin/go run . ledger "$1" "$2" "$3" "$4" "$5"' sh \
    host.docker.internal:4151 "$topic" "$channel" "/ledger/$(basename "$expected")" 120
}
rf3_report=$(run_ledger "$rf3" "$rf3_ledger")
rf5_report=$(run_ledger "$rf5" "$rf5_ledger")
[ "$(printf '%s' "$rf3_report" | jq -r '.missing')" -eq 0 ]
[ "$(printf '%s' "$rf5_report" | jq -r '.missing')" -eq 0 ]
jq -n \
  --argjson rf3 "$rf3_report" \
  --argjson rf5 "$rf5_report" \
  '{cluster:5,rf3_single_node_failures:5,rf5_two_node_pairs:10,rf3:$rf3,rf5:$rf5,missing:($rf3.missing + $rf5.missing)}'
