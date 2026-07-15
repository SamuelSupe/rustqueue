#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue4 -f $root/deploy/multinode-compose.yml"

cleanup() {
  if [ -n "${sampler_pid:-}" ]; then
    kill "$sampler_pid" >/dev/null 2>&1 || true
    wait "$sampler_pid" >/dev/null 2>&1 || true
  fi
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster4-up

for _ in $(seq 1 120); do
  ready=0
  for port in 4151 5151 6151 7151; do
    curl -fsS "http://127.0.0.1:$port/ping" >/dev/null 2>&1 && ready=$((ready + 1))
  done
  if [ "$ready" -eq 4 ] && curl -fsS "http://127.0.0.1:4151/v1/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
[ "${ready:-0}" -eq 4 ]
curl -fsS "http://127.0.0.1:4151/v1/health" >/dev/null

result_dir="$root/benchmarks/results"
mkdir -p "$result_dir"
containers=$($compose ps -q)
sample_file="$result_dir/network-scale-$(date +%s).stats.tsv"
"$root/scripts/resource-sampler.sh" "$sample_file" $containers &
sampler_pid=$!

docker run --rm --network rustqueue4_default \
  -e HOST=rustqueue-1 \
  -e TCP_PORT=4150 \
  -e HTTP_ENDPOINTS=http://rustqueue-1:4151,http://rustqueue-2:4151,http://rustqueue-3:4151,http://rustqueue-4:4151 \
  -e PARTITIONS="${PARTITIONS:-1024}" \
  -e CONSUMERS="${CONSUMERS:-32}" \
  -e DURATION_SECONDS="${DURATION_SECONDS:-8}" \
  -v "$root:/work:ro" -w /work python:3.12-alpine \
  python tests/acceptance/network_scale.py
kill "$sampler_pid" >/dev/null 2>&1 || true
wait "$sampler_pid" >/dev/null 2>&1 || true
sampler_pid=
printf 'continuous resource samples written to %s\n' "$sample_file"
