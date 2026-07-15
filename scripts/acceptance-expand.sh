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

CLUSTER_SIZE=4 COMPOSE_PROFILES=plus $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster4-up

for _ in $(seq 1 90); do
  ready=0
  for port in 4151 5151 6151 7151; do
    curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
  done
  [ "$ready" -eq 4 ] && break
  sleep 1
done
[ "${ready:-0}" -eq 4 ]

docker run --rm -v "$root:/work:ro" -w /work python:3.12-alpine \
  python tests/acceptance/expand.py
