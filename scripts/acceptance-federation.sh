#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueuefed9 -f $root/deploy/multinode-compose.yml"
profiles=plus,five,nine

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=9 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

CLUSTER_SIZE=9 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" federation9-up

for _ in $(seq 1 180); do
  ready=0
  for port in 4151 5151 6151 7151 8151 9151 10151 11151 12151; do
    curl -fsS "http://127.0.0.1:$port/v1/health" >/dev/null 2>&1 && ready=$((ready + 1))
  done
  [ "$ready" -eq 9 ] && break
  sleep 1
done
[ "${ready:-0}" -eq 9 ]

docker run --rm -e HOST=host.docker.internal -v "$root:/work:ro" -w /work python:3.12-alpine \
  python tests/acceptance/federation.py

migration_metric_owners=0
for port in 4151 5151 6151 7151 8151 9151 10151 11151 12151; do
  if curl -fsS "http://127.0.0.1:$port/metrics" | grep -q '^rustqueue_federation_migration_info'; then
    migration_metric_owners=$((migration_metric_owners + 1))
  fi
done
[ "$migration_metric_owners" -eq 1 ]
printf '%s\n' '{"cells":3,"nodes":9,"cross_cell_ledger":true,"migration_metric_leaders":1}'
