#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/deploy/compat-compose.yml"
project="rustqueue-compat"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ "$status" -ne 0 ]; then
    docker compose -p "$project" -f "$compose" ps --all >&2 || true
    for service in rustqueue-plain rustqueue-1; do
      container=$(docker compose -p "$project" -f "$compose" ps -q "$service" 2>/dev/null || true)
      if [ -n "$container" ]; then
        docker inspect --format '{{json .State.Health}}' "$container" >&2 || true
      fi
    done
    docker compose -p "$project" -f "$compose" logs --no-color >&2 || true
  fi
  docker compose -p "$project" -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
  chmod 600 "$root/deploy/dev-certs/node-1.key" 2>/dev/null || true
  exit "$status"
}
trap cleanup EXIT INT TERM

"$root/scripts/generate-dev-certs.sh" "$root/deploy/dev-certs"
# The broker runs as uid 65532. Linux bind mounts preserve the host uid, so the
# disposable test key must be world-readable while the compatibility stack runs.
chmod 644 "$root/deploy/dev-certs/node-1.key"
docker compose -p "$project" -f "$compose" up -d --wait auth rustqueue-plain rustqueue-1
docker compose -p "$project" -f "$compose" run --rm go-core
docker compose -p "$project" -f "$compose" run --rm python-core
docker compose -p "$project" -f "$compose" run --rm go-secure
docker compose -p "$project" -f "$compose" run --rm python-secure
