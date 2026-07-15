#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/deploy/compat-compose.yml"
project="rustqueue-compat"

cleanup() {
  docker compose -p "$project" -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

"$root/scripts/generate-dev-certs.sh" "$root/deploy/dev-certs"
docker compose -p "$project" -f "$compose" up -d --wait auth rustqueue-plain rustqueue-1
docker compose -p "$project" -f "$compose" run --rm go-core
docker compose -p "$project" -f "$compose" run --rm python-core
docker compose -p "$project" -f "$compose" run --rm go-secure
docker compose -p "$project" -f "$compose" run --rm python-secure
