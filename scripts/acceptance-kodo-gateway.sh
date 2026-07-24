#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${PROJECT:-rustqueue-kodo-gateway-e2e}"
BROKER_IMAGE="${BROKER_IMAGE:-rustqueue:dev}"
COMPAT_IMAGE="${COMPAT_IMAGE:-rustqueue-go-compat:kodo-e2e}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
COMPOSE="$ROOT/tests/kodo-gateway/compose.yaml"

cleanup() {
  code=$?
  if [[ $code -ne 0 ]]; then
    docker compose -p "$PROJECT" -f "$COMPOSE" logs --no-color || true
  fi
  docker compose -p "$PROJECT" -f "$COMPOSE" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$code"
}
trap cleanup EXIT

[[ "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "PROJECT must be a narrow Docker Compose project name" >&2
  exit 2
}
if [[ "$BUILD_IMAGES" == "1" ]]; then
  make -C "$ROOT" image
  docker build -t "$COMPAT_IMAGE" "$ROOT/tests/compat/go"
fi

docker compose -p "$PROJECT" -f "$COMPOSE" down --volumes --remove-orphans >/dev/null 2>&1 || true
BROKER_IMAGE="$BROKER_IMAGE" COMPAT_IMAGE="$COMPAT_IMAGE" \
  docker compose -p "$PROJECT" -f "$COMPOSE" up \
    --abort-on-container-exit --exit-code-from acceptance
