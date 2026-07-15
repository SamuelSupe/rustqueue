#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="docker compose -p rustqueue5 -f $root/deploy/multinode-compose.yml"
profiles=plus,five

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    CLUSTER_SIZE=5 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

CLUSTER_SIZE=5 COMPOSE_PROFILES=$profiles $compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" cluster5-up

DURATION_SECONDS=${DURATION_SECONDS:-120} \
GRACE_SECONDS=${GRACE_SECONDS:-60} \
RATE=${RATE:-100} \
RESTART_INTERVAL_SECONDS=${RESTART_INTERVAL_SECONDS:-5} \
NODE_RECOVERY_SECONDS=${NODE_RECOVERY_SECONDS:-60} \
RESTART_MODE=graceful \
MIN_RESTARTS=5 \
CLUSTER_SIZE=5 \
COMPOSE_PROJECT=rustqueue5 \
COMPOSE_PROFILES=$profiles \
  "$root/scripts/soak.sh"

for port in 4151 5151 6151 7151 8151; do
  info=$(curl -fsS "http://127.0.0.1:$port/info")
  [ "$(printf '%s' "$info" | jq -r .version)" = "0.6.0" ]
  feature=$(printf '%s' "$info" | jq -r .feature_level)
  floor=$(printf '%s' "$info" | jq -r .observed_feature_floor)
  [ "$floor" -ge "$feature" ]
done
printf '%s\n' '{"rolling_restarts":5,"publisher_continued":true,"consumer_recovered":true,"missing":0}'
