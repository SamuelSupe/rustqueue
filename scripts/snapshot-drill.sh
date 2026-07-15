#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=${RUSTQUEUE_IMAGE:-rustqueue:dev}
suffix=$$
source_volume="rustqueue-snapshot-source-$suffix"
backup_volume="rustqueue-snapshot-backup-$suffix"
target_volume="rustqueue-snapshot-target-$suffix"
source_container="rustqueue-snapshot-source-$suffix"
restored_container="rustqueue-snapshot-restored-$suffix"

cleanup() {
  docker rm -f "$source_container" "$restored_container" >/dev/null 2>&1 || true
  docker volume rm "$source_volume" "$backup_volume" "$target_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_ready() {
  container=$1
  for _ in $(seq 1 40); do
    if docker exec "$container" curl -fsS http://127.0.0.1:4151/ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  docker logs "$container"
  return 1
}

for volume in "$source_volume" "$backup_volume" "$target_volume"; do
  docker volume create "$volume" >/dev/null
done
docker run --rm \
  -v "$source_volume:/data" \
  -v "$backup_volume:/backups" \
  -v "$target_volume:/restore" \
  alpine:3.21 sh -c 'chown 65532:65532 /data /backups /restore'

docker run -d --name "$source_container" \
  -v "$source_volume:/data" \
  -v "$root/rustqueue.example.toml:/etc/rustqueue/rustqueue.toml:ro" \
  "$image" --config /etc/rustqueue/rustqueue.toml >/dev/null
wait_ready "$source_container"

index=0
while [ "$index" -lt 20 ]; do
  docker exec "$source_container" curl -fsS -X POST \
    --data-binary "snapshot-$index" \
    'http://127.0.0.1:4151/pub?topic=snapshot_drill' >/dev/null
  index=$((index + 1))
done
before=$(docker exec "$source_container" curl -fsS \
  'http://127.0.0.1:4151/stats?format=json')
docker stop "$source_container" >/dev/null

docker run --rm \
  -v "$source_volume:/data" \
  -v "$backup_volume:/backups" \
  "$image" snapshot export \
  --data-path /data --snapshot-dir /backups --name drill
docker run --rm \
  -v "$backup_volume:/backups:ro" \
  "$image" snapshot verify --snapshot-dir /backups --name drill
docker run --rm \
  -v "$backup_volume:/backups:ro" \
  -v "$target_volume:/restore" \
  "$image" snapshot restore \
  --snapshot-dir /backups --name drill --target /restore/data

docker run -d --name "$restored_container" \
  -e RUSTQUEUE_DATA_PATH=/volume/data \
  -v "$target_volume:/volume" \
  -v "$root/rustqueue.example.toml:/etc/rustqueue/rustqueue.toml:ro" \
  "$image" --config /etc/rustqueue/rustqueue.toml >/dev/null
wait_ready "$restored_container"
after=$(docker exec "$restored_container" curl -fsS \
  'http://127.0.0.1:4151/stats?format=json')

docker run --rm -e BEFORE="$before" -e AFTER="$after" python:3.12-alpine python -c '
import json
import os

before = json.loads(os.environ["BEFORE"])
after = json.loads(os.environ["AFTER"])
count = sum(topic["message_count"] for topic in after["topics"])
result = {
    "snapshot_export": True,
    "snapshot_verify": True,
    "restore_boot": True,
    "stats_equal": before == after,
    "message_count": count,
}
print(json.dumps(result, separators=(",", ":")))
if before != after or count != 20:
    raise SystemExit(1)
'
