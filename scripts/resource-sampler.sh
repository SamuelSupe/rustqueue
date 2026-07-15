#!/bin/sh
set -eu

output=$1
shift
interval=${RESOURCE_SAMPLE_INTERVAL_SECONDS:-1}
: > "$output"

while :; do
  timestamp=$(date +%s)
  for container in "$@"; do
    docker stats --no-stream --format \
      "$timestamp\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}" \
      "$container" >> "$output" 2>/dev/null || true
  done
  sleep "$interval"
done
