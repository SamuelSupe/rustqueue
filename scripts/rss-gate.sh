#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
messages=${MESSAGES:-10000000}
message_bytes=${MESSAGE_BYTES:-1024}
limit=${MAX_BYTES_PER_MESSAGE:-128}
batch_size=${BATCH_SIZE:-64}
project=rustqueue-rss
compose="docker compose -p $project -f $root/docker-compose.yml"
results="$root/benchmarks/results"
run_id=$(date +%s)
resource_samples="$results/rss-gate-$run_id.stats.tsv"
rss_samples="$results/rss-gate-$run_id.rss.tsv"
resource_sampler_pid=
rss_sampler_pid=

cleanup() {
  [ -z "$resource_sampler_pid" ] || kill "$resource_sampler_pid" >/dev/null 2>&1 || true
  [ -z "$rss_sampler_pid" ] || kill "$rss_sampler_pid" >/dev/null 2>&1 || true
  if [ "${KEEP_CLUSTER:-0}" != 1 ]; then
    $compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

$compose down -v --remove-orphans >/dev/null 2>&1 || true
make -C "$root" image
$compose up -d --no-build
for _ in $(seq 1 60); do
  curl -fsS http://127.0.0.1:4151/v1/health >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS http://127.0.0.1:4151/v1/health >/dev/null

container=$($compose ps -q rustqueue)
rss_bytes() {
  docker exec "$container" sh -c "awk '/VmRSS:/ { print \$2 * 1024 }' /proc/1/status"
}

mkdir -p "$results"
"$root/scripts/resource-sampler.sh" "$resource_samples" "$container" &
resource_sampler_pid=$!
(
  printf 'timestamp\trss_bytes\n'
  while :; do
    printf '%s\t%s\n' "$(date +%s)" "$(rss_bytes)"
    sleep "${RESOURCE_SAMPLE_INTERVAL_SECONDS:-1}"
  done
) > "$rss_samples" &
rss_sampler_pid=$!

before=$(rss_bytes)
docker exec "$container" /usr/local/bin/rustqueue-bench \
  --address 127.0.0.1:4150 \
  --topic rss-gate \
  --messages "$messages" \
  --message-bytes "$message_bytes" \
  --batch-size "$batch_size" \
  --producers 16 \
  --consumers 0 >/tmp/rustqueue-rss-gate.out
sleep 5
after=$(rss_bytes)
kill "$resource_sampler_pid" "$rss_sampler_pid" >/dev/null 2>&1 || true
wait "$resource_sampler_pid" "$rss_sampler_pid" 2>/dev/null || true
resource_sampler_pid=
rss_sampler_pid=
peak=$(awk 'NR > 1 && $2 > max { max=$2 } END { print max + 0 }' "$rss_samples")
delta=$((after - before))
[ "$delta" -lt 0 ] && delta=0
per_message=$((delta / messages))
cat /tmp/rustqueue-rss-gate.out
printf '%s\n' "{\"messages\":$messages,\"message_bytes\":$message_bytes,\"batch_size\":$batch_size,\"rss_before\":$before,\"rss_after\":$after,\"rss_peak\":$peak,\"rss_delta\":$delta,\"bytes_per_message\":$per_message,\"limit\":$limit,\"resource_samples\":\"$resource_samples\",\"rss_samples\":\"$rss_samples\"}"
[ "$per_message" -le "$limit" ]
