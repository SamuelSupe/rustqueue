#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
messages=${MESSAGES:-10000000}
message_bytes=${MESSAGE_BYTES:-1024}
growth_limit=${MAX_GROWTH_AFTER_WARM_BYTES:-67108864}
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
first_half=$((messages / 2))
second_half=$((messages - first_half))
docker exec "$container" /usr/local/bin/rustqueue-bench \
  --address 127.0.0.1:4150 \
  --topic rss-gate \
  --messages "$first_half" \
  --message-bytes "$message_bytes" \
  --batch-size "$batch_size" \
  --producers 16 \
  --consumers 0 >/tmp/rustqueue-rss-gate-first.out
sleep 5
warm=$(rss_bytes)
docker exec "$container" /usr/local/bin/rustqueue-bench \
  --address 127.0.0.1:4150 \
  --topic rss-gate \
  --messages "$second_half" \
  --message-bytes "$message_bytes" \
  --batch-size "$batch_size" \
  --producers 16 \
  --consumers 0 >/tmp/rustqueue-rss-gate-second.out
sleep 5
after=$(rss_bytes)
kill "$resource_sampler_pid" "$rss_sampler_pid" >/dev/null 2>&1 || true
wait "$resource_sampler_pid" "$rss_sampler_pid" 2>/dev/null || true
resource_sampler_pid=
rss_sampler_pid=
peak=$(awk 'NR > 1 && $2 > max { max=$2 } END { print max + 0 }' "$rss_samples")
delta=$((after - before))
growth_after_warm=$((after - warm))
[ "$delta" -lt 0 ] && delta=0
[ "$growth_after_warm" -lt 0 ] && growth_after_warm=0
cat /tmp/rustqueue-rss-gate-first.out
cat /tmp/rustqueue-rss-gate-second.out
printf '%s\n' "{\"messages\":$messages,\"message_bytes\":$message_bytes,\"batch_size\":$batch_size,\"rss_before\":$before,\"rss_after_warm\":$warm,\"rss_after\":$after,\"rss_peak\":$peak,\"rss_delta\":$delta,\"growth_after_warm\":$growth_after_warm,\"growth_limit\":$growth_limit,\"resource_samples\":\"$resource_samples\",\"rss_samples\":\"$rss_samples\"}"
[ "$growth_after_warm" -le "$growth_limit" ]
