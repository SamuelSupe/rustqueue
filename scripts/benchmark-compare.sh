#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
result_dir="$root/benchmarks/results"
messages=${MESSAGES:-100000}
producers=${PRODUCERS:-16}
consumers=${CONSUMERS:-16}
runs=${RUNS:-3}
payloads=${PAYLOADS:-"100 1024 10240"}
warmup_seconds=${WARMUP_SECONDS:-60}
duration_seconds=${DURATION_SECONDS:-600}
fixed_rate=${FIXED_RATE:-100}
sampler_pid=
mkdir -p "$result_dir"

docker build -t rustqueue:bench "$root"
"$root/scripts/generate-dev-certs.sh"
"$root/scripts/generate-cluster-configs.sh" 5

cleanup() {
  if [ -n "$sampler_pid" ]; then
    kill "$sampler_pid" >/dev/null 2>&1 || true
    wait "$sampler_pid" >/dev/null 2>&1 || true
  fi
  docker rm -f rq-bench-single rq-bench-nsq-strict rq-bench-nsq-default >/dev/null 2>&1 || true
  docker compose -p rustqueue-benchmark -f "$root/deploy/benchmark-compose.yml" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
cleanup

docker run -d --name rq-bench-single --cpus 2 --memory 2g \
  -p 14150:4150 -p 14151:4151 rustqueue:bench >/dev/null
docker run -d --name rq-bench-nsq-strict --cpus 2 --memory 2g \
  -p 15150:4150 -p 15151:4151 nsqio/nsq:v1.3.0 \
  /nsqd --mem-queue-size=0 --sync-every=1 --data-path=/data >/dev/null
docker run -d --name rq-bench-nsq-default --cpus 2 --memory 2g \
  -p 16150:4150 -p 16151:4151 nsqio/nsq:v1.3.0 \
  /nsqd --mem-queue-size=0 --sync-every=2500 --data-path=/data >/dev/null

for port in 14151 15151 16151; do
  until curl -fsS "http://127.0.0.1:$port/ping" >/dev/null; do sleep 1; done
done

run_bench() {
  name=$1
  port=$2
  container=$3
  payload=$4
  run=$5
  rate=${6:-}
  duration_args=""
  if [ "$duration_seconds" -gt 0 ]; then
    duration_args="--duration-seconds $duration_seconds"
  fi
  rate_args=""
  if [ -n "$rate" ]; then
    rate_args="--rate $rate"
  fi
  stats_file="$result_dir/$name-$payload-run$run.stats.tsv"
  "$root/scripts/resource-sampler.sh" "$stats_file" $container &
  sampler_pid=$!
  status=0
  docker run --rm --entrypoint /usr/local/bin/rustqueue-bench rustqueue:bench \
    --address "host.docker.internal:$port" --topic "benchmark-$payload-$run" \
    --messages "$messages" --message-bytes "$payload" --producers "$producers" \
    --consumers "$consumers" --warmup-seconds "$warmup_seconds" \
    $duration_args $rate_args --json \
    > "$result_dir/$name-$payload-run$run.json" || status=$?
  kill "$sampler_pid" >/dev/null 2>&1 || true
  wait "$sampler_pid" >/dev/null 2>&1 || true
  sampler_pid=
  [ "$status" -eq 0 ]
}

for payload in $payloads; do
  run=1
  while [ "$run" -le "$runs" ]; do
    run_bench rustqueue-single-durable 14150 rq-bench-single "$payload" "$run"
    run_bench nsq-sync-every-1 15150 rq-bench-nsq-strict "$payload" "$run"
    run_bench nsq-sync-every-2500 16150 rq-bench-nsq-default "$payload" "$run"
    run_bench rustqueue-single-durable-fixed 14150 rq-bench-single "$payload" "$run" "$fixed_rate"
    run_bench nsq-sync-every-1-fixed 15150 rq-bench-nsq-strict "$payload" "$run" "$fixed_rate"
    run_bench nsq-sync-every-2500-fixed 16150 rq-bench-nsq-default "$payload" "$run" "$fixed_rate"
    run=$((run + 1))
  done
done

docker rm -f rq-bench-single rq-bench-nsq-strict rq-bench-nsq-default >/dev/null
docker compose -p rustqueue-benchmark -f "$root/deploy/benchmark-compose.yml" up -d
until curl -fsS http://127.0.0.1:17151/v1/health >/dev/null; do sleep 1; done

for payload in $payloads; do
  run=1
  while [ "$run" -le "$runs" ]; do
    cluster_containers="rustqueue-benchmark-rustqueue-1-1 rustqueue-benchmark-rustqueue-2-1 rustqueue-benchmark-rustqueue-3-1 rustqueue-benchmark-rustqueue-4-1 rustqueue-benchmark-rustqueue-5-1"
    run_bench rustqueue-3-replica-quorum 17150 "$cluster_containers" "$payload" "$run"
    run_bench rustqueue-3-replica-quorum-fixed 17150 "$cluster_containers" "$payload" "$run" "$fixed_rate"
    run=$((run + 1))
  done
done

for payload in $payloads; do
  for name in \
    rustqueue-single-durable nsq-sync-every-1 nsq-sync-every-2500 rustqueue-3-replica-quorum \
    rustqueue-single-durable-fixed nsq-sync-every-1-fixed nsq-sync-every-2500-fixed \
    rustqueue-3-replica-quorum-fixed; do
    jq -s '{
      runs: length,
      median: (sort_by(.messages_per_second) | .[length / 2 | floor])
    }' "$result_dir/$name-$payload-run"*.json > "$result_dir/$name-$payload-median.json"
  done
done

printf 'benchmark reports written to %s\n' "$result_dir"
