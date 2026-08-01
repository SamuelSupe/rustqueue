#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE="${RELEASE:-0.8.4}"
BASELINE_REF="${BASELINE_REF:-v0.8.3}"
CANDIDATE_REF="${CANDIDATE_REF:-HEAD}"
PAIRS="${PAIRS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-30}"
MEASUREMENT_SECONDS="${MEASUREMENT_SECONDS:-120}"
BOOTSTRAP_ITERATIONS="${BOOTSTRAP_ITERATIONS:-100000}"
BOOTSTRAP_SEED="${BOOTSTRAP_SEED:-802}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-1800}"
CASES="${CASES:-raw_write sustainable low_load_latency}"
QUALIFICATION_DEV="${QUALIFICATION_DEV:-0}"
KEEP_IMAGES="${KEEP_IMAGES:-0}"
RESULT_ROOT="$ROOT/benchmarks/results"
EVIDENCE_OUTPUT="${EVIDENCE_OUTPUT:-$ROOT/benchmarks/qualifications/v0.8.4-orbstack.json}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="$RESULT_ROOT/qualification-$RUN_ID"
RUNS_FILE="$RUN_DIR/runs.ndjson"
INPUT_FILE="$RUN_DIR/input.json"
EVIDENCE_FILE="$RUN_DIR/evidence.json"
BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/rustqueue-qualify.XXXXXX")"
BASELINE_SOURCE="$BUILD_ROOT/baseline"
CANDIDATE_SOURCE="$BUILD_ROOT/candidate"
TARGET_ROOT="$RESULT_ROOT/.qualification-target"
BASELINE_RUNTIME="$BUILD_ROOT/baseline-runtime"
CANDIDATE_RUNTIME="$BUILD_ROOT/candidate-runtime"
TOOLS_RUNTIME="$BUILD_ROOT/tools-runtime"
PREFIX="rq-q-$$"
NETWORK="$PREFIX-net"
BASELINE_IMAGE="rustqueue:qualify-baseline-$$"
CANDIDATE_IMAGE="rustqueue:qualify-candidate-$$"
TOOLS_IMAGE="rustqueue:qualify-tools-$$"
ACTIVE_BROKER=""
ACTIVE_VOLUME=""
SAMPLER_PID=""
SEQUENCE=0
FINAL_DRAIN_ATTEMPTS=30

die() {
  printf 'benchmark qualification: %s\n' "$*" >&2
  exit 1
}

require_positive_integer() {
  local name=$1 value=$2
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer"
}

cleanup_run() {
  if [[ -n "$SAMPLER_PID" ]]; then
    kill "$SAMPLER_PID" >/dev/null 2>&1 || true
    wait "$SAMPLER_PID" >/dev/null 2>&1 || true
    SAMPLER_PID=""
  fi
  if [[ -n "$ACTIVE_BROKER" ]]; then
    docker rm -f "$ACTIVE_BROKER" >/dev/null 2>&1 || true
    ACTIVE_BROKER=""
  fi
  if [[ -n "$ACTIVE_VOLUME" ]]; then
    docker volume rm "$ACTIVE_VOLUME" >/dev/null 2>&1 || true
    ACTIVE_VOLUME=""
  fi
}

cleanup() {
  cleanup_run
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  if [[ "$KEEP_IMAGES" != 1 ]]; then
    docker image rm "$BASELINE_IMAGE" "$CANDIDATE_IMAGE" "$TOOLS_IMAGE" \
      >/dev/null 2>&1 || true
  fi
  rm -rf "$BUILD_ROOT"
}
trap cleanup EXIT INT TERM

for command in docker git jq tar awk; do
  command -v "$command" >/dev/null 2>&1 || die "required command is missing: $command"
done
require_positive_integer PAIRS "$PAIRS"
require_positive_integer WARMUP_SECONDS "$WARMUP_SECONDS"
require_positive_integer MEASUREMENT_SECONDS "$MEASUREMENT_SECONDS"
require_positive_integer BOOTSTRAP_ITERATIONS "$BOOTSTRAP_ITERATIONS"
require_positive_integer DRAIN_TIMEOUT_SECONDS "$DRAIN_TIMEOUT_SECONDS"

CASE_COUNT=0
seen_cases=" "
for scenario in $CASES; do
  case "$scenario" in
    raw_write|sustainable|low_load_latency) ;;
    *) die "unknown case $scenario" ;;
  esac
  [[ "$seen_cases" != *" $scenario "* ]] || die "case $scenario is duplicated"
  seen_cases="$seen_cases$scenario "
  CASE_COUNT=$((CASE_COUNT + 1))
done
[[ "$CASE_COUNT" -gt 0 ]] || die "CASES must enable at least one case"

docker_context="$(docker context show)"
docker_os="$(docker info --format '{{.OperatingSystem}}')"
if [[ "$docker_context" != "orbstack" && "$docker_os" != *OrbStack* ]]; then
  die "Docker must use OrbStack (context=$docker_context, os=$docker_os)"
fi

baseline_commit="$(git -C "$ROOT" rev-parse --verify "$BASELINE_REF^{commit}")"
candidate_commit="$(git -C "$ROOT" rev-parse --verify "$CANDIDATE_REF^{commit}")"
BASELINE_TARGET="$TARGET_ROOT/$baseline_commit"
CANDIDATE_TARGET="$TARGET_ROOT/$candidate_commit"
if [[ "$QUALIFICATION_DEV" == 0 ]]; then
  [[ "$BASELINE_REF" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    die "baseline reference must be a version tag"
  tag_commit="$(git -C "$ROOT" rev-parse --verify "refs/tags/$BASELINE_REF^{commit}")"
  [[ "$baseline_commit" == "$tag_commit" ]] ||
    die "baseline must resolve to the exact $BASELINE_REF tag commit"
fi

case "$EVIDENCE_OUTPUT" in
  "$ROOT/benchmarks/qualifications/"*)
    [[ "$QUALIFICATION_DEV" == 0 ]] ||
      die "development runs cannot publish committed qualification evidence"
    [[ "$PAIRS" == 10 && "$WARMUP_SECONDS" == 30 && "$MEASUREMENT_SECONDS" == 120 ]] ||
      die "committed evidence requires 10 pairs, 30s warmup and 120s measurement"
    [[ "$DRAIN_TIMEOUT_SECONDS" == 1800 ]] ||
      die "committed evidence requires an 1800s complete-drain timeout"
    [[ "$CASES" == "raw_write sustainable low_load_latency" ]] ||
      die "committed evidence requires all three qualification cases"
    ;;
esac

mkdir -p \
  "$RUN_DIR" \
  "$BASELINE_SOURCE" \
  "$CANDIDATE_SOURCE" \
  "$BASELINE_TARGET" \
  "$CANDIDATE_TARGET" \
  "$BASELINE_RUNTIME" \
  "$CANDIDATE_RUNTIME" \
  "$TOOLS_RUNTIME"
: >"$RUNS_FILE"
git -C "$ROOT" archive "$baseline_commit" | tar -x -C "$BASELINE_SOURCE"
git -C "$ROOT" archive "$candidate_commit" | tar -x -C "$CANDIDATE_SOURCE"

read_workspace_version() {
  awk '
    /^\[workspace\.package\]$/ { workspace = 1; next }
    /^\[/ { workspace = 0 }
    workspace && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$1/Cargo.toml"
}

baseline_version="$(read_workspace_version "$BASELINE_SOURCE")"
candidate_version="$(read_workspace_version "$CANDIDATE_SOURCE")"
if [[ "$QUALIFICATION_DEV" == 0 ]]; then
  expected_baseline_version="${BASELINE_REF#v}"
  [[ "$baseline_version" == "$expected_baseline_version" ]] ||
    die "baseline workspace version is $baseline_version, expected $expected_baseline_version"
  [[ "$candidate_version" == "$RELEASE" ]] ||
    die "candidate workspace version is $candidate_version, expected $RELEASE"
fi

candidate_context="$CANDIDATE_SOURCE"
tool_source="$candidate_commit"
if [[ "$QUALIFICATION_DEV" == 1 ]]; then
  candidate_context="$ROOT"
  candidate_version="$(read_workspace_version "$ROOT")"
  CANDIDATE_TARGET="$BUILD_ROOT/candidate-target"
  mkdir -p "$CANDIDATE_TARGET"
  tool_source="worktree"
fi

build_release() {
  local source=$1 target=$2 version=$3
  shift 3
  local -a binary_args=()
  for binary in "$@"; do
    binary_args+=(--bin "$binary")
  done
  docker run --rm \
    -e RUSTUP_TOOLCHAIN=1.88.0 \
    -e CARGO_INCREMENTAL=0 \
    -e CARGO_TARGET_DIR=/target \
    -e "RUSTQUEUE_BUILD_VERSION=$version" \
    -v "$source:/work" \
    -v "$target:/target" \
    -v rustqueue-cargo-registry:/usr/local/cargo/registry \
    -v rustqueue-rustup:/usr/local/rustup \
    -w /work \
    rust:1.88-bookworm \
    cargo build --locked --release "${binary_args[@]}"
}

printf 'Compiling exact baseline Broker %s (%s)\n' "$BASELINE_REF" "$baseline_commit"
build_release "$BASELINE_SOURCE" "$BASELINE_TARGET" "$baseline_version" rustqueued
cp "$BASELINE_TARGET/release/rustqueued" "$BASELINE_RUNTIME/rustqueued"
if command -v shasum >/dev/null 2>&1; then
  baseline_binary_sha256="$(
    shasum -a 256 "$BASELINE_RUNTIME/rustqueued" | awk '{print $1}'
  )"
else
  baseline_binary_sha256="$(
    sha256sum "$BASELINE_RUNTIME/rustqueued" | awk '{print $1}'
  )"
fi
docker build --target broker \
  -f "$ROOT/benchmarks/Dockerfile.qualify" \
  -t "$BASELINE_IMAGE" "$BASELINE_RUNTIME"

printf 'Compiling candidate Broker %s (%s)\n' "$CANDIDATE_REF" "$candidate_commit"
build_release \
  "$candidate_context" \
  "$CANDIDATE_TARGET" \
  "$candidate_version" \
  rustqueued \
  rustqueue-bench \
  rustqueue-qualify
cp "$CANDIDATE_TARGET/release/rustqueued" "$CANDIDATE_RUNTIME/rustqueued"
cp "$CANDIDATE_TARGET/release/rustqueue-bench" "$TOOLS_RUNTIME/rustqueue-bench"
cp "$CANDIDATE_TARGET/release/rustqueue-qualify" "$TOOLS_RUNTIME/rustqueue-qualify"
if command -v shasum >/dev/null 2>&1; then
  candidate_binary_sha256="$(
    shasum -a 256 "$CANDIDATE_RUNTIME/rustqueued" | awk '{print $1}'
  )"
else
  candidate_binary_sha256="$(
    sha256sum "$CANDIDATE_RUNTIME/rustqueued" | awk '{print $1}'
  )"
fi
docker build --target broker \
  -f "$ROOT/benchmarks/Dockerfile.qualify" \
  -t "$CANDIDATE_IMAGE" "$CANDIDATE_RUNTIME"
printf 'Building common load generator and qualification evaluator\n'
docker build --target tools \
  -f "$ROOT/benchmarks/Dockerfile.qualify" \
  -t "$TOOLS_IMAGE" "$TOOLS_RUNTIME"

docker network create "$NETWORK" >/dev/null

sample_rss() {
  local output=$1 broker=$2
  {
    printf 'timestamp_utc\trss_bytes\n'
    while docker inspect "$broker" >/dev/null 2>&1; do
      rss="$(
        docker exec "$broker" awk '/VmRSS:/ { print $2 * 1024 }' /proc/1/status \
          2>/dev/null || true
      )"
      if [[ "$rss" =~ ^[0-9]+$ ]]; then
        printf '%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$rss"
      fi
      sleep 1
    done
  } >"$output"
}

wait_for_broker() {
  local broker=$1
  for _ in $(seq 1 60); do
    if docker exec "$broker" curl -fsS http://127.0.0.1:4151/v1/health \
      >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  docker logs "$broker" >&2 || true
  die "Broker $broker did not become healthy"
}

run_variant() {
  local scenario=$1 pair=$2 position=$3 variant=$4
  local image commit producers consumers batch rate attempt
  case "$variant" in
    baseline)
      image="$BASELINE_IMAGE"
      commit="$baseline_commit"
      ;;
    candidate)
      image="$CANDIDATE_IMAGE"
      commit="$candidate_commit"
      ;;
    *)
      die "unknown variant $variant"
      ;;
  esac
  case "$scenario" in
    raw_write)
      producers=16
      consumers=0
      batch=64
      rate=""
      ;;
    sustainable)
      producers=16
      consumers=16
      batch=64
      rate=""
      ;;
    low_load_latency)
      producers=16
      consumers=16
      batch=1
      rate=100
      ;;
    *)
      die "unknown case $scenario"
      ;;
  esac

  SEQUENCE=$((SEQUENCE + 1))
  local label="${scenario}-p${pair}-${variant}"
  local report="$RUN_DIR/$label.json"
  local error_log="$RUN_DIR/$label.stderr.log"
  local rss_file="$RUN_DIR/$label.rss.tsv"
  ACTIVE_BROKER="$PREFIX-b$SEQUENCE"
  ACTIVE_VOLUME="$PREFIX-v$SEQUENCE"

  docker volume create "$ACTIVE_VOLUME" >/dev/null
  docker run -d --name "$ACTIVE_BROKER" --network "$NETWORK" \
    --cpus 2 --memory 2g \
    -e RUSTQUEUE_DATA_PATH=/data \
    -v "$ACTIVE_VOLUME:/data" \
    "$image" >/dev/null
  wait_for_broker "$ACTIVE_BROKER"
  sample_rss "$rss_file" "$ACTIVE_BROKER" &
  SAMPLER_PID=$!

  printf '[%02d/%02d] %s pair=%d position=%d variant=%s\n' \
    "$SEQUENCE" "$((PAIRS * CASE_COUNT * 2))" "$scenario" "$pair" "$position" "$variant"
  local -a bench_args=(
    --address "$ACTIVE_BROKER:4150"
    --topic "q-$scenario-$pair-$variant"
    --messages 100000
    --message-bytes 1024
    --producers "$producers"
    --consumers "$consumers"
    --batch-size "$batch"
    --warmup-seconds "$WARMUP_SECONDS"
    --duration-seconds "$MEASUREMENT_SECONDS"
    --drain-timeout-seconds "$DRAIN_TIMEOUT_SECONDS"
    --json
  )
  if [[ -n "$rate" ]]; then
    bench_args+=(--rate "$rate")
  fi
  set +e
  docker run --rm --network "$NETWORK" --cpus 2 --memory 2g \
    --entrypoint /usr/local/bin/rustqueue-bench "$TOOLS_IMAGE" \
    "${bench_args[@]}" >"$report" 2>"$error_log"
  benchmark_status=$?
  set -e

  kill "$SAMPLER_PID" >/dev/null 2>&1 || true
  wait "$SAMPLER_PID" >/dev/null 2>&1 || true
  SAMPLER_PID=""
  if [[ "$benchmark_status" -ne 0 ]]; then
    cat "$error_log" >&2
    cleanup_run
    die "$label exited with status $benchmark_status"
  fi
  jq -e \
    --argjson producers "$producers" \
    --argjson consumers "$consumers" \
    --argjson batch "$batch" \
    '.message_bytes == 1024
      and .producers == $producers
      and .consumers == $consumers
      and .batch_size == $batch
      and .messages > 0
      and .publish_messages_per_second > 0
      and .latency_us_p99 > 0' \
    "$report" >/dev/null || {
      cleanup_run
      die "$label produced an invalid benchmark report"
    }

  final_depth=null
  final_in_flight=null
  final_deferred=null
  if [[ "$consumers" -gt 0 ]]; then
    topic="$(jq -r '.topic' "$report")"
    channel="$(jq -r '.channel' "$report")"
    for ((attempt = 1; attempt <= FINAL_DRAIN_ATTEMPTS; attempt++)); do
      stats="$(
        docker exec "$ACTIVE_BROKER" curl -fsS \
          "http://127.0.0.1:4151/stats?format=json&include_clients=false&topic=$topic&channel=$channel"
      )"
      channel_stats="$(
        jq -c --arg topic "$topic" --arg channel "$channel" '
          [.topics[]
            | select(.topic_name == $topic)
            | .channels[]
            | select(.channel_name == $channel)][0]
        ' <<<"$stats"
      )"
      [[ "$channel_stats" != null ]] || {
        cleanup_run
        die "$label final Channel stats are missing"
      }
      final_depth="$(jq -r '.depth' <<<"$channel_stats")"
      final_in_flight="$(jq -r '.in_flight_count' <<<"$channel_stats")"
      final_deferred="$(jq -r '.deferred_count' <<<"$channel_stats")"
      if [[ "$final_depth" -eq 0 && "$final_in_flight" -eq 0 && "$final_deferred" -eq 0 ]]; then
        break
      fi
      if [[ "$attempt" -eq "$FINAL_DRAIN_ATTEMPTS" ]]; then
        cleanup_run
        die "$label left Channel backlog after drain"
      fi
      sleep 1
    done
  fi

  prometheus="$(
    docker exec "$ACTIVE_BROKER" curl -fsS http://127.0.0.1:4151/metrics
  )"
  metric_value() {
    local name=$1
    awk -v name="$name" '$1 == name { print $2; found = 1; exit } END { if (!found) exit 1 }' \
      <<<"$prometheus"
  }
  publish_group_commits="$(metric_value rustqueue_publish_group_commits_total)"
  publish_group_requests="$(metric_value rustqueue_publish_group_requests_total)"
  publish_group_max_requests="$(metric_value rustqueue_publish_group_max_requests)"
  channel_group_commits="$(metric_value rustqueue_channel_group_commits_total)"
  channel_group_requests="$(metric_value rustqueue_channel_group_requests_total)"
  channel_group_max_requests="$(metric_value rustqueue_channel_group_max_requests)"
  channel_fsync_count="$(metric_value rustqueue_channel_fsync_duration_seconds_count)"
  channel_fsync_sum="$(metric_value rustqueue_channel_fsync_duration_seconds_sum)"
  channel_group_wait_count="$(
    metric_value rustqueue_channel_group_commit_wait_duration_seconds_count
  )"
  channel_group_wait_sum="$(
    metric_value rustqueue_channel_group_commit_wait_duration_seconds_sum
  )"
  consumer_fetch_batches="$(metric_value rustqueue_consumer_fetch_batches_total)"
  consumer_fetch_messages="$(metric_value rustqueue_consumer_fetch_messages_total)"
  aggregate_channel_depth="$(metric_value rustqueue_channel_depth_total)"
  aggregate_channel_in_flight="$(metric_value rustqueue_channel_in_flight_total)"
  aggregate_channel_deferred="$(metric_value rustqueue_channel_deferred_total)"
  if [[ "$consumers" -gt 0 ]] &&
    [[ "$aggregate_channel_depth" -ne 0 ||
      "$aggregate_channel_in_flight" -ne 0 ||
      "$aggregate_channel_deferred" -ne 0 ]]; then
    cleanup_run
    die "$label left aggregate Channel backlog after drain"
  fi

  rss_peak="$(awk 'NR > 1 && $2 > peak { peak=$2 } END { print peak + 0 }' "$rss_file")"
  [[ "$rss_peak" -gt 0 ]] || {
    cleanup_run
    die "$label did not capture Broker RSS"
  }

  jq -c \
    --arg case "$scenario" \
    --arg variant "$variant" \
    --arg commit "$commit" \
    --argjson pair "$pair" \
    --argjson sequence "$SEQUENCE" \
    --argjson position "$position" \
    --argjson status "$benchmark_status" \
    --argjson rss_peak "$rss_peak" \
    --argjson final_depth "$final_depth" \
    --argjson final_in_flight "$final_in_flight" \
    --argjson final_deferred "$final_deferred" \
    --argjson publish_group_commits "$publish_group_commits" \
    --argjson publish_group_requests "$publish_group_requests" \
    --argjson publish_group_max_requests "$publish_group_max_requests" \
    --argjson channel_group_commits "$channel_group_commits" \
    --argjson channel_group_requests "$channel_group_requests" \
    --argjson channel_group_max_requests "$channel_group_max_requests" \
    --argjson channel_fsync_count "$channel_fsync_count" \
    --argjson channel_fsync_sum "$channel_fsync_sum" \
    --argjson channel_group_wait_count "$channel_group_wait_count" \
    --argjson channel_group_wait_sum "$channel_group_wait_sum" \
    --argjson consumer_fetch_batches "$consumer_fetch_batches" \
    --argjson consumer_fetch_messages "$consumer_fetch_messages" \
    --argjson aggregate_channel_depth "$aggregate_channel_depth" \
    --argjson aggregate_channel_in_flight "$aggregate_channel_in_flight" \
    --argjson aggregate_channel_deferred "$aggregate_channel_deferred" \
    '{
      case: $case,
      pair: $pair,
      sequence: $sequence,
      position_in_pair: $position,
      variant: $variant,
      commit: $commit,
      benchmark_exit_code: $status,
      metrics: {
        messages,
        received_unique_messages,
        duplicate_messages,
        missing_messages,
        delivery_verified,
        delivery_complete,
        drain_timed_out,
        final_channel_depth: $final_depth,
        final_in_flight: $final_in_flight,
        final_deferred: $final_deferred,
        publish_messages_per_second,
        receive_messages_per_second,
        pub_ack_p99_us: .latency_us_p99,
        rss_peak_bytes: $rss_peak,
        broker_profile: {
          publish_group_commits: $publish_group_commits,
          publish_group_requests: $publish_group_requests,
          publish_group_max_requests: $publish_group_max_requests,
          channel_group_commits: $channel_group_commits,
          channel_group_requests: $channel_group_requests,
          channel_group_max_requests: $channel_group_max_requests,
          channel_fsync_count: $channel_fsync_count,
          channel_fsync_sum_seconds: $channel_fsync_sum,
          channel_group_wait_count: $channel_group_wait_count,
          channel_group_wait_sum_seconds: $channel_group_wait_sum,
          consumer_fetch_batches: $consumer_fetch_batches,
          consumer_fetch_messages: $consumer_fetch_messages,
          aggregate_channel_depth: $aggregate_channel_depth,
          aggregate_channel_in_flight: $aggregate_channel_in_flight,
          aggregate_channel_deferred: $aggregate_channel_deferred
        }
      }
    }' "$report" >>"$RUNS_FILE"
  cleanup_run
}

for scenario in $CASES; do
  for pair in $(seq 1 "$PAIRS"); do
    if (( pair % 2 == 1 )); then
      run_variant "$scenario" "$pair" 1 baseline
      run_variant "$scenario" "$pair" 2 candidate
    else
      run_variant "$scenario" "$pair" 1 candidate
      run_variant "$scenario" "$pair" 2 baseline
    fi
  done
done

baseline_image_id="$(docker image inspect "$BASELINE_IMAGE" --format '{{.Id}}')"
candidate_image_id="$(docker image inspect "$CANDIDATE_IMAGE" --format '{{.Id}}')"
docker_server_version="$(docker version --format '{{.Server.Version}}')"
docker_architecture="$(docker info --format '{{.Architecture}}')"
docker_cpus="$(docker info --format '{{.NCPU}}')"
docker_memory_bytes="$(docker info --format '{{.MemTotal}}')"
docker_storage_driver="$(docker info --format '{{.Driver}}')"
orbstack_version="$(orbctl version 2>/dev/null | head -n 1 || printf unknown)"
macos_version="$(sw_vers -productVersion 2>/dev/null || printf unknown)"
hardware_model="$(sysctl -n hw.model 2>/dev/null || printf unknown)"

jq -n \
  --arg docker_context "$docker_context" \
  --arg docker_os "$docker_os" \
  --arg docker_server_version "$docker_server_version" \
  --arg docker_architecture "$docker_architecture" \
  --argjson docker_cpus "$docker_cpus" \
  --argjson docker_memory_bytes "$docker_memory_bytes" \
  --arg docker_storage_driver "$docker_storage_driver" \
  --arg orbstack_version "$orbstack_version" \
  --arg macos_version "$macos_version" \
  --arg hardware_model "$hardware_model" \
  --arg tool_source "$tool_source" \
  '{
    platform: "OrbStack on macOS",
    comparison_scope: "same-host relative only",
    macos_version: $macos_version,
    hardware_model: $hardware_model,
    orbstack_version: $orbstack_version,
    docker: {
      context: $docker_context,
      operating_system: $docker_os,
      server_version: $docker_server_version,
      architecture: $docker_architecture,
      cpus: $docker_cpus,
      memory_bytes: $docker_memory_bytes,
      storage_driver: $docker_storage_driver
    },
    resource_limits: {
      broker: {cpus: 2, memory_bytes: 2147483648},
      load_generator: {cpus: 2, memory_bytes: 2147483648}
    },
    tool_source: $tool_source
  }' >"$RUN_DIR/environment-core.json"

if command -v shasum >/dev/null 2>&1; then
  environment_fingerprint="$(shasum -a 256 "$RUN_DIR/environment-core.json" | awk '{print $1}')"
else
  environment_fingerprint="$(sha256sum "$RUN_DIR/environment-core.json" | awk '{print $1}')"
fi
jq --arg fingerprint "$environment_fingerprint" \
  '. + {fingerprint_sha256: $fingerprint}' \
  "$RUN_DIR/environment-core.json" >"$RUN_DIR/environment.json"

jq -n \
  --argjson pairs "$PAIRS" \
  --argjson warmup "$WARMUP_SECONDS" \
  --argjson measurement "$MEASUREMENT_SECONDS" \
  --argjson drain_timeout "$DRAIN_TIMEOUT_SECONDS" \
  --argjson iterations "$BOOTSTRAP_ITERATIONS" \
  --argjson seed "$BOOTSTRAP_SEED" \
  --arg cases "$CASES" \
  '{
    pairs: $pairs,
    warmup_seconds: $warmup,
    measurement_seconds: $measurement,
    drain_timeout_seconds: $drain_timeout,
    alternating_order: "AB_then_BA",
    bootstrap_iterations: $iterations,
    bootstrap_seed: $seed,
    throughput_regression_ratio: 0.95,
    latency_rss_regression_ratio: 1.10,
    scenarios: [
      {
        name: "raw_write",
        topic_count: 1,
        channel_count: 0,
        producers: 16,
        consumers: 0,
        message_bytes: 1024,
        batch_size: 64,
        rate: "saturation",
        headline_metric: "publish_messages_per_second"
      },
      {
        name: "sustainable",
        topic_count: 1,
        channel_count: 1,
        producers: 16,
        consumers: 16,
        message_bytes: 1024,
        batch_size: 64,
        rate: "saturation",
        headline_metric: "receive_messages_per_second",
        completion_contract: "depth=0, in_flight=0, deferred=0, missing=0, duplicates=0"
      },
      {
        name: "low_load_latency",
        topic_count: 1,
        channel_count: 1,
        producers: 16,
        consumers: 16,
        message_bytes: 1024,
        batch_size: 1,
        messages_per_second: 100,
        headline_metric: "pub_ack_p99_us",
        completion_contract: "depth=0, in_flight=0, deferred=0, missing=0, duplicates=0"
      }
    ]
  }
  | .scenarios = [
      .scenarios[]
      | select(.name as $name | ($cases | split(" ") | index($name)) != null)
    ]' >"$RUN_DIR/protocol.json"

generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg release "$RELEASE" \
  --arg generated_at "$generated_at" \
  --arg baseline_ref "$BASELINE_REF" \
  --arg baseline_commit "$baseline_commit" \
  --arg baseline_image "$baseline_image_id" \
  --arg baseline_binary_sha256 "$baseline_binary_sha256" \
  --arg candidate_ref "$CANDIDATE_REF" \
  --arg candidate_commit "$candidate_commit" \
  --arg candidate_image "$candidate_image_id" \
  --arg candidate_binary_sha256 "$candidate_binary_sha256" \
  --slurpfile environment "$RUN_DIR/environment.json" \
  --slurpfile protocol "$RUN_DIR/protocol.json" \
  --slurpfile runs "$RUNS_FILE" \
  '{
    schema_version: 1,
    release: $release,
    generated_at_utc: $generated_at,
    baseline: {
      revision: $baseline_ref,
      commit: $baseline_commit,
      image_id: $baseline_image,
      binary_sha256: $baseline_binary_sha256
    },
    candidate: {
      revision: $candidate_ref,
      commit: $candidate_commit,
      image_id: $candidate_image,
      binary_sha256: $candidate_binary_sha256
    },
    environment: $environment[0],
    protocol: $protocol[0],
    runs: $runs
  }' >"$INPUT_FILE"

set +e
docker run --rm \
  -v "$RUN_DIR:/results:ro" \
  --entrypoint /usr/local/bin/rustqueue-qualify \
  "$TOOLS_IMAGE" --input /results/input.json >"$EVIDENCE_FILE"
qualification_status=$?
set -e

mkdir -p "$(dirname "$EVIDENCE_OUTPUT")"
cp "$EVIDENCE_FILE" "$EVIDENCE_OUTPUT"
printf 'Raw qualification artifacts: %s\n' "$RUN_DIR"
printf 'Compact qualification evidence: %s\n' "$EVIDENCE_OUTPUT"
exit "$qualification_status"
