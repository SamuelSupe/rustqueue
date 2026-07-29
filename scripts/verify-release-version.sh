#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED="${1:-}"

if [[ ! "$EXPECTED" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <major.minor.patch>" >&2
  exit 2
fi

read_workspace_version() {
  awk '
    /^\[workspace\.package\]$/ { workspace = 1; next }
    /^\[/ { workspace = 0 }
    workspace && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$ROOT/Cargo.toml"
}

read_chart_field() {
  local field="$1"
  awk -v field="$field" '
    $1 == field ":" {
      gsub(/"/, "", $2)
      print $2
      exit
    }
  ' "$ROOT/deploy/helm/rustqueue/Chart.yaml"
}

read_ui_version() {
  awk -F'"' '$2 == "version" { print $4; exit }' \
    "$ROOT/console-ui/package.json"
}

require_version() {
  local label="$1"
  local actual="$2"
  if [[ "$actual" != "$EXPECTED" ]]; then
    echo "$label version is $actual, expected $EXPECTED" >&2
    exit 1
  fi
}

verify_lock() {
  local lock="$1"
  awk -v expected="$EXPECTED" '
    $1 == "name" && $3 ~ /^"rustqueue/ && $3 != "\"rustqueue-fuzz\"" {
      package = $3
      getline
      wanted = "\"" expected "\""
      if ($1 != "version" || $3 != wanted) {
        printf "%s has version %s in %s, expected %s\n",
          package, $3, FILENAME, wanted > "/dev/stderr"
        failed = 1
      }
      checked += 1
    }
    END {
      if (checked == 0) {
        printf "no RustQueue packages found in %s\n", FILENAME > "/dev/stderr"
        exit 1
      }
      exit failed
    }
  ' "$lock"
}

require_version "workspace" "$(read_workspace_version)"
require_version "Helm Chart" "$(read_chart_field version)"
require_version "Helm app" "$(read_chart_field appVersion)"
require_version "Console UI" "$(read_ui_version)"
verify_lock "$ROOT/Cargo.lock"
verify_lock "$ROOT/fuzz/Cargo.lock"

grep -Fq "tag: \"$EXPECTED\"" "$ROOT/deploy/helm/rustqueue/values.yaml" || {
  echo "operator image tag is not $EXPECTED" >&2
  exit 1
}
grep -Fq "image: rustqueue:$EXPECTED" "$ROOT/deploy/helm/rustqueue/values.yaml" || {
  echo "broker image tag is not $EXPECTED" >&2
  exit 1
}
grep -Fq "Current release: [v$EXPECTED]" "$ROOT/README.md" || {
  echo "README current release is not v$EXPECTED" >&2
  exit 1
}
[[ -f "$ROOT/docs/releases/v$EXPECTED.md" ]] || {
  echo "docs/releases/v$EXPECTED.md is missing" >&2
  exit 1
}

qualification="$ROOT/benchmarks/qualifications/v$EXPECTED-orbstack.json"
if [[ "$EXPECTED" == "0.8.2" && -f "$qualification" ]]; then
  command -v jq >/dev/null 2>&1 || {
    echo "jq is required to verify Broker qualification evidence" >&2
    exit 1
  }
  jq -e --arg version "$EXPECTED" '
    .schema_version == 1
    and .release == $version
    and .baseline.revision == "v0.8.1"
    and (.baseline.commit | test("^[0-9a-f]{40}$"))
    and (.candidate.commit | test("^[0-9a-f]{40}$"))
    and (.baseline.binary_sha256 | test("^[0-9a-f]{64}$"))
    and (.candidate.binary_sha256 | test("^[0-9a-f]{64}$"))
    and .environment.platform == "OrbStack on macOS"
    and .environment.tool_source == .candidate.commit
    and .environment.resource_limits.broker.cpus == 2
    and .environment.resource_limits.broker.memory_bytes == 2147483648
    and .environment.resource_limits.load_generator.cpus == 2
    and .environment.resource_limits.load_generator.memory_bytes == 2147483648
    and .protocol.pairs == 10
    and .protocol.warmup_seconds == 30
    and .protocol.measurement_seconds == 120
    and .protocol.alternating_order == "AB_then_BA"
    and .protocol.throughput_regression_ratio == 0.95
    and .protocol.latency_rss_regression_ratio == 1.10
    and ([.protocol.scenarios[].name] | sort)
      == ["low_load_latency", "raw_write", "sustainable"]
    and (.runs | length) == 60
    and all(.runs[]; .benchmark_exit_code == 0)
    and all(
      .runs[]
      | select(.case != "raw_write");
      .metrics.delivery_verified == true
      and .metrics.delivery_complete == true
      and .metrics.drain_timed_out == false
      and .metrics.missing_messages == 0
      and .metrics.duplicate_messages == 0
      and .metrics.final_channel_depth == 0
      and .metrics.final_in_flight == 0
      and .metrics.final_deferred == 0
      and .metrics.broker_profile.aggregate_channel_depth == 0
      and .metrics.broker_profile.aggregate_channel_in_flight == 0
      and .metrics.broker_profile.aggregate_channel_deferred == 0
    )
    and ([.statistics[] | "\(.case):\(.metric)"] | sort) == [
      "low_load_latency:pub_ack_p99_us",
      "low_load_latency:rss_peak_bytes",
      "raw_write:publish_messages_per_second",
      "sustainable:receive_messages_per_second"
    ]
    and all(.statistics[]; .regression == false)
    and .verdict.status == "pass"
    and (.verdict.hard_failures | length) == 0
    and (.verdict.regressions | length) == 0
  ' "$qualification" >/dev/null || {
    echo "Broker qualification evidence is invalid or failed" >&2
    exit 1
  }
fi

echo "RustQueue release metadata is consistent at $EXPECTED"
