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

echo "RustQueue release metadata is consistent at $EXPECTED"
