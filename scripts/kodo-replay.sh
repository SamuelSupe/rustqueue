#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KODO_SOURCE="${KODO_SOURCE_DIR:-"$ROOT/../kodo-master"}"
REVIEWED_SOURCES=(
  "go.mod:6952906dc0c673292f761febfb024bbdc3fdbe84dffd9804fac204f1c84f9823"
  "config/cfg.go:5b6e53e0ee0fe01871d4b715b15ef0d4b7b0b2ce9b125bcc81f20d2754770fb7"
  "nsq/nsq.go:731670a0cb1ed223314a23e78b81e6c51b683a75efe4170a39c49f01fe189c8c"
  "nsq/lookupd.go:a693864a774949d2225cf57a1eb42e0064596516a6ef97462e449b890e0c9802"
  "nsq/nsqadmin.go:e12dde1526d05aa0af2e704340d6ca750a89509cf7f25a953f04c14427c4a39b"
  "nsq/metrics.go:51894ffaeba1e198c6ce7d129884d7b700d7d66e7eec7d2232f8e4e06de9928f"
  "utils/err.go:299d3b9503ddb11aadb378799927c930caafabf899f92d4dd14b5c0b92a27a5b"
)

if [[ ! -f "$KODO_SOURCE/go.mod" ]]; then
  echo "KODO_SOURCE_DIR must point to a Kodo source checkout" >&2
  exit 1
fi
for reviewed in "${REVIEWED_SOURCES[@]}"; do
  source_path="${reviewed%%:*}"
  expected_sha256="${reviewed#*:}"
  if [[ ! -f "$KODO_SOURCE/$source_path" ]]; then
    echo "Kodo source is missing $source_path" >&2
    exit 1
  fi
  actual_sha256="$(shasum -a 256 "$KODO_SOURCE/$source_path" | awk '{print $1}')"
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "Kodo $source_path does not match the reviewed upstream source" >&2
    echo "expected: $expected_sha256" >&2
    echo "actual:   $actual_sha256" >&2
    exit 1
  fi
done

DOCKER_ARGS=(
  --rm
  --network none
  -e GOCACHE=/tmp/go-build
  -e GOTOOLCHAIN=local
  -e GOMAXPROCS=1
  -e GOFLAGS=-p=1
  -e RUSTQUEUE_REPLAY_FIXTURES=/replay/fixtures
  -v "$KODO_SOURCE:/source/kodo:ro"
  -v "$ROOT/tests/kodo-replay:/replay:ro"
)

docker run "${DOCKER_ARGS[@]}" golang:1.25-bookworm sh -ec '
  mkdir -p /tmp/kodo/nsq /tmp/kodo/config
  cp /source/kodo/nsq/nsqadmin.go /tmp/kodo/nsq/nsqadmin.go
  cp /replay/kodo_replay_test.go /tmp/kodo/nsq/kodo_replay_test.go
  cp /replay/logger_stub.go /tmp/kodo/nsq/logger_stub.go
  cp /replay/config_stub.go /tmp/kodo/config/config_stub.go
  cp /replay/kodo_stub.go /tmp/kodo/kodo_stub.go
  cp /replay/go.mod /tmp/kodo/go.mod
  cd /tmp/kodo
  go test ./nsq -run "^TestRustQueueStatsReplay$" -count=1
'
