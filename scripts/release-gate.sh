#!/usr/bin/env bash
set -euo pipefail

FULL="${FULL:-1}"
K8S_ACCEPTANCE="${K8S_ACCEPTANCE:-0}"
FUZZ_SECONDS="${FUZZ_SECONDS:-1}"

make fmt
make check
make test
make clippy
make helm-lint
make helm-template
FUZZ_SECONDS="$FUZZ_SECONDS" ./scripts/fuzz-smoke.sh

if [[ "$FULL" == "1" ]]; then
  make compat
fi

if [[ "$K8S_ACCEPTANCE" == "1" ]]; then
  [[ "$(kubectl config current-context)" == "orbstack" ]] || {
    echo "K8S_ACCEPTANCE=1 requires the OrbStack Kubernetes context" >&2
    exit 1
  }
  make k8s-acceptance
  make k8s-multi-acceptance
fi

echo "RustQueue release gate passed"
