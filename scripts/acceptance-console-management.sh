#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--no-build" ]]; then
  export BUILD_IMAGES=0
  shift
fi

export CONSOLE_MANAGEMENT_ENABLED=true
export NAMESPACE="${NAMESPACE:-rustqueue-console-management-e2e}"
export RELEASE="${RELEASE:-rustqueue-console-management-e2e}"
export QUEUE="${QUEUE:-queue}"

exec "$(dirname "$0")/acceptance-k8s.sh"
