#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-}"
VERSION="${2:-}"
OUTPUT_ARG="${3:-release}"

"$ROOT/scripts/verify-release-version.sh" "$VERSION"
mkdir -p "$OUTPUT_ARG"
OUTPUT="$(cd "$OUTPUT_ARG" && pwd)"

case "$MODE" in
  binaries)
    ARCH="${4:-}"
    case "$ARCH" in
      x86_64|aarch64) ;;
      *)
        echo "binary architecture must be x86_64 or aarch64" >&2
        exit 2
        ;;
    esac

    BINARIES=(
      rustqueued
      rustqueue-discovery
      rustqueue-proxy
      rustqueue-bench
      rustqueuectl
      rustqueue-console
      rustqueue-operator
    )
    for binary in "${BINARIES[@]}"; do
      [[ -x "$ROOT/.docker-bin/$binary" ]] || {
        echo "missing release binary: .docker-bin/$binary" >&2
        exit 1
      }
    done
    [[ -f "$ROOT/console-ui/dist/index.html" ]] || {
      echo "console-ui/dist is missing; run make console-ui-build" >&2
      exit 1
    }

    STAGING="$(mktemp -d)"
    trap 'rm -rf "$STAGING"' EXIT
    PACKAGE="rustqueue-$VERSION"
    mkdir -p "$STAGING/$PACKAGE/bin" "$STAGING/$PACKAGE/console-ui"
    for binary in "${BINARIES[@]}"; do
      install -m 0755 "$ROOT/.docker-bin/$binary" "$STAGING/$PACKAGE/bin/$binary"
    done
    cp -R "$ROOT/console-ui/dist/." "$STAGING/$PACKAGE/console-ui/"
    install -m 0644 "$ROOT/README.md" "$STAGING/$PACKAGE/README.md"
    install -m 0644 \
      "$ROOT/rustqueue.example.toml" \
      "$STAGING/$PACKAGE/rustqueue.example.toml"
    tar -C "$STAGING" -czf \
      "$OUTPUT/rustqueue-$VERSION-linux-$ARCH.tar.gz" \
      "$PACKAGE"
    ;;
  common)
    git -C "$ROOT" archive \
      --format=tar.gz \
      --prefix="rustqueue-$VERSION/" \
      --output="$OUTPUT/rustqueue-$VERSION-source.tar.gz" \
      HEAD
    helm package "$ROOT/deploy/helm/rustqueue" --destination "$OUTPUT"
    ;;
  *)
    echo "usage: $0 <binaries|common> <version> [output-dir] [architecture]" >&2
    exit 2
    ;;
esac
