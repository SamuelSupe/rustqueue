#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
address=${1:-host.docker.internal:4150}
http_address=${2:-host.docker.internal:4151}

docker run --rm \
  -e GOTOOLCHAIN=local \
  -v "$root/tests/compat/go:/work" \
  -w /work \
  golang:1.26-alpine \
  /bin/sh -c '/usr/local/go/bin/go mod download && /usr/local/go/bin/go run . core "$1" "$2"' sh "$address" "$http_address"
