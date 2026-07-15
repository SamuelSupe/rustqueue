#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tcp_address=${1:-127.0.0.1:4150}
http_address=${2:-127.0.0.1:4151}

docker run --rm \
  --network host \
  -v "$root/tests/compat/python:/work:ro" \
  -w /work \
  python:3.12-slim \
  sh -c 'pip install -q --disable-pip-version-check --root-user-action=ignore -r requirements.txt && python main.py core "$1" "$2"' sh "$tcp_address" "$http_address"
