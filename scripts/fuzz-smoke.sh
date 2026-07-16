#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
seconds=${FUZZ_SECONDS:-10}
case "$seconds" in
  ''|*[!0-9]*) printf 'FUZZ_SECONDS must be an integer\n' >&2; exit 2 ;;
esac
[ "$seconds" -gt 0 ] || { printf 'FUZZ_SECONDS must be greater than zero\n' >&2; exit 2; }

host_ca=
if command -v security >/dev/null 2>&1; then
  host_ca=$(mktemp)
  trap 'rm -f "$host_ca"' EXIT INT TERM
  security find-certificate -a -p /Library/Keychains/System.keychain > "$host_ca"
  docker build -q --secret "id=host_ca,src=$host_ca" \
    -f "$root/fuzz/Dockerfile" -t rustqueue-fuzz:dev "$root" >/dev/null
else
  docker build -q -f "$root/fuzz/Dockerfile" -t rustqueue-fuzz:dev "$root" >/dev/null
fi
toolchain=$(docker run --rm rustqueue-fuzz:dev sh -c "rustup show active-toolchain | awk '{print \$1}'")
for target in protocol storage_record compression; do
  docker run --rm \
    -e CARGO_INCREMENTAL=0 \
    -e RUSTUP_TOOLCHAIN="$toolchain" \
    -v "$root:/work" \
    -v rustqueue-cargo-registry:/usr/local/cargo/registry \
    -w /work/fuzz \
    rustqueue-fuzz:dev \
    cargo fuzz run "$target" -- -max_total_time="$seconds"
done
