.PHONY: test check fmt clippy release-bin image operator-release-bin operator-image \
	helm-lint helm-template k8s-acceptance k8s-multi-acceptance up down compat compat-go compat-python \
	fuzz-smoke benchmark

RUST_IMAGE := rust:1.88-bookworm
CARGO_CACHE := rustqueue-cargo-registry
RUSTUP_CACHE := rustqueue-rustup
RUN := docker run --rm -e RUSTUP_TOOLCHAIN=1.88.0 -e CARGO_INCREMENTAL=0 \
	-e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
	-v $(CURDIR):/work -w /work -v $(CARGO_CACHE):/usr/local/cargo/registry \
	-v $(RUSTUP_CACHE):/usr/local/rustup $(RUST_IMAGE)

test:
	$(RUN) cargo test --locked --workspace --features rustqueue-queue/crash-injection

check:
	$(RUN) cargo check --locked --workspace --all-features

fmt:
	$(RUN) cargo fmt --all -- --check

clippy:
	$(RUN) cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

release-bin:
	$(RUN) cargo build --locked --release \
		--bin rustqueued --bin rustqueue-discovery --bin rustqueue-proxy --bin rustqueue-bench
	mkdir -p .docker-bin
	cp target/release/rustqueued target/release/rustqueue-discovery \
		target/release/rustqueue-proxy target/release/rustqueue-bench .docker-bin/

image: release-bin
	docker build -f Dockerfile.runtime -t rustqueue:dev .

operator-release-bin:
	$(RUN) cargo build --locked --release --bin rustqueue-operator
	mkdir -p .docker-bin
	cp target/release/rustqueue-operator .docker-bin/

operator-image: operator-release-bin
	docker build -f Dockerfile.operator -t rustqueue-operator:dev .

helm-lint:
	helm lint deploy/helm/rustqueue

helm-template:
	helm template rustqueue deploy/helm/rustqueue --namespace rustqueue

k8s-acceptance:
	./scripts/acceptance-k8s.sh

k8s-multi-acceptance:
	./scripts/acceptance-multi-broker-k8s.sh

up: image
	docker compose up -d --no-build

down:
	docker compose down

compat: image
	./scripts/compat-matrix.sh

compat-go:
	./scripts/compat-go.sh

compat-python:
	./scripts/compat-python.sh

fuzz-smoke:
	./scripts/fuzz-smoke.sh

benchmark:
	./scripts/benchmark-compare.sh
