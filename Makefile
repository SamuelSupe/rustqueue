.PHONY: test check fmt clippy release-bin operator-release-bin image operator-image helm-lint helm-template k8s-acceptance up down dev-certs cluster-up cluster-down cluster4-up cluster4-down cluster5-up cluster5-down cluster5-rf5-up cluster5-rf5-down cluster9-up cluster9-down federation9-up federation9-down compat compat-core compat-go compat-python fuzz-smoke acceptance-4 acceptance-5 acceptance-9 acceptance-federation acceptance-rolling acceptance-discovery acceptance-expand acceptance-network-scale rss-gate snapshot-drill crash-smoke soak benchmark

RUST_IMAGE := rust:1.88-bookworm
CARGO_CACHE := rustqueue-cargo-registry
RUSTUP_CACHE := rustqueue-rustup
RUN := docker run --rm -e RUSTUP_TOOLCHAIN=1.88.0 -e CARGO_INCREMENTAL=0 -e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin -v $(CURDIR):/work -w /work -v $(CARGO_CACHE):/usr/local/cargo/registry -v $(RUSTUP_CACHE):/usr/local/rustup $(RUST_IMAGE)

test:
	$(RUN) cargo test --locked --workspace

check:
	$(RUN) cargo check --locked --workspace

fmt:
	$(RUN) cargo fmt --all -- --check

clippy:
	$(RUN) cargo clippy --locked --workspace --all-targets -- -D warnings

release-bin:
	$(RUN) cargo build --locked --release --bin rustqueued --bin rustqueue-bench
	mkdir -p .docker-bin
	cp target/release/rustqueued target/release/rustqueue-bench .docker-bin/

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
	helm template rustqueue deploy/helm/rustqueue --namespace rustqueue-system

k8s-acceptance:
	./scripts/acceptance-k8s.sh

up: image
	docker compose up -d --no-build

down:
	docker compose down

dev-certs:
	./scripts/generate-dev-certs.sh

cluster-up: dev-certs image
	docker compose -f deploy/cluster-compose.yml up -d --no-build

cluster-down:
	docker compose -f deploy/cluster-compose.yml down

cluster4-up: dev-certs image
	./scripts/generate-cluster-configs.sh 4
	CLUSTER_SIZE=4 COMPOSE_PROFILES=plus docker compose -p rustqueue4 -f deploy/multinode-compose.yml up -d --no-build

cluster4-down:
	CLUSTER_SIZE=4 COMPOSE_PROFILES=plus docker compose -p rustqueue4 -f deploy/multinode-compose.yml down

cluster5-up: dev-certs image
	./scripts/generate-cluster-configs.sh 5
	CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five docker compose -p rustqueue5 -f deploy/multinode-compose.yml up -d --no-build

cluster5-down:
	CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five docker compose -p rustqueue5 -f deploy/multinode-compose.yml down

cluster5-rf5-up: dev-certs image
	METADATA_RF=5 ./scripts/generate-cluster-configs.sh 5
	CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five docker compose -p rustqueue5rf5 -f deploy/multinode-compose.yml up -d --no-build

cluster5-rf5-down:
	CLUSTER_SIZE=5 COMPOSE_PROFILES=plus,five docker compose -p rustqueue5rf5 -f deploy/multinode-compose.yml down

cluster9-up: dev-certs image
	./scripts/generate-cluster-configs.sh 9
	CLUSTER_SIZE=9 COMPOSE_PROFILES=plus,five,nine docker compose -p rustqueue9 -f deploy/multinode-compose.yml up -d --no-build

cluster9-down:
	CLUSTER_SIZE=9 COMPOSE_PROFILES=plus,five,nine docker compose -p rustqueue9 -f deploy/multinode-compose.yml down

federation9-up: dev-certs image
	FEDERATION_CELL_SIZE=3 ./scripts/generate-cluster-configs.sh 9
	CLUSTER_SIZE=9 COMPOSE_PROFILES=plus,five,nine docker compose -p rustqueuefed9 -f deploy/multinode-compose.yml up -d --no-build

federation9-down:
	CLUSTER_SIZE=9 COMPOSE_PROFILES=plus,five,nine docker compose -p rustqueuefed9 -f deploy/multinode-compose.yml down

compat: image
	./scripts/compat-matrix.sh

compat-core: compat-go compat-python

compat-go:
	./scripts/compat-go.sh

compat-python:
	./scripts/compat-python.sh

fuzz-smoke:
	./scripts/fuzz-smoke.sh

acceptance-4:
	./scripts/acceptance-4.sh

acceptance-5:
	./scripts/acceptance-5.sh

acceptance-9:
	./scripts/acceptance-9.sh

acceptance-federation:
	./scripts/acceptance-federation.sh

acceptance-rolling:
	./scripts/acceptance-rolling.sh

acceptance-discovery:
	./scripts/acceptance-discovery.sh

acceptance-expand:
	./scripts/acceptance-expand.sh

acceptance-network-scale:
	./scripts/acceptance-network-scale.sh

rss-gate:
	./scripts/rss-gate.sh

snapshot-drill: image
	./scripts/snapshot-drill.sh

crash-smoke: cluster5-up
	DURATION_SECONDS=60 GRACE_SECONDS=60 RATE=200 RESTART_INTERVAL_SECONDS=5 MIN_RESTARTS=5 ./scripts/soak.sh

soak: cluster5-up
	./scripts/soak.sh

benchmark:
	./scripts/benchmark-compare.sh
