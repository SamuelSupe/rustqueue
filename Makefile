.PHONY: test check fmt clippy rustfmt-component clippy-component release-bin image image-from-dist operator-release-bin operator-image console-ui-build console-ui-check kodo-replay kodo-gateway-acceptance \
	helm-lint helm-template k8s-acceptance k8s-console-management-acceptance k8s-multi-acceptance up down compat compat-go compat-python \
	fuzz-smoke benchmark release-gate

RUST_IMAGE := rust:1.88-bookworm
CARGO_CACHE := rustqueue-cargo-registry
RUSTUP_CACHE := rustqueue-rustup
UI_IMAGE := node:24-bookworm-slim
UI_MODULES := rustqueue-console-node-modules
UI_COREPACK := rustqueue-console-corepack
UI_DOCKER_ARGS ?=
BUILD_VERSION ?=
MAX_STORAGE_FEATURE_LEVEL ?=
RUN := docker run --rm -e RUSTUP_TOOLCHAIN=1.88.0 -e CARGO_INCREMENTAL=0 \
	-e RUSTQUEUE_BUILD_VERSION=$(BUILD_VERSION) \
	-e RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL=$(MAX_STORAGE_FEATURE_LEVEL) \
	-e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
	-v $(CURDIR):/work -w /work -v $(CARGO_CACHE):/usr/local/cargo/registry \
	-v $(RUSTUP_CACHE):/usr/local/rustup $(RUST_IMAGE)

test:
	$(RUN) cargo test --locked --workspace --features rustqueue-queue/crash-injection

check:
	$(RUN) cargo check --locked --workspace --all-features

rustfmt-component:
	$(RUN) rustup component add rustfmt

clippy-component:
	$(RUN) rustup component add clippy

fmt: rustfmt-component
	$(RUN) cargo fmt --all -- --check

clippy: clippy-component
	$(RUN) cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

release-bin:
	$(RUN) cargo build --locked --release \
		--bin rustqueued --bin rustqueue-discovery --bin rustqueue-proxy --bin rustqueue-bench \
		--bin rustqueuectl --bin rustqueue-console
	mkdir -p .docker-bin
	cp target/release/rustqueued target/release/rustqueue-discovery \
		target/release/rustqueue-proxy target/release/rustqueue-bench \
		target/release/rustqueuectl target/release/rustqueue-console .docker-bin/

console-ui-build:
	docker run --rm $(UI_DOCKER_ARGS) -e CI=true -e HOME=/tmp -e COREPACK_HOME=/tmp/corepack \
		-v $(CURDIR):/work -w /work/console-ui -v $(UI_MODULES):/work/console-ui/node_modules \
		-v $(UI_COREPACK):/tmp/corepack \
		$(UI_IMAGE) sh -lc 'corepack enable && corepack pnpm install --frozen-lockfile && corepack pnpm build'

console-ui-check:
	docker run --rm $(UI_DOCKER_ARGS) -e CI=true -e HOME=/tmp -e COREPACK_HOME=/tmp/corepack \
		-v $(CURDIR):/work -w /work/console-ui -v $(UI_MODULES):/work/console-ui/node_modules \
		-v $(UI_COREPACK):/tmp/corepack \
		$(UI_IMAGE) sh -lc 'corepack enable && corepack pnpm install --frozen-lockfile && corepack pnpm check'

image: release-bin console-ui-build
	docker build -f Dockerfile.runtime -t rustqueue:dev .

image-from-dist: release-bin
	test -f console-ui/dist/index.html
	docker build -f Dockerfile.runtime -t rustqueue:dev .

operator-release-bin:
	$(RUN) cargo build --locked --release --bin rustqueue-operator
	mkdir -p .docker-bin
	cp target/release/rustqueue-operator .docker-bin/

operator-image: operator-release-bin
	docker build -f Dockerfile.operator -t rustqueue-operator:dev .

helm-lint:
	helm lint deploy/helm/rustqueue
	helm lint deploy/helm/rustqueue --set monitoring.serviceMonitor.enabled=true \
		--set monitoring.prometheusRule.enabled=true
	helm lint deploy/helm/rustqueue --set queue.kodoCompatibility.enabled=true \
		--set queue.imagePullPolicy=Never
	! helm template rustqueue deploy/helm/rustqueue \
		--set queue.kodoCompatibility.cleanupEnabled=true
	! rg -n 'x-kubernetes-preserve-unknown-fields:[[:space:]]*false' \
		deploy/helm/rustqueue/crds

helm-template:
	helm template rustqueue deploy/helm/rustqueue --namespace rustqueue --include-crds
	helm template rustqueue deploy/helm/rustqueue --namespace rustqueue \
		--set monitoring.serviceMonitor.enabled=true \
		--set monitoring.prometheusRule.enabled=true
	helm template rustqueue deploy/helm/rustqueue --namespace rustqueue \
		--set queue.kodoCompatibility.enabled=true \
		--set queue.imagePullPolicy=Never

k8s-acceptance:
	./scripts/acceptance-k8s.sh

k8s-console-management-acceptance:
	./scripts/acceptance-console-management.sh

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

kodo-replay:
	bash ./scripts/kodo-replay.sh

kodo-gateway-acceptance:
	bash ./scripts/acceptance-kodo-gateway.sh

fuzz-smoke:
	./scripts/fuzz-smoke.sh

benchmark:
	./scripts/benchmark-compare.sh

release-gate:
	./scripts/release-gate.sh
