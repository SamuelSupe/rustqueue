FROM node:24-bookworm-slim AS ui-builder
WORKDIR /ui
RUN corepack enable
COPY console-ui/package.json console-ui/pnpm-lock.yaml console-ui/pnpm-workspace.yaml ./
COPY console-ui/vendor ./vendor
RUN corepack pnpm install --frozen-lockfile
COPY console-ui/ ./
RUN corepack pnpm build

FROM rust:1.88-bookworm AS builder
ARG RUSTQUEUE_BUILD_VERSION
ARG RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL
ENV RUSTUP_TOOLCHAIN=1.88.0
ENV RUSTQUEUE_BUILD_VERSION=$RUSTQUEUE_BUILD_VERSION
ENV RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL=$RUSTQUEUE_MAX_STORAGE_FEATURE_LEVEL
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin rustqueued --bin rustqueue-discovery \
      --bin rustqueue-proxy --bin rustqueue-bench --bin rustqueuectl --bin rustqueue-console && \
    cp target/release/rustqueued /tmp/rustqueued && \
    cp target/release/rustqueue-discovery /tmp/rustqueue-discovery && \
    cp target/release/rustqueue-proxy /tmp/rustqueue-proxy && \
    cp target/release/rustqueue-bench /tmp/rustqueue-bench && \
    cp target/release/rustqueuectl /tmp/rustqueuectl && \
    cp target/release/rustqueue-console /tmp/rustqueue-console

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /data /var/lib/rustqueue && chown -R 65532:65532 /data /var/lib/rustqueue
COPY --from=builder /tmp/rustqueued /usr/local/bin/rustqueued
COPY --from=builder /tmp/rustqueue-discovery /usr/local/bin/rustqueue-discovery
COPY --from=builder /tmp/rustqueue-proxy /usr/local/bin/rustqueue-proxy
COPY --from=builder /tmp/rustqueue-bench /usr/local/bin/rustqueue-bench
COPY --from=builder /tmp/rustqueuectl /usr/local/bin/rustqueuectl
COPY --from=builder /tmp/rustqueue-console /usr/local/bin/rustqueue-console
COPY --from=ui-builder /ui/dist /usr/share/rustqueue-console
USER 65532:65532
WORKDIR /var/lib/rustqueue
EXPOSE 4150 4151 4161 4180
ENTRYPOINT ["/usr/local/bin/rustqueued"]
