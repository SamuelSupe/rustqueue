FROM rust:1.88-bookworm AS builder
ENV RUSTUP_TOOLCHAIN=1.88.0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin rustqueued --bin rustqueue-discovery \
      --bin rustqueue-proxy --bin rustqueue-bench && \
    cp target/release/rustqueued /tmp/rustqueued && \
    cp target/release/rustqueue-discovery /tmp/rustqueue-discovery && \
    cp target/release/rustqueue-proxy /tmp/rustqueue-proxy && \
    cp target/release/rustqueue-bench /tmp/rustqueue-bench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /data /var/lib/rustqueue && chown -R 65532:65532 /data /var/lib/rustqueue
COPY --from=builder /tmp/rustqueued /usr/local/bin/rustqueued
COPY --from=builder /tmp/rustqueue-discovery /usr/local/bin/rustqueue-discovery
COPY --from=builder /tmp/rustqueue-proxy /usr/local/bin/rustqueue-proxy
COPY --from=builder /tmp/rustqueue-bench /usr/local/bin/rustqueue-bench
USER 65532:65532
WORKDIR /var/lib/rustqueue
EXPOSE 4150 4151 4161
ENTRYPOINT ["/usr/local/bin/rustqueued"]
