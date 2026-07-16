# RustQueue 0.7

RustQueue is an NSQ V2 compatible, Kubernetes-native, share-nothing message
queue written in Rust. Each broker owns one RWO PVC and persists messages and
channel acknowledgements locally. There is no Raft, broker-to-broker message
replication, leader routing, or global topic catalog.

The complete architecture and reliability contract is documented in
[`docs/architecture/share-nothing-v7.md`](docs/architecture/share-nothing-v7.md).

## Architecture

```text
producer -> node-local proxy -> one publish-ready broker -> that broker's PVC

consumer -> discovery /lookup -> every broker that owns the topic
                              -> direct NSQ TCP connections

operator -> eligible nodes -> StatefulSet ordinal + retained RWO PVC
```

- A successful `PUB`, `MPUB`, or `DPUB` has passed local segment `fsync`.
- Concurrent publishes to one topic use a bounded group commit (up to 64
  requests or 8 MiB, with at most 1 ms coalescing delay) and all wait for the
  same durable boundary before receiving `OK`.
- IDs are durably reserved in blocks, so restarts may create harmless gaps but
  never reuse a broker-scoped message ID.
- A successful `FIN` or `REQ` has passed the local channel WAL `fsync`.
- Delivery is at least once. A restart redelivers messages without durable FIN.
- The broker PVC is the only copy; permanent PVC loss loses its messages.
- Topics and channels are broker-local. Lookup consumers union all owners.
- Messages are retained for 30 seconds before a channel exists so discovery and
  `SUB` can catch a newly selected owner without a normal-path miss.
- The stable v7 wire limit is 32 MiB; the default single-message limit is
  20 MiB and the default MPUB body limit is 64 MiB.

## Components

- `rustqueued`: local durable NSQ broker and HTTP management API.
- `rustqueue-discovery`: stateless EndpointSlice-derived lookup service.
- `rustqueue-proxy`: bounded producer TCP/HTTP proxy; TCP is connection-pinned.
- `rustqueue-operator`: creates the StatefulSet, retained PVCs, discovery,
  proxy, RBAC and drain-aware one-at-a-time rolling updates.

The broker implements `IDENTIFY`, `AUTH`, `SUB`, `PUB`, `MPUB`, `DPUB`, `RDY`,
`FIN`, `REQ`, `TOUCH`, `NOP`, and `CLS`, including TLS/mTLS, Snappy, Deflate,
output buffering, sampling, fan-out and ephemeral channels.

## Local development

The host does not need Rust. Builds and tests use the pinned Docker toolchain:

```sh
make check
make test
make clippy
make image
docker compose up -d
```

Then publish through the compatible HTTP API:

```sh
curl -X POST --data-binary 'hello' \
  'http://127.0.0.1:4151/pub?topic=events'
curl 'http://127.0.0.1:4151/stats?format=json'
```

Run the official client behavior matrix:

```sh
make compat
```

This covers official Go and Python clients across direct publishing and
consumption, lookup, MPUB, DPUB, REQ, TOUCH, RDY, fan-out, sampling,
ephemeral channels, Snappy, Deflate, TLS, mTLS and external AUTH.

## Kubernetes

Prerequisites:

- Kubernetes 1.28 or later;
- an SSD-backed `ReadWriteOnce` StorageClass that supports Pod `fsGroup`
  ownership;
- eligible nodes labelled for brokers;
- the broker runtime and operator images in the cluster registry.

```sh
kubectl label node worker-1 rustqueue.io/eligible=true

helm upgrade --install rustqueue deploy/helm/rustqueue \
  --namespace rustqueue --create-namespace \
  --set queue.image=registry.example/rustqueue:0.7.0 \
  --set queue.storageClassName=ssd-rwo
```

The producer endpoints are exposed by the proxy Service. Consumers should use
both discovery replicas as lookupd endpoints. The generated runtime Secret is
internal; set `queue.registrySecretName` to use a pre-created Secret containing
`admin-token` and `registry-token`. Client TLS is optional and always supplied
through an existing Kubernetes Secret; the operator does not run a CA.

Runtime Pods run as UID/GID 65532 with a read-only root filesystem. Broker Pods
set `fsGroup=65532` and `fsGroupChangePolicy=OnRootMismatch` so dynamically
provisioned PVCs are writable without an init container.

For a functional validation on local OrbStack Kubernetes:

```sh
make k8s-acceptance
make k8s-multi-acceptance
```

The first acceptance validates the operator, StatefulSet/PVC attachment,
discovery, proxy, official Go client behavior, Pod restart with the same PVC,
durable backlog recovery, and the safety gate that refuses to roll the only
broker. The second runs three real broker Pods with three independent RWO PVCs,
first proves that an unsupported storage feature is blocked without replacing
a Pod, then publishes and consumes through discovery during an image rollout.
The acknowledged-message ledger must finish with `missing=0`. OrbStack has one
physical node, so this test uses capacity-only Kubernetes Node objects and
test-only direct Pod placement; production anti-affinity is unchanged. A unit
fixture covers discovery indexing for 500 brokers. No 500-broker deployment or
load test is part of the functional gate.

## Disk pressure

The broker checks its data filesystem once per second. At either the configured
high watermark or minimum-free threshold it stops accepting publishes. HTTP
returns `429` with `Retry-After`; TCP returns `E_THROTTLED`. Consumption and FIN
remain available.

Normal segment GC runs first. If pressure lasts beyond the configured grace
period and protective eviction is enabled, the oldest complete local segment
may be removed. RustQueue persists every affected channel gap and a structured
audit intent before deleting the segment. Protective eviction is a last-resort
availability policy and intentionally loses the evicted messages.

A runtime storage write, fsync, or checksum failure is fail-closed: the storage
handle is isolated, readiness is withdrawn, and the broker exits so Kubernetes
can restart it through normal tail recovery. Payload reader leases also prevent
GC, protective eviction, or topic deletion from racing an active disk read.

The producer proxy defaults to 10,000 simultaneous TCP connections, a 64 MiB
request-body limit, and a 512 MiB node-wide in-flight HTTP body budget.
Override them with `RUSTQUEUE_PROXY_MAX_CONNECTIONS`,
`RUSTQUEUE_PROXY_MAX_BODY_BYTES`, and
`RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES`.

## Local HTTP endpoints

Compatible endpoints:

- `/pub`, `/mpub`, `/stats`, `/ping`, `/info`
- `/topic/create|delete|empty|pause|unpause`
- `/channel/create|delete|empty|pause|unpause`

Native broker endpoints:

- `GET /v1/health`
- `GET /v1/registry`
- `GET /v1/capabilities`
- `GET|POST /v1/drain`
- `GET /v1/stats`
- `POST /v1/storage/scrub`
- `GET /metrics`

Discovery serves `/lookup`, `/topics`, `/channels`, `/nodes`, `/ping`, `/info`,
`/v1/publishers`, `/v1/health`, and `/metrics`. The producer proxy also exposes
`/metrics` on its HTTP listener.

Latency histograms cover durable fsync, group-commit queueing, publish ACK,
payload reads, scrub/GC, proxy backend calls, and discovery registry polling.

## Storage and upgrades

Format v7 is a clean break. A v6 or older directory is refused; there is no
in-place migration. Within v7, record tags and existing fields are append-only.
Every binary declares its reader/writer feature range, and every PVC persists
its active writer and minimum reader levels in `/data/COMPATIBILITY`. The
operator probes the target image and every running broker before replacement;
an incompatible target is blocked before a PVC is touched.

Adding a record kind is a two-step rollout: first roll the capable binary to all
brokers, then explicitly increase `storageFeatureLevel`. The storage layer
rejects writes above the active feature level. Once activated, the PVC rollback
fence rejects a binary that cannot read the enabled level, so recovery is a
forward upgrade instead of an unsafe rollback. The operator still drains and
replaces one broker at a time while preserving retained PVC identity.

The on-disk layout is:

```text
/data/FORMAT
/data/COMPATIBILITY
/data/broker.meta
/data/topics/<hex-topic>/manifest
/data/topics/<hex-topic>/segments/*.rqlog
/data/topics/<hex-topic>/channels/<hex-channel>.checkpoint
/data/topics/<hex-topic>/channels/<hex-channel>.wal
/data/dlq-outbox/                # compact CRC-protected binary transfer intents
/data/audit/
```

Tail short writes are truncated during startup. A complete CRC-corrupt record,
middle corruption, invalid channel WAL or identity mismatch fails closed.

Crash-boundary integration tests stop worker processes with actual `SIGKILL`
after append, fsync-before-ACK, checkpoint file fsync-before-rename, and GC
delete boundaries. Reopened stores are checked against an acknowledged-message
ledger; ambiguous pre-ACK writes may duplicate, but acknowledged entries may
not be missing. Injection code is available only under the compile-time
`crash-injection` test feature and is absent from normal release behavior.

The fuzz smoke matrix covers protocol, compression, storage records, channel
WAL/checkpoint replay, topic manifests, proxy HTTP metadata, and discovery
registry responses.

## Non-goals

RustQueue 0.7 does not provide message replication, backups, exactly-once
delivery, a global channel catalog, online data migration, cross-region
replication, or a management UI.
