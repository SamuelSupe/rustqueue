# RustQueue

[![CI](https://github.com/SamuelSupe/rustqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/SamuelSupe/rustqueue/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.28%2B-326CE5.svg)](https://kubernetes.io/)

RustQueue 0.7 is a Kubernetes-native, NSQ V2-compatible message queue for
trusted internal networks. It is written in Rust and uses a deliberately
simple share-nothing model: each Broker owns one durable RWO PVC, while
Kubernetes provides scheduling, rollout and discovery.

> Status: production candidate for workloads that accept single-PVC durability
> and at-least-once delivery. This project does not replicate messages between
> Brokers and is not an HA replacement for a replicated log.

The complete architecture and reliability contract is documented in
[`docs/architecture/share-nothing-v7.md`](docs/architecture/share-nothing-v7.md).

## At a glance

| Area | Contract |
| --- | --- |
| Durability | `PUB`/`MPUB`/`DPUB` return only after local segment `fsync`; `FIN`/`REQ` use a durable channel WAL |
| Delivery | At least once; a restart may redeliver a message without a durable `FIN` |
| Compatibility | NSQ V2 core commands, lookup, TLS/mTLS, AUTH, Snappy, Deflate, fan-out and ephemeral channels |
| Deployment | Kubernetes 1.28+, SSD-backed RWO PVCs, StatefulSet Broker ordinals, retained claims |
| Scaling | Add Brokers without a central queue coordinator; consumers connect to every owner of a Topic |
| Data model | Local Topic/Channel state; no cross-Broker ordering or global channel catalog |

The single-copy model is intentional. If a PVC is permanently lost, the
messages stored on that Broker are lost. Configure disk pressure protection,
monitor the exported metrics, and choose PVC/storage failure policies that fit
your workload before deploying to production.

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
- Per-topic publish and channel-commit workers retire after 60 idle seconds.
  The defaults cap durable topics at 10,000 and each worker class at 1,024;
  exhausted capacity returns retryable HTTP `429` instead of growing tasks and
  memory without bound.
- IDs are durably reserved in blocks, so restarts may create harmless gaps but
  never reuse a broker-scoped message ID.
- Concurrent `FIN` and `REQ` for one topic share a bounded group commit (up to
  64 requests with at most 1 ms coalescing delay). A successful response has
  passed every affected local channel WAL `fsync`.
- Delivery is at least once. A restart redelivers messages without durable FIN.
- The broker PVC is the only copy; permanent PVC loss loses its messages.
- Topics and channels are broker-local. Lookup consumers union all owners.
- Messages are retained for 90 seconds before a channel exists. This covers the
  official Go client's default 60-second lookup poll plus its 30% jitter and
  lets `SUB` catch a newly selected owner without a normal-path miss.
- The stable v7 wire limit is 32 MiB; the default single-message limit is
  20 MiB and the default MPUB body limit is 64 MiB.
- Publish bodies are written to the segment with vectored header/metadata/body
  I/O. The durable path does not build a second batch body or a full record
  buffer, and admission charges the input plus bounded encoding metadata.
- Backlog has no message-count ceiling. Sealed segments keep only constant-size
  summaries in RAM; fixed-size message metadata is paged through one bounded
  Broker cache. Publish admission is therefore governed by the configured PVC
  watermarks, not by an arbitrary number of messages.
- GC locates retention boundaries by monotonic indexes instead of rescanning a
  full backlog every five seconds. A normal tick rotates across at most 128
  Topics, and it seals an active segment only when that whole tail is already
  reclaimable. Scrub snapshots immutable files under the Topic lock, then
  verifies them lock-free with a default 64 MiB/s I/O limit.
- Slow consumers share a 512 MiB node-wide delivery working-set budget; each
  connection can request at most 32 MiB of payload. Cache misses charge both the
  file-read buffer and delivered body before I/O, and cancelled reads return
  their memory permits and in-flight reservations.

## Components

- `rustqueued`: local durable NSQ broker and HTTP management API.
- `rustqueue-discovery`: stateless EndpointSlice-derived lookup service with
  incremental topic/channel indexes and revision-head registry polling.
- `rustqueue-proxy`: bounded producer TCP/HTTP proxy; new TCP connections choose
  a least-active ready Broker and are rotated after a jittered five-minute
  default lifetime so a small long-lived producer pool follows fleet changes.
- `rustqueue-operator`: creates the StatefulSet, retained PVCs, discovery,
  proxy, RBAC, disruption budgets, PVC expansion and drain-aware one-at-a-time
  rolling updates.
- `rustqueue-console`: Kubernetes and broker observability backend serving the
  bilingual Carbon UI, with default-off native Topic/Channel management.
- `rustqueuectl`: Kubernetes-aware cluster status, targeted maintenance,
  rollout, storage expansion, fan-out stats and scrub commands.

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
  ownership and `allowVolumeExpansion: true` for online growth;
- eligible nodes labelled for brokers;
- the broker runtime and operator images in the cluster registry.

```sh
kubectl label node worker-1 rustqueue.io/eligible=true

helm upgrade --install rustqueue deploy/helm/rustqueue \
  --namespace rustqueue --create-namespace \
  --set queue.image=registry.example/rustqueue:0.7.1 \
  --set queue.storageClassName=ssd-rwo
```

The producer endpoints are exposed by the proxy Service. Consumers should use
both discovery replicas as lookupd endpoints. The generated runtime Secret is
internal; set `queue.registrySecretName` to use a pre-created Secret containing
`admin-token`, `registry-token` and `console-token`. The console token can read
broker observations and, only when Console management is explicitly enabled,
call the narrow native Topic/Channel management API. It cannot authorize drain,
scrub, upgrade or the NSQ-compatible admin API. Client TLS is optional and always supplied
through an existing Kubernetes Secret; the operator does not run a CA.

The chart enables RustQueue Console by default as a ClusterIP-only Service. It
does not create an Ingress and has no built-in login. Put the Service behind
your existing VPN, SSO or authenticated Ingress when broader access is needed:

```sh
kubectl -n rustqueue port-forward svc/rustqueue-console 4180:4180
```

Open `http://127.0.0.1:4180`. Console never reads message bodies, Secrets or
container logs. It keeps a bounded 15-minute live trend in memory and resets
that trend when its Pod restarts. Topic/Channel management is disabled by
default; enable it with `--set console.management.enabled=true`. See the [Console security and deployment
guide](docs/operations/console.md).

Console polls a small observation head every two seconds. It fetches the full
Topic/Channel catalog only on Broker, registry-revision or management-fence
changes, with a 30-second fallback refresh. This keeps observation traffic
bounded by small per-Broker heads instead of transferring every catalog on
every poll.

Runtime Pods run as UID/GID 65532 with a read-only root filesystem. Broker Pods
set `fsGroup=65532` and `fsGroupChangePolicy=OnRootMismatch` so dynamically
provisioned PVCs are writable without an init container.

The chart runs two Operator replicas. A Kubernetes Lease permits only one to
mutate resources, while the standby takes over after lease expiry. PDBs protect
the Operator, discovery and Broker availability from concurrent voluntary
disruption. PDBs cannot make a single-copy Broker PVC redundant.

Operational state is persisted in the RustQueue status. Standard-style
Conditions report readiness, progress, degradation, storage, upgrade,
maintenance, Broker availability and retained PVCs. `currentOperation` records
the exact target and stage; the last 20 replaced operations remain in
`operationHistory`. See the [Kubernetes operations runbook](docs/operations/kubernetes.md).

Examples using a locally built `rustqueuectl`:

```sh
rustqueuectl --namespace rustqueue status
rustqueuectl --namespace rustqueue brokers
rustqueuectl --namespace rustqueue maintenance rustqueue-4 enable
rustqueuectl --namespace rustqueue maintenance rustqueue-4 disable
rustqueuectl --namespace rustqueue storage 500Gi
rustqueuectl --namespace rustqueue stats
rustqueuectl --namespace rustqueue scrub
```

Prometheus Operator resources are optional:

```sh
helm upgrade --install rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --set monitoring.serviceMonitor.enabled=true \
  --set monitoring.prometheusRule.enabled=true
```

The default rules cover loss of Operator leadership, disk pressure, storage
errors, protective message eviction, sustained throttling and high fsync p99.

For a functional validation on local OrbStack Kubernetes:

```sh
make k8s-acceptance
make k8s-multi-acceptance
```

The first acceptance validates the HA operator, StatefulSet/PVC attachment,
discovery, proxy, official Go client behavior, Pod restart with the same PVC,
durable backlog recovery, and the safety gate that refuses to roll the only
broker. The second runs three real broker Pods with three independent RWO PVCs
and two genuinely different A/B broker binaries. It validates PDBs, online PVC
growth, targeted maintenance, canary approval, Operator leader failover,
durable operation history, two-step feature activation and rollback fencing
while publishing and consuming through discovery. The acknowledged-message
ledger must finish with `missing=0`. OrbStack has one
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
request-body limit, a 512 MiB node-wide in-flight HTTP body budget, and a
jittered 300-second TCP tunnel lifetime. A zero lifetime disables rotation.
Override them with `RUSTQUEUE_PROXY_MAX_CONNECTIONS`,
`RUSTQUEUE_PROXY_MAX_BODY_BYTES`, `RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES`, and
`RUSTQUEUE_PROXY_TCP_MAX_CONNECTION_AGE_SECONDS`.

Discovery gives every Kubernetes EndpointSlice list an explicit 1.5-second
deadline. Override it with `RUSTQUEUE_ENDPOINT_SLICE_TIMEOUT_MS`; timeouts are
reported by `rustqueue_discovery_endpoint_slice_timeouts_total` and stale
Broker observations still expire after five seconds.

## Local HTTP endpoints

Compatible endpoints:

- `/pub`, `/mpub`, `/stats`, `/ping`, `/info`
- `/topic/create|delete|empty|pause|unpause`
- `/channel/create|delete|empty|pause|unpause`

Native broker endpoints:

- `GET /v1/health`
- `GET /v1/registry`
- `GET /v1/registry/head`
- `GET /v1/capabilities`
- `GET|POST /v1/drain`
- `GET /v1/stats`
- `GET /v1/observe` (console token, no message bodies)
- `GET /v1/observe/head` (console token, lightweight runtime/revision head)
- `POST /v1/manage/topics/{action}` (console token, only when enabled)
- `POST /v1/manage/channels/{action}` (console token, only when enabled)
- `POST /v1/manage/fences/sync` (console token, only when enabled)
- `POST /v1/storage/scrub`
- `GET /metrics`

Discovery serves `/lookup`, `/topics`, `/channels`, `/nodes`, `/ping`, `/info`,
`/v1/publishers`, `/v1/publishers/head`, `/v1/health`, and `/metrics`. The
producer proxy also exposes `/metrics` on its HTTP listener.

Horizontal scaling is deliberately bounded by client concurrency rather than a
central coordinator. Least-active placement prevents random TCP connection
collisions, but 16 persistent producer connections can use at most 16 Brokers
at once. Lookup queries no longer scan every registry and steady-state polling
transfers only small revision/readiness heads; nevertheless a consumer still
needs one connection per actual Topic owner. This is a share-nothing cost, not
an unbounded or zero-cost scaling claim.

Latency histograms cover publish and channel-WAL fsync, publish and FIN/REQ
group-commit queueing, publish and channel ACK, payload reads, scrub/GC, proxy
backend calls, and discovery registry polling. Queue aggregates have fixed
cardinality by default; `[metrics].detailed_queue_metrics` enables bounded
per-topic/channel series up to `max_detailed_series`.
Delivery-budget bytes, waiters and cumulative waits are exported as bounded
aggregate gauges/counters.

## Storage and upgrades

Format v7 is a clean break. A v6 or older directory is refused; there is no
in-place migration. Within v7, record tags and existing fields are append-only.
Every binary declares its reader/writer feature range, and every PVC persists
its active writer and minimum reader levels in `/data/COMPATIBILITY`. The
operator probes the target image and every running broker before replacement;
an incompatible target is blocked before a PVC is touched.

Rollouts are durable status-driven operations. A replacement must be Ready
before another Broker is drained. They may be paused, optionally stop after one
canary for explicit revision approval, fail closed after a configured timeout,
and be retried with a new nonce. `rollbackToImage` uses the same preflight and
is rejected after the PVC reader fence makes that image unsafe. TLS Secret
resource-version changes are included in the Pod revision and use this same
rolling path.

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
/data/topics/<hex-topic>/segments/*.rqidx  # sealed-segment recovery metadata
/data/topics/<hex-topic>/channels/<hex-channel>.checkpoint
/data/topics/<hex-topic>/channels/<hex-channel>.wal
/data/dlq-outbox/                # compact CRC-protected binary transfer intents
/data/audit/
```

Tail short writes are truncated during startup. Sealed segments reopen from an
atomic recovery index by reading its CRC-protected fixed header and first/last
queue entries; individual metadata entries carry their own CRC and are paged on
demand. Full sidecar checksums run in background scrub, so startup is O(segment
count), not O(message count). Missing or invalid indexes fall back to a full
segment scan.
Cold payload corruption is isolated by payload-read CRC or background scrub.
Automatic scrub and normal GC start after a configurable 30-second quiet period
so a large PVC can become Ready without immediately competing with maintenance
I/O; active-tail corruption, invalid channel WAL and identity
mismatch fail startup closed.

Crash-boundary integration tests stop worker processes with actual `SIGKILL`
after append, fsync-before-ACK, checkpoint file fsync-before-rename, and GC
delete boundaries. Reopened stores are checked against an acknowledged-message
ledger; ambiguous pre-ACK writes may duplicate, but acknowledged entries may
not be missing. Injection code is available only under the compile-time
`crash-injection` test feature and is absent from normal release behavior.

The fuzz smoke matrix covers protocol, compression, storage records, channel
WAL/checkpoint replay, topic manifests, proxy HTTP metadata, and discovery
registry responses.

`./scripts/release-gate.sh` is the repeatable production gate. It runs format,
build, unit, clippy, Helm, fuzz and official-client compatibility checks;
`K8S_ACCEPTANCE=1` additionally requires the OrbStack context and runs base,
Console-management and multi-broker Kubernetes acceptances, including forced
Console restart after one owner of a multi-owner operation completes. The same
non-Kubernetes gate runs in GitHub Actions.

## Non-goals

RustQueue 0.7 does not provide message replication, backups, exactly-once
delivery, a global channel catalog, online data migration, cross-region
replication, or Broker/PVC lifecycle controls in Console.
