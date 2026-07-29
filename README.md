# RustQueue

**Durable, NSQ-compatible messaging for Kubernetes — built in Rust.**

[![CI](https://github.com/SamuelSupe/rustqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/SamuelSupe/rustqueue/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/SamuelSupe/rustqueue)](https://github.com/SamuelSupe/rustqueue/releases/latest)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.28%2B-326CE5.svg)](https://kubernetes.io/)

[Architecture](docs/architecture/share-nothing-v7.md) ·
[NSQ performance boundaries](docs/architecture/nsq-performance.md) ·
[Kubernetes operations](docs/operations/kubernetes.md) ·
[Console operations](docs/operations/console.md) ·
[v0.8.3 release](https://github.com/SamuelSupe/rustqueue/releases/tag/v0.8.3)

RustQueue 0.8.3 is a Kubernetes-native, NSQ V2-compatible message queue for
trusted internal networks. It is written in Rust and uses a deliberately
simple share-nothing model: each Broker owns one durable RWO PVC, while
Kubernetes provides scheduling, rollout and discovery.

> Current release: [v0.8.3](https://github.com/SamuelSupe/rustqueue/releases/tag/v0.8.3).
> RustQueue is a production candidate for workloads that accept single-PVC
> durability and at-least-once delivery. It does not replicate messages between
> Brokers and is not an HA replacement for a replicated log.

The complete architecture and reliability contract is documented in
[`docs/architecture/share-nothing-v7.md`](docs/architecture/share-nothing-v7.md).

## At a glance

| Area | Contract |
| --- | --- |
| Durability | `PUB`/`MPUB`/`DPUB` return only after local segment `fsync`; `FIN`/`REQ` use a durable channel WAL |
| Delivery | At least once; a restart may redeliver a message without a durable `FIN` |
| Compatibility | NSQ V2 core commands, lookup, standard Stats fields, TLS/mTLS, AUTH, Snappy, Deflate, fan-out and ephemeral channels |
| Kodo | Default-off compatibility profile: stable publish Gateways from `/nodes`, real Broker owners from `/lookup`, and no upstream Kodo change |
| Message size | Conservative 20 MiB default; stable 100 MiB protocol/storage ceiling; Kodo profile validates exactly 104,857,500 bytes |
| Deployment | Kubernetes 1.28+, SSD-backed RWO PVCs, StatefulSet Broker ordinals, retained claims |
| Scaling | Add Brokers without a central queue coordinator; consumers connect to every owner of a Topic |
| Data model | Local Topic/Channel state; no cross-Broker ordering or global channel catalog |

The single-copy model is intentional. If a PVC is permanently lost, the
messages stored on that Broker are lost. Configure disk pressure protection,
monitor the exported metrics, and choose PVC/storage failure policies that fit
your workload before deploying to production.

## What's new in 0.8.3

- **Deadline-indexed delivery.** Channel and TCP-session leases now use an
  ordered deadline index instead of rescanning every in-flight delivery on
  each fetch or session event. `TOUCH`, `FIN`, `REQ`, completion, and disconnect
  update the same index, so high-RDY consumers avoid stale timer buildup.
- **NSQ scheduler parity.** NSQ uses an in-flight priority queue; RustQueue now
  matches that scheduler shape while keeping token-checked at-least-once
  delivery and the durable Channel WAL acknowledgement boundary.
- **Durability-aware comparison.** Benchmark documentation now distinguishes
  RustQueue's acknowledgement-after-fsync semantics from NSQ diskqueue writes
  and its optional memory queue. `--sync-every=1` is reported as an NSQ write
  profile, not as an equal durability claim.
- **No format migration.** The disk format remains v7, and the NSQ/Kodo wire
  contract is unchanged from 0.8.2.

See the [v0.8.3 release notes](https://github.com/SamuelSupe/rustqueue/releases/tag/v0.8.3)
and [NSQ performance boundaries](docs/architecture/nsq-performance.md) for
the contract and benchmark interpretation.

## Download 0.8.3

Every release contains native Linux binaries, the Console UI, source, the Helm
Chart and a checksum manifest:

| Asset | Contents |
| --- | --- |
| `rustqueue-0.8.3-linux-x86_64.tar.gz` | Linux x86_64 binaries, Console UI and example configuration |
| `rustqueue-0.8.3-linux-aarch64.tar.gz` | Linux ARM64 binaries, Console UI and example configuration |
| `rustqueue-0.8.3-source.tar.gz` | Source archive for the tagged commit |
| `rustqueue-0.8.3.tgz` | Helm Chart |
| `SHA256SUMS-0.8.3` | SHA-256 checksums for every downloadable artifact |

```sh
arch="$(uname -m)"
curl -LO "https://github.com/SamuelSupe/rustqueue/releases/download/v0.8.3/rustqueue-0.8.3-linux-${arch}.tar.gz"
curl -LO "https://github.com/SamuelSupe/rustqueue/releases/download/v0.8.3/SHA256SUMS-0.8.3"
sha256sum --check --ignore-missing SHA256SUMS-0.8.3
tar -xzf "rustqueue-0.8.3-linux-${arch}.tar.gz"
```

## Architecture

```text
standard producer -> proxy Service ----------------------------+
                                                               |
Kodo producer -> discovery /nodes -> Gateway Service ----------+
                                      (3 advertised identities) |
                                                               v
                                            one publish-ready Broker -> its PVC

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
- Normal GC retains every message accepted while no durable Channel exists,
  across restart and without a bootstrap timeout. The first durable Channel
  starts at that persisted unrouted boundary. Deleting the last durable Channel
  starts a new boundary at the current Topic tail; ephemeral Channels do not
  clear it.
- Once a durable Channel exists, a later Channel can still bootstrap from the
  last 90 seconds. This covers one official Go client default 60-second lookup
  poll plus its 30% jitter. The Kodo profile forces 180 seconds so one failed
  lookup request still gets a second discovery opportunity.
- The stable v7 single-message limit is 100 MiB. The conservative defaults
  remain 20 MiB per message and 64 MiB per MPUB body; the opt-in Kodo profile
  raises them to 100 MiB and 128 MiB respectively.
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

- `rustqueued`: local durable NSQ broker, standard NSQ Stats compatibility and
  authenticated HTTP management API.
- `rustqueue-discovery`: stateless EndpointSlice-derived lookup service with
  incremental topic/channel indexes and revision-head registry polling.
- `rustqueue-proxy`: bounded producer TCP/HTTP proxy; new TCP connections choose
  a least-active ready Broker and are rotated after a jittered five-minute
  default lifetime so a small long-lived producer pool follows fleet changes.
  In Kodo mode the same binary runs as three publish Gateway Pods behind one
  highly available Service. Each Gateway terminates the NSQ producer protocol,
  enforces the 100 MiB single-message limit, and may retry another Broker only
  after an explicit pre-commit rejection. A failure after the full body was
  sent is returned as ambiguous and is never retried automatically.
- `rustqueue-operator`: creates the StatefulSet, retained PVCs, discovery,
  proxy, RBAC, disruption budgets, PVC expansion and drain-aware one-at-a-time
  rolling updates. The 0.8 Kodo profile adds atomic Discovery cutover,
  producer-restart fencing and fail-closed decommissioning.
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

### Standard deployment

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
  --set queue.image=registry.example/rustqueue:0.8.3 \
  --set queue.storageClassName=ssd-rwo
```

The producer endpoints are exposed by the proxy Service. Consumers should use
both discovery replicas as lookupd endpoints. The generated runtime Secret is
internal; set `queue.registrySecretName` to use a pre-created Secret containing
`admin-token`, `registry-token` and `console-token`. The console token can read
broker observations and, only when Console management is explicitly enabled,
call the narrow native Topic/Channel management API. It cannot authorize drain,
scrub, upgrade or the NSQ-compatible admin API. Client TLS is optional and
always supplied through an existing Kubernetes Secret; the operator does not
run a CA.

### Kodo compatibility in 0.8

Kodo compatibility is implemented entirely by RustQueue as a separate,
default-off deployment profile. Kodo continues to use RustQueue Discovery:
`/nodes` returns publish Gateways and `/lookup` returns consumer-facing Broker
owners.

```sh
helm upgrade --install rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --set 'queue.image=registry.example/rustqueue@sha256:<64-lowercase-hex-digest>' \
  --set queue.kodoCompatibility.enabled=true
```

The chart pins this profile to three Brokers, storage feature level 2, a
180-second bootstrap-retention window, a 100 MiB RustQueue single-message
ceiling, a one-CPU/2 GiB Broker request, and three stable Gateway Pods with
required hostname anti-affinity. The profile therefore needs three schedulable
nodes. Each Gateway requests one CPU and 768 MiB, is not CPU-limited, and is
limited to 1 GiB memory so its fixed 512 MiB in-flight body budget is
scheduler-backed.
The reviewed Kodo source enforces an application maximum of 104857500 bytes
(100 MiB minus 100 bytes) and sets the go-nsq producer write deadline to three
seconds. A maximum-size publish therefore needs more than roughly 34 MiB/s of
application goodput from Kodo to the Gateway, with additional headroom for
scheduling and network variance. Validate that exact 104857500-byte path with
the included acceptance before rollout. The profile also requires an immutable
lowercase `@sha256` image; `imagePullPolicy=Never` is accepted only for a
preloaded local test image.

For an existing deployment whose running binary still advertises the legacy
32 MiB capability, first roll the new immutable image with Kodo compatibility
disabled and wait for `Ready`; enable the Kodo profile in a second change. The
operator will not cross the 100 MiB/feature-level-2 fence while an old Broker is
still running.

Activation is fail-closed and ordered. The operator first waits for at least
two Ready Brokers, then starts all three Gateways before disrupting an old
Broker. This allows a migration to recover while one of the three target
Brokers is unavailable; the Broker target remains three. It rolls Discovery
Pods to a distinct `kodo` mode label while the Discovery Service still selects
the existing `direct` mode. Only after the complete Kodo-mode set is Ready does
it atomically switch the Service selector. Broker rollout and maintenance
remain blocked until
`KodoCutoverReady=True` and `KodoProducerRestartConfirmed=True`.

`queue.kodoCompatibility.cutoverGraceSeconds` defaults to 630 seconds because
Kodo's default `nsq.refresh_at` is 300 seconds. Set it to at least two deployed
Kodo `refresh_at` intervals plus 30 seconds. This preserves a second refresh
opportunity if the first request overlaps the Discovery Service cutover. New
installations may point Kodo at the Discovery Service after
`KodoGatewaysAdvertised=True`.

The reviewed Kodo `refreshNodes()` implementation replaces cached producers
when an advertised address changes. RustQueue still requires an explicit
publisher restart as a cutover fence, so rollout safety does not depend on that
private cache behavior remaining unchanged in a later Kodo build. After
`KodoGatewaysAdvertised=True`, restart every Kodo publisher process and wait
for it to become Ready, then change
`queue.kodoCompatibility.producerRestartNonce` to a new value:

```sh
helm upgrade rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --reuse-values \
  --set-string queue.kodoCompatibility.producerRestartNonce="$(date +%s)"
```

The Operator captures the nonce only after Gateway advertisement and will not
drain, replace, or place a Broker into maintenance until the value changes.
Losing Gateway advertisement resets the fence and requires another restart
confirmation. An existing Kodo deployment is fully migrated only after both
`KodoCutoverReady=True` and `KodoProducerRestartConfirmed=True`.
During an existing-cluster migration, do not publish messages above the
previous Broker limit until the RustQueue resource itself reports `Ready=True`;
Gateway advertisement and explicit producer restart intentionally precede the
Broker configuration rollout.

`/nodes` then advertises three producer identities on ports `4150`, `4152`, and
`4153` of one ClusterIP Gateway Service. Every port targets every Ready Gateway,
and every Gateway can publish to every publish-ready Broker. A single Gateway
restart therefore does not turn an advertised identity into a dead Pod address.
`/lookup` continues to expose the real Broker owners for consumption. During a
Broker restart it returns the healthy topic shards instead of blocking every
consumer; the separate complete-inventory signal remains false until all three
Stats shards are back.
When ServiceMonitor support is enabled, the dedicated Gateway monitor scrapes
the metrics-only port `4160`; Gateway Stats ports do not accept HTTP publish
requests. Any increase in unknown publish outcomes raises the critical
`RustQueueAmbiguousPublish` alert.

Gateway ingress accepts only Pods matching
`queue.kodoCompatibility.allowedPodSelector` (by default
`app.kubernetes.io/name=kodo`) in the RustQueue namespace. Label the Kodo
workload accordingly. For a separate namespace, set
`allowedNamespaceSelector` to labels present on that namespace; the chart's
general same-namespace policy explicitly excludes Gateway and Broker Pods.
In Kodo mode, Broker TCP ingress is restricted to RustQueue Pods and that same
Kodo peer selector. Broker HTTP remains reachable for stats and monitoring, but
its publish endpoints require the internal admin token. Kodo consumers and
publishers share the NSQ TCP protocol and port, so a permitted Kodo Pod remains
technically able to publish directly; keep the selector limited to the trusted
Kodo workload and use the advertised Gateway addresses for every producer.
Discovery `/nodes` advertises those Gateways for publishing while `/lookup`
continues to advertise the currently healthy real Broker owners for
consumption. Automatic Kodo channel cleanup is hard-disabled: the CRD accepts only
`cleanupEnabled: false`, every RustQueue binary rejects an attempt to enable
the compatibility cleanup path, and `/channel/delete` returns a 404 whose body
does not contain `CHANNEL_NOT_FOUND`, so unchanged Kodo stops before contacting
Brokers.
Cleanup must remain disabled until RustQueue has a cluster-wide atomic,
authenticated deletion transaction.

Enabling this profile activates the storage feature-level-2 rollback fence;
records above the legacy 72 MiB boundary require that feature level. Disabling
the profile is an explicit decommission, not a migration back to unsafe direct
publishing. First stop every Kodo workload using this Discovery Service, then
set `queue.kodoCompatibility.enabled=false` and
`queue.kodoCompatibility.decommissionConfirmed=true`. Without that
confirmation the Operator fails closed before changing any Broker, Discovery,
or Gateway resource. After confirmation it completes the target Broker rollout
while the Gateways remain available, switches Discovery to direct mode, and
removes the Gateway resources; no Kodo workload may be restarted against that
direct mode. The profile retains feature level 2 as the monotonic storage floor
and preserves enough delivery memory to consume previously stored 100 MiB
messages. Channel `message_count` remains monotonic across empty and eviction,
while `requeue_count` and `timeout_count` are persisted with the Channel WAL
and checkpoint. Kodo can therefore calculate rate thresholds across Broker
restarts without a counter rollback. Run the real Kodo
`nsqadmin.go` parser and admin-flow replay before releasing this profile. The
replay is fully offline and copies that upstream
file unchanged into an isolated test module; it also rejects an unreviewed
hash for the Kodo producer discovery, producer client, or admin source. Only
unrelated Kodo globals are stubbed:

```sh
KODO_SOURCE_DIR=/path/to/kodo make kodo-replay
# Or include it in the complete gate:
KODO_ACCEPTANCE=1 KODO_SOURCE_DIR=/path/to/kodo ./scripts/release-gate.sh
```

### Console and operations

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

The v0.8.3 CI/CD workflow publishes a Release only after the non-Kubernetes
release gate, both native Linux builds, packaging and checksum verification
succeed. The v0.8.0 Kodo compatibility baseline additionally passed the
unmodified Kodo source replay, an exact 104,857,500-byte `PUB`/`DPUB` with one
Gateway failover, and a three-Broker operational ledger with 3,239 expected and
unique messages, zero missing, zero duplicates and zero publish errors.

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
`RUSTQUEUE_PROXY_MAX_MESSAGE_BYTES`, `RUSTQUEUE_PROXY_MAX_BODY_BYTES`,
`RUSTQUEUE_PROXY_MAX_INFLIGHT_BYTES`,
`RUSTQUEUE_PROXY_TCP_COMMAND_TIMEOUT_MS`, and
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

In the Kodo compatibility profile, automatic `/channel/delete` cleanup is
intentionally fail-closed. Use RustQueue's authenticated native management
flow for administrative changes.

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

RustQueue 0.8 keeps disk format v7. Format v7 is a clean break: a v6 or older
directory is refused and there is no in-place migration. Within v7, record tags
and existing fields are append-only.
Every binary declares its reader/writer feature range and protocol message/body
limits, and every PVC persists its active writer and minimum reader levels in
`/data/COMPATIBILITY`. The operator probes the target image and every running
broker before replacement; an incompatible target is blocked before a PVC is
touched.

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
`KODO_ACCEPTANCE=1 KODO_SOURCE_DIR=/path/to/kodo` additionally runs the pinned,
unmodified Kodo parser/admin replay and the 100 MiB Gateway acceptance;
`K8S_ACCEPTANCE=1` additionally requires the OrbStack context and runs base,
Console-management and multi-broker Kubernetes acceptances, including forced
Console restart after one owner of a multi-owner operation completes. The same
non-Kubernetes gate runs in GitHub Actions.

## Non-goals

RustQueue 0.8 does not provide message replication, backups, exactly-once
delivery, a global channel catalog, online data migration, cross-region
replication, or Broker/PVC lifecycle controls in Console.
