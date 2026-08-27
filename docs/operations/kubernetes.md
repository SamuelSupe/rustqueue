# Kubernetes operations runbook

RustQueue v7 is share-nothing. Every Broker ordinal has one retained RWO PVC
and that PVC is the only copy of its messages. These controls make operations
predictable; they do not turn a Broker into a replicated shard.

## Installation preflight

- Use Kubernetes 1.28 or newer.
- Label only SSD-capable nodes with `rustqueue.io/eligible=true`.
- Treat that label as persistent membership intent. Cordon and Node NotReady
  do not reduce the Broker target; remove the label only for an intentional,
  drain-aware scale-down.
- Use an RWO StorageClass with `allowVolumeExpansion: true` and working
  `fsGroup` ownership.
- Keep at least two eligible nodes if image rolling is required. A one-Broker
  deployment intentionally refuses replacement.
- Supply client certificates through `queue.clientTlsSecretName`; the Operator
  does not issue certificates.

The chart starts two Operator replicas. They coordinate through the
`rustqueue-operator-leader` Lease. Both remain ready, but only the Lease holder
reconciles RustQueue resources. The Broker and discovery PDBs are owned by the
RustQueue resource; the Operator PDB is owned by Helm.

## Reading cluster state

```sh
rustqueuectl --namespace rustqueue --name rustqueue status
rustqueuectl --namespace rustqueue --name rustqueue brokers
```

Important status fields:

- `conditions`: readiness and the reason for any storage, Pod, upgrade or
  maintenance block;
- `currentOperation`: stable ID, target, revision, current Broker and phase;
- `operationHistory`: the last 20 replaced operations;
- `orphanedPvcs`: retained claims whose ordinal is above the desired replica
  count;
- `activeStorageFeatureLevel`: highest durable writer fence observed from the
  Broker fleet.

An Operator restart never treats status alone as proof that a mutation
finished. Every reconcile checks the current Pod, revision, readiness, drain
state, PVC request/capacity and compatibility report before taking the next
step.

## Topic delivery policy

`RustQueueTopic.spec` persists the desired delivery boundary:

```yaml
spec:
  deliveryMode: TTL_DISCARD
  messageTtlSeconds: 3600
```

`RELIABLE` requires `messageTtlSeconds` to be absent; `TTL_DISCARD` requires a
positive value. Existing and automatically created Topics remain `RELIABLE`.
Use the Console management flow for online changes so impact preview,
single-owner fencing, operation replay, and typed confirmation are enforced.
Do not edit the policy during a visible owner migration.

Helm does not upgrade an already installed CRD. Before enabling this policy on
an upgraded cluster, apply the checked-in CRDs and wait for them to become
Established:

```sh
kubectl apply -f deploy/helm/rustqueue/crds
kubectl wait --for=condition=Established crd/rustqueuetopics.rustqueue.io --timeout=60s
```

## WebSocket TTL live subscriptions

The optional WebSocket feed is disabled by default and is available only for
Topics whose persisted `deliveryMode` is `TTL_DISCARD`. It is served by each
Broker's existing HTTP listener at:

```text
GET /v1/ws/topics/{topic}
Sec-WebSocket-Protocol: rustqueue.live.v1
```

The endpoint does not create Topics. A missing Topic or a `RELIABLE` Topic is
rejected during the handshake with a stable error code. One connection covers
one Topic and starts at the current tail; there is no replay, reconnect resume,
WebSocket ACK or WebSocket-layer retry. A complete frame write only means that
RustQueue finished the socket write—it does not mean that the client processed
the message.

Discover owners first, then connect directly to every Broker HTTP address in
the Discovery `/lookup` response. Do not send WebSocket traffic through the
producer proxy or assume that one Broker is a global Topic owner. During a
visible migration, union the owner streams and deduplicate with the message ID
in each binary frame. Owner shutdown, migration, Topic deletion, or a change
to `RELIABLE` closes affected connections; repeat `/lookup` before reconnecting.

Configure the same values in the runtime `[websocket]` block, the Helm
`queue.websocket` values, and the RustQueue Operator/CRD `websocket` spec:

```toml
[websocket]
enabled = false
max_connections = 1024
max_connections_per_topic = 256
frame_inflight_bytes = 134217728
allowed_origins = []
```

For example, a deployment can opt in with equivalent Helm fields:

```sh
helm upgrade rustqueue deploy/helm/rustqueue \
  --namespace rustqueue --reuse-values \
  --set queue.websocket.enabled=true \
  --set queue.websocket.maxConnections=1024 \
  --set queue.websocket.maxConnectionsPerTopic=256 \
  --set queue.websocket.frameInflightBytes=134217728
```

`allowed_origins` is an exact browser Origin allowlist. Browser upgrades with
an Origin not in the list are rejected; a missing Origin is allowed for a
service client but still requires normal authentication. Discovery's
read-only `/lookup` endpoint uses the same allowlist. Service clients can use
an `Authorization: Bearer` header during upgrade. Browser clients use the
`auth_required` / `auth` text-control exchange, and credentials must not be
placed in a query parameter. Authorization reuses the existing subscription
permission and the virtual Channel `websocket#ephemeral`.

The binary data frame begins with 24 bytes of big-endian metadata followed by
the raw message body: `RQW1` at `0..4`, header length `24` at `4..6`, attempts
at `6..8`, signed Broker nanosecond timestamp at `8..16`, and unsigned
broker-scoped message ID at `16..24`. Text frames carry `auth_required`,
`ready`, and stable error controls. A client should ignore unknown control
fields so the protocol can evolve.

Each connection has at most one outstanding reservation. The Broker releases
the reservation and closes the connection if the frame cannot be completely
written before the smaller of the socket timeout and remaining message TTL.
Pause stops new WebSocket deliveries without creating reliable backlog. The
global frame budget, per-topic and total connection caps are fail-closed; watch
connection count, frame messages/bytes, authentication failures, capacity
rejections, non-TTL rejections and slow-consumer closes before raising limits.

The feature reuses the existing Broker HTTP Service and NetworkPolicy. It does
not add a port or create a default Ingress. If external browser access is
needed, put the existing HTTP endpoint behind the deployment's TLS and
authentication boundary and keep the Origin allowlist narrow.

## Targeted Broker maintenance

```sh
rustqueuectl -n rustqueue maintenance rustqueue-4 enable
rustqueuectl -n rustqueue maintenance rustqueue-4 disable
```

`enable` removes the Broker from publish readiness but keeps it visible to
consumers until stored messages, channel depth and in-flight leases reach zero.
It never discards backlog. The operation stays `Draining` indefinitely when a
durable channel is abandoned. `disable` restores service. Only one declarative
maintenance request exists per RustQueue, which prevents conflicting drains.

## Rolling an image

Set the image through Helm or patch the RustQueue. The Operator performs target
image capability preflight, validates every current Broker, quiesces publishing
and freezes new delivery, waits for already-issued leases and delivery buffers
to reach a stable zero boundary, then replaces one highest outdated ordinal at
a time. The durable backlog stays on that ordinal's PVC and does not block
rollout. The Operator will not touch the next Pod until the replacement is
Ready. A Broker that predates the delivery-freeze response contract is handled
fail-safe: its first upgrade waits for the legacy full-empty drain instead of
assuming that a momentary zero in-flight count is a stable barrier.

Canary approval is optional:

```sh
helm upgrade rustqueue deploy/helm/rustqueue \
  --namespace rustqueue \
  --set queue.image=registry.example/rustqueue:0.10.0 \
  --set queue.rollout.requireCanaryApproval=true

rustqueuectl -n rustqueue rollout approve
```

Useful controls:

```sh
rustqueuectl -n rustqueue rollout pause
rustqueuectl -n rustqueue rollout resume
rustqueuectl -n rustqueue rollout retry
rustqueuectl -n rustqueue rollout rollback registry.example/rustqueue:0.10.0
rustqueuectl -n rustqueue rollout forward
```

Pause and canary-wait time do not trigger the rollout timeout. Other stalled
stages fail closed after `rollout.timeoutSeconds`; `retry` writes a new nonce
and begins a new durable operation. `rollback` remains selected until
`forward` clears it. The rollback target must still satisfy every PVC reader
fence.

For a new storage record feature, roll the reader-capable image completely,
then increase `storageFeatureLevel`. Never combine those two changes. Once a
PVC advances its minimum-reader fence, an old binary is intentionally blocked.

## PVC growth and retained claims

```sh
rustqueuectl -n rustqueue storage 500Gi
```

Only growth is accepted. The Operator checks the StorageClass, patches every
existing claim, waits for reported capacity, and exposes `StorageReady=False`
until completion. A smaller request, class mismatch or unsupported expansion
enters `StorageBlocked` without mutating a claim. New claims created from the
StatefulSet's immutable template are reconciled to the requested size on the
next pass.

Scale-down always drains the highest ordinal and retains its PVC. Retained
claims appear in `status.orphanedPvcs`; RustQueue never deletes them
automatically. Deleting one is an explicit irreversible acknowledgement that
its remaining messages are no longer required. Unlike rolling replacement,
scale-down requires the Broker to be fully empty.

## Alerts and failure response

Enable the chart's ServiceMonitor and PrometheusRule only when Prometheus
Operator CRDs are installed. Treat these default alerts as follows:

- `RustQueueBrokerDiskPressure`: reduce producers or grow the PVC immediately;
- `RustQueueStorageErrors`: isolate the node/PVC and inspect Broker recovery
  logs before resuming traffic;
- `RustQueueProtectiveEviction`: critical data-loss notification; the Broker
  removed old messages to keep the disk operable;
- `RustQueueOperatorHasNoLeader`: deployment changes are frozen, but existing
  Broker traffic remains independent;
- `RustQueueBrokerMetricsMissing`, `RustQueueDiscoverySourceUnavailable`, or
  `RustQueueProxyHasNoPublishBackend`: treat missing telemetry as a runtime
  outage until the corresponding Service endpoints are verified;
- `RustQueueKodoGatewayMetricsMissing` or
  `RustQueueKodoStatsInventoryIncomplete`: restore all three Gateway and Broker
  Stats shards before trusting Kodo depth or channel-idleness observations;
- sustained throttling or high fsync p99: investigate PVC latency/capacity and
  producer arrival rate.

The bundled rules select the current Helm namespace and queue Services, so a
healthy RustQueue release cannot mask a failed release in the same Prometheus.

PDBs affect voluntary Eviction API calls only. Node loss, `kubectl delete pod`,
and the Operator's already-drained replacement are not prevented by a PDB.
Permanent PVC loss permanently loses that Broker's unconsumed messages.

## Release verification

```sh
./scripts/release-gate.sh
K8S_ACCEPTANCE=1 ./scripts/release-gate.sh
```

The second command is destructive only inside the dedicated OrbStack test
namespaces. It validates real A/B binaries, canary pause, Lease failover,
online PVC expansion, rollback fencing and a Go-client `missing=0` ledger. It
does not claim multi-physical-node or 500-Broker scale evidence.
