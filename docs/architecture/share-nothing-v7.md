# RustQueue format v7 share-nothing architecture

Status: accepted implementation contract
Target release: 0.7.0
Data format: v7, clean directories only

## 1. Goal

RustQueue v7 replaces the v6 Raft federation with a Kubernetes-native,
share-nothing queue. A broker owns one PVC and never replicates, migrates, or
forwards the messages stored on that PVC. Kubernetes supplies process
scheduling and broker discovery; it is not used as a message or ACK database.

The external data plane remains compatible with the NSQ V2 core protocol. The
internal architecture is intentionally smaller than v6:

```text
producer -> node proxy -> one ready broker -> local PVC

consumer -> discovery /lookup -> every broker that owns the topic
                                  -> direct NSQ TCP connections

operator -> StatefulSet/PVC lifecycle only
```

There is no broker-to-broker message path and no cluster consensus path.

## 2. Accepted product semantics

### 2.1 Ownership and durability

- The broker reached by a publish request becomes the permanent owner of that
  message.
- The broker PVC is the only copy. Permanent PVC loss means permanent message
  loss.
- `PUB`, `MPUB`, and `DPUB` succeed only after the local message segment has
  passed `fsync`.
- `FIN` and `REQ` succeed only after the local channel WAL has passed `fsync`.
- Local group commit may combine requests, but never weakens the fsync boundary.
- Message ID ranges are reserved durably in large blocks. A restart may leave
  an ID gap, but can never reuse an ID; reserving a new ID does not add a
  metadata fsync to every publish.
- `TOUCH`, RDY, and in-flight deadlines remain process memory. A broker restart
  may redeliver a message without a durable `FIN`.
- Delivery is at least once. Cross-broker duplicates are accepted and there is
  no cluster-wide deduplication.

### 2.2 Topic and channel scope

- A broker is the shard. There is no public or internal global partition.
- Topics and channels are local broker objects and are created on first use.
- A channel created on one broker does not create a channel on another broker.
- Pause, empty, delete, and create compatibility endpoints operate only on the
  addressed broker.
- `rqctl --all` may fan management calls out to discovered owners, but must
  return per-broker results and must never claim atomic success.
- A direct `SUB` sees only the addressed broker. Lookup-based consumers connect
  to all discovered owners of the topic.

### 2.3 Bootstrap retention

Every broker retains each topic message for at least 30 seconds even when no
channel exists. When a local channel is created, its initial cursor starts at
the oldest message still inside this bootstrap window.

This deliberately prefers duplicates over misses during the normal discovery
window. The guarantee is bounded:

- If discovery and `SUB` complete within 30 seconds, messages accepted by a new
  fallback owner remain consumable by the newly created local channel.
- A newly created channel may receive a small amount of data published before
  its `SUB`.
- If all consumers are absent for longer than the bootstrap window and a topic
  appears on a previously unused broker, complete replay is not guaranteed.

### 2.4 Routing and scale

- Producers connect to a node-local proxy DaemonSet.
- The proxy chooses a random publish-ready broker for each new TCP connection
  and for each HTTP request.
- A persistent TCP connection stays on one broker until it closes.
- Ambiguous failures may cause a producer retry to create a duplicate on a
  different broker.
- Popular topics may spread to every broker. Consumers connecting to every
  owner, including approximately 500 owners, is an accepted product tradeoff.
- Consumers bypass the producer proxy and use discovery plus direct broker
  connections.

### 2.5 Ordering and identity

- Storage append order exists only inside one broker/topic log.
- There is no cross-broker order and no global delivery completion order.
- NSQ wire message IDs are opaque broker-scoped handles. Applications must use
  a business key in the payload when they require cross-broker deduplication.
- A broker persists its identity and message sequence high-water mark on its
  PVC so a normal Pod restart does not reuse IDs.

## 3. Runtime components

### 3.1 Broker

`rustqueued` is a self-contained local broker:

- NSQ TCP V2 and compatible HTTP endpoints.
- Client TLS/mTLS, AUTH, Snappy, and Deflate.
- Local topic segments and channel WAL/checkpoints.
- Local health, metrics, stats, drain, scrub, and disk-pressure endpoints.
- No OpenRaft node, internal replication server, leader routing, peer
  discovery, or membership controller.

The broker exposes a small authenticated discovery endpoint containing:

- broker identity and advertised addresses;
- process/readiness/drain state;
- monotonically increasing registry revision;
- local topics and their local channels.

### 3.2 Discovery

`rustqueue-discovery` is a stateless deployment with two independent replicas.
Each replica:

1. watches the broker Kubernetes EndpointSlice;
2. polls broker registries every 2 seconds with bounded concurrency sized for
   the 500-broker target;
3. expires stale broker observations after 5 seconds;
4. serves `/lookup`, `/topics`, `/channels`, `/nodes`, `/ping`, and `/info`.

Discovery state is derived and is never authoritative message metadata. A
restart reconstructs the complete index from ready brokers. Replicas do not
coordinate and clients may union their answers exactly as with multiple
`nsqlookupd` instances.

### 3.3 Producer proxy

`rustqueue-proxy` runs as a DaemonSet on application nodes so older Kubernetes
versions do not need `PreferSameNode` support.

- TCP is a bounded L4 pass-through. It chooses one backend per accepted
  connection and does not parse or replay commands.
- HTTP is a bounded reverse proxy. Node-level connection and in-flight body
  budgets reject excess load instead of allowing proxy memory to grow without
  limit. It may retry only before receiving a backend response; an ambiguous
  retry is allowed to duplicate a message.
- The proxy obtains publish-ready broker addresses from discovery and never
  reads topic ownership.
- If no broker is available it returns a retryable error instead of buffering
  messages locally.

### 3.4 Operator

The v7 operator has no certificate authority, Raft bootstrap, Cell layout, or
membership reconciler. It manages:

- one broker StatefulSet;
- one RWO SSD PVC per ordinal with `Retain` semantics;
- required one-broker-per-node anti-affinity;
- automatic scale-up to the count of eligible labelled nodes;
- conservative highest-ordinal scale-down;
- declarative targeted Broker maintenance;
- durable operation status and bounded operation history;
- drain, canary approval, pause/retry and one-Pod-at-a-time replacement;
- compatibility-gated rollback and TLS Secret revision rollout;
- online PVC request expansion with shrink prevention;
- Broker/discovery disruption budgets;
- discovery and proxy Deployments/DaemonSet, Services, RBAC, and NetworkPolicy.

Two Operator replicas coordinate through one namespaced Lease. A standby does
not mutate resources until the previous holder expires. Each operation stage is
persisted in RustQueue status, but reconciliation still checks live Pod, drain,
PVC and capability state before advancing. This makes restart continuation
idempotent without treating status as an execution log.

Drain has two readiness dimensions:

1. stop accepting new publishes so proxies exclude the broker;
2. remain visible to lookup consumers until all durable channel depth is zero.

If drain times out, scale-down remains blocked. Forced discard is a separate,
audited administrative action.

## 4. Format v7 storage

```text
/data/
  FORMAT
  COMPATIBILITY
  broker.meta
  topics/
    <encoded-topic>/
      manifest
      segments/
        segment-00000000000000000001.rqlog
      channels/
        <encoded-channel>.checkpoint
        <encoded-channel>.wal
      delayed.index
  dlq-outbox/
  audit/
```

### 4.1 Message segment

- Immutable, length-prefixed binary records.
- Record header contains format, kind, message ID, timestamp, delayed target,
  payload length, and CRC32C.
- The payload is stored once per broker regardless of channel count.
- Tail short writes are truncated during recovery.
- A bad record before the valid tail fails broker startup and requires operator
  action; a broker must not silently skip it.
- A runtime write, fsync, or integrity failure poisons the affected storage
  handle, removes the broker from readiness, and terminates the process for a
  clean recovery. No later request may append through an uncertain file
  offset.

### 4.2 Channel state

- Each local channel stores barrier/cursor, ACK floor, bounded sparse ACK,
  requeue targets, attempts that reached a durable `REQ`, paused state, and
  retention cursor.
- State mutations append to a per-channel WAL.
- Periodic checkpoints use temporary-file, file fsync, rename, and directory
  fsync before the covered WAL can be removed.
- In-flight leases are not checkpointed. Recovery makes their messages
  immediately eligible for redelivery.

### 4.3 GC and disk pressure

A complete segment may be deleted only when it is older than:

- every durable local channel retention cursor;
- the 30-second bootstrap floor;
- every active reader reference;
- every pending DLQ outbox reference.

High disk watermark first rejects publishes with retryable `429`. Protective
eviction remains opt-in and local: after the configured grace period it may
delete the oldest complete segment, persist the resulting channel gaps, and
write an audit record. There is no quorum because there is no replica.

Reader references are acquired while holding the topic lock. GC, protective
eviction, and topic deletion inspect those leases under the same lock order, so
a segment cannot disappear between delivery reservation and payload read.

### 4.4 DLQ

DLQ transfer uses a compact CRC-protected binary local outbox. The source message is not durably
finished until the DLQ message append is durable. Recovery retries incomplete
outbox work. A crash may duplicate the DLQ entry but must not delete the source
first.

## 5. Public behavior

Retained NSQ commands:

`IDENTIFY`, `AUTH`, `SUB`, `PUB`, `MPUB`, `DPUB`, `RDY`, `FIN`, `REQ`, `TOUCH`,
`NOP`, and `CLS`.

Retained compatible HTTP endpoints are local broker endpoints. Lookup endpoints
move to discovery. Native v6 cluster APIs are removed, including partition,
replica, migration, rebalance, transfer-leader, federation, and cluster
operation resources.

`/ping` remains process liveness. Broker readiness requires local storage,
clock, and admission health only; it has no cluster-wide dependency.

## 6. Security boundary

- The deployment targets a trusted internal Kubernetes network.
- Client-facing broker TLS/mTLS and external AUTH remain optional and compatible.
- Certificates are mounted from user-provided Kubernetes Secrets.
- There is no operator-managed CA.
- Discovery-to-broker polling uses a bounded bearer token, dedicated Service
  Account, and NetworkPolicy.
- Management endpoints use a separate bearer token.
- TCP proxy TLS is pass-through; all backends serving one proxy address must
  present a certificate valid for that client-visible DNS name.

## 7. Rolling upgrades

- v7 does not read v6 directories or snapshots.
- v7 record tags and existing fields are append-only.
- Every binary exposes its data format and minimum-reader, maximum-reader, and
  maximum-writer feature levels through `--capabilities-output` and the
  authenticated `/v1/capabilities` endpoint.
- Every PVC atomically persists its active writer feature, minimum required
  reader feature, and generation in `/data/COMPATIBILITY`.
- Before changing a Pod, the operator runs the target image as a capability
  probe and polls every current broker. A format mismatch, unsupported desired
  feature, or target binary behind a PVC fence blocks the rollout.
- Every record kind declares the feature level required to write it, and the
  segment append path rejects records above the PVC's active writer feature.
- A new record kind therefore uses a two-step release: roll a binary capable of
  reading it to every broker, then explicitly raise `storageFeatureLevel`.
  Activation is configuration, not distributed feature negotiation.
- After activation, the durable minimum-reader fence rejects an incompatible
  binary on startup. Rollback is allowed only while it remains compatible;
  otherwise recovery proceeds by forward upgrade.
- A newly replaced Pod must become Ready before another outdated Pod can be
  drained. Optional canary approval stops after exactly one current Ready Pod.
- Paused and approval-wait stages are stable. Other stages stop after the
  configured timeout and require an explicit retry nonce or rollback request.
- The image, rendered Broker configuration, auth Secret and client TLS Secret
  revisions all contribute to one desired Pod revision.

Unknown record kinds continue to fail closed. Safety comes from preventing any
such kind from being emitted until all old readers have left the fleet, rather
than from skipping data that an old binary cannot interpret.

## 8. Removed v6 architecture

The final v7 tree must not contain active or optional implementations of:

- OpenRaft and the consensus crate;
- Root, Catalog, Cell metadata, Home Cell, routing buckets, and topology epochs;
- partition/RF/replica/learner/membership state;
- gateway leader routing and broker-to-broker publish/fetch/ACK forwarding;
- online partition expansion and cross-Cell migration;
- replica repair, rebalance, leader transfer, or Raft snapshot installation;
- libp2p discovery, PeerId identity, Gossipsub, Kademlia, Noise, and Yamux;
- internal Raft HTTP/2 endpoints and automated internal PKI.

No feature flag may restore the v6 mode.

## 9. Acceptance contract

The implementation is complete only when all of the following pass:

1. A child process is stopped with actual `SIGKILL` at append-before-fsync and
   fsync-before-response boundaries. After reopen, an acknowledged-message
   ledger has `missing=0`; the pre-response write may be redelivered.
2. Actual `SIGKILL` at channel WAL append/fsync and checkpoint
   file-fsync-before-rename boundaries preserves durable FIN/REQ semantics.
   GC delete boundaries reopen successfully without losing acknowledged live
   records.
3. Tail corruption truncates safely; middle corruption isolates the topic.
4. A new fallback owner is discovered and subscribed within 5 seconds, and the
   30-second bootstrap ledger has `missing=0` with duplicates reported.
5. Official Go and Python NSQ clients pass direct, lookup, compression, TLS,
   AUTH, MPUB, DPUB, REQ, TOUCH, fan-out, sampling, and ephemeral cases.
6. Broker restart reattaches its PVC and resumes its local backlog.
7. StatefulSet scale-up follows eligible nodes; scale-down blocks until the
   highest ordinal drains to zero.
8. Controller tests cover capability preflight, rollback fences, durable
   operation status, replacement readiness, canary approval, Lease expiry,
   PVC quantity safety, targeted maintenance, drain-aware rolling, and refusal
   to replace a lone broker. OrbStack runs three actual broker Pods with
   independent RWO PVCs and two distinct A/B capability binaries. It validates
   PDBs, online PVC growth, targeted maintenance, Operator failover, canary
   continuation, feature activation and rollback fencing while an official
   Go-client ledger publishes and consumes through proxy and lookup. It
   requires `missing=0`, zero publish errors, and three consumer connections.
   The single-node topology is not evidence of failure-domain behavior.
9. Disk admission, DLQ outbox recovery, scrub, and protective eviction tests
   pass.
10. OrbStack Kubernetes functional acceptance passes. A real 500-node cluster
    is not required; discovery behavior is tested with synthetic endpoint and
    registry fixtures representing 500 brokers.
11. Prometheus histograms expose fsync, group-commit wait, publish ACK, payload
    read, scrub/GC, proxy backend, and discovery polling latency without
    per-topic or per-broker cardinality.
12. Fuzz targets cover protocol and compression plus storage record, channel
    WAL/checkpoint, topic manifest, proxy HTTP metadata, and registry-response
    parsing.
13. The release gate runs formatting, check, tests, clippy, Helm rendering,
    fuzz smoke and official Go/Python compatibility. OrbStack acceptance is an
    explicit local release gate because CI does not emulate multi-node PVC
    detach/attach behavior.

## 10. Explicit non-goals

- v6 migration or mixed v6/v7 operation;
- message replication or backup;
- exactly-once delivery;
- global deduplication, order, channel catalog, or atomic management;
- online broker data migration;
- consumer connection fan-out reduction;
- cross-region replication;
- management UI.
