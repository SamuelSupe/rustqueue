# RustQueue format v7 share-nothing architecture

Status: accepted implementation contract
Target release: 0.10.0
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
- By default, `PUB`, `MPUB`, and `DPUB` succeed only after the local message
  segment has passed `fsync`.
- `queue.publish_ack_mode = "write_ack"` is an explicit weaker alternative:
  success follows append/write, while consumers remain bounded by the last
  background-fsynced position. A crash or power loss can lose the acknowledged
  tail.
- `queue.publish_ack_mode = "nsq_relaxed"` also exposes the appended tail
  immediately. A crash can therefore lose messages that were both acknowledged
  and delivered. Neither relaxed mode is a durable-PUB benchmark result.
- Recovery derives lost-tail gaps from surviving Topic metadata and durable
  Channel commands. This prevents an acknowledged vanished position from
  aliasing a later publish; derived gap ranges do not consume Channel depth or
  the out-of-order ACK window.
- Relaxed background fsync runs when the first configured message, byte, or
  interval threshold is reached. Any sync failure isolates local storage and
  stops subsequent writes.
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

### 2.3 Unrouted and bootstrap retention

Every broker persists the earliest Topic position accepted while no durable
Channel exists. Normal GC cannot cross that unrouted boundary, regardless of
the bootstrap window or a Broker restart. The first durable Channel starts
immediately before the boundary and can therefore consume every successfully
published message from that zero-Channel interval. Deleting the last durable
Channel starts a new boundary at the current Topic tail. Ephemeral Channels do
not satisfy or clear this durability boundary.

Once at least one durable Channel exists, a later Channel starts at the oldest
message still inside the configured bootstrap window. The default is 90
seconds. The Kodo compatibility profile requires 180 seconds so the default Go
consumer can survive one failed 60-second Lookupd poll, including its 30%
initial jitter.

This separates two guarantees:

- A Topic with no durable Channel retains all acknowledged publishes until its
  first durable Channel is created.
- Additional Channels prefer duplicates over misses during the normal
  discovery window, but do not receive unbounded Topic history.
- Explicit Topic empty/delete and opt-in protective eviction remain
  intentional destructive operations; protective eviction writes an audit
  record before advancing the unrouted boundary.

These guarantees describe the default `RELIABLE` mode. A Topic whose persisted
policy is `TTL_DISCARD` instead has a positive whole-second absolute lifetime
measured from its Broker-assigned publish timestamp. The same deadline applies
before routing, during DPUB/deferred and requeue delay, and after delivery while
the message is in flight. Expiry advances a persisted Topic prefix and evicts
that prefix from every durable and ephemeral Channel; late `FIN`, `REQ`, and
`TOUCH` therefore observe the normal not-in-flight error. Enabling or shortening
TTL applies to existing backlog by original publish time. Extending or disabling
TTL cannot restore an already expired prefix.

### 2.4 Routing and scale

- Producers connect to a node-local proxy DaemonSet.
- The proxy chooses a least-active publish-ready broker for each new TCP
  connection and a randomized ready broker for each HTTP request.
- A TCP connection stays on one broker until it closes or reaches the
  configured jittered maximum age (300 seconds by default, zero disables it).
- Ambiguous failures may cause a producer retry to create a duplicate on a
  different broker.
- Popular topics may spread to every broker. Consumers connecting to every
  owner, including approximately 500 owners, is an accepted product tradeoff.
- Consumers bypass the producer proxy and use discovery plus direct broker
  connections.
- The number of Brokers used concurrently by TCP publishing cannot exceed the
  producer connection count. Rotation spreads a small long-lived pool across
  the fleet over time, but it cannot turn 16 streams into 500-way concurrent
  disk I/O.

### 2.5 WebSocket TTL live subscriptions

The Broker HTTP listener optionally exposes a best-effort live subscription for
short-lived Topics:

```text
GET /v1/ws/topics/{topic}
Sec-WebSocket-Protocol: rustqueue.live.v1
```

The feature is default-off. A handshake succeeds only when the addressed
Topic already exists locally and its persisted policy is `TTL_DISCARD`; the
endpoint never creates a Topic, and `RELIABLE` or unknown Topics are rejected
with a stable error code. One connection represents one Topic and begins at
the current tail. There is no historical replay, reconnect resume, consumer
ACK or WebSocket retry.

Clients first query Discovery `/lookup` and then connect directly to every
healthy Broker owner returned for the Topic. The producer proxy is not a
WebSocket fan-out layer. During a visible owner migration, clients may union
the streams and deduplicate using the broker-scoped message ID in the frame.
Discovery's read-only lookup response uses the same allowed-origin policy as
the Broker WebSocket endpoint.

Each message is written as one complete binary frame. Its first 24 bytes are
big-endian metadata, followed by the original body:

| Offset | Size | Field |
| --- | ---: | --- |
| `0..4` | 4 | ASCII `RQW1` |
| `4..6` | 2 | Header length, fixed at `24` |
| `6..8` | 2 | Delivery attempts |
| `8..16` | 8 | Broker timestamp, signed `i64` nanoseconds |
| `16..24` | 8 | Broker-scoped message ID, unsigned `u64` |
| `24..` | — | Original message body |

Text frames are control messages. `auth_required`, client `auth`, `ready` and
stable error codes are part of the v1 control contract. Subscription
authorization reuses the existing AUTH rules with the virtual Channel
`websocket#ephemeral`. Service clients can send an HTTP
`Authorization: Bearer` header during upgrade; browser clients must use the
control exchange and must not put credentials in a query string. A browser
Origin must exactly match the configured allowlist. A missing Origin is
allowed for non-browser clients but does not bypass authentication.

The HTTP `101 Switching Protocols` response establishes only the transport.
The live subscription boundary is the server's `{"type":"ready",...}` control
frame. No message capture is promised before `ready`, including during the
AUTH exchange after `auth_required`; an HTTP upgrade is not a capture
boundary.

Handshake failures reuse the HTTP `ApiError` shape: the stable code is in the
JSON `message` field and the human-readable explanation is in `detail`. Once
upgraded, terminal errors use a text control of the form
`{"type":"error","code":"...","detail":"..."}` before close:

| Code | Meaning |
| --- | --- |
| `E_WS_QUERY_FORBIDDEN` | Query parameters are forbidden; use the path and headers. |
| `E_WS_ORIGIN` | Browser Origin is not an exact configured allowlist entry. |
| `E_WS_SUBPROTOCOL` | `rustqueue.live.v1` was not negotiated. |
| `E_BAD_TOPIC` | Topic does not exist on this Broker. |
| `E_WS_TTL_REQUIRED` | Existing Topic is not `TTL_DISCARD` or changed away from it. |
| `E_WS_CAPACITY` | Connection, Channel or frame-memory capacity is exhausted. |
| `E_WS_CONTROL` | Unsupported client text/binary data frame was received. |
| `E_WS_HEARTBEAT` | Peer missed the heartbeat liveness window. |
| `E_AUTH_TIMEOUT` | `auth` was not received before the AUTH deadline. |
| `E_BAD_AUTH` | AUTH control was non-text, malformed, or missing a valid `auth`/`secret`. |
| `E_AUTH_OVERLOADED` | AUTH service or capacity gate is exhausted; retry later. |
| `E_AUTH_FAILED` | Authentication service or credential validation failed. |
| `E_UNAUTHORIZED` | AUTH does not permit the Topic subscription. |
| `E_TOPIC_CLOSED` | Topic was deleted, tombstoned, or closed. |
| `E_OWNER_CHANGED` | Topic owner/fence changed; perform a fresh lookup. |
| `E_DRAINING` | Broker is draining and closes the live subscription. |
| `E_SHUTDOWN` | Broker is shutting down; reconnect after discovery. |

The delivery path creates one ephemeral Channel per connection and reserves at
most one message at a time. A reservation is released when the complete frame
cannot be written, when the connection closes, or when the Broker is shutting
down; there is no reliable backlog. The write deadline is the smaller of the
socket timeout and the message's remaining TTL. If a frame cannot finish by
that deadline, the connection closes rather than leaving a partial frame.
Paused Topics stop taking messages. Topic deletion, migration, owner shutdown,
or a change to `RELIABLE` closes affected connections and leaves reconnection
to the client's next lookup.

Runtime defaults are bounded and opt-in:

```toml
[websocket]
enabled = false
max_connections = 1024
max_connections_per_topic = 256
frame_inflight_bytes = 134217728
allowed_origins = []
```

The node-wide frame budget and connection caps protect publishers from slow
consumers. Metrics are aggregate by default and include connection count,
completed frame messages/bytes, authentication failures, capacity rejections,
non-TTL Topic rejections and slow-consumer closes.

### 2.6 Ordering and identity

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
- Optional best-effort WebSocket delivery for existing `TTL_DISCARD` Topics;
  it shares the Broker HTTP listener and does not add a listener or port.
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

1. lists the broker Kubernetes EndpointSlice with a 1.5-second request deadline;
2. polls a small broker revision/readiness head every 2 seconds and downloads
   the full registry only for a new Broker or changed registry revision;
3. expires stale broker observations after 5 seconds;
4. maintains topic, channel, consumer, and publisher indexes incrementally, so
   lookup requests do not scan every Broker registry;
5. serves `/lookup`, `/topics`, `/channels`, `/nodes`, `/ping`, and `/info`;
   `/lookup` returns the healthy owners during a partial Broker outage while
   the complete-inventory metric remains false.

WebSocket clients use `/lookup` only for owner discovery. Discovery does not
proxy or multiplex the WebSocket data path.

Discovery state is derived and is never authoritative message metadata. A
restart reconstructs the complete index from ready brokers. Replicas do not
coordinate and clients may union their answers exactly as with multiple
`nsqlookupd` instances. A timed-out EndpointSlice request is canceled, counted,
and followed by the normal 5-second stale-observation fail-closed behavior.

### 3.3 Producer proxy

`rustqueue-proxy` runs as a DaemonSet on application nodes so older Kubernetes
versions do not need `PreferSameNode` support.

- TCP is a bounded L4 pass-through. It chooses the ready backend with the fewest
  active proxy connections and does not parse or replay commands. A jittered
  maximum tunnel age periodically reconnects official producers so connection
  placement adapts to Broker fleet changes; the boundary remains an ambiguous
  failure and therefore may produce an at-least-once duplicate on retry.
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
- automatic scaling to the count of explicitly labelled member nodes, bounded
  by `minBrokers` and `maxBrokers`; cordon and transient Node readiness changes
  do not express scale-down intent;
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

Drain has two completion levels:

1. `quiesced`: stop accepting new publishes and wait for in-flight leases to
   reach zero; rolling replacement may now restart the same ordinal and PVC;
2. `empty`: additionally wait for stored messages and durable channel depth to
   reach zero; scale-down and targeted maintenance require this level.

Rolling replacement preserves backlog on the reattached PVC and therefore does
not wait for it to drain. If empty drain times out, scale-down and maintenance
remain blocked. Older v7 Brokers that do not report the delivery-freeze state
must reach their legacy full-empty drain before replacement; the Operator does
not synthesize a quiesced result from an unstable in-flight sample. Forced
discard is a separate, audited administrative action.

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
        segment-00000000000000000001.rqidx
      channels/
        <encoded-channel>.checkpoint
        <encoded-channel>.wal
  dlq-outbox/
  audit/
```

### 4.1 Message segment

- Immutable, length-prefixed binary records. New publishes use vectored
  header/metadata/body writes, so a 10-20 MiB body is not copied into a batch
  Vec and then copied again into a complete record Vec.
- Record header contains format, kind, message ID, timestamp, delayed target,
  payload length, and CRC32C.
- The payload is stored once per broker regardless of channel count.
- Tail short writes are truncated during recovery.
- Each sealed segment has an atomic CRC-protected recovery index containing
  record locations and fixed-size, per-entry-CRC queue metadata. Storage locations
  recover from the fixed header; queue metadata is validated in bounded pages at
  startup to recover ranges and per-segment delivery-deadline bounds. Delivery
  pages are loaded on demand into a Broker-wide byte-bounded cache, while
  background scrub validates the full
  sidecar checksum. A missing or bad index safely falls back to a full scan and
  is rebuilt.
- There is no backlog message-count limit. Sealed segments retain one summary
  in memory, so cold metadata residency scales with segment count rather than
  message count; disk high-watermark and minimum-free-space policy controls
  publish rejection. Sealing also releases unused message-metadata and record-
  location buffer capacity before returning its Broker-wide cache budget;
  spilled Topics cannot retain buffers from their former active tails.
- DPUB deadlines stay in queue metadata. Sealed segments retain only minimum and
  maximum deadlines, rather than one in-memory position per delayed message.
  Statistics count fully deferred segments by range and stream bounded pages for
  mixed deadlines, excluding ACKs, in-flight deliveries, and duplicate requeue
  bookkeeping. Metadata read failures propagate from the Rust statistics APIs
  as `Result` errors, fail HTTP statistics/metrics, and isolate storage; they
  never produce a successful partial snapshot.
- The active tail is always fully scanned. Cold indexed payload corruption is
  detected and isolated on payload read or by background scrub. Automatic scrub
  and normal GC wait for the configured startup quiet period (30 seconds by
  default), while disk-pressure admission remains active immediately.
  Scrub pins an immutable file list while holding the Topic lock, then releases
  that lock and verifies the files sequentially with a configurable bandwidth
  ceiling (`scrub_bytes_per_second`, 64 MiB/s by default).
- A runtime write, fsync, or integrity failure poisons the affected storage
  handle, removes the broker from readiness, and terminates the process for a
  clean recovery. No later request may append through an uncertain file
  offset.

### 4.2 Channel state

- Each local channel stores barrier/cursor, ACK floor, bounded sparse ACK,
  requeue targets, attempts that reached a durable `REQ`, paused state, and
  retention cursor.
- Scanning future DPUB messages retains one rewind position and the earliest
  wakeup time per Channel. When that deadline passes, delivery revisits shared
  Topic metadata from the rewind position; no per-message DPUB scan entries
  are added to the Channel checkpoint. This scan cursor is transient and
  recovery rescans from the ACK floor. Explicit REQ targets and attempts keep
  their existing WAL and checkpoint semantics.
  Recovery removes legacy scan deadlines that match shared metadata and have
  no delivery attempt, reading at most one metadata page at a time per Channel.
  Recorded position fences are captured before absent state is discarded.
- State mutations append to a per-channel WAL.
- Concurrent FIN/REQ mutations for one topic are combined into groups of at
  most 64 requests or 1 ms. Each touched durable channel is fsynced once before
  any request in that group is acknowledged.
- Periodic checkpoints use temporary-file, file fsync, rename, and directory
  fsync before the covered WAL can be removed.
- In-flight leases are not checkpointed. Recovery makes their messages
  immediately eligible for redelivery.
- Payload bodies retained for network delivery consume a node-wide working-set
  budget before disk read and keep that reservation through the socket write.
  Cache misses conservatively charge both the read buffer and delivered body.
  A per-connection fetch cap prevents one slow consumer from monopolizing the
  node budget; cancellation returns both the memory permit and queue lease.

### 4.3 GC and disk pressure

A complete segment may be deleted only when it is older than:

- every durable local channel retention cursor;
- the persisted unrouted boundary when no durable Channel exists;
- the configured bootstrap time floor;
- every active reader reference;
- every pending DLQ outbox reference.

`TTL_DISCARD` first persists the Channel eviction WALs and Topic
`expiredThroughPosition`; ordinary GC later reclaims Segments once active
readers and other references release them. Scheduler work is deadline-ordered
and bounded per batch. Delivery reservation and socket handoff recheck the
absolute deadline, so scheduler delay cannot expose an expired message.

High disk watermark first rejects publishes with retryable `429`. Protective
eviction remains opt-in and local: after the configured grace period it may
delete the oldest complete segment, persist the resulting channel gaps, and
write an audit record. There is no quorum because there is no replica.

Reader references are acquired while holding the topic lock. GC, protective
eviction, and topic deletion inspect those leases under the same lock order, so
a segment cannot disappear between delivery reservation and payload read.

The normal GC retention plan uses sealed-segment timestamp, message-ID,
position, and log-index summaries plus the bounded active tail. It does not
linearly walk a multi-million-message backlog on every five-second pass.
Physical deletion remains a contiguous immutable-segment prefix, and cached
metadata pages are invalidated with actual reclamation.

A normal GC tick examines at most 128 Topics using a rotating cursor. It does
not fsync a manifest or seal a segment until a reclaimable immutable prefix
exists; an active tail is sealed only when every message in that tail is below
the retention boundary. Consequently an abandoned channel cannot create a new
segment and sidecar pair every five seconds.

### 4.4 DLQ

DLQ transfer uses a compact CRC-protected binary local outbox. The source message is not durably
finished until the DLQ message append is durable. Recovery retries incomplete
outbox work. A crash may duplicate the DLQ entry but must not delete the source
first.

DLQ termination applies only to `RELIABLE` Topics. `TTL_DISCARD` continues
normal retries until its absolute deadline, bypasses retention/max-attempt DLQ
rules, and removes any incomplete source outbox intent when the policy is
enabled. Expiry itself never publishes a DLQ message.

## 5. Public behavior

Retained NSQ commands:

`IDENTIFY`, `AUTH`, `SUB`, `PUB`, `MPUB`, `DPUB`, `RDY`, `FIN`, `REQ`, `TOUCH`,
`NOP`, and `CLS`.

Retained compatible HTTP endpoints are local broker endpoints. Lookup endpoints
move to discovery. Native v6 cluster APIs are removed, including partition,
replica, migration, rebalance, transfer-leader, federation, and cluster
operation resources.

The optional native Broker endpoint `GET /v1/ws/topics/{topic}` is available
only when WebSocket support is enabled and the Topic is `TTL_DISCARD`. Clients
must negotiate `rustqueue.live.v1`; a successful frame write is best effort and
does not acknowledge application processing. `/v1/capabilities` advertises the
`websocket_ttl_live_v1` capability when the binary supports this protocol.

`/ping` remains process liveness. Broker readiness requires local storage,
clock, and admission health only; it has no cluster-wide dependency.

## 6. Security boundary

- The deployment targets a trusted internal Kubernetes network.
- Client-facing broker TLS/mTLS and external AUTH remain optional and compatible.
- WebSocket Origin checks are exact matches against `websocket.allowed_origins`;
  the same allowlist is used by Discovery's read-only `/lookup` response.
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
4. A Topic published before any durable Channel survives the configured
   bootstrap interval, normal GC, and Broker restart. A durable Channel created
   afterward receives the complete acknowledged ledger with `missing=0`.
   Discovery still indexes a new fallback owner within 5 seconds, and an
   official Go consumer uses the unchanged 60-second poll and 30% jitter.
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
11. Prometheus histograms expose publish/channel fsync, group-commit wait,
    publish/FIN/REQ ACK, payload read, scrub/GC, proxy backend, and discovery
    polling latency. Queue metrics are aggregate-only by default; optional
    topic/channel labels have a global series budget and report omissions.
12. Fuzz targets cover protocol and compression plus storage record, channel
    WAL/checkpoint, topic manifest, proxy HTTP metadata, and registry-response
    parsing.
13. The release gate runs formatting, check, tests, clippy, Helm rendering,
    fuzz smoke and official Go/Python compatibility. OrbStack acceptance is an
    explicit local release gate because CI does not emulate multi-node PVC
    detach/attach behavior.
14. Console management acceptance creates a real three-owner topic, forces the
    Console Pod down after one owner is durably recorded, and verifies the new
    Pod completes the remaining owners without reapplying the finished owner.
15. WebSocket acceptance verifies the `rustqueue.live.v1` handshake and 24-byte
    `RQW1` frame layout, rejects missing and `RELIABLE` Topics without creating
    them, confirms no replay across connect/disconnect, and exercises AUTH,
    exact Origin checks, TTL deadline closure, Topic pause/delete/migration,
    connection and frame-budget rejection, and reservation cleanup.

## 10. Explicit non-goals

- v6 migration or mixed v6/v7 operation;
- message replication or backup;
- exactly-once delivery;
- global deduplication, order, channel catalog, or atomic management;
- online broker data migration;
- consumer connection fan-out reduction;
- reliable WebSocket delivery, replay, resume, client ACK or a shared
  WebSocket consumer group;
- cross-region replication;
- Broker/PVC lifecycle controls in Console.
