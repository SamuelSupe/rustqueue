# RustQueue and NSQ performance boundaries

This comparison uses NSQ `v1.3.0`, the version used by
`scripts/benchmark-compare.sh`. It distinguishes protocol compatibility from
durability and scheduling behavior: equal TCP commands do not imply equal work
before an acknowledgement is returned.

## What RustQueue 0.8.3 changes

RustQueue now keeps each Channel's in-flight deadlines in an ordered index.
The Broker can find expired deliveries without scanning every in-flight message,
and a TCP session can wait for its next client deadline without scanning its
entire RDY window on every event. `TOUCH`, `FIN`, `REQ`, disconnect, and
delivery handoff remove or update the same index, so stale deadlines do not
accumulate.

The on-disk format remains v7. A successful `FIN` or `REQ` still crosses the
Channel WAL group `fsync` before the broker replies; this release changes the
runtime scheduler, not the acknowledged-delivery contract.

For publish groups, RustQueue now releases the Topic state lock while the
active segment file is syncing. A separate commit gate keeps tail mutations
serialized, and reservation stops at a durable position that advances only
after the sync succeeds. Existing durable messages can therefore continue to
flow while the next group waits for its acknowledged-durability boundary.

## Material differences from NSQ

| Area | NSQ v1.3.0 | RustQueue | Performance consequence |
| --- | --- | --- | --- |
| In-flight timeout lookup | `inFlightPQ` priority queue plus ID map | Ordered deadline index plus ID map | Both avoid a full in-flight scan. RustQueue 0.8.3 closes the prior CPU and lock-contention gap at high RDY. |
| Publish acknowledgement | `Topic.put` queues to memory or `go-diskqueue`; the diskqueue writer replies after `writeOne`, and performs its scheduled `sync` on a later I/O-loop iteration | A publish group appends, calls segment `fsync`, then replies | RustQueue has higher durable-PUB latency by design. NSQ `--sync-every=1` is not an acknowledgement-after-fsync equivalent. |
| FIN / REQ acknowledgement | In-flight state is memory-resident; shutdown flushes outstanding messages to the backend | Channel state is appended to a WAL and group-fsynced before success | RustQueue trades throughput for a stronger confirmed-ack crash boundary. Do not compare raw consume rate without stating this difference. |
| Normal-message memory queue | Configurable `mem-queue-size`; the default may absorb normal traffic before disk | No acknowledged in-memory Topic fast path; data is retained in the local segment before PUB success | A default NSQ benchmark can be much faster while shifting its durability window to memory / OS cache. The comparison script sets NSQ `mem-queue-size=0` to avoid that mismatch. |
| Topic fan-out and locking | A Topic message pump allocates a message object per additional Channel and feeds each Channel queue | One Topic segment is shared by cursor-based Channels; publish append/rotation remains serialized, but group `fsync` runs outside the reservation lock and delivery stops at the durable tail | RustQueue avoids one durable payload log per Channel without making existing durable reservations wait for the next PUB fsync. |

## How to read benchmark results

`scripts/benchmark-compare.sh` produces three distinct durability profiles:

- `rustqueue-local-fsync`: a PUB acknowledgement follows the local segment
  group `fsync`.
- `nsq-sync-every-1`: NSQ requests a diskqueue sync every write, but its PUB
  response follows the write before the next I/O-loop sync.
- `nsq-sync-every-2500`: NSQ's more relaxed sync cadence.

Treat the first two as a storage-write comparison, not an equal
acknowledged-durability comparison. Record publish acknowledgement latency,
complete unique delivery, duplicates, drain state, and RSS alongside throughput.
The benchmark aborts when a consumer run has missing deliveries, unexpected
duplicates, or an incomplete drain.

## Next candidates to measure before changing semantics

1. Profile Channel WAL `FIN`/`REQ` commit time against reservation latency;
   those durable state transitions still use the Topic state lock.
2. Measure an explicitly opt-in relaxed acknowledgement mode separately from
   the durable default; it must document crash-time redelivery and never be
   mixed with the durable-PUB result.
3. Compare many-Channel fan-out with equal memory budgets. NSQ allocates a
   message object per Channel and can persist each Channel backlog separately;
   RustQueue's shared segment reduces durable payload duplication but must also
   keep cursor and metadata lock costs bounded.

Primary source paths: NSQ's
[`channel.go`](https://github.com/nsqio/nsq/blob/v1.3.0/nsqd/channel.go),
[`topic.go`](https://github.com/nsqio/nsq/blob/v1.3.0/nsqd/topic.go), and
[`go-diskqueue`](https://github.com/nsqio/go-diskqueue/blob/v1.1.0/diskqueue.go).
