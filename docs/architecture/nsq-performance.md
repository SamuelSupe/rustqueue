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
| Publish acknowledgement | `Topic.put` queues to memory or `go-diskqueue`; the diskqueue writer replies after `writeOne`, and performs its scheduled `sync` on a later I/O-loop iteration | Default `durable` mode appends, calls segment `fsync`, then replies | RustQueue has higher durable-PUB latency by design. NSQ `--sync-every=1` is not an acknowledgement-after-fsync equivalent. |
| FIN / REQ acknowledgement | In-flight state is memory-resident; shutdown flushes outstanding messages to the backend | Channel state is appended to a WAL and group-fsynced before success | RustQueue trades throughput for a stronger confirmed-ack crash boundary. Do not compare raw consume rate without stating this difference. |
| Relaxed publish path | Configurable `mem-queue-size`; diskqueue replies after write and syncs later by count or timer | `write_ack` replies after append but delays delivery until fsync; `nsq_relaxed` replies and delivers after append | Compare each mode under its own durability label. The script keeps NSQ `mem-queue-size=0`, so the relaxed comparison isolates diskqueue sync cadence rather than adding NSQ's memory queue. |
| Topic fan-out and locking | A Topic message pump allocates a message object per additional Channel and feeds each Channel queue | One Topic segment is shared by cursor-based Channels; publish append/rotation remains serialized, but group `fsync` runs outside the reservation lock and delivery stops at the durable tail | RustQueue avoids one durable payload log per Channel without making existing durable reservations wait for the next PUB fsync. |

## How to read benchmark results

`scripts/benchmark-compare.sh` always produces three distinct durability
profiles:

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

Set `RUN_RELAXED=1` to add two separately named RustQueue profiles:

- `rustqueue-write-ack`: ACK after append, consume after background fsync.
- `rustqueue-nsq-relaxed`: ACK and consume after append.

Both use the first reached `RELAXED_SYNC_MESSAGES` (default 2500),
`RELAXED_SYNC_BYTES` (default 8 MiB), or `RELAXED_SYNC_INTERVAL_MS` (default
10 ms) boundary. Never combine either result with `rustqueue-local-fsync`.
Track `rustqueue_publish_unsynced_messages`,
`rustqueue_publish_unsynced_bytes`, and
`rustqueue_publish_sync_lag_seconds` alongside throughput and ACK latency.

## Next candidates to measure before changing semantics

1. Profile Channel WAL `FIN`/`REQ` commit time against reservation latency;
   those durable state transitions still use the Topic state lock.
2. Profile `write_ack` and `nsq_relaxed` under mixed producer/consumer load,
   including ACK latency, durable-position lag, RSS, and the time from append
   acknowledgement to fsync. Keep both separate from the durable-PUB result.
3. Compare many-Channel fan-out with equal memory budgets. NSQ allocates a
   message object per Channel and can persist each Channel backlog separately;
   RustQueue's shared segment reduces durable payload duplication but must also
   keep cursor and metadata lock costs bounded.
4. Keep NSQ's memory queue as a separate profile. Neither RustQueue relaxed
   mode bypasses the segment append, so comparing either one with a non-zero
   NSQ `mem-queue-size` measures an additional architectural difference.
5. Inspect publish latency outliers at segment rotation and durable message-ID
   block reservation. These paths sync metadata infrequently and should not be
   inferred from median steady-state throughput.

Primary source paths: NSQ's
[`channel.go`](https://github.com/nsqio/nsq/blob/v1.3.0/nsqd/channel.go),
[`topic.go`](https://github.com/nsqio/nsq/blob/v1.3.0/nsqd/topic.go), and
[`go-diskqueue`](https://github.com/nsqio/go-diskqueue/blob/v1.1.0/diskqueue.go).
