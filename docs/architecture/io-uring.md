# Optional io_uring storage I/O

The Linux Broker can use io_uring for payload reads and group data syncs.
The setting defaults to false and takes effect at startup:

```toml
[storage]
io_uring_enabled = true
```

`RUSTQUEUE_IO_URING_ENABLED=true` overrides the configuration file. Kubernetes
uses Helm `queue.ioUringEnabled` / RustQueue `spec.ioUringEnabled`.
If the default pod policy blocks io_uring, the optional
`queue.brokerSeccompProfilePath` / `spec.brokerSeccompProfilePath` selects an
administrator-installed node-local seccomp profile permitting these calls.
Linux 5.10+ and permission to create/use an io_uring instance are required.
Requested but unavailable support is a startup error, not a silent fallback.
Disable the setting and restart to return to portable I/O; no data conversion
is needed. `--check-config` validates configuration and the target platform,
but actual kernel/container availability is checked during Broker startup.

The startup log, native Broker stats `io_uring_enabled`, and Prometheus
`rustqueue_io_uring_enabled` expose the selected, initialized backend.

## Scope and resource bounds

- Payload readers keep their bounded worker pool, queue, FD cache, CRC checks,
  segment leases, and delivery budget. On Linux with the switch enabled,
  independent coalesced ranges are submitted concurrently through tokio-uring.
  Each batch contains at most 64 reads and 2 MiB of temporary read buffers,
  or one larger message. Output order follows the requested payload order.
  A batch containing only one contiguous range uses the cheaper positional
  read, avoiding ring/descriptor overhead where there is no I/O to overlap.
  Isolated bodies larger than the 2 MiB coalescing limit transfer their
  verified buffer directly to the result, avoiding a second large allocation
  and copy. Smaller bodies retain the original copy/checksum path.
- Multi-file group data syncs for FIN/REQ and TTL discard use a dedicated
  io_uring worker. Its input queue holds at most 64
  jobs, at most 64 jobs are active, and at most 64 file sync/close operations
  run concurrently across all jobs. Independent Channel WALs can sync in
  parallel. The caller's existing commit gate still protects each Topic.
- Single-file data syncs, including publish and relaxed background sync,
  retain the direct portable call to avoid adding a dispatch round trip to
  every durable acknowledgement.
- Segment append remains vectored buffered I/O. Network I/O remains Tokio.
  Checkpoint/manifest rename, directory sync, segment rotation, recovery, and
  maintenance operations retain their existing portable implementation.

The implementation uses tokio-uring's owned buffers and file handles. All
submitted reads complete before a job releases its segment leases or byte
budget, including after client cancellation. Duplicated read descriptors are
explicitly closed after each job so shared-cache invalidation can release
deleted segment space. A failed sync group drains outstanding operations
before reporting the error; storage errors retain the Broker's fail-closed
behavior.

## Durability

Appends complete before data syncs are submitted. A durable publish advances
the durable tail and replies only after its sync completes. FIN/REQ succeeds
only after every touched Channel WAL has synced. Switching I/O backends does
not change the v7 disk format, per-Topic order, CRC verification, ACK contract,
or the crash-loss windows of `write_ack` and `nsq_relaxed`.

## Validation and performance

Run `make test` for portable behavior and `make test-io-uring` on a Linux
environment where io_uring is permitted. The latter enables the real backend
for payload-reader tests, sync-error handling, recovery, and SIGKILL crash
boundaries, and verifies restart with the backend disabled. These tests fail
if io_uring is unavailable rather than reporting skipped work as success.
For Docker environments that deny io_uring by default, pass an appropriate
profile explicitly, for example:

```sh
make test-io-uring RUST_DOCKER_ARGS='--security-opt seccomp=/absolute/path/io-uring.json'
```

For A/B measurements, build once and restart the Broker with the switch off
and on, using fresh data directories on the same storage. Use the existing
`rustqueue-bench` with identical arguments and a fixed durable ACK mode.
Check `delivery_complete`, unique receive counts, missing/duplicate messages,
end-to-end receive throughput, PUB-ACK p99, CPU, and RSS. Include cache-miss
backlog reads and multiple Topics/Channels; a single hot cached Topic does
not exercise all of the new concurrency. Keep the two backend results
separate from comparisons that change durability settings.

The focused queue-layer benchmark exercises one Topic, 16 durable Channels,
64 messages of 128 bytes per round, and concurrent FINs across all Channels.
It verifies every delivery's ID/body and reopens using portable I/O to verify
that acknowledged messages remain finished. It includes 20 warm-up rounds and
300 measured rounds and reports per-delivery throughput and per-round p99;
these are not TCP throughput or individual PUB-ACK latency measurements.
On a Linux host/container permitting io_uring, run alternating off/on trials:

```sh
cargo bench --locked -p rustqueue-queue --bench io_uring
cargo bench --locked -p rustqueue-queue --bench io_uring -- --io-uring
```

Set `TMPDIR` to storage representative of the deployment. Compare several
trials on the same CPU allocation and filesystem; include the network benchmark
above when evaluating the complete Broker rather than only the queue layer.

To isolate the cache-miss delivery path from publish/FIN sync latency, use the
same benchmark's backlog mode. It durably publishes 1 KiB messages before
measurement, sets the application payload cache to one byte, warms up with
2,048 deliveries, then measures 32,768 deliveries. Every ID/body is verified.
It measures fetch throughput and per-fetch p99 only; it does not time FIN or
flush the Linux page cache. Use batch size 1 for individual reads and 64 for
coalesced reads (supported range: 1..=64):

```sh
cargo bench --locked -p rustqueue-queue --bench io_uring -- --read-backlog --batch-size=1
cargo bench --locked -p rustqueue-queue --bench io_uring -- --read-backlog --batch-size=1 --io-uring
```

For large-message copy costs, add `--message-bytes=4194304` (4 MiB) with
`--batch-size=1`. Sizes from 1 byte to 20 MiB are accepted. The benchmark adjusts
the message count to the payload size, measuring 64..=32,768 messages after
2..=2,048 warm-up deliveries. Fetches may return fewer than the requested batch
size when the Broker's byte budget limits a batch.

io_uring can reduce I/O submission overhead and overlap independent storage
operations. It cannot remove device flush latency, and extra dispatch costs
can outweigh benefits for small cached workloads. Enable it based on measured
results for the deployment's storage and workload.
