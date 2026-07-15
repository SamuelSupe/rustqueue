#!/usr/bin/env python3
import json
import os
import socket
import struct
import threading
import time
import urllib.parse
import urllib.request


HOST = os.getenv("HOST", "host.docker.internal")
TCP_PORT = int(os.getenv("TCP_PORT", "4150"))
HTTP_ENDPOINTS = os.getenv("HTTP_ENDPOINTS", f"http://{HOST}:4151").split(",")
PARTITIONS = int(os.getenv("PARTITIONS", "1024"))
CONSUMERS = int(os.getenv("CONSUMERS", "32"))
DURATION_SECONDS = float(os.getenv("DURATION_SECONDS", "8"))
MESSAGES = int(os.getenv("MESSAGES", "64"))
TOPIC = os.getenv("TOPIC", f"network-scale-{int(time.time())}")
CHANNEL = os.getenv("CHANNEL", "workers")
CREATE_TOPIC = os.getenv("CREATE_TOPIC", "1") == "1"

stop = threading.Event()
start = threading.Event()
condition = threading.Condition()
ready = 0
consumed = set()
duplicates = 0
errors = []


def request(path, data=None, timeout=1800):
    req = urllib.request.Request(HTTP_ENDPOINTS[0] + path, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read()
        return response.status, body


def metric_value(text, name, labels=""):
    prefix = name + labels + " "
    for line in text.splitlines():
        if line.startswith(prefix):
            return float(line[len(prefix) :])
    raise RuntimeError(f"metric not found: {name}{labels}")


def metrics_snapshot():
    nodes = []
    for endpoint in HTTP_ENDPOINTS:
        with urllib.request.urlopen(endpoint + "/metrics", timeout=60) as response:
            nodes.append(response.read().decode())
    gateway = nodes[0]
    return {
        "consumer_requests": metric_value(
            gateway, "rustqueue_consumer_fetch_requests_total"
        ),
        "consumer_empty": metric_value(
            gateway, "rustqueue_consumer_fetch_empty_total"
        ),
        "consumer_batches": metric_value(
            gateway, "rustqueue_consumer_fetch_batches_total"
        ),
        "consumer_messages": metric_value(
            gateway, "rustqueue_consumer_fetch_messages_total"
        ),
        "internal_fetch_requests": sum(
            metric_value(
                node,
                "rustqueue_internal_rpc_requests_total",
                '{operation="fetch"}',
            )
            for node in nodes
        ),
        "redirects": sum(
            metric_value(node, "rustqueue_leader_redirects_total") for node in nodes
        ),
        "retries": sum(
            metric_value(node, "rustqueue_internal_rpc_retries_total")
            for node in nodes
        ),
        "ack_batches": metric_value(gateway, "rustqueue_ack_batches_total"),
        "ack_messages": metric_value(gateway, "rustqueue_ack_messages_total"),
        "storage_errors": sum(
            metric_value(node, "rustqueue_storage_errors_total") for node in nodes
        ),
    }


def delta(after, before, name):
    return int(after[name] - before[name])


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed while reading NSQ frame")
        data.extend(chunk)
    return bytes(data)


def read_frame(sock):
    size = struct.unpack(">I", read_exact(sock, 4))[0]
    frame = read_exact(sock, size)
    return struct.unpack(">I", frame[:4])[0], frame[4:]


def consumer():
    global ready, duplicates
    sock = None
    try:
        sock = socket.create_connection((HOST, TCP_PORT), timeout=30)
        sock.settimeout(120)
        sock.sendall(b"  V2")
        sock.sendall(f"SUB {TOPIC} {CHANNEL}\n".encode())
        frame_type, body = read_frame(sock)
        if frame_type != 0 or body != b"OK":
            raise RuntimeError(f"unexpected SUB response: {frame_type} {body!r}")
        sock.settimeout(0.25)
        with condition:
            ready += 1
            condition.notify_all()
        if not start.wait(120):
            raise RuntimeError("consumer start gate timed out")
        sock.sendall(b"RDY 64\n")
        while not stop.is_set():
            try:
                frame_type, body = read_frame(sock)
            except socket.timeout:
                continue
            if frame_type == 0 and body == b"_heartbeat_":
                sock.sendall(b"NOP\n")
                continue
            if frame_type == 1:
                raise RuntimeError(f"NSQ error frame: {body.decode(errors='replace')}")
            if frame_type != 2 or len(body) < 26:
                continue
            message_id = body[10:26]
            payload = body[26:].decode()
            with condition:
                if payload in consumed:
                    duplicates += 1
                consumed.add(payload)
                condition.notify_all()
            sock.sendall(b"FIN " + message_id + b"\n")
    except Exception as error:  # noqa: BLE001 - report exact acceptance failure
        with condition:
            errors.append(str(error))
            condition.notify_all()
    finally:
        if sock is not None:
            sock.close()


if CREATE_TOPIC:
    request(
        "/topic/create?"
        + urllib.parse.urlencode(
            {
                "topic": TOPIC,
                "partitions": PARTITIONS,
                "replication_factor": 3,
            }
        ),
        b"",
    )
request(
    "/channel/create?" + urllib.parse.urlencode({"topic": TOPIC, "channel": CHANNEL}),
    b"",
)
_, topology_body = request(
    "/v1/partitions?" + urllib.parse.urlencode({"topic": TOPIC}), timeout=120
)
topology = json.loads(topology_body)
active = [item for item in topology["partitions"] if item["lifecycle"] == "active"]
if len(active) != PARTITIONS:
    raise RuntimeError(f"expected {PARTITIONS} active partitions, found {len(active)}")

subscribe_started = time.monotonic()
threads = [threading.Thread(target=consumer, daemon=True) for _ in range(CONSUMERS)]
for thread in threads:
    thread.start()
with condition:
    deadline = time.monotonic() + 120
    while ready != CONSUMERS and not errors and time.monotonic() < deadline:
        condition.wait(deadline - time.monotonic())
if ready != CONSUMERS or errors:
    raise RuntimeError(f"only {ready}/{CONSUMERS} consumers ready: {errors}")
subscribe_seconds = time.monotonic() - subscribe_started

baseline = metrics_snapshot()
start.set()
time.sleep(DURATION_SECONDS)
idle = metrics_snapshot()

run = f"scale:{time.time_ns()}"
expected = {f"{run}:{sequence}" for sequence in range(MESSAGES)}
body = ("\n".join(sorted(expected)) + "\n").encode()
request(
    "/mpub?"
    + urllib.parse.urlencode({"topic": TOPIC, "partition": 0, "binary": "false"}),
    body,
    timeout=120,
)
with condition:
    deadline = time.monotonic() + 120
    while not expected.issubset(consumed) and not errors and time.monotonic() < deadline:
        condition.wait(deadline - time.monotonic())
missing = sorted(expected - consumed)
stop.set()
for thread in threads:
    thread.join(10)
time.sleep(1)
final = metrics_snapshot()

external = delta(idle, baseline, "consumer_requests")
internal = delta(idle, baseline, "internal_fetch_requests")
delivery_batches = delta(final, idle, "consumer_batches")
delivery_empty = delta(final, idle, "consumer_empty")
delivery_nonempty = delivery_batches - delivery_empty
max_external = int(CONSUMERS * DURATION_SECONDS * 20 + CONSUMERS * 2)
result = {
    "topic": TOPIC,
    "active_partitions": len(active),
    "consumers": CONSUMERS,
    "idle_seconds": DURATION_SECONDS,
    "subscribe_seconds": round(subscribe_seconds, 3),
    "consumer_fetch_requests": external,
    "consumer_fetch_requests_per_second": round(external / DURATION_SECONDS, 2),
    "consumer_fetch_requests_per_consumer_second": round(
        external / DURATION_SECONDS / CONSUMERS, 2
    ),
    "internal_fetch_requests": internal,
    "internal_requests_per_consumer_fetch": round(internal / max(external, 1), 3),
    "empty_fetches": delta(idle, baseline, "consumer_empty"),
    "redirects": delta(idle, baseline, "redirects"),
    "retries": delta(idle, baseline, "retries"),
    "published": len(expected),
    "consumed_unique": len(expected & consumed),
    "duplicates": duplicates,
    "missing": len(missing),
    "delivery_fetch_batches": delivery_batches,
    "delivery_empty_fetches": delivery_empty,
    "delivery_nonempty_batches": delivery_nonempty,
    "delivered_messages": delta(final, idle, "consumer_messages"),
    "ack_batches": delta(final, idle, "ack_batches"),
    "ack_messages": delta(final, idle, "ack_messages"),
    "storage_errors": delta(final, baseline, "storage_errors"),
}

if subscribe_seconds > 15:
    raise RuntimeError(f"ACTIVE channel subscription fast path is too slow: {result}")
if external <= 0 or external > max_external:
    raise RuntimeError(f"consumer polling rate is outside the long-poll bound: {result}")
if internal > external * 2 + 32:
    raise RuntimeError(f"internal fetch amplification detected: {result}")
if missing or errors:
    raise RuntimeError(
        f"delivery ledger failed: {result}, missing={missing[:10]}, errors={errors}"
    )
if not 0 < delivery_nonempty < MESSAGES:
    raise RuntimeError(f"FetchBatch did not combine deliveries: {result}")
if not 0 < result["ack_batches"] < MESSAGES:
    raise RuntimeError(f"ACK pipeline did not combine FIN commands: {result}")
if result["ack_messages"] < MESSAGES or result["storage_errors"] != 0:
    raise RuntimeError(f"ACK/storage invariant failed: {result}")
print(json.dumps(result, sort_keys=True))
