#!/usr/bin/env python3
import json
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


HOST = "host.docker.internal"
HTTP_PORT = 4151
TCP_PORT = 5150
TOPIC = f"expand-{int(time.time())}"
CHANNEL = "workers"
PUBLISH_COUNT = 600


def http(path, data=None, method=None):
    request = urllib.request.Request(
        f"http://{HOST}:{HTTP_PORT}{path}", data=data, method=method
    )
    if data and path.startswith("/v1/"):
        request.add_header("content-type", "application/json")
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read()
        if not body:
            value = None
        else:
            try:
                value = json.loads(body)
            except json.JSONDecodeError:
                value = body.decode()
        return response.status, value


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


published = set()
consumed = set()
duplicates = 0
lock = threading.Lock()
consumer_error = []
publisher_error = []
expected_ready = threading.Event()


def consume():
    global duplicates
    try:
        sock = socket.create_connection((HOST, TCP_PORT), timeout=15)
        sock.settimeout(5)
        sock.sendall(b"  V2")
        sock.sendall(f"SUB {TOPIC} {CHANNEL}\n".encode())
        frame_type, body = read_frame(sock)
        if frame_type != 0 or body != b"OK":
            raise RuntimeError(f"unexpected SUB response: {frame_type} {body!r}")
        sock.sendall(b"RDY 2500\n")
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            with lock:
                complete = expected_ready.is_set() and published <= consumed
            if complete:
                sock.sendall(b"CLS\n")
                sock.close()
                return
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
            sequence = int(payload.split(":", 1)[1])
            with lock:
                if sequence in consumed:
                    duplicates += 1
                consumed.add(sequence)
            sock.sendall(b"FIN " + message_id + b"\n")
        raise RuntimeError("consumer timed out waiting for acknowledged messages")
    except Exception as error:  # noqa: BLE001 - acceptance process reports exact error
        consumer_error.append(error)


def publish():
    try:
        for sequence in range(PUBLISH_COUNT):
            payload = f"seq:{sequence}".encode()
            path = f"/pub?topic={urllib.parse.quote(TOPIC)}"
            for attempt in range(20):
                try:
                    http(path, payload, "POST")
                    with lock:
                        published.add(sequence)
                    break
                except (urllib.error.URLError, TimeoutError):
                    if attempt == 19:
                        raise
                    time.sleep(0.05)
            time.sleep(0.004)
    except Exception as error:  # noqa: BLE001
        publisher_error.append(error)


http(
    f"/topic/create?topic={urllib.parse.quote(TOPIC)}&partitions=4&replication_factor=3",
    b"",
    "POST",
)
http(
    f"/channel/create?topic={urllib.parse.quote(TOPIC)}&channel={CHANNEL}",
    b"",
    "POST",
)

consumer = threading.Thread(target=consume, daemon=True)
publisher = threading.Thread(target=publish, daemon=True)
consumer.start()
publisher.start()

deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    with lock:
        if len(published) >= 75:
            break
    time.sleep(0.02)
else:
    raise RuntimeError("publisher did not establish traffic before expansion")

_, expansion = http(
    f"/v1/topics/{urllib.parse.quote(TOPIC)}/partitions",
    json.dumps({"target_partitions": 8}).encode(),
    "POST",
)
operation_id = expansion["operation_id"]
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    _, operation = http(f"/v1/cluster/operations/{operation_id}")
    if operation["state"] == "completed":
        break
    if operation["state"] == "needs_operator":
        raise RuntimeError(f"expansion requires operator: {operation}")
    time.sleep(0.2)
else:
    raise RuntimeError("partition expansion did not complete")

# Explicitly exercise every new partition while the original TCP connection remains open.
for offset, partition in enumerate(range(4, 8), start=PUBLISH_COUNT):
    http(
        f"/pub?topic={urllib.parse.quote(TOPIC)}&partition={partition}",
        f"seq:{offset}".encode(),
        "POST",
    )
    with lock:
        published.add(offset)

publisher.join(60)
expected_ready.set()
consumer.join(120)
if publisher.is_alive() or consumer.is_alive():
    raise RuntimeError("traffic threads did not finish")
if publisher_error or consumer_error:
    raise RuntimeError(f"traffic failure: {publisher_error + consumer_error}")

_, topology = http(f"/v1/partitions?topic={urllib.parse.quote(TOPIC)}")
active = [item for item in topology["partitions"] if item["lifecycle"] == "active"]
key_slots = [item for item in active if item["key_routing"]]
with lock:
    missing = sorted(published - consumed)
    result = {
        "topic": TOPIC,
        "active_partitions": len(active),
        "permanent_key_routing_slots": len(key_slots),
        "acknowledged": len(published),
        "consumed_unique": len(consumed),
        "duplicates": duplicates,
        "missing": len(missing),
    }
if len(active) != 8 or len(key_slots) != 4 or missing:
    raise RuntimeError(f"expansion invariant failed: {result}, missing={missing[:10]}")
print(json.dumps(result, sort_keys=True))
