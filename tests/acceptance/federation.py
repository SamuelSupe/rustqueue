#!/usr/bin/env python3
import json
import os
import socket
import struct
import time
import urllib.error
import urllib.parse
import urllib.request


HOST = os.getenv("HOST", "host.docker.internal")
TOPIC = os.getenv("TOPIC", f"federation-{int(time.time())}")
CHANNEL = "workers"
HTTP_CELL_1 = 4151
HTTP_CELL_2 = 7151
HTTP_CELL_3 = 10151
TCP_CELL_3 = 10150


def http(port, path, data=None, attempts=60):
    headers = {}
    if data and data[:1] in (b"{", b"["):
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"http://{HOST}:{port}{path}", data=data, headers=headers
    )
    for attempt in range(attempts):
        try:
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
        except urllib.error.HTTPError as error:
            if error.code not in (409, 429, 503) or attempt + 1 == attempts:
                raise
        except urllib.error.URLError:
            if attempt + 1 == attempts:
                raise
        time.sleep(0.1)
    raise RuntimeError("HTTP retry loop exhausted")


def post_message(port, payload, partition=None):
    query = {"topic": TOPIC}
    if partition is not None:
        query["partition"] = partition
    return http(
        port,
        "/pub?" + urllib.parse.urlencode(query),
        payload.encode(),
        attempts=120,
    )


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


def wait_migration(operation_id):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        _, operations = http(HTTP_CELL_3, "/v1/federation/operations")
        operation = operations["partition_migrations"].get(str(operation_id))
        if operation and operation["phase"] == "completed":
            return operation
        if operation and operation["phase"] == "needs_operator":
            raise RuntimeError(f"migration requires operator: {operation}")
        time.sleep(0.2)
    raise RuntimeError("partition migration did not complete")


def create_subscription(channel):
    sock = socket.create_connection((HOST, TCP_CELL_3), timeout=20)
    sock.settimeout(5)
    sock.sendall(b"  V2")
    sock.sendall(f"SUB {TOPIC} {channel}\n".encode())
    frame_type, body = read_frame(sock)
    if frame_type != 0 or body != b"OK":
        raise RuntimeError(f"unexpected SUB response: {frame_type} {body!r}")
    return sock


http(
    HTTP_CELL_1,
    "/topic/create?"
    + urllib.parse.urlencode(
        {"topic": TOPIC, "partitions": 3, "replication_factor": 3}
    ),
    b"",
)
http(
    HTTP_CELL_3,
    "/channel/create?" + urllib.parse.urlencode({"topic": TOPIC, "channel": CHANNEL}),
    b"",
)

sock = create_subscription(CHANNEL)
sock.sendall(b"RDY 2500\n")
expected = set()
consumed = set()
duplicates = 0
requeued_once = False

for sequence in range(30):
    payload = f"before:{sequence}"
    post_message(HTTP_CELL_2, payload)
    expected.add(payload)

_, migration = http(
    HTTP_CELL_3,
    "/v1/federation/migrations",
    json.dumps(
        {"topic": TOPIC, "partition": 0, "target_cell_id": 2}
    ).encode(),
)
operation_id = migration["operation_id"]

for sequence in range(100):
    payload = f"during:{sequence}"
    post_message(HTTP_CELL_3, payload)
    expected.add(payload)

operation = wait_migration(operation_id)
for sequence in range(20):
    payload = f"after:{sequence}"
    post_message(HTTP_CELL_3, payload, partition=0)
    expected.add(payload)

deadline = time.monotonic() + 180
while expected - consumed and time.monotonic() < deadline:
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
    if not requeued_once:
        sock.sendall(b"TOUCH " + message_id + b"\n")
        sock.sendall(b"REQ " + message_id + b" 10\n")
        requeued_once = True
        continue
    if payload in consumed:
        duplicates += 1
    consumed.add(payload)
    sock.sendall(b"FIN " + message_id + b"\n")

missing = sorted(expected - consumed)
sock.sendall(b"CLS\n")
sock.close()
if missing:
    raise RuntimeError(f"cross-Cell ledger has missing messages: {missing[:10]}")

_, route = http(
    HTTP_CELL_3,
    "/v1/federation/route?"
    + urllib.parse.urlencode({"topic": TOPIC, "partition": 0}),
)
if route["home_cell_id"] != 2:
    raise RuntimeError(f"partition did not cut over to Cell 2: {route}")

ephemeral = "probe#ephemeral"
probe = create_subscription(ephemeral)
probe.sendall(b"CLS\n")
read_frame(probe)
probe.close()
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    _, channels = http(
        HTTP_CELL_2,
        "/channels?" + urllib.parse.urlencode({"topic": TOPIC}),
    )
    if ephemeral not in channels["channels"]:
        break
    time.sleep(0.2)
else:
    raise RuntimeError("federated ephemeral channel did not expire")

print(
    json.dumps(
        {
            "topic": TOPIC,
            "published": len(expected),
            "consumed_unique": len(consumed),
            "duplicates": duplicates,
            "missing": 0,
            "touch_req_redelivery": requeued_once,
            "migration_phase": operation["phase"],
            "partition_0_home_cell": route["home_cell_id"],
            "ephemeral_deleted": True,
        },
        sort_keys=True,
    )
)
