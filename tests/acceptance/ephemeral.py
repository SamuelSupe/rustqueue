#!/usr/bin/env python3
import json
import socket
import struct
import time
import urllib.request


HOST = "host.docker.internal"
TOPIC = "ephemeral-acceptance"
CHANNEL = "workers#ephemeral"


def read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed while reading NSQ frame")
        data += chunk
    return data


def subscribe(port):
    sock = socket.create_connection((HOST, port), timeout=10)
    sock.sendall(b"  V2")
    sock.sendall(f"SUB {TOPIC} {CHANNEL}\n".encode())
    size = struct.unpack(">I", read_exact(sock, 4))[0]
    frame = read_exact(sock, size)
    if frame != struct.pack(">I", 0) + b"OK":
        raise RuntimeError(f"unexpected SUB response: {frame!r}")
    return sock


def close_subscription(sock):
    sock.sendall(b"CLS\n")
    size = struct.unpack(">I", read_exact(sock, 4))[0]
    frame = read_exact(sock, size)
    if frame != struct.pack(">I", 0) + b"CLOSE_WAIT":
        raise RuntimeError(f"unexpected CLS response: {frame!r}")
    sock.close()


def channels():
    url = f"http://{HOST}:5151/channels?topic={TOPIC}"
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)["channels"]


def wait_for(expected, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        present = CHANNEL in channels()
        if present == expected:
            return
        time.sleep(0.1)
    raise RuntimeError(f"channel presence did not become {expected}: {channels()}")


first = subscribe(5150)
second = subscribe(6150)
wait_for(True)
close_subscription(first)
time.sleep(1)
if CHANNEL not in channels():
    raise RuntimeError("channel was deleted while another gateway lease was active")
close_subscription(second)
wait_for(False)
print(json.dumps({"created": True, "survived_one_disconnect": True, "deleted": True}))
