import json
import logging
import ssl
import sys
import time
import urllib.parse
import urllib.request

import nsq
from tornado.ioloop import IOLoop, PeriodicCallback

logging.disable(logging.CRITICAL)


def create_topic(http_address, topic):
    request = urllib.request.Request(
        f"http://{http_address}/topic/create?topic={urllib.parse.quote(topic)}",
        data=b"",
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5):
        pass


def channels(http_address, topic):
    endpoint = f"http://{http_address}/channels?topic={urllib.parse.quote(topic)}"
    try:
        with urllib.request.urlopen(endpoint, timeout=1) as response:
            return set(json.load(response)["channels"])
    except Exception:
        return set()


def wait_channel(http_address, topic, channel, present):
    deadline = time.time() + 10
    while time.time() < deadline:
        if (channel in channels(http_address, topic)) == present:
            return
        time.sleep(0.05)
    raise RuntimeError(f"channel {channel} presence did not become {present}")


def run_behavior_matrix(tcp_address, http_address, options):
    loop = IOLoop.current()
    topic = f"compat_python_{time.time_ns()}"
    create_topic(http_address, topic)
    state = {
        "published": False,
        "primary": set(),
        "audit": set(),
        "req_attempts": 0,
        "deferred_at": None,
        "published_at": None,
        "error": None,
    }

    def finish_touch(message):
        message.finish()
        state["primary"].add("touch")

    def primary_handler(message):
        body = message.body.decode()
        if body == "req":
            state["req_attempts"] = max(state["req_attempts"], message.attempts)
            if message.attempts == 1:
                return False
        if body == "touch":
            message.enable_async()
            message.touch()
            loop.call_later(0.3, finish_touch, message)
            return None
        if body == "later":
            state["deferred_at"] = time.monotonic()
        state["primary"].add(body)
        return True

    def audit_handler(message):
        state["audit"].add(message.body.decode())
        return True

    reader_options = dict(options)
    reader_options.update(max_in_flight=2, requeue_delay=0, msg_timeout=2)
    primary = nsq.Reader(
        topic=topic,
        channel="workers",
        message_handler=primary_handler,
        lookupd_http_addresses=[http_address],
        lookupd_poll_interval=1,
        **reader_options,
    )
    audit = nsq.Reader(
        topic=topic,
        channel="audit",
        message_handler=audit_handler,
        nsqd_tcp_addresses=[tcp_address],
        **reader_options,
    )
    writer = nsq.Writer([tcp_address], **options)

    def publish_when_ready():
        if state["published"]:
            return
        if not {"workers", "audit"}.issubset(channels(http_address, topic)):
            return
        if not writer.conns:
            return
        state["published"] = True
        state["published_at"] = time.monotonic()
        writer.mpub(topic, [b"one", b"two", b"req", b"touch"])
        writer.dpub(topic, 300, b"later")

    expected = {"one", "two", "req", "touch", "later"}

    def finish_when_complete():
        if expected.issubset(state["primary"]) and expected.issubset(state["audit"]):
            loop.stop()

    def timeout():
        state["error"] = "behavior matrix timed out"
        loop.stop()

    publish_poller = PeriodicCallback(publish_when_ready, 50)
    finish_poller = PeriodicCallback(finish_when_complete, 50)
    publish_poller.start()
    finish_poller.start()
    timeout_handle = loop.call_later(25, timeout)
    loop.start()
    loop.remove_timeout(timeout_handle)
    publish_poller.stop()
    finish_poller.stop()
    primary.close()
    audit.close()
    loop.call_later(0.3, loop.stop)
    loop.start()

    if state["error"]:
        raise RuntimeError(state["error"])
    if state["req_attempts"] < 2:
        raise RuntimeError("REQ did not redeliver with a higher attempt count")
    if (
        state["deferred_at"] is None
        or state["published_at"] is None
        or state["deferred_at"] - state["published_at"] < 0.25
    ):
        raise RuntimeError("DPUB was delivered before its delay")
    return writer


def run_sampling(tcp_address, http_address, options, writer):
    loop = IOLoop.current()
    topic = f"compat_python_sample_{time.time_ns()}"
    create_topic(http_address, topic)
    state = {"published": False, "count": 0, "error": None}

    def handler(_message):
        state["count"] += 1
        if state["count"] == 50:
            loop.call_later(0.3, loop.stop)
        return True

    sample_options = dict(options)
    sample_options.update(sample_rate=50, max_in_flight=100)
    reader = nsq.Reader(
        topic=topic,
        channel="workers",
        message_handler=handler,
        nsqd_tcp_addresses=[tcp_address],
        **sample_options,
    )

    def publish_when_ready():
        if state["published"] or "workers" not in channels(http_address, topic):
            return
        state["published"] = True
        writer.mpub(topic, [f"sample-{index:03d}".encode() for index in range(100)])

    def timeout():
        state["error"] = f"sample delivered {state['count']} messages"
        loop.stop()

    poller = PeriodicCallback(publish_when_ready, 50)
    poller.start()
    timeout_handle = loop.call_later(15, timeout)
    loop.start()
    loop.remove_timeout(timeout_handle)
    poller.stop()
    reader.close()
    if state["error"]:
        raise RuntimeError(state["error"])
    if state["count"] != 50:
        raise RuntimeError(f"sample delivered {state['count']} messages, expected 50")


def run_ephemeral(tcp_address, http_address, options, writer):
    loop = IOLoop.current()
    topic = f"compat_python_ephemeral_{time.time_ns()}"
    channel = "temporary#ephemeral"
    create_topic(http_address, topic)
    first = {"published": False, "received": False, "error": None}

    def first_handler(message):
        first["received"] = message.body == b"first"
        loop.stop()
        return True

    reader = nsq.Reader(
        topic=topic,
        channel=channel,
        message_handler=first_handler,
        nsqd_tcp_addresses=[tcp_address],
        **options,
    )

    def publish_first():
        if first["published"] or channel not in channels(http_address, topic):
            return
        first["published"] = True
        writer.pub(topic, b"first")

    poller = PeriodicCallback(publish_first, 50)
    poller.start()
    timeout_handle = loop.call_later(10, loop.stop)
    loop.start()
    loop.remove_timeout(timeout_handle)
    poller.stop()
    reader.close()
    loop.call_later(0.5, loop.stop)
    loop.start()
    if not first["received"]:
        raise RuntimeError("ephemeral channel did not receive its live message")
    wait_channel(http_address, topic, channel, False)

    published = {"done": False, "error": None}

    def stale_published(_conn, data):
        published["done"] = True
        if isinstance(data, Exception):
            published["error"] = str(data)
        loop.stop()

    writer.pub(topic, b"stale", stale_published)
    timeout_handle = loop.call_later(10, loop.stop)
    loop.start()
    loop.remove_timeout(timeout_handle)
    if not published["done"] or published["error"]:
        raise RuntimeError(f"failed to publish stale marker: {published['error']}")

    second = {"armed": False, "received": None, "error": None}

    def second_handler(message):
        second["received"] = message.body
        if message.body == b"stale":
            second["error"] = "new ephemeral channel received stale data"
        loop.stop()
        return True

    reader = nsq.Reader(
        topic=topic,
        channel=channel,
        message_handler=second_handler,
        nsqd_tcp_addresses=[tcp_address],
        **options,
    )

    def arm_second():
        if second["armed"] or channel not in channels(http_address, topic):
            return
        second["armed"] = True
        loop.call_later(0.4, writer.pub, topic, b"fresh")

    poller = PeriodicCallback(arm_second, 50)
    poller.start()
    timeout_handle = loop.call_later(10, loop.stop)
    loop.start()
    loop.remove_timeout(timeout_handle)
    poller.stop()
    reader.close()
    if second["error"] or second["received"] != b"fresh":
        raise RuntimeError(second["error"] or "ephemeral fresh message was not delivered")


def connection_options(mode, arguments):
    options = {
        "heartbeat_interval": 1,
        "output_buffer_size": 4096,
        "output_buffer_timeout": 100,
        "deflate": True,
    }
    if mode == "secure":
        if len(arguments) != 7:
            raise RuntimeError("secure mode requires CA, client certificate, and client key")
        options.update(
            tls_v1=True,
            tls_options={
                "ca_certs": arguments[4],
                "certfile": arguments[5],
                "keyfile": arguments[6],
                "cert_reqs": ssl.CERT_REQUIRED,
            },
            auth_secret="compat-secret",
        )
    return options


mode = sys.argv[1] if len(sys.argv) > 1 else "core"
tcp_address = sys.argv[2] if len(sys.argv) > 2 else "rustqueue-plain:4150"
http_address = sys.argv[3] if len(sys.argv) > 3 else "rustqueue-plain:4151"
options = connection_options(mode, sys.argv)
writer = run_behavior_matrix(tcp_address, http_address, options)
run_sampling(tcp_address, http_address, options, writer)
run_ephemeral(tcp_address, http_address, options, writer)
print(f"python {mode} compatibility matrix: ok")
