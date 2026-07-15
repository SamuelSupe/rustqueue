import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        request = urlparse(self.path)
        if request.path == "/health":
            self.respond(200, {"status": "ok"})
            return
        if request.path != "/auth":
            self.respond(404, {"message": "not found"})
            return
        query = parse_qs(request.query)
        valid = (
            query.get("auth_secret") == ["compat-secret"]
            and query.get("tls") == ["true"]
            and query.get("common_name") == ["rustqueue-1"]
        )
        if not valid:
            self.respond(403, {"message": "denied"})
            return
        self.respond(
            200,
            {
                "ttl": 1,
                "identity": "compat-client",
                "identity_url": "https://example.invalid/compat-client",
                "authorizations": [
                    {
                        "permissions": ["publish", "subscribe"],
                        "topic": ".*",
                        "channels": [".*"],
                    }
                ],
            },
        )

    def respond(self, status, body):
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, _format, *_args):
        pass


ThreadingHTTPServer(("0.0.0.0", 4181), Handler).serve_forever()
