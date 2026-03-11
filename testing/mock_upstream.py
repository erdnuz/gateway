#!/usr/bin/env python3
"""
mock_upstream.py – Fake upstream service for gate simulation.

Responds to any HTTP method with a random JSON payload after a short
simulated latency (10-50ms). Useful for producing 200 OK responses so the
edge gateway can cache and serve subsequent requests from its Redis store.

Usage:
    python3 testing/mock_upstream.py [PORT]

Default port: 9000
"""

import http.server
import json
import os
import random
import socketserver
import sys
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("MOCK_PORT", "9000"))

_MESSAGES = [
    "access granted",
    "auth success",
    "user verified",
    "token valid",
    "session ok",
    "request permitted",
    "rate limit ok",
    "quota available",
    "service ready",
    "operation complete",
]

_USERS = ["alice", "bob", "carol", "dave"]


class FakeUpstreamHandler(http.server.BaseHTTPRequestHandler):
    def _handle(self):
        # Simulate upstream latency: 10-50 ms
        time.sleep(random.uniform(0.01, 0.05))

        body = json.dumps({
            "message": random.choice(_MESSAGES),
            "user": random.choice(_USERS),
            "path": self.path,
            "method": self.command,
            "ts": time.time(),
        }).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_DELETE = _handle
    do_PATCH = _handle
    do_HEAD = _handle

    def log_message(self, fmt, *args):  # noqa: ANN001
        # Print a concise single-line log to stdout.
        print(f"upstream  {self.command:<7} {self.path}  →  {args[1] if len(args) > 1 else '?'}", flush=True)


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    with ReusableTCPServer(("0.0.0.0", PORT), FakeUpstreamHandler) as httpd:
        print(f"mock upstream listening on 0.0.0.0:{PORT}", flush=True)
        httpd.serve_forever()
