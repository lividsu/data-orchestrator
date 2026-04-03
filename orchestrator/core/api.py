from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from threading import Thread
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urlparse


class OrchestratorApiServer:
    def __init__(self, orchestrator: Any, host: str = "127.0.0.1", port: int = 8765) -> None:
        self._orchestrator = orchestrator
        self._host = host
        self._port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}"

    def start(self) -> None:
        if self._server is not None:
            return
        orchestrator = self._orchestrator

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._ok({"status": "ok"})
                    return
                if parsed.path == "/pipelines":
                    self._ok(orchestrator.list_pipelines())
                    return
                if parsed.path.startswith("/pipeline/"):
                    pipeline_id = parsed.path[len("/pipeline/") :]
                    pipeline = orchestrator.get_pipeline(pipeline_id)
                    if pipeline is None:
                        self._not_found("pipeline not found")
                        return
                    self._ok(pipeline.model_dump())
                    return
                if parsed.path == "/upcoming":
                    query = parse_qs(parsed.query)
                    hours = int((query.get("hours") or ["2"])[0])
                    self._ok(orchestrator.get_upcoming_runs(hours=hours))
                    return
                if parsed.path.startswith("/live-output/"):
                    run_id = parsed.path[len("/live-output/") :]
                    self._ok(orchestrator.get_live_output(run_id))
                    return
                self._not_found("route not found")

            def do_POST(self):
                if self.path.startswith("/trigger/"):
                    pipeline_id = self.path[len("/trigger/") :]
                    if orchestrator.get_pipeline(pipeline_id) is None:
                        self._not_found("pipeline not found")
                        return
                    content_length = int(self.headers.get("Content-Length", 0))
                    runtime_kwargs = None
                    if content_length > 0:
                        try:
                            body = self.rfile.read(content_length).decode("utf-8")
                            payload = json.loads(body)
                            runtime_kwargs = payload.get("runtime_kwargs")
                        except Exception:
                            pass
                    run_id = orchestrator.trigger_async(pipeline_id, runtime_kwargs=runtime_kwargs)
                    self._ok({"run_id": run_id})
                    return
                if self.path.startswith("/pause/"):
                    pipeline_id = self.path[len("/pause/") :]
                    orchestrator.pause(pipeline_id)
                    self._ok({"status": "paused"})
                    return
                if self.path.startswith("/resume/"):
                    pipeline_id = self.path[len("/resume/") :]
                    orchestrator.resume(pipeline_id)
                    self._ok({"status": "resumed"})
                    return
                self._not_found("route not found")

            def _ok(self, payload: Any):
                body = json.dumps(payload, default=str).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _not_found(self, message: str):
                body = json.dumps({"error": message}).encode("utf-8")
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt: str, *args: Any):
                return

        class ReusableThreadingHTTPServer(ThreadingHTTPServer):
            allow_reuse_address = True

        self._server = ReusableThreadingHTTPServer((self._host, self._port), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=1)
        self._server = None
        self._thread = None
