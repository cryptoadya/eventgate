"""HTTP test control; only worker boundaries acknowledge a pause or resume."""

import asyncio
import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer
from threading import Condition, Thread
from typing import Literal

import psycopg
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from eventgate.contracts.control import ArmRequest, Dependencies, WorkerState, WorkerStatus
from eventgate.contracts.payment import parse_json_object
from eventgate_demo.failpoints import FailpointConflict, FailpointStore


class TransitionUnavailable(RuntimeError):
    """A safe transition could not be confirmed within the control budget."""


class ControlPlane:
    def __init__(
        self, store: FailpointStore, bootstrap: str, topic: str, operation_seconds: float
    ) -> None:
        self.store = store
        self._admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})
        self._topic = topic
        self._transition_timeout = 1.9 * operation_seconds
        self._probe_seconds = min(0.5, operation_seconds / 3)
        self._condition = Condition()
        self._desired_pause = False
        self._transitioning = False
        self._state: WorkerState = "starting"
        self._dependencies = Dependencies(kafka=False, postgres=False)
        self._next_probe = 0.0

    @property
    def pause_requested(self) -> bool:
        with self._condition:
            return self._desired_pause

    def _status_locked(self) -> WorkerStatus:
        return WorkerStatus(
            contract_version="payment-v1",
            role=self.store.role,
            state=self._state,
            run_id=self.store.run_id,
            scenario_id=self.store.scenario_id,
            process_instance_id=self.store.instance,
            dependencies=self._dependencies,
        )

    def status(self) -> WorkerStatus:
        with self._condition:
            return self._status_locked()

    def transition(self, paused: bool) -> WorkerStatus:
        wanted: WorkerState = "paused" if paused else "ready"
        with self._condition:
            if self._state in ("starting", "error") or self._transitioning:
                raise FailpointConflict("Worker cannot accept this transition")
            self._transitioning = True
            self._desired_pause = paused
            try:
                completed = self._condition.wait_for(
                    lambda: self._state in (wanted, "error"), timeout=self._transition_timeout
                )
                if not completed or self._state != wanted:
                    raise TransitionUnavailable("Worker did not reach the requested safe state")
                return self._status_locked()
            finally:
                self._transitioning = False

    async def observe_boundary(
        self,
        connection: psycopg.AsyncConnection[tuple[object, ...]],
        *,
        paused: bool,
        assigned: bool = True,
    ) -> None:
        wanted: Literal["paused", "ready"] = "paused" if paused else "ready"
        with self._condition:
            due = time.monotonic() >= self._next_probe or self._state != wanted
            if not assigned:
                self._state = "starting"
                self._dependencies = Dependencies(kafka=False, postgres=False)
                return
            if not due:
                return
        kafka_ok = postgres_ok = False
        try:
            metadata = self._admin.list_topics(timeout=self._probe_seconds)
            topic = metadata.topics.get(self._topic)
            kafka_ok = bool(
                topic
                and not topic.error
                and len(topic.partitions) == 1
                and topic.partitions[0].leader >= 0
            )
        except KafkaException:
            pass
        try:
            cursor = await asyncio.wait_for(connection.execute("SELECT 1"), self._probe_seconds)
            postgres_ok = await asyncio.wait_for(cursor.fetchone(), self._probe_seconds) == (1,)
        except (psycopg.Error, TimeoutError):
            pass
        with self._condition:
            self._dependencies = Dependencies(kafka=kafka_ok, postgres=postgres_ok)
            self._state = wanted if kafka_ok and postgres_ok else "starting"
            self._next_probe = time.monotonic() + 1
            self._condition.notify_all()

    def stopping(self) -> None:
        with self._condition:
            self._state = "error"
            self._dependencies = Dependencies(kafka=False, postgres=False)
            self._condition.notify_all()

    def crash_if_armed(self, target_id: str) -> None:
        receipt = self.store.fire(target_id)
        if receipt is not None:
            print(
                json.dumps(
                    {"event": "failpoint_fired", **receipt.model_dump(mode="json")},
                    separators=(",", ":"),
                ),
                flush=True,
            )
            os._exit(86)


class ControlServer(ThreadingHTTPServer):
    def server_bind(self) -> None:
        # HTTPServer otherwise performs a hostname lookup while binding the local socket.
        TCPServer.server_bind(self)
        self.server_name = "eventgate-control"
        self.server_port = self.server_address[1]


@contextmanager
def serve_control(plane: ControlPlane, port: int = 8080) -> Iterator[int]:
    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(plane._transition_timeout)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _reply(self, status: int, body: dict[str, object]) -> None:
            payload = json.dumps(body, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except OSError:
                pass

        def do_GET(self) -> None:
            if self.path == "/test/status":
                self._reply(200, plane.status().model_dump(mode="json"))
            else:
                self._reply(404, {"error": "Unknown test endpoint"})

        def do_POST(self) -> None:
            if self.path not in ("/test/pause", "/test/resume", "/test/failpoints/arm"):
                self._reply(404, {"error": "Unknown test endpoint"})
                return
            result: dict[str, object]
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 8192:
                    raise ValueError("Invalid request size")
                body = parse_json_object(self.rfile.read(size))
                if self.path == "/test/failpoints/arm":
                    receipt = plane.store.arm(ArmRequest.model_validate(body))
                    result = {"token": receipt.token, "state": receipt.state}
                else:
                    if body:
                        raise ValueError("Expected an empty JSON object")
                    state = plane.transition(self.path == "/test/pause")
                    result = state.model_dump(mode="json")
            except FailpointConflict as error:
                self._reply(409, {"error": str(error)})
            except (TransitionUnavailable, OSError) as error:
                self._reply(503, {"error": type(error).__name__})
            except ValueError:
                self._reply(400, {"error": "Invalid test request"})
            else:
                self._reply(200, result)

    with ControlServer(("0.0.0.0", port), Handler) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
        thread.start()
        try:
            yield server.server_port
        finally:
            plane.stopping()
            server.shutdown()
            thread.join()
