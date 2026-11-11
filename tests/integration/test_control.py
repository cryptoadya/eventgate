import asyncio
import http.client
import threading
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.contracts.control import ArmRequest
from eventgate.contracts.payment import PaymentRequested, make_completion
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate.kafka_io import ConfirmedProducer, InputReceipt, KafkaIO, OutboundMessage
from eventgate_demo.cli import _control_plane
from eventgate_demo.control import ControlPlane, serve_control
from eventgate_demo.failpoints import FailpointStore
from eventgate_demo.publisher import run_publisher

REQUEST = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)


def test_pause_restart_and_durable_failpoint(
    running_payment: tuple[EnvironmentManager, EnvironmentHandle],
) -> None:
    manager, handle = running_payment
    adapter = PaymentV1Adapter(manager)
    deadline = time.monotonic() + 30
    start = adapter.wait_ready(handle, "consumer", deadline)
    adapter.pause(handle, "publisher", deadline)
    assert adapter.pause(handle, "consumer", deadline).state == "paused"
    command = ArmRequest(
        token=str(uuid4()), name="after_db_commit_before_offset_commit", target_id=REQUEST.event_id
    )
    assert adapter.arm(handle, "consumer", command, deadline).state == "armed"
    kafka = KafkaIO(handle.kafka_bootstrap)
    kafka.send_confirmed(
        handle.config.kafka.input_topic,
        [OutboundMessage(REQUEST.event_id.encode(), REQUEST.model_dump_json().encode())],
        deadline,
    )
    assert adapter.status(handle, "consumer", deadline).state == "paused"
    with psycopg.connect(handle.postgres_dsn) as conn:
        assert conn.execute("SELECT count(*) FROM payments").fetchone() == (0,)
    assert adapter.resume(handle, "consumer", deadline).state == "ready"

    while time.monotonic() < deadline:
        state = manager.role_state(handle, "consumer", deadline)
        if state.exit_code == 86 and not state.running:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("The armed consumer did not exit with code 86")

    receipt = adapter.read_receipt(handle, "consumer", deadline)
    assert (receipt.token, receipt.state, receipt.process_instance_id) == (
        command.token,
        "fired",
        start.process_instance_id,
    )
    manager.restart_role(handle, "consumer", deadline)
    current = adapter.wait_ready(handle, "consumer", deadline)
    assert current.process_instance_id != start.process_instance_id
    assert adapter.arm(handle, "consumer", command, deadline).state == "fired"
    assert adapter.resume(handle, "publisher", deadline).state == "ready"
    assert manager.role_state(handle, "consumer", deadline).running


def test_http_pause_cannot_acknowledge_an_inflight_publish(
    running_payment: tuple[EnvironmentManager, EnvironmentHandle],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manager, handle = running_payment
    adapter = PaymentV1Adapter(manager)
    deadline = time.monotonic() + 15
    adapter.pause(handle, "publisher", deadline)
    with psycopg.connect(handle.postgres_dsn) as conn:
        conn.execute("INSERT INTO processed_events (event_id) VALUES (%s)", (REQUEST.event_id,))
        completion = make_completion(REQUEST)
        conn.execute(
            "INSERT INTO outbox (completion_id, source_event_id, payload) VALUES (%s, %s, %s::jsonb)",
            (completion.completion_id, REQUEST.event_id, completion.model_dump_json()),
        )
    reached_send = threading.Event()
    allow_delivery = threading.Event()
    stop = threading.Event()
    instance = str(uuid4())
    plane = ControlPlane(
        FailpointStore(
            tmp_path / "failpoint.json", handle.run_id, handle.scenario_id, "publisher", instance
        ),
        handle.kafka_bootstrap,
        handle.config.kafka.output_topic,
        0.6,
    )
    real_send = ConfirmedProducer.send

    def delayed_send(
        self: ConfirmedProducer, topic: str, key: bytes, payload: bytes, deadline: float
    ) -> InputReceipt:
        reached_send.set()
        assert allow_delivery.wait(4)
        # The test releases the delayed call after inspecting the HTTP state.
        return real_send(self, topic, key, payload, deadline)

    monkeypatch.setattr(ConfirmedProducer, "send", delayed_send)
    errors: list[BaseException] = []

    def work() -> None:
        try:
            asyncio.run(
                run_publisher(
                    handle.kafka_bootstrap,
                    handle.postgres_dsn,
                    handle.config.kafka.output_topic,
                    3,
                    stop,
                    control=plane,
                    process_instance_id=instance,
                )
            )
        except BaseException as error:
            errors.append(error)

    with serve_control(plane, port=0) as port:
        thread = threading.Thread(target=work)
        thread.start()
        try:
            assert reached_send.wait(5)
            client = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                client.request("POST", "/test/pause", body=b"{}")
                response = client.getresponse()
                assert response.status == 503
                assert b"TransitionUnavailable" in response.read()
            finally:
                client.close()
            assert plane.pause_requested
            assert plane.status().state == "ready"
            with psycopg.connect(handle.postgres_dsn) as conn:
                assert conn.execute("SELECT published_at FROM outbox").fetchone() == (None,)
            allow_delivery.set()
            while time.monotonic() < deadline and plane.status().state != "paused":
                assert not errors, repr(errors)
                time.sleep(0.05)
            assert plane.status().state == "paused", (plane.status(), plane.pause_requested, errors)
            with psycopg.connect(handle.postgres_dsn) as conn:
                row = conn.execute("SELECT published_at FROM outbox").fetchone()
                assert row is not None and row[0] is not None
        finally:
            allow_delivery.set()
            stop.set()
            thread.join(timeout=6)
        assert not thread.is_alive() and not errors


def test_control_is_disabled_outside_test_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVENTGATE_TEST_MODE", "0")
    monkeypatch.delenv("EVENTGATE_CONTROL_DIR", raising=False)
    assert _control_plane("publisher", str(uuid4()), "unused", "unused", 1) is None
