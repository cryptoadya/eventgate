import asyncio
import threading
import time

import psycopg
import pytest
from confluent_kafka import TopicPartition

from eventgate.config import VerifyConfig
from eventgate.contracts.payment import PaymentRequested, make_completion
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate.kafka_io import KafkaIO, OutboundMessage
from eventgate_demo import consumer
from eventgate_demo.db import init_db

REQUEST_A = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)
REQUEST_B = PaymentRequested(
    schema_version=1, event_id="evt-1002", order_id="order-1043", amount_minor=7500, currency="EUR"
)


def outbound(request: PaymentRequested) -> OutboundMessage:
    return OutboundMessage(request.event_id.encode(), request.model_dump_json().encode())


def test_container_delivery_is_confirmed_and_marked(
    running_payment: tuple[EnvironmentManager, EnvironmentHandle],
) -> None:
    manager, handle = running_payment
    kafka = KafkaIO(handle.kafka_bootstrap)
    topics = handle.config.kafka
    deadline = time.monotonic() + 20
    [sent] = kafka.send_confirmed(topics.input_topic, [outbound(REQUEST_A)], deadline)
    assert sent.offset == 0
    while time.monotonic() < deadline:
        offsets = kafka.committed_offsets(handle.consumer_group, topics.input_topic, deadline)
        with psycopg.connect(handle.postgres_dsn) as conn:
            row = conn.execute("SELECT completion_id, payload, published_at FROM outbox").fetchone()
        if offsets == {0: sent.offset + 1} and row and row[2] is not None:
            break
        time.sleep(0.15)
    else:
        pytest.fail("The input was not committed and published within the test deadline")

    expected = make_completion(REQUEST_A)
    records = kafka.read_until(topics.output_topic, {0: 1}, deadline)
    assert len(records) == 1
    assert records[0].key == expected.completion_id.encode()
    from eventgate.contracts.payment import parse_completed

    assert parse_completed(records[0].payload or b"", records[0].key) == expected
    assert row[:2] == (expected.completion_id, expected.model_dump())
    assert '"event":"message_received"' in manager.collect_logs(handle)


@pytest.mark.parametrize("error_kind", ["db", "offset_commit"])
def test_consumer_does_not_advance_past_failed_input(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    monkeypatch: pytest.MonkeyPatch,
    error_kind: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)
    manager.start_infrastructure(handle)
    with psycopg.connect(handle.postgres_dsn) as conn:
        init_db(conn)
        if error_kind == "db":
            conn.execute("ALTER TABLE payments ADD CHECK (event_id <> 'evt-1001')")

    kafka = KafkaIO(handle.kafka_bootstrap)
    topic = integration_config.kafka.input_topic
    deadline = time.monotonic() + 20
    sent = kafka.send_confirmed(topic, [outbound(REQUEST_A), outbound(REQUEST_B)], deadline)
    assert [item.offset for item in sent] == [0, 1]

    def fail_commit(*args: object) -> None:
        raise consumer.OffsetCommitError("Injected failure at the Kafka offset commit boundary")

    if error_kind == "offset_commit":
        monkeypatch.setattr(consumer, "commit_message", fail_commit)
        expected_error: type[Exception] = consumer.OffsetCommitError
    else:
        expected_error = psycopg.errors.CheckViolation

    with pytest.raises(expected_error):
        asyncio.run(
            asyncio.wait_for(
                consumer.run_consumer(
                    handle.kafka_bootstrap,
                    handle.postgres_dsn,
                    topic,
                    handle.consumer_group,
                    5,
                    threading.Event(),
                    process_instance_id="test-consumer",
                ),
                timeout=12,
            )
        )
    assert kafka.committed_offsets(handle.consumer_group, topic, deadline) == {0: None}
    with psycopg.connect(handle.postgres_dsn) as conn:
        rows = conn.execute("SELECT event_id FROM payments ORDER BY payment_id").fetchall()
    assert rows == ([] if error_kind == "db" else [(REQUEST_A.event_id,)])
    assert '"event_id":"evt-1002"' not in capsys.readouterr().out


def test_offset_callback_wait_keeps_prefetched_input() -> None:
    from collections import deque

    from confluent_kafka import Message

    from eventgate_demo.consumer import OffsetCommits, commit_message

    callbacks: OffsetCommits = deque()
    pending: deque[Message] = deque()

    class Record:
        def topic(self) -> str:
            return "test.request"

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return 0

    first = Record()
    second = Record()

    class Client:
        count = 0

        def commit(self, *, message: Record, asynchronous: bool) -> None:
            assert message is first and asynchronous

        def poll(self, timeout: float) -> Record | None:
            assert timeout > 0
            self.count += 1
            if self.count == 1:
                return second
            callbacks.append((None, [TopicPartition("test.request", 0, 1)]))
            return None

    commit_message(Client(), first, callbacks, pending, time.monotonic() + 1)
    assert list(pending) == [second]
