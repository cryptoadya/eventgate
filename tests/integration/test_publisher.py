import asyncio
import threading
import time

import psycopg
import pytest
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ResourceType,
)

from eventgate.config import VerifyConfig
from eventgate.contracts.payment import PaymentRequested
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate.kafka_io import ConfirmedProducer, DeliveryUncertain, KafkaIO
from eventgate_demo import publisher
from eventgate_demo.db import init_db
from eventgate_demo.publisher import run_publisher
from eventgate_demo.stable import process_stable


def test_no_second_publish_when_first_callback_never_arrived() -> None:
    class DelayedProducer:
        count = 0

        def produce(self, *args: object, **kwargs: object) -> None:
            self.count += 1

        def poll(self, timeout: float) -> int:
            time.sleep(timeout)
            return 0

    client = DelayedProducer()
    sender = ConfirmedProducer(client)
    for _ in range(2):
        with pytest.raises(DeliveryUncertain):
            sender.send("test", b"key", b"payload", time.monotonic() + 0.05)
    assert client.count == 1


@pytest.mark.parametrize("error_kind", ["delivery", "mark"])
def test_publication_errors_leave_outbox_pending(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    error_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)
    manager.start_infrastructure(handle)
    with psycopg.connect(handle.postgres_dsn) as conn:
        init_db(conn)
        if error_kind == "mark":
            conn.execute("ALTER TABLE outbox ADD CHECK (published_at IS NULL)")

    output_topic = integration_config.kafka.output_topic
    if error_kind == "delivery":
        resource = ConfigResource(
            ResourceType.TOPIC,
            output_topic,
            incremental_configs=[
                ConfigEntry(
                    "max.message.bytes", "1024", incremental_operation=AlterConfigOpType.SET
                )
            ],
        )
        admin = AdminClient({"bootstrap.servers": handle.kafka_bootstrap})
        admin.incremental_alter_configs([resource], request_timeout=5)[resource].result(timeout=5)

    async def exercise() -> None:
        request = PaymentRequested(
            schema_version=1,
            event_id="evt-1001",
            order_id="order-1042",
            amount_minor=5000,
            currency="EUR",
        )
        async with await psycopg.AsyncConnection.connect(
            handle.postgres_dsn, autocommit=True
        ) as conn:
            await process_stable(conn, request)
        stop = threading.Event()
        if error_kind == "mark":
            with pytest.raises(psycopg.errors.CheckViolation):
                await asyncio.wait_for(
                    run_publisher(
                        handle.kafka_bootstrap,
                        handle.postgres_dsn,
                        integration_config.kafka.output_topic,
                        3,
                        stop,
                    ),
                    timeout=8,
                )
        else:
            async with await psycopg.AsyncConnection.connect(
                handle.postgres_dsn, autocommit=True
            ) as conn:
                await conn.execute(
                    "UPDATE outbox SET payload = payload || jsonb_build_object('note', repeat('x', 8192))"
                )
            rejected: list[str] = []

            def log(event: str, process_instance_id: str, **fields: object) -> None:
                if event == "delivery_failed":
                    rejected.append(str(fields["reason"]))
                    stop.set()

            monkeypatch.setattr(publisher, "log_event", log)
            await asyncio.wait_for(
                run_publisher(handle.kafka_bootstrap, handle.postgres_dsn, output_topic, 5, stop),
                timeout=10,
            )
            assert len(rejected) == 1
            assert "MSG_SIZE_TOO_LARGE" in rejected[0]

    asyncio.run(exercise())
    with psycopg.connect(handle.postgres_dsn) as conn:
        assert conn.execute("SELECT published_at FROM outbox").fetchall() == [(None,)]
    kafka = KafkaIO(handle.kafka_bootstrap)
    assert kafka.end_offsets(integration_config.kafka.output_topic, time.monotonic() + 5) == {
        0: 1 if error_kind == "mark" else 0
    }
