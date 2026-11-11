import socket

import psycopg
import pytest
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from eventgate.config import VerifyConfig
from eventgate.environment import EnvironmentError, EnvironmentHandle, EnvironmentManager


def test_environments_are_isolated_and_cleanup_leaves_neighbour_running(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
) -> None:
    manager, handles = environments
    first = manager.create(integration_config, "baseline", "normal")
    second = manager.create(integration_config, "candidate", "normal")
    for handle in (first, second):
        handles.append(handle)
        manager.start_infrastructure(handle)

    with psycopg.connect(first.postgres_dsn) as conn:
        conn.execute("CREATE TABLE isolation_marker (value int)")
        conn.execute("INSERT INTO isolation_marker VALUES (1)")
    with psycopg.connect(second.postgres_dsn) as conn:
        assert conn.execute("SELECT to_regclass('public.isolation_marker')").fetchone() == (None,)

    for handle in (first, second):
        with_topic = Consumer(
            {
                "bootstrap.servers": handle.kafka_bootstrap,
                "group.id": handle.consumer_group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        try:
            for topic in (
                integration_config.kafka.input_topic,
                integration_config.kafka.output_topic,
            ):
                partition = TopicPartition(topic, 0)
                assert with_topic.get_watermark_offsets(partition, timeout=5) == (0, 0)
                assert with_topic.committed([partition], timeout=5)[0].offset < 0
        finally:
            with_topic.close()

    delivery: list[KafkaError | None] = []

    def delivered(error: KafkaError | None, message: Message) -> None:
        delivery.append(error)

    producer = Producer({"bootstrap.servers": first.kafka_bootstrap, "acks": "all"})
    producer.produce(
        integration_config.kafka.input_topic,
        key=b"isolation-marker",
        value=b"first environment only",
        on_delivery=delivered,
    )
    assert producer.flush(5) == 0
    assert delivery == [None]
    partition = TopicPartition(integration_config.kafka.input_topic, 0)
    for handle, expected_end in ((first, 1), (second, 0)):
        observer = Consumer(
            {
                "bootstrap.servers": handle.kafka_bootstrap,
                "group.id": handle.project + "-isolation-check",
                "enable.auto.commit": False,
            }
        )
        try:
            assert observer.get_watermark_offsets(partition, timeout=5) == (0, expected_end)
        finally:
            observer.close()

    manager.destroy(first)
    manager.destroy(first)
    assert not any(manager.owned_resources(first).values())
    assert all(manager.owned_resources(second).values())
    with psycopg.connect(second.postgres_dsn) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)
    manager.destroy(second)


def test_retries_real_port_conflict(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)
    allocate = manager._allocate_ports
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        conflict = occupied.getsockname()[1]
        attempts = iter([(conflict, allocate()[1])])

        def ports() -> tuple[int, int]:
            return next(attempts, None) or allocate()

        monkeypatch.setattr(manager, "_allocate_ports", ports)
        manager.start_infrastructure(handle)
        assert handle.kafka_port != conflict
        assert handle.startup_diagnostics


def test_partial_start_can_be_cleaned(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)

    def reject(*args: object) -> None:
        raise EnvironmentError("Injected preparation failure after Compose startup")

    monkeypatch.setattr(manager, "_prepare_kafka", reject)
    with pytest.raises(EnvironmentError, match="Injected"):
        manager.start_infrastructure(handle)
    assert manager.collect_logs(handle)
    assert any(manager.owned_resources(handle).values())
    manager.destroy(handle)
    assert not any(manager.owned_resources(handle).values())
