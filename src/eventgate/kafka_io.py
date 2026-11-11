"""Bounded, independently observable Kafka operations for the single-partition contract."""

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from eventgate.environment import remaining


class DeliveryFailed(RuntimeError):
    """Kafka produced a terminal delivery error."""


class DeliveryUncertain(TimeoutError):
    """The outcome is unknown; do not publish another message on this producer."""


class ObservationError(RuntimeError):
    """The complete Kafka interval could not be read."""

    def __init__(self, message: str, records: list["KafkaRecord"] | None = None) -> None:
        self.partial_records = records or []
        super().__init__(message)


class ObservationTimeout(TimeoutError):
    def __init__(self, records: list["KafkaRecord"]) -> None:
        self.partial_records = records
        super().__init__("Kafka observation did not finish before the deadline")


@dataclass(frozen=True)
class OutboundMessage:
    key: bytes
    payload: bytes


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    payload: bytes | None
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class InputReceipt:
    topic: str
    partition: int
    offset: int
    key: bytes
    payload: bytes


class ConfirmedProducer:
    def __init__(self, client: Producer) -> None:
        self._client = client
        self._pending = False

    @classmethod
    def open(cls, bootstrap: str, operation_seconds: float) -> "ConfirmedProducer":
        return cls(
            Producer(
                {
                    "bootstrap.servers": bootstrap,
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 0,
                    "delivery.timeout.ms": max(1, int(operation_seconds * 1000)),
                }
            )
        )

    def send(self, topic: str, key: bytes, payload: bytes, deadline: float) -> InputReceipt:
        if self._pending:
            raise DeliveryUncertain("The previous Kafka delivery is still unresolved")
        remaining(deadline)
        result: list[tuple[KafkaError | None, Message]] = []

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            result.append((error, message))
            self._pending = False

        self._client.produce(topic, value=payload, key=key, partition=0, on_delivery=on_delivery)
        self._pending = True
        while not result:
            try:
                self._client.poll(min(remaining(deadline), 0.1))
            except TimeoutError:
                raise DeliveryUncertain(
                    "Kafka delivery callback did not arrive before the deadline"
                ) from None
        error, message = result[0]
        if error is not None:
            raise DeliveryFailed(f"Kafka rejected delivery: {error.name()}")
        try:
            remaining(deadline)
        except TimeoutError:
            raise DeliveryUncertain(
                "Kafka delivery was not confirmed within the deadline"
            ) from None
        offset = message.offset()
        if message.topic() != topic or message.partition() != 0 or offset is None or offset < 0:
            raise DeliveryUncertain("Kafka delivery callback did not identify the expected record")
        return InputReceipt(topic, 0, offset, key, payload)


class KafkaIO:
    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap

    def send_confirmed(
        self, topic: str, messages: Iterable[OutboundMessage], deadline: float
    ) -> list[InputReceipt]:
        producer = ConfirmedProducer.open(self.bootstrap, remaining(deadline))
        return [
            producer.send(topic, message.key, message.payload, deadline) for message in messages
        ]

    @contextmanager
    def _observer(self, group_id: str | None = None) -> Iterator[Consumer]:
        client = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": group_id or f"eventgate-observer-{uuid4().hex}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "allow.auto.create.topics": False,
                "auto.offset.reset": "error",
            }
        )
        try:
            yield client
        finally:
            client.close()

    def committed_offsets(
        self, group_id: str, topic: str, deadline: float
    ) -> dict[int, int | None]:
        # A query for a group's commit does not subscribe or join that group.
        with self._observer(group_id) as client:
            [position] = client.committed([TopicPartition(topic, 0)], timeout=remaining(deadline))
        if position.error:
            raise ObservationError(f"Cannot read committed Kafka offset: {position.error.name()}")
        return {0: position.offset if position.offset >= 0 else None}

    def end_offsets(self, topic: str, deadline: float) -> dict[int, int]:
        with self._observer() as client:
            offsets = client.get_watermark_offsets(
                TopicPartition(topic, 0), timeout=remaining(deadline)
            )
        if offsets is None or offsets[0] != 0:
            raise ObservationError("Cannot observe a complete Kafka topic starting at offset 0")
        return {0: offsets[1]}

    def read_until(
        self, topic: str, end_offsets: dict[int, int], deadline: float
    ) -> list[KafkaRecord]:
        if set(end_offsets) != {0} or end_offsets[0] < 0:
            raise ValueError("Expected an exclusive end offset for partition 0")
        end = end_offsets[0]
        records: list[KafkaRecord] = []
        with self._observer() as client:
            offsets = client.get_watermark_offsets(
                TopicPartition(topic, 0), timeout=remaining(deadline)
            )
            if offsets is None or offsets[0] != 0 or offsets[1] < end:
                raise ObservationError("Kafka no longer contains the requested offset interval")
            client.assign([TopicPartition(topic, 0, 0)])
            while len(records) < end:
                try:
                    record = client.poll(min(0.25, remaining(deadline)))
                except TimeoutError:
                    raise ObservationTimeout(records) from None
                if record is None:
                    continue
                error = record.error()
                if error:
                    raise ObservationError(f"Cannot read Kafka record: {error.name()}", records)
                if (
                    record.topic() != topic
                    or record.partition() != 0
                    or record.offset() != len(records)
                ):
                    raise ObservationError(
                        "Kafka returned a gap or an unexpected record in the offset interval",
                        records,
                    )
                _, timestamp = record.timestamp()
                records.append(
                    KafkaRecord(topic, 0, len(records), record.key(), record.value(), timestamp)
                )
        return records
