"""Read payments sequentially and confirm offsets only after PostgreSQL commits."""

import asyncio
import json
import time
from collections import deque
from collections.abc import Awaitable, Callable
from threading import Event
from uuid import uuid4

import psycopg
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from eventgate.contracts.payment import PaymentRequested, parse_requested
from eventgate.environment import remaining
from eventgate_demo.control import ControlPlane
from eventgate_demo.stable import process_stable

type OffsetCommits = deque[tuple[KafkaError | None, list[TopicPartition]]]
type PaymentHandler = Callable[
    [psycopg.AsyncConnection[tuple[object, ...]], PaymentRequested], Awaitable[None]
]


class OffsetCommitError(RuntimeError):
    """The exact processed offset could not be confirmed."""


def log_event(event: str, process_instance_id: str, **fields: object) -> None:
    print(
        json.dumps(
            {"event": event, "process_instance_id": process_instance_id, **fields},
            separators=(",", ":"),
        ),
        flush=True,
    )


def commit_message(
    consumer: Consumer,
    message: Message,
    callbacks: OffsetCommits,
    prefetched: deque[Message],
    deadline: float,
) -> None:
    offset = message.offset()
    if offset is None or offset < 0:
        raise OffsetCommitError("Invalid input offset")
    try:
        remaining(deadline)
        consumer.commit(message=message, asynchronous=True)
        while not callbacks:
            received = consumer.poll(min(0.1, remaining(deadline)))
            if received is not None:
                prefetched.append(received)
        remaining(deadline)
    except TimeoutError:
        raise OffsetCommitError(
            "Kafka offset commit did not finish before the operation deadline"
        ) from None

    error, positions = callbacks.popleft()
    if error or len(positions) != 1:
        raise OffsetCommitError("Kafka did not acknowledge the requested offset commit")
    position = positions[0]
    if (
        position.error
        or position.topic != message.topic()
        or position.partition != message.partition()
        or position.offset != offset + 1
    ):
        raise OffsetCommitError("Kafka acknowledged a different offset commit")


async def run_consumer(
    bootstrap: str,
    dsn: str,
    topic: str,
    group: str,
    operation_seconds: float,
    stop: Event,
    *,
    process_instance_id: str | None = None,
    handler: PaymentHandler = process_stable,
    control: ControlPlane | None = None,
) -> None:
    instance = process_instance_id or str(uuid4())
    commits: OffsetCommits = deque()
    prefetched: deque[Message] = deque()

    def on_commit(error: KafkaError | None, positions: list[TopicPartition]) -> None:
        commits.append((error, positions))

    def on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if partitions:
            log_event("worker_ready", instance, role="consumer")

    client = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 1000,
            "on_commit": on_commit,
            "max.poll.interval.ms": max(300000, int(operation_seconds * 2000)),
        }
    )
    try:
        connection = await asyncio.wait_for(
            psycopg.AsyncConnection.connect(dsn, autocommit=True), timeout=operation_seconds
        )
        async with connection:
            await asyncio.wait_for(connection.execute("SELECT 1"), timeout=operation_seconds)
            client.subscribe([topic], on_assign=on_assign)
            paused_partitions: set[tuple[str, int]] = set()
            while not stop.is_set():
                if control is not None:
                    assigned = client.assignment()
                    current = {(part.topic, part.partition) for part in assigned}
                    pause = control.pause_requested
                    if pause and current != paused_partitions:
                        if assigned:
                            client.pause(assigned)
                        paused_partitions = current
                    elif not pause and paused_partitions:
                        if assigned:
                            client.resume(assigned)
                        paused_partitions.clear()
                    await control.observe_boundary(
                        connection, paused=pause, assigned=bool(assigned)
                    )
                    if pause:
                        buffered = client.poll(0.1)
                        if buffered is not None:
                            if buffered.error():
                                raise RuntimeError(
                                    "Kafka poll failed while the consumer was paused"
                                )
                            prefetched.append(buffered)
                        continue
                received = prefetched.popleft() if prefetched else client.poll(0.1)
                if received is None:
                    continue
                if stop.is_set():
                    break
                if control is not None and control.pause_requested:
                    prefetched.appendleft(received)
                    continue
                error = received.error()
                if error:
                    raise RuntimeError(f"Kafka input read failed: {error.name()}")
                deadline = time.monotonic() + operation_seconds
                payload = received.value()
                if payload is None:
                    raise ValueError("Kafka input has no payload")
                request = parse_requested(payload, received.key())
                log_event(
                    "message_received",
                    instance,
                    topic=received.topic(),
                    partition=received.partition(),
                    offset=received.offset(),
                    event_id=request.event_id,
                )
                await asyncio.wait_for(handler(connection, request), timeout=remaining(deadline))
                log_event("database_committed", instance, event_id=request.event_id)
                if control is not None:
                    control.crash_if_armed(request.event_id)
                commit_message(client, received, commits, prefetched, deadline)
                log_event(
                    "offset_committed",
                    instance,
                    event_id=request.event_id,
                    offset=received.offset(),
                )
    finally:
        client.close()
