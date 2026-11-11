"""Publish pending outbox messages and mark them only after Kafka acknowledges delivery."""

import asyncio
import json
import time
from threading import Event
from uuid import uuid4

import psycopg

from eventgate.environment import remaining
from eventgate.kafka_io import ConfirmedProducer, DeliveryFailed, KafkaIO
from eventgate_demo.consumer import log_event
from eventgate_demo.control import ControlPlane


async def run_publisher(
    bootstrap: str,
    dsn: str,
    topic: str,
    operation_seconds: float,
    stop: Event,
    *,
    process_instance_id: str | None = None,
    control: ControlPlane | None = None,
) -> None:
    instance = process_instance_id or str(uuid4())
    sender = ConfirmedProducer.open(bootstrap, operation_seconds)
    connection = await asyncio.wait_for(
        psycopg.AsyncConnection.connect(dsn, autocommit=True), timeout=operation_seconds
    )
    async with connection:
        await asyncio.wait_for(connection.execute("SELECT 1"), timeout=operation_seconds)
        KafkaIO(bootstrap).end_offsets(topic, time.monotonic() + operation_seconds)
        log_event("worker_ready", instance, role="publisher")
        backoff = 0.1
        while not stop.is_set():
            if control is not None:
                paused = control.pause_requested
                await control.observe_boundary(connection, paused=paused)
                if paused:
                    await asyncio.sleep(0.05)
                    continue
            deadline = time.monotonic() + operation_seconds
            cursor = await asyncio.wait_for(
                connection.execute(
                    "SELECT completion_id, payload FROM public.outbox "
                    "WHERE published_at IS NULL ORDER BY completion_id LIMIT 1"
                ),
                timeout=remaining(deadline),
            )
            row = await asyncio.wait_for(cursor.fetchone(), timeout=remaining(deadline))
            if row is None:
                await asyncio.sleep(0.1)
                continue
            completion_id, payload = row
            wire = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
            try:
                receipt = sender.send(topic, completion_id.encode(), wire, deadline)
            except DeliveryFailed as error:
                log_event(
                    "delivery_failed", instance, completion_id=completion_id, reason=str(error)
                )
                await asyncio.sleep(backoff)
                backoff = min(2 * backoff, 1)
                continue
            log_event(
                "publish_acknowledged", instance, completion_id=completion_id, offset=receipt.offset
            )
            if control is not None:
                control.crash_if_armed(completion_id)
            cursor = await asyncio.wait_for(
                connection.execute(
                    "UPDATE public.outbox SET published_at = clock_timestamp() "
                    "WHERE completion_id = %s AND published_at IS NULL",
                    (completion_id,),
                ),
                timeout=remaining(deadline),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("The acknowledged outbox row could not be marked")
            log_event("outbox_marked", instance, completion_id=completion_id)
            backoff = 0.1
