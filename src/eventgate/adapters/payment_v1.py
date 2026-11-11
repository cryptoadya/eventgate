"""Manage the test protocol of a payment-v1 application."""

import asyncio
import http.client
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import psycopg
from pydantic import ValidationError

from eventgate.contracts.control import (
    ROLE_FAILPOINT,
    ArmRequest,
    ArmResponse,
    FailpointReceipt,
    Role,
    WorkerStatus,
)
from eventgate.contracts.payment import parse_completed, parse_json_object
from eventgate.environment import EnvironmentHandle, EnvironmentManager, remaining
from eventgate.kafka_io import (
    InputReceipt,
    KafkaIO,
    KafkaRecord,
    ObservationError,
    ObservationTimeout,
)
from eventgate.models import (
    DatabaseSnapshot,
    ObservedCompletion,
    OutboxRow,
    PaymentRow,
    ScenarioEvidence,
)


class ControlError(RuntimeError):
    """The application did not confirm a valid test protocol operation."""


class ControlUnavailable(ControlError):
    """The loopback test API could not be reached."""


class ProgressTimeout(TimeoutError):
    """Observed dependencies did not show the required input/outbox progress in time."""


class PaymentV1Adapter:
    def __init__(self, manager: EnvironmentManager) -> None:
        self.manager = manager

    @staticmethod
    def _request(
        handle: EnvironmentHandle,
        role: Role,
        path: str,
        deadline: float,
        body: bytes | None = None,
    ) -> bytes:
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            handle.control_ports[role],
            timeout=min(remaining(deadline), handle.config.timeouts.control_seconds),
        )
        try:
            connection.request(
                "GET" if body is None else "POST",
                path,
                body=body,
                headers={"Content-Type": "application/json"} if body is not None else {},
            )
            response = connection.getresponse()
            if response.status != 200:
                raise ControlError(f"{role} {path} returned HTTP {response.status}")
            data = response.read(16385)
            if len(data) > 16384:
                raise ControlError(f"{role} returned an oversized test response")
            return data
        except (OSError, http.client.HTTPException) as error:
            raise ControlUnavailable(
                f"Cannot reach {role} control API: {type(error).__name__}"
            ) from None
        finally:
            connection.close()

    @staticmethod
    def _status(handle: EnvironmentHandle, role: Role, payload: bytes) -> WorkerStatus:
        try:
            status = WorkerStatus.model_validate(parse_json_object(payload))
        except ValueError:
            raise ControlError(f"{role} returned an invalid payment-v1 status") from None
        if (status.run_id, status.scenario_id, status.role) != (
            handle.run_id,
            handle.scenario_id,
            role,
        ):
            raise ControlError(f"{role} status does not belong to the requested environment")
        return status

    def status(self, handle: EnvironmentHandle, role: Role, deadline: float) -> WorkerStatus:
        return self._status(handle, role, self._request(handle, role, "/test/status", deadline))

    def wait_ready(self, handle: EnvironmentHandle, role: Role, deadline: float) -> WorkerStatus:
        delay = 0.1
        while True:
            try:
                status = self.status(handle, role, deadline)
            except ControlUnavailable:
                pass
            else:
                if (
                    status.state == "ready"
                    and status.dependencies.kafka
                    and status.dependencies.postgres
                ):
                    return status
                if status.state == "error":
                    raise ControlError(f"{role} reported an error before readiness")
            if not self.manager.role_state(handle, role, deadline).running:
                raise ControlError(f"{role} stopped before readiness")
            time.sleep(min(delay, remaining(deadline)))
            delay = min(delay * 2, 1)

    def _transition(
        self,
        handle: EnvironmentHandle,
        role: Role,
        deadline: float,
        action: Literal["pause", "resume"],
        expected: Literal["paused", "ready"],
    ) -> WorkerStatus:
        status = self._status(
            handle, role, self._request(handle, role, f"/test/{action}", deadline, b"{}")
        )
        if status.state != expected or not (
            status.dependencies.kafka and status.dependencies.postgres
        ):
            raise ControlError(f"{role} did not confirm {expected} with working dependencies")
        return status

    def pause(self, handle: EnvironmentHandle, role: Role, deadline: float) -> WorkerStatus:
        return self._transition(handle, role, deadline, "pause", "paused")

    def resume(self, handle: EnvironmentHandle, role: Role, deadline: float) -> WorkerStatus:
        return self._transition(handle, role, deadline, "resume", "ready")

    def arm(
        self,
        handle: EnvironmentHandle,
        role: Role,
        command: ArmRequest,
        deadline: float,
    ) -> ArmResponse:
        payload = self._request(
            handle, role, "/test/failpoints/arm", deadline, command.model_dump_json().encode()
        )
        try:
            response = ArmResponse.model_validate(parse_json_object(payload))
        except ValueError:
            raise ControlError(f"{role} returned an invalid failpoint arm response") from None
        if response.token != command.token:
            raise ControlError(f"{role} acknowledged a different failpoint token")
        return response

    def read_receipt(
        self, handle: EnvironmentHandle, role: Role, deadline: float
    ) -> FailpointReceipt:
        container = self.manager.container_id(handle, role, deadline)
        with TemporaryDirectory(prefix="eventgate-receipt-") as work:
            path = Path(work) / "failpoint.json"
            self.manager._docker(
                "cp",
                f"{container}:/var/lib/eventgate-control/failpoint.json",
                str(path),
                deadline=deadline,
            )
            try:
                receipt = FailpointReceipt.model_validate(parse_json_object(path.read_bytes()))
            except (ValueError, OSError):
                raise ControlError(f"{role} did not leave a valid failpoint receipt") from None
        if (receipt.run_id, receipt.scenario_id, receipt.role, receipt.name) != (
            handle.run_id,
            handle.scenario_id,
            role,
            ROLE_FAILPOINT[role],
        ):
            raise ControlError(
                f"{role} failpoint receipt belongs to a different environment or stage"
            )
        return receipt

    @staticmethod
    def _control_deadline(handle: EnvironmentHandle, deadline: float) -> float:
        return min(deadline, time.monotonic() + handle.config.timeouts.control_seconds)

    def read_database(self, handle: EnvironmentHandle, deadline: float) -> DatabaseSnapshot:
        async def read() -> DatabaseSnapshot:
            async with asyncio.timeout(remaining(deadline)):
                async with await psycopg.AsyncConnection.connect(handle.postgres_dsn) as conn:
                    await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    healthy = await conn.execute("SELECT 1")
                    if await healthy.fetchone() != (1,):
                        raise ObservationError("PostgreSQL did not confirm the observer connection")
                    inbox = await conn.execute(
                        "SELECT event_id FROM public.processed_events ORDER BY event_id"
                    )
                    payments = await conn.execute(
                        "SELECT payment_id, event_id, order_id, amount_minor, currency "
                        "FROM public.payments ORDER BY payment_id"
                    )
                    outbox = await conn.execute(
                        "SELECT completion_id, source_event_id, payload, published_at "
                        "FROM public.outbox ORDER BY completion_id"
                    )
                    return DatabaseSnapshot(
                        [row[0] for row in await inbox.fetchall()],
                        [PaymentRow(*row) for row in await payments.fetchall()],
                        [OutboxRow(*row) for row in await outbox.fetchall()],
                    )

        return asyncio.run(read())

    def pending_outbox(
        self, handle: EnvironmentHandle, deadline: float, *, source_event_id: str | None = None
    ) -> bool:
        async def read() -> bool:
            async with asyncio.timeout(remaining(deadline)):
                async with await psycopg.AsyncConnection.connect(
                    handle.postgres_dsn, autocommit=True
                ) as conn:
                    cursor = await conn.execute(
                        "SELECT EXISTS (SELECT 1 FROM public.outbox "
                        "WHERE published_at IS NULL AND (%s::text IS NULL OR source_event_id = %s))",
                        (source_event_id, source_event_id),
                    )
                    result = await cursor.fetchone()
                    if result not in ((True,), (False,)):
                        raise ObservationError("Could not read pending outbox state")
                    return result == (True,)

        return asyncio.run(read())

    @staticmethod
    def offsets_cover(offsets: dict[int, int | None], receipts: list[InputReceipt]) -> bool:
        return bool(receipts) and all(
            (offset := offsets.get(receipt.partition)) is not None and offset >= receipt.offset + 1
            for receipt in receipts
        )

    def wait_progress(
        self,
        handle: EnvironmentHandle,
        receipts: list[InputReceipt],
        deadline: float,
        *,
        source_event_id: str | None = None,
    ) -> dict[int, int | None]:
        if not receipts:
            raise ValueError("Progress requires confirmed input")
        kafka = KafkaIO(handle.kafka_bootstrap)
        delay = 0.1
        while True:
            if time.monotonic() >= deadline:
                raise ProgressTimeout(
                    "Confirmed input offsets or outbox progress were not observed"
                )
            offsets = kafka.committed_offsets(
                handle.consumer_group,
                handle.config.kafka.input_topic,
                self._control_deadline(handle, deadline),
            )
            if self.offsets_cover(offsets, receipts) and not self.pending_outbox(
                handle,
                self._control_deadline(handle, deadline),
                source_event_id=source_event_id,
            ):
                return offsets
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(delay * 2, 1)

    @staticmethod
    def decode_records(records: list[KafkaRecord]) -> list[ObservedCompletion]:
        observed: list[ObservedCompletion] = []
        for record in records:
            try:
                if record.payload is None:
                    raise ValueError("Output has no payload")
                completion = parse_completed(record.payload, record.key)
            except (ValueError, RecursionError) as error:
                message = (
                    ", ".join(
                        ".".join(map(str, item["loc"])) or "payload"
                        for item in error.errors(include_input=False)
                    )
                    if isinstance(error, ValidationError)
                    else type(error).__name__
                )
                observed.append(ObservedCompletion(record, None, f"Invalid completion: {message}"))
            else:
                observed.append(ObservedCompletion(record, completion))
        return observed

    def collect_final(
        self, handle: EnvironmentHandle, evidence: ScenarioEvidence, deadline: float
    ) -> None:
        evidence.evidence_complete = False
        kafka = KafkaIO(handle.kafka_bootstrap)
        if not evidence.receipts:
            raise ObservationError("Cannot finalize a snapshot without confirmed inputs")
        for role in ("consumer", "publisher"):
            status = self.pause(handle, role, self._control_deadline(handle, deadline))
            evidence.paused_workers[role] = status
            evidence.note(
                "worker_paused", role=role, process_instance_id=status.process_instance_id
            )

        evidence.database = self.read_database(handle, self._control_deadline(handle, deadline))
        evidence.committed_offsets = kafka.committed_offsets(
            handle.consumer_group,
            handle.config.kafka.input_topic,
            self._control_deadline(handle, deadline),
        )
        evidence.output_end_offsets = kafka.end_offsets(
            handle.config.kafka.output_topic, self._control_deadline(handle, deadline)
        )
        try:
            records = kafka.read_until(
                handle.config.kafka.output_topic, evidence.output_end_offsets, deadline
            )
        except (ObservationError, ObservationTimeout) as error:
            if error.partial_records:
                evidence.output_records = self.decode_records(error.partial_records)
            raise
        evidence.output_records = self.decode_records(records)
        if not self.offsets_cover(evidence.committed_offsets, evidence.receipts):
            raise ObservationError("Committed offsets do not cover the confirmed inputs")
        remaining(deadline)
        evidence.evidence_complete = True
        evidence.note(
            "snapshot_collected",
            payments=len(evidence.database.payments),
            output_records=len(records),
        )
