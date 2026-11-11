"""Execute reproducible payment-v1 experiments and retain their observations."""

import time
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from eventgate.adapters.payment_v1 import ControlError, PaymentV1Adapter, ProgressTimeout
from eventgate.config import ScenarioID
from eventgate.contracts.control import (
    ROLE_FAILPOINT,
    ArmRequest,
    FailpointReceipt,
    Role,
    WorkerStatus,
)
from eventgate.contracts.payment import PaymentRequested, completion_id_for, parse_json_object
from eventgate.environment import EnvironmentHandle, EnvironmentManager, remaining
from eventgate.kafka_io import KafkaIO, OutboundMessage
from eventgate.models import RestartEvidence, ScenarioEvidence, ScenarioIssue, TimelineEntry

REQUEST_A = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)
REQUEST_B = PaymentRequested(
    schema_version=1, event_id="evt-1002", order_id="order-1043", amount_minor=7500, currency="EUR"
)


@dataclass(frozen=True)
class ScenarioContext:
    manager: EnvironmentManager
    handle: EnvironmentHandle


class ScenarioProtocolError(RuntimeError):
    """A required scenario operation could not be reproduced."""


def parse_worker_logs(logs: str) -> list[TimelineEntry]:
    events: list[TimelineEntry] = []
    for line in logs.splitlines():
        prefix, separator, content = line.partition("|")
        role: Role | None = next(
            (role for role in ROLE_FAILPOINT if prefix.strip() == f"{role}-1"), None
        )
        if not separator or role is None:
            continue
        timestamp, space, payload = content.strip().partition(" ")
        if not space or not payload.startswith("{"):
            continue
        try:
            at = datetime.fromisoformat(timestamp)
            details = parse_json_object(payload.encode())
            event = details.pop("event", None)
        except (ValueError, RecursionError):
            continue
        if at.tzinfo is not None and isinstance(event, str):
            details["role"] = role
            events.append(TimelineEntry(at, "application", event, details))
    return events


class _Experiment:
    def __init__(self, context: ScenarioContext, evidence: ScenarioEvidence) -> None:
        self.manager = context.manager
        self.handle = context.handle
        self.evidence = evidence
        self.adapter = PaymentV1Adapter(self.manager)
        self.kafka = KafkaIO(self.handle.kafka_bootstrap)
        self.timeouts = self.handle.config.timeouts
        self.stage = "readiness"

    def _control(self, outer: float = float("inf")) -> float:
        return min(outer, time.monotonic() + self.timeouts.control_seconds)

    def _progress_deadline(self) -> float:
        return time.monotonic() + self.timeouts.progress_seconds

    def _status(
        self, role: Role, deadline: float, *, publisher_paused: bool = False
    ) -> WorkerStatus:
        status = self.adapter.status(self.handle, role, self._control(deadline))
        wanted = "paused" if role == "publisher" and publisher_paused else "ready"
        if status.state != wanted or not (
            status.dependencies.kafka and status.dependencies.postgres
        ):
            raise ControlError(f"{role} must report {wanted} with working dependencies")
        return status

    def _send(
        self,
        request: PaymentRequested,
        deadline: float,
        *,
        publisher_paused: bool = False,
    ) -> None:
        self.stage = "readiness"
        self._status("consumer", deadline)
        self._status("publisher", deadline, publisher_paused=publisher_paused)
        self.stage = "input_delivery"
        [receipt] = self.kafka.send_confirmed(
            self.handle.config.kafka.input_topic,
            [OutboundMessage(request.event_id.encode(), request.model_dump_json().encode())],
            deadline,
        )
        self.evidence.receipts.append(receipt)
        self.evidence.note(
            "input_confirmed",
            event_id=request.event_id,
            topic=receipt.topic,
            partition=receipt.partition,
            offset=receipt.offset,
        )

    def _wait_progress(
        self,
        deadline: float,
        *,
        first: bool = False,
    ) -> dict[int, int | None]:
        self.stage = "progress"
        offsets = self.adapter.wait_progress(
            self.handle,
            self.evidence.receipts,
            deadline,
            source_event_id=REQUEST_A.event_id if first else None,
        )
        self.evidence.note(
            "first_input_processed" if first else "progress_observed",
            committed_offset=offsets.get(0),
        )
        if not first:
            self.evidence.progress_observed = True
        return offsets

    def _finish(self) -> None:
        self.stage = "final_observation"
        deadline = self._progress_deadline()
        self.adapter.collect_final(self.handle, self.evidence, deadline)
        self.evidence.input_end_offsets = self.kafka.end_offsets(
            self.handle.config.kafka.input_topic,
            self._control(deadline),
        )

    def normal(self) -> None:
        deadline = self._progress_deadline()
        self._send(REQUEST_A, deadline)
        self._wait_progress(deadline)
        self._finish()

    def duplicate_delivery(self) -> None:
        deadline = self._progress_deadline()
        self._send(REQUEST_A, deadline)
        self._wait_progress(deadline, first=True)
        self._send(REQUEST_A, deadline)
        self._wait_progress(deadline)
        self._finish()

    def consumer_restart(self) -> None:
        deadline = self._progress_deadline()
        self._send(REQUEST_A, deadline)
        offsets = self._wait_progress(deadline, first=True)
        self.stage = "restart"
        before = self.adapter.read_database(self.handle, self._control(deadline))
        consumer = self._status("consumer", deadline)
        publisher = self._status("publisher", deadline)
        stopped = self.manager.stop_role(self.handle, "consumer", self._control())
        if stopped.running or stopped.exit_code != 0:
            raise ScenarioProtocolError("Consumer did not stop cleanly with exit code 0")
        self.evidence.note("worker_stopped", role="consumer", exit_code=stopped.exit_code)
        startup = time.monotonic() + self.timeouts.startup_seconds
        self.manager.restart_role(self.handle, "consumer", startup)
        current = self.adapter.wait_ready(self.handle, "consumer", startup)
        if current.process_instance_id == consumer.process_instance_id:
            raise ScenarioProtocolError("Consumer did not get a new process instance")
        if self._status("publisher", startup).process_instance_id != publisher.process_instance_id:
            raise ScenarioProtocolError("Publisher changed during a consumer-only restart")
        self.evidence.restart = RestartEvidence(
            "consumer",
            stopped.exit_code,
            consumer.process_instance_id,
            current.process_instance_id,
            before,
            offsets,
        )
        self.evidence.note(
            "worker_restarted", role="consumer", process_instance_id=current.process_instance_id
        )
        deadline = self._progress_deadline()
        self._send(REQUEST_B, deadline)
        self._wait_progress(deadline)
        self._finish()

    def _arm(self, role: Role, target: str) -> ArmRequest:
        command = ArmRequest(token=str(uuid4()), name=ROLE_FAILPOINT[role], target_id=target)
        response = self.adapter.arm(self.handle, role, command, self._control())
        if response.state != "armed":
            raise ScenarioProtocolError("A new failpoint must be armed before sending input")
        self.evidence.note(
            "failpoint_armed", role=role, name=command.name, token=command.token, target_id=target
        )
        return command

    def _crash_receipt(
        self,
        role: Role,
        command: ArmRequest,
        old: WorkerStatus,
        deadline: float,
    ) -> FailpointReceipt:
        self.stage = "crash_window"
        delay = 0.1
        while True:
            try:
                remaining(deadline)
            except TimeoutError:
                raise ScenarioProtocolError(f"{role} did not reach its armed failpoint") from None
            state = self.manager.role_state(self.handle, role, self._control(deadline))
            if not state.running:
                if state.exit_code != 86:
                    raise ScenarioProtocolError(
                        f"{role} stopped with exit code {state.exit_code}, expected 86"
                    )
                break
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(delay * 2, 1)
        receipt = self.adapter.read_receipt(self.handle, role, self._control(deadline))
        if (
            receipt.state != "fired"
            or receipt.token != command.token
            or receipt.target_id != command.target_id
            or receipt.name != command.name
            or receipt.process_instance_id != old.process_instance_id
        ):
            raise ScenarioProtocolError("The durable receipt does not prove the requested crash")
        self.evidence.note("worker_crashed", role=role, exit_code=86, token=receipt.token)
        return receipt

    def _restart_crashed(self, role: Role, old: WorkerStatus) -> WorkerStatus:
        self.stage = "restart"
        startup = time.monotonic() + self.timeouts.startup_seconds
        self.manager.restart_role(self.handle, role, startup)
        current = self.adapter.wait_ready(self.handle, role, startup)
        if current.process_instance_id == old.process_instance_id:
            raise ScenarioProtocolError(f"{role} did not start a new process instance")
        self.evidence.note(
            "worker_restarted", role=role, process_instance_id=current.process_instance_id
        )
        return current

    def crash_after_db_commit(self) -> None:
        self.stage = "crash_window"
        publisher = self.adapter.pause(self.handle, "publisher", self._control())
        consumer = self._status("consumer", self._control())
        command = self._arm("consumer", REQUEST_A.event_id)
        deadline = self._progress_deadline()
        self._send(REQUEST_A, deadline, publisher_paused=True)
        receipt = self._crash_receipt("consumer", command, consumer, deadline)
        database = self.adapter.read_database(self.handle, self._control(deadline))
        offsets = self.kafka.committed_offsets(
            self.handle.consumer_group,
            self.handle.config.kafka.input_topic,
            self._control(deadline),
        )
        if (
            not any(payment.event_id == REQUEST_A.event_id for payment in database.payments)
            or not any(
                row.source_event_id == REQUEST_A.event_id and row.published_at is None
                for row in database.outbox
            )
            or self.adapter.offsets_cover(offsets, self.evidence.receipts)
        ):
            raise ScenarioProtocolError(
                "The database and Kafka offsets do not prove a pre-commit crash"
            )
        self.evidence.note("database_committed_before_offset", committed_offset=offsets.get(0))
        current = self._restart_crashed("consumer", consumer)
        resumed = self.adapter.resume(self.handle, "publisher", self._control())
        if resumed.process_instance_id != publisher.process_instance_id:
            raise ScenarioProtocolError("Publisher changed during a consumer-only restart")
        self.evidence.restart = RestartEvidence(
            "consumer",
            86,
            consumer.process_instance_id,
            current.process_instance_id,
            database,
            offsets,
            receipt,
        )
        self._wait_progress(self._progress_deadline())
        self._finish()
        self.stage = "crash_window"
        events = parse_worker_logs(self.manager.collect_logs(self.handle))
        self.evidence.timeline.extend(events)
        [sent] = self.evidence.receipts
        instances = {
            event.details.get("process_instance_id")
            for event in events
            if event.event == "message_received"
            and event.details.get("event_id") == REQUEST_A.event_id
            and event.details.get("topic") == sent.topic
            and event.details.get("partition") == sent.partition
            and event.details.get("offset") == sent.offset
        }
        if not {
            consumer.process_instance_id,
            current.process_instance_id,
        } <= instances or self.evidence.input_end_offsets != {0: 1}:
            raise ScenarioProtocolError("The same confirmed input was not proven to be replayed")

    def crash_after_publish_ack(self) -> None:
        self.stage = "crash_window"
        publisher = self._status("publisher", self._control())
        consumer = self._status("consumer", self._control())
        target = completion_id_for(REQUEST_A.event_id)
        command = self._arm("publisher", target)
        deadline = self._progress_deadline()
        self._send(REQUEST_A, deadline)
        receipt = self._crash_receipt("publisher", command, publisher, deadline)
        database = self.adapter.read_database(self.handle, self._control(deadline))
        output_topic = self.handle.config.kafka.output_topic
        end_offsets = self.kafka.end_offsets(output_topic, self._control(deadline))
        raw_output = self.kafka.read_until(output_topic, end_offsets, self._control(deadline))
        if not raw_output or not any(
            row.completion_id == target and row.published_at is None for row in database.outbox
        ):
            raise ScenarioProtocolError(
                "The Kafka output and pending outbox do not prove a post-ack crash"
            )
        previous = self.adapter.decode_records(raw_output)
        offsets = self.kafka.committed_offsets(
            self.handle.consumer_group,
            self.handle.config.kafka.input_topic,
            self._control(deadline),
        )
        self.evidence.note(
            "output_observed_before_outbox_mark", completion_id=target, records=len(previous)
        )

        current = self._restart_crashed("publisher", publisher)
        if (
            self._status("consumer", self._control()).process_instance_id
            != consumer.process_instance_id
        ):
            raise ScenarioProtocolError("Consumer changed during a publisher-only restart")
        self.evidence.restart = RestartEvidence(
            "publisher",
            86,
            publisher.process_instance_id,
            current.process_instance_id,
            database,
            offsets,
            receipt,
            previous,
        )
        self._wait_progress(self._progress_deadline())
        self._finish()
        self.stage = "crash_window"
        self.evidence.timeline.extend(parse_worker_logs(self.manager.collect_logs(self.handle)))
        if self.evidence.input_end_offsets != {0: 1}:
            raise ScenarioProtocolError("The publisher crash experiment requires exactly one input")


def scenario_inputs(scenario_id: ScenarioID) -> list[PaymentRequested]:
    return {
        "normal": [REQUEST_A],
        "duplicate_delivery": [REQUEST_A, REQUEST_A],
        "consumer_restart": [REQUEST_A, REQUEST_B],
        "crash_after_db_commit": [REQUEST_A],
        "crash_after_publish_ack": [REQUEST_A],
    }[scenario_id]


def run_scenario(
    context: ScenarioContext, scenario_id: ScenarioID, *, evidence: ScenarioEvidence | None = None
) -> ScenarioEvidence:
    if scenario_id != context.handle.scenario_id:
        raise ValueError("Scenario does not match its environment")
    if evidence is None:
        evidence = ScenarioEvidence(scenario_id=scenario_id, inputs=scenario_inputs(scenario_id))
    experiment = _Experiment(context, evidence)
    actions = {
        "normal": experiment.normal,
        "duplicate_delivery": experiment.duplicate_delivery,
        "consumer_restart": experiment.consumer_restart,
        "crash_after_db_commit": experiment.crash_after_db_commit,
        "crash_after_publish_ack": experiment.crash_after_publish_ack,
    }
    try:
        actions[scenario_id]()
    except Exception as error:
        code = "observation_error"
        if isinstance(error, ProgressTimeout):
            code = "progress_unresolved"
        elif (
            isinstance(error, (ControlError, ScenarioProtocolError))
            or experiment.stage == "restart"
        ):
            code = "contract_or_scenario"
        elif experiment.stage == "input_delivery":
            code = "input_delivery_error"
        evidence.evidence_complete = False
        evidence.errors.append(
            ScenarioIssue(code, experiment.stage, f"{type(error).__name__}: {error}")
        )
        evidence.note("scenario_interrupted", stage=experiment.stage, reason=code)
    return evidence
