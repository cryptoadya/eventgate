from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.contracts.control import Dependencies, WorkerStatus
from eventgate.contracts.payment import make_completion
from eventgate.kafka_io import InputReceipt, KafkaRecord
from eventgate.models import DatabaseSnapshot, OutboxRow, PaymentRow, ScenarioEvidence
from eventgate.scenarios import REQUEST_A


@pytest.fixture
def observed() -> Callable[..., ScenarioEvidence]:
    def create(*, payment_count=1, outputs=1, extra=None, conflict=False) -> ScenarioEvidence:
        request = REQUEST_A
        completion = make_completion(request)
        payload = completion.model_dump(mode="json")
        if extra is not None:
            payload["trace"] = extra
        records = [
            KafkaRecord(
                "payments.completed",
                0,
                offset,
                completion.completion_id.encode(),
                completion.model_validate(
                    {**payload, **({"trace": "different"} if conflict and offset else {})}
                )
                .model_dump_json()
                .encode(),
            )
            for offset in range(outputs)
        ]
        evidence = ScenarioEvidence(
            scenario_id="normal",
            inputs=[request],
            receipts=[
                InputReceipt(
                    "payments.requested",
                    0,
                    0,
                    request.event_id.encode(),
                    request.model_dump_json().encode(),
                )
            ],
            database=DatabaseSnapshot(
                [request.event_id],
                [
                    PaymentRow(
                        i + 1,
                        request.event_id,
                        request.order_id,
                        request.amount_minor,
                        request.currency,
                    )
                    for i in range(payment_count)
                ],
                [OutboxRow(completion.completion_id, request.event_id, payload, datetime.now(UTC))],
            ),
            committed_offsets={0: 1},
            input_end_offsets={0: 1},
            output_end_offsets={0: outputs},
            output_records=PaymentV1Adapter.decode_records(records),
            evidence_complete=True,
            progress_observed=True,
        )
        for role in ("consumer", "publisher"):
            evidence.paused_workers[role] = WorkerStatus(
                contract_version="payment-v1",
                role=role,
                state="paused",
                run_id="test",
                scenario_id="normal",
                process_instance_id=str(uuid4()),
                dependencies=Dependencies(kafka=True, postgres=True),
            )
        return evidence

    return create
