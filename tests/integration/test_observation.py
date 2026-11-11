import time

import psycopg
import pytest

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.contracts.payment import PaymentRequested, make_completion
from eventgate.environment import EnvironmentHandle, EnvironmentManager
from eventgate.kafka_io import KafkaIO, ObservationError, OutboundMessage
from eventgate.models import ScenarioEvidence

REQUEST = PaymentRequested(
    schema_version=1, event_id="evt-1001", order_id="order-1042", amount_minor=5000, currency="EUR"
)


def prepare(
    manager: EnvironmentManager, handle: EnvironmentHandle
) -> tuple[PaymentV1Adapter, KafkaIO, ScenarioEvidence]:
    kafka = KafkaIO(handle.kafka_bootstrap)
    adapter = PaymentV1Adapter(manager)
    deadline = time.monotonic() + 15
    evidence = ScenarioEvidence(scenario_id=handle.scenario_id, inputs=[REQUEST])
    evidence.receipts.extend(
        kafka.send_confirmed(
            handle.config.kafka.input_topic,
            [OutboundMessage(REQUEST.event_id.encode(), REQUEST.model_dump_json().encode())],
            deadline,
        )
    )
    adapter.wait_progress(handle, evidence.receipts, deadline)
    return adapter, kafka, evidence


def test_snapshot_preserves_duplicate_rows_and_all_raw_kafka_records(
    running_payment: tuple[EnvironmentManager, EnvironmentHandle],
) -> None:
    manager, handle = running_payment
    adapter, kafka, evidence = prepare(manager, handle)
    deadline = time.monotonic() + 15
    with psycopg.connect(handle.postgres_dsn) as conn:
        conn.execute(
            "INSERT INTO payments (event_id, order_id, amount_minor, currency) "
            "SELECT event_id, order_id, amount_minor, currency FROM payments"
        )
    completion = make_completion(REQUEST)
    kafka.send_confirmed(
        handle.config.kafka.output_topic,
        [
            OutboundMessage(
                completion.completion_id.encode(), completion.model_dump_json().encode()
            ),
            OutboundMessage(b"bad", b"\xff"),
        ],
        deadline,
    )
    adapter.collect_final(handle, evidence, deadline)
    assert evidence.evidence_complete
    assert evidence.database is not None
    assert len(evidence.database.payments) == 2
    assert evidence.database.payments[0].payment_id != evidence.database.payments[1].payment_id
    assert evidence.output_end_offsets == {0: 3}
    assert evidence.output_records is not None
    assert [item.record.offset for item in evidence.output_records] == [0, 1, 2]
    assert [item.completion for item in evidence.output_records] == [completion, completion, None]
    assert evidence.output_records[2].record.payload == b"\xff"
    assert evidence.output_records[2].error is not None
    assert evidence.committed_offsets == {0: 1}
    assert adapter.status(handle, "consumer", deadline).state == "paused"
    assert adapter.status(handle, "publisher", deadline).state == "paused"


@pytest.mark.parametrize("error", [ObservationError("Unavailable output"), TimeoutError("Expired")])
def test_observer_failure_does_not_make_a_complete_snapshot(
    running_payment: tuple[EnvironmentManager, EnvironmentHandle],
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    manager, handle = running_payment
    adapter, _, evidence = prepare(manager, handle)

    def fail(*args: object) -> None:
        raise error

    monkeypatch.setattr(KafkaIO, "read_until", fail)
    with pytest.raises(type(error)):
        adapter.collect_final(handle, evidence, time.monotonic() + 10)
    assert evidence.database is not None
    assert evidence.output_records is None
    assert not evidence.evidence_complete
