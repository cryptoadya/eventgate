from collections.abc import Callable
from dataclasses import replace

import pytest

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.invariants import evaluate
from eventgate.kafka_io import KafkaRecord
from eventgate.models import CheckStatus, ScenarioEvidence, Verdict


@pytest.mark.parametrize(
    ("count", "expected"), [(0, Verdict.FAIL), (1, Verdict.PASS), (2, Verdict.FAIL)]
)
def test_requires_exactly_one_payment(
    observed: Callable[..., ScenarioEvidence], count: int, expected: Verdict
) -> None:
    result = evaluate(observed(payment_count=count))
    assert result.verdict == expected
    assert len(result.checks) == 8


@pytest.mark.parametrize(
    ("field", "value"), [("order_id", "bad"), ("amount_minor", 8), ("currency", "USD")]
)
def test_payment_fields(
    observed: Callable[..., ScenarioEvidence], field: str, value: object
) -> None:
    evidence = observed()
    assert evidence.database is not None
    evidence.database.payments[0] = replace(evidence.database.payments[0], **{field: value})
    result = evaluate(evidence)
    assert result.verdict == Verdict.FAIL
    assert next(c for c in result.checks if c.id == "payment_fields").status == CheckStatus.FAIL


@pytest.mark.parametrize("payload", [b"invalid json", b'{"schema_version":2}', b"\xff", None])
def test_malformed_output_fails_without_losing_bytes(
    observed: Callable[..., ScenarioEvidence], payload: bytes | None
) -> None:
    evidence = observed()
    evidence.output_records = PaymentV1Adapter.decode_records(
        [KafkaRecord("payments.completed", 0, 0, b"bad", payload)]
    )
    assert evidence.output_records[0].record.payload == payload
    assert evaluate(evidence).verdict == Verdict.FAIL


def test_wrong_key_unknown_effect_missing_completion_and_pending_outbox(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    evidence = observed()
    assert evidence.database is not None and evidence.output_records is not None
    record = evidence.output_records[0].record
    evidence.output_records = PaymentV1Adapter.decode_records([replace(record, key=b"wrong")])
    assert evaluate(evidence).verdict == Verdict.FAIL
    evidence.output_records = []
    evidence.database.inbox.append("unknown")
    evidence.database.outbox[0] = replace(evidence.database.outbox[0], published_at=None)
    result = evaluate(evidence)
    assert {c.id for c in result.checks if c.status == CheckStatus.FAIL} >= {
        "completion_contract",
        "no_unknown_effects",
        "outbox_published",
    }


def test_outbox_and_output_must_match_inside_each_version(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    evidence = observed(extra="same")
    assert evidence.database is not None
    payload = evidence.database.outbox[0].payload
    assert isinstance(payload, dict)
    payload["trace"] = "different"
    assert evaluate(evidence).verdict == Verdict.FAIL
