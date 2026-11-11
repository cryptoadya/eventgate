import json

import pytest
from pydantic import ValidationError

from eventgate.contracts.payment import (
    PaymentRequested,
    make_completion,
    parse_completed,
    parse_requested,
)

REQUEST = {
    "schema_version": 1,
    "event_id": "evt-1001",
    "order_id": "order-1042",
    "amount_minor": 5000,
    "currency": "EUR",
}


def test_request_and_completion() -> None:
    request = parse_requested(json.dumps(REQUEST).encode(), b"evt-1001")
    completion = make_completion(request)
    assert completion.model_dump() == {
        "schema_version": 1,
        "completion_id": "payment.completed:evt-1001",
        "source_event_id": "evt-1001",
        "order_id": "order-1042",
        "amount_minor": 5000,
        "currency": "EUR",
    }
    assert make_completion(request) == completion
    extended = {**completion.model_dump(), "trace": {"labels": ["sample", 1]}}
    assert (
        parse_completed(json.dumps(extended).encode(), b"payment.completed:evt-1001").model_dump()
        == extended
    )


@pytest.mark.parametrize("amount", [0, -1, 0.5, 5000.0, True, "5000", 2**63])
def test_invalid_amount(amount: object) -> None:
    with pytest.raises(ValidationError, match="amount_minor"):
        PaymentRequested.model_validate({**REQUEST, "amount_minor": amount})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", 1.0),
        ("event_id", "bad.id"),
        ("order_id", ""),
        ("currency", "USD"),
        ("unexpected", "extra"),
    ],
)
def test_request_rejects_invalid_fields(field: str, value: object) -> None:
    with pytest.raises(ValidationError, match=field):
        PaymentRequested.model_validate({**REQUEST, field: value})


@pytest.mark.parametrize(
    "payload",
    [
        b'{"amount_minor": 1, "amount_minor": 2}',
        b'{"x": NaN}',
        b'{"x": Infinity}',
        b'{"x": -Infinity}',
        b"[]",
        b"\xff",
    ],
)
def test_rejects_non_contract_json(payload: bytes) -> None:
    with pytest.raises(ValueError):
        parse_requested(payload, b"evt-1001")


def test_kafka_key_must_match_payload() -> None:
    with pytest.raises(ValueError, match="key"):
        parse_requested(json.dumps(REQUEST).encode(), b"wrong")
    payload = make_completion(PaymentRequested.model_validate(REQUEST)).model_dump_json().encode()
    with pytest.raises(ValueError, match="key"):
        parse_completed(payload, b"wrong")
