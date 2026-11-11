"""Wire validation and deterministic identifiers for payment-v1."""

import json
import math
from typing import Annotated, Literal, Never

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

MAX_AMOUNT_MINOR = 2**63 - 1
type SchemaV1 = Annotated[int, Field(strict=True, ge=1, le=1)]
type PaymentID = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")]
type AmountMinor = Annotated[int, Field(ge=1, le=MAX_AMOUNT_MINOR)]


class PaymentRequested(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    schema_version: SchemaV1
    event_id: PaymentID
    order_id: PaymentID
    amount_minor: AmountMinor
    currency: Literal["EUR"]


class PaymentCompleted(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow", frozen=True, allow_inf_nan=False)

    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)
    schema_version: SchemaV1
    completion_id: str
    source_event_id: PaymentID
    order_id: PaymentID
    amount_minor: AmountMinor
    currency: Literal["EUR"]

    @model_validator(mode="after")
    def check_identity(self) -> "PaymentCompleted":
        if self.completion_id != completion_id_for(self.source_event_id):
            raise ValueError("completion_id must be derived from source_event_id")
        return self


def completion_id_for(event_id: str) -> str:
    return f"payment.completed:{event_id}"


def make_completion(request: PaymentRequested) -> PaymentCompleted:
    return PaymentCompleted(
        schema_version=1,
        completion_id=completion_id_for(request.event_id),
        source_event_id=request.event_id,
        order_id=request.order_id,
        amount_minor=request.amount_minor,
        currency=request.currency,
    )


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Never:
    raise ValueError(f"Non-finite JSON number: {value}")


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("JSON number is outside the finite floating-point range")
    return result


def parse_json_object(payload: bytes) -> dict[str, JsonValue]:
    """Decode a UTF-8 JSON object without discarding repeated fields."""
    result: JsonValue = json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
        parse_float=_finite_float,
    )
    if not isinstance(result, dict):
        raise ValueError("Expected a JSON object")
    return result


def parse_requested(payload: bytes, key: bytes | None) -> PaymentRequested:
    request = PaymentRequested.model_validate(parse_json_object(payload))
    if key != request.event_id.encode("utf-8"):
        raise ValueError("Kafka key must match event_id")
    return request


def parse_completed(payload: bytes, key: bytes | None) -> PaymentCompleted:
    completion = PaymentCompleted.model_validate(parse_json_object(payload))
    if key != completion.completion_id.encode("utf-8"):
        raise ValueError("Kafka key must match completion_id")
    return completion
