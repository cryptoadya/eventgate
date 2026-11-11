"""Remove technical metadata without hiding any business multiplicity or conflict."""

import json

from pydantic import JsonValue

from eventgate.models import BusinessOutbox, BusinessPayment, BusinessSnapshot, ScenarioEvidence


def canonical_json(value: JsonValue) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def normalize(evidence: ScenarioEvidence) -> BusinessSnapshot:
    database = evidence.database
    records = evidence.output_records
    if not evidence.evidence_complete or database is None or records is None:
        raise ValueError("A business snapshot requires complete database and Kafka observations")

    grouped: dict[str, dict[str, dict[str, JsonValue]]] = {}
    for observed in records:
        if observed.completion is not None:
            payload = observed.completion.model_dump(mode="json")
            grouped.setdefault(observed.completion.completion_id, {})[canonical_json(payload)] = (
                payload
            )

    return BusinessSnapshot(
        inbox=sorted(database.inbox),
        payments=[
            BusinessPayment(
                event_id=row.event_id,
                order_id=row.order_id,
                amount_minor=row.amount_minor,
                currency=row.currency,
            )
            for row in sorted(
                database.payments,
                key=lambda row: (row.event_id, row.order_id, row.amount_minor, row.currency),
            )
        ],
        outbox=[
            BusinessOutbox(
                completion_id=row.completion_id,
                source_event_id=row.source_event_id,
                payload=row.payload,
                published=row.published_at is not None,
            )
            for row in sorted(
                database.outbox,
                key=lambda row: (
                    row.completion_id,
                    row.source_event_id,
                    canonical_json(row.payload),
                ),
            )
        ],
        completions={
            key: [payload for _, payload in sorted(variants.items())]
            for key, variants in sorted(grouped.items())
        },
        physical_output_count=len(records),
        logical_output_count=len(grouped),
    )
