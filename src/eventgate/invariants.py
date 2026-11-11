"""Evaluate each application version against the payment-v1 business contract."""

from collections import Counter

from pydantic import JsonValue

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.contracts.payment import PaymentCompleted, PaymentRequested, make_completion
from eventgate.models import (
    CheckResult,
    CheckStatus,
    ExecutionState,
    ScenarioEvidence,
    ScenarioResult,
    Verdict,
)
from eventgate.normalize import canonical_json, normalize


def _check(
    name: str,
    expected: str,
    problems: list[str],
    complete: bool,
    actual: JsonValue = None,
) -> CheckResult:
    if problems:
        return CheckResult(
            id=name,
            status=CheckStatus.FAIL,
            explanation="; ".join(problems),
            expected=expected,
            actual=actual,
        )
    if not complete:
        return CheckResult(
            id=name,
            status=CheckStatus.NOT_EVALUATED,
            explanation="A complete observation was unavailable",
            expected=expected,
            actual=actual,
        )
    return CheckResult(
        id=name, status=CheckStatus.PASS, explanation=expected, expected=expected, actual=actual
    )


def _count_issues(
    expected: set[str],
    counts: Counter[str],
    noun: str,
    complete: bool,
) -> list[str]:
    return [
        f"{key}: expected 1 {noun}; observed {counts[key]}"
        for key in sorted(expected)
        if counts[key] > 1 or (complete and counts[key] != 1)
    ]


def _required_match(actual: PaymentCompleted, expected: PaymentCompleted) -> bool:
    fields = PaymentCompleted.model_fields
    return actual.model_dump(include=set(fields)) == expected.model_dump(include=set(fields))


def _scenario_proof(evidence: ScenarioEvidence) -> list[str]:
    expected_count = len(evidence.inputs)
    if (
        not evidence.progress_observed
        or evidence.committed_offsets is None
        or not PaymentV1Adapter.offsets_cover(evidence.committed_offsets, evidence.receipts)
        or [item.offset for item in evidence.receipts] != list(range(expected_count))
        or evidence.input_end_offsets != {0: expected_count}
        or any(
            receipt.key != request.event_id.encode()
            for receipt, request in zip(evidence.receipts, evidence.inputs, strict=False)
        )
        or set(evidence.paused_workers) != {"consumer", "publisher"}
    ):
        return ["Confirmed inputs, worker pause, or final committed offsets do not prove progress"]
    scenario = evidence.scenario_id
    if scenario == "normal":
        return []
    if scenario == "duplicate_delivery":
        first, second = evidence.receipts
        names = [item.event for item in evidence.timeline]
        if first.payload != second.payload or "first_input_processed" not in names:
            return ["Duplicate delivery did not prove a second copy after first-input progress"]
        return []

    restart = evidence.restart
    if (
        restart is None
        or restart.old_instance_id == restart.new_instance_id
        or restart.previous_database is None
    ):
        return ["The required restart and pre-restart observation were not proven"]
    first_request = evidence.inputs[0]
    first_event = first_request.event_id
    before = restart.previous_database
    if scenario == "consumer_restart":
        old_payments = [row for row in before.payments if row.event_id == first_event]
        if (
            restart.role != "consumer"
            or restart.exit_code != 0
            or not old_payments
            or evidence.database is None
            or any(row not in evidence.database.payments for row in old_payments)
            or restart.previous_offsets is None
            or not PaymentV1Adapter.offsets_cover(restart.previous_offsets, evidence.receipts[:1])
            or not any(
                row.source_event_id == first_event and row.published_at is not None
                for row in before.outbox
            )
        ):
            return ["Graceful consumer restart did not preserve the first completed request"]
        return []

    receipt = restart.failpoint
    role = "consumer" if scenario == "crash_after_db_commit" else "publisher"
    completion_id = make_completion(first_request).completion_id
    target = first_event if role == "consumer" else completion_id
    if (
        restart.role != role
        or restart.exit_code != 86
        or receipt is None
        or receipt.state != "fired"
        or receipt.target_id != target
        or receipt.process_instance_id != restart.old_instance_id
        or not any(
            row.source_event_id == first_event and row.published_at is None for row in before.outbox
        )
    ):
        return ["The expected crash and pending outbox state were not proven"]
    firings = [
        event
        for event in evidence.timeline
        if event.source == "application"
        and event.event == "failpoint_fired"
        and event.details.get("token") == receipt.token
    ]
    if len(firings) != 1:
        return ["The durable failpoint must fire exactly once"]

    if role == "consumer":
        sent = evidence.receipts[0]
        if (
            restart.previous_offsets is None
            or PaymentV1Adapter.offsets_cover(restart.previous_offsets, evidence.receipts)
            or not any(row.event_id == first_event for row in before.payments)
        ):
            return ["A committed payment without a committed input offset was not observed"]
        observed_instances = {
            details.get("process_instance_id")
            for event in evidence.timeline
            if event.source == "application"
            and event.event == "message_received"
            and (details := event.details).get("topic") == sent.topic
            and details.get("partition") == sent.partition
            and details.get("offset") == sent.offset
            and details.get("event_id") == first_event
            and isinstance(details.get("process_instance_id"), str)
        }
        return (
            []
            if {restart.old_instance_id, restart.new_instance_id} <= observed_instances
            else ["Kafka input replay was not proven"]
        )

    records = evidence.output_records or []
    previous = restart.previous_output or []
    matching = [
        item
        for item in records
        if item.completion is not None and item.completion.completion_id == completion_id
    ]
    if not previous or records[: len(previous)] != previous or len(matching) < 2:
        return [
            "Publisher recovery must preserve the first Kafka record and publish a physical copy"
        ]
    return []


def check_invariants(evidence: ScenarioEvidence) -> list[CheckResult]:
    expected: dict[str, PaymentRequested] = {
        request.event_id: request for request in evidence.inputs
    }
    completions = {key: make_completion(request) for key, request in expected.items()}
    database, output = evidence.database, evidence.output_records
    complete = evidence.evidence_complete and database is not None and output is not None

    inbox_counts = Counter(database.inbox if database is not None else [])
    payment_counts = (
        Counter(row.event_id for row in database.payments) if database is not None else Counter()
    )
    payment_issues: list[str] = []
    outbox_issues: list[str] = []
    unpublished: list[str] = []
    unknown: list[str] = []
    outbox_payloads: dict[str, list[JsonValue]] = {}
    outbox_counts: Counter[str] = Counter()
    if database is not None:
        unknown.extend(
            f"Unknown inbox event: {key}" for key in sorted(inbox_counts.keys() - expected.keys())
        )
        for row in database.payments:
            request = expected.get(row.event_id)
            if request is None:
                unknown.append(f"Unknown payment event: {row.event_id}")
            elif (row.order_id, row.amount_minor, row.currency) != (
                request.order_id,
                request.amount_minor,
                request.currency,
            ):
                payment_issues.append(
                    f"{row.event_id}: payment order, amount, or currency does not match input"
                )
        for outbox_row in database.outbox:
            outbox_counts[outbox_row.source_event_id] += 1
            outbox_payloads.setdefault(outbox_row.completion_id, []).append(outbox_row.payload)
            wanted = completions.get(outbox_row.source_event_id)
            if wanted is None:
                unknown.append(f"Unknown outbox event: {outbox_row.source_event_id}")
            else:
                try:
                    outbox_completion = PaymentCompleted.model_validate(outbox_row.payload)
                except ValueError:
                    outbox_issues.append(f"{outbox_row.completion_id}: invalid outbox payload")
                else:
                    if outbox_row.completion_id != wanted.completion_id or not _required_match(
                        outbox_completion, wanted
                    ):
                        outbox_issues.append(
                            f"{outbox_row.completion_id}: outbox identity or payload does not match input"
                        )
            if outbox_row.published_at is None and complete:
                unpublished.append(f"{outbox_row.completion_id}: outbox row is still pending")
        outbox_issues.extend(_count_issues(set(expected), outbox_counts, "outbox row", complete))

    outputs: dict[str, list[str]] = {}
    output_issues: list[str] = []
    for item in output or []:
        actual = item.completion
        if actual is None:
            output_issues.append(
                f"Kafka offset {item.record.offset}: {item.error or 'invalid completion'}"
            )
            continue
        payload = canonical_json(actual.model_dump(mode="json"))
        outputs.setdefault(actual.completion_id, []).append(payload)
        wanted = completions.get(actual.source_event_id)
        if wanted is None:
            unknown.append(f"Unknown completion event: {actual.source_event_id}")
        elif not _required_match(actual, wanted):
            output_issues.append(
                f"{actual.completion_id}: completion business fields do not match input"
            )
        if any(
            payload != canonical_json(row) for row in outbox_payloads.get(actual.completion_id, [])
        ):
            output_issues.append(f"{actual.completion_id}: Kafka and outbox payloads differ")

    output_issues.extend(
        f"{key}: conflicting physical copies"
        for key, variants in outputs.items()
        if len(set(variants)) > 1
    )
    if complete:
        output_issues.extend(
            f"{expected_completion.completion_id}: missing logical completion"
            for expected_completion in completions.values()
            if expected_completion.completion_id not in outputs
        )

    checks = [
        _check(
            "inbox_count",
            "One inbox row per input",
            _count_issues(set(expected), inbox_counts, "inbox row", complete),
            complete,
            dict(inbox_counts) if database is not None else None,
        ),
        _check(
            "payment_count",
            "One payment per input",
            _count_issues(set(expected), payment_counts, "payment", complete),
            complete,
            dict(payment_counts) if database is not None else None,
        ),
        _check(
            "payment_fields",
            "Payments match input orders, amounts, and currency",
            payment_issues,
            complete,
        ),
        _check("outbox_contract", "One matching outbox row per input", outbox_issues, complete),
        _check("outbox_published", "No unpublished outbox rows remain", unpublished, complete),
        _check(
            "completion_contract",
            "One consistent logical completion per input",
            output_issues,
            complete,
        ),
        _check("no_unknown_effects", "No effects for unknown inputs", unknown, complete),
    ]
    other_errors = [
        error.message for error in evidence.errors if error.code != "application_progress"
    ]
    progress_errors = [
        error.message for error in evidence.errors if error.code == "application_progress"
    ]
    progress = _check(
        "application_progress",
        "Required progress and recovery were independently proven",
        progress_errors or (_scenario_proof(evidence) if complete else []),
        complete,
    )
    if other_errors or (
        not complete
        and not progress_errors
        and not any(check.status == CheckStatus.FAIL for check in checks)
    ):
        progress.status = CheckStatus.ERROR
        progress.explanation = (
            "; ".join(other_errors + progress_errors) or "Required evidence is incomplete"
        )
    checks.append(progress)
    return checks


def evaluate(evidence: ScenarioEvidence) -> ScenarioResult:
    checks = check_invariants(evidence)
    statuses = {check.status for check in checks}
    verdict = (
        Verdict.ERROR
        if CheckStatus.ERROR in statuses
        else Verdict.FAIL
        if CheckStatus.FAIL in statuses
        else Verdict.PASS
    )
    complete = (
        evidence.evidence_complete
        and evidence.database is not None
        and evidence.output_records is not None
    )
    return ScenarioResult(
        scenario_id=evidence.scenario_id,
        execution_state=ExecutionState.COMPLETED if complete else ExecutionState.ABORTED,
        evidence_complete=complete,
        verdict=verdict,
        reason_codes=sorted(
            {error.code for error in evidence.errors}
            | {
                check.id
                for check in checks
                if check.status in (CheckStatus.FAIL, CheckStatus.ERROR)
            }
        ),
        checks=checks,
        errors=evidence.errors,
        business=normalize(evidence) if complete else None,
        timeline=sorted(evidence.timeline, key=lambda item: item.at),
    )
