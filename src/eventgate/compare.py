"""Compare complete business snapshots and aggregate the release decision."""

from collections.abc import Iterable

from pydantic import JsonValue

from eventgate.config import SCENARIO_ORDER, ComparisonPolicy
from eventgate.models import (
    BusinessSnapshot,
    CheckStatus,
    ComparisonResult,
    Difference,
    ExecutionState,
    RunResult,
    ScenarioResult,
    Verdict,
)
from eventgate.normalize import canonical_json


def _without_extras(payload: JsonValue, allowed: set[str]) -> JsonValue:
    if isinstance(payload, dict):
        return {key: value for key, value in payload.items() if key not in allowed}
    return payload


def _projection(snapshot: BusinessSnapshot, policy: ComparisonPolicy) -> dict[str, JsonValue]:
    allowed = set(policy.allowed_extra_output_fields)
    outbox: list[JsonValue] = []
    for item in snapshot.outbox:
        row = item.model_dump(mode="json")
        row["payload"] = _without_extras(item.payload, allowed)
        outbox.append(row)
    completions: dict[str, JsonValue] = {}
    for key, variants in snapshot.completions.items():
        payloads = [_without_extras(variant, allowed) for variant in variants]
        canonical = {canonical_json(payload): payload for payload in payloads}
        completions[key] = [payload for _, payload in sorted(canonical.items())]
    return {
        "inbox": list(snapshot.inbox),
        "payments": [item.model_dump(mode="json") for item in snapshot.payments],
        "outbox": sorted(outbox, key=canonical_json),
        "completions": completions,
    }


def compare(
    baseline: ScenarioResult,
    candidate: ScenarioResult,
    policy: ComparisonPolicy,
) -> ComparisonResult:
    if baseline.scenario_id != candidate.scenario_id:
        raise ValueError("Scenario results must match before comparison")
    differences: list[Difference] = []
    diff_status = CheckStatus.NOT_EVALUATED
    if (
        baseline.evidence_complete
        and candidate.evidence_complete
        and baseline.business is not None
        and candidate.business is not None
    ):
        left, right = (
            _projection(baseline.business, policy),
            _projection(candidate.business, policy),
        )
        differences = [
            Difference(path=key, baseline=left[key], candidate=right[key])
            for key in left
            if canonical_json(left[key]) != canonical_json(right[key])
        ]
        diff_status = CheckStatus.FAIL if differences else CheckStatus.PASS

    verdict, reason = Verdict.PASS, "equivalent"
    if Verdict.ERROR in (baseline.verdict, candidate.verdict):
        verdict, reason = Verdict.ERROR, "execution_error"
    elif baseline.verdict == Verdict.FAIL:
        verdict = Verdict.FAIL
        reason = (
            "both_versions_failed"
            if candidate.verdict == Verdict.FAIL
            else "invalid_baseline_candidate_improved"
        )
    elif candidate.verdict == Verdict.FAIL:
        verdict, reason = Verdict.FAIL, "candidate_regression"
    elif differences:
        verdict, reason = Verdict.FAIL, "unexpected_difference"
    elif diff_status == CheckStatus.NOT_EVALUATED:
        verdict, reason = Verdict.ERROR, "snapshot_unavailable"

    return ComparisonResult(
        scenario_id=baseline.scenario_id,
        verdict=verdict,
        reason=reason,
        diff_status=diff_status,
        differences=differences,
    )


def aggregate_verdict(values: Iterable[Verdict]) -> Verdict:
    choices = set(values)
    if not choices or Verdict.ERROR in choices:
        return Verdict.ERROR
    return Verdict.FAIL if Verdict.FAIL in choices else Verdict.PASS


def finalize_run(result: RunResult, policy: ComparisonPolicy) -> None:
    baseline = {item.scenario_id: item for item in result.versions.get("baseline", [])}
    candidate = {item.scenario_id: item for item in result.versions.get("candidate", [])}
    selected = result.selected_scenarios
    result.comparisons = [
        compare(baseline[scenario], candidate[scenario], policy)
        for scenario in selected
        if scenario in baseline and scenario in candidate
    ]
    pairs = [*baseline.values(), *candidate.values()]
    all_pairs = bool(selected) and len(result.comparisons) == len(selected)
    result.evidence_complete = all_pairs and all(
        item.evidence_complete and item.execution_state == ExecutionState.COMPLETED
        for item in pairs
    )
    values = [item.verdict for item in pairs] + [item.verdict for item in result.comparisons]
    if (
        not all_pairs
        or result.interrupted
        or result.errors
        or any(not item.success for item in result.cleanup)
    ):
        values.append(Verdict.ERROR)
    result.verdict = aggregate_verdict(values)
    result.exit_code = 130 if result.interrupted else result.verdict.exit_code
    result.release_eligible = (
        result.verdict == Verdict.PASS
        and result.mode == "release"
        and result.selection_coverage == "full"
        and tuple(selected) == SCENARIO_ORDER
        and result.evidence_complete
        and len(result.cleanup) == 2 * len(selected)
    )
