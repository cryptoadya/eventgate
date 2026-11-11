from collections.abc import Callable

from eventgate.compare import aggregate_verdict
from eventgate.invariants import evaluate
from eventgate.models import CheckStatus, ScenarioEvidence, ScenarioIssue, Verdict


def test_error_takes_precedence_without_losing_proven_defects(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    evidence = observed(payment_count=2)
    evidence.evidence_complete = False
    evidence.output_records = None
    evidence.errors.append(
        ScenarioIssue("observation_error", "final_observation", "Kafka unavailable")
    )
    result = evaluate(evidence)
    assert result.verdict == Verdict.ERROR
    assert next(c for c in result.checks if c.id == "payment_count").status == CheckStatus.FAIL
    assert (
        next(c for c in result.checks if c.id == "completion_contract").status
        == CheckStatus.NOT_EVALUATED
    )
    assert aggregate_verdict([Verdict.FAIL, Verdict.ERROR]) == Verdict.ERROR


def test_proven_progress_failure_does_not_require_a_complete_snapshot(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    evidence = observed()
    evidence.evidence_complete = False
    evidence.database = None
    evidence.output_records = None
    evidence.errors.append(ScenarioIssue("application_progress", "progress", "Worker stopped"))
    result = evaluate(evidence)
    assert result.verdict == Verdict.FAIL
    assert (
        next(c for c in result.checks if c.id == "payment_count").status
        == CheckStatus.NOT_EVALUATED
    )
    evidence.errors.clear()
    assert evaluate(evidence).verdict == Verdict.ERROR


def test_development_subset_is_pass_without_release_eligibility(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    from datetime import UTC, datetime

    from eventgate.compare import finalize_run
    from eventgate.config import ComparisonPolicy
    from eventgate.models import RunResult

    normal = evaluate(observed())
    result = RunResult(
        run_id="demo",
        started_at=datetime.now(UTC),
        mode="development",
        selection_coverage="partial",
        selected_scenarios=["normal"],
        versions={"baseline": [normal], "candidate": [normal]},
    )
    finalize_run(result, ComparisonPolicy(allowed_extra_output_fields=[]))
    assert result.verdict == Verdict.PASS and result.exit_code == 0
    assert result.evidence_complete and not result.release_eligible
