from collections.abc import Callable

from eventgate.compare import compare
from eventgate.config import ComparisonPolicy
from eventgate.invariants import evaluate
from eventgate.models import CheckStatus, ScenarioEvidence, Verdict


def test_physical_duplicates_do_not_change_comparison(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    result = compare(
        evaluate(observed()),
        evaluate(observed(outputs=2)),
        ComparisonPolicy(allowed_extra_output_fields=[]),
    )
    assert result.verdict == Verdict.PASS
    assert result.diff_status == CheckStatus.PASS


def test_allowlist_applies_only_across_versions(observed: Callable[..., ScenarioEvidence]) -> None:
    baseline = evaluate(observed(extra="base"))
    changed = evaluate(observed(extra="candidate"))
    policy = ComparisonPolicy(allowed_extra_output_fields=["trace"])
    assert compare(baseline, changed, policy).verdict == Verdict.PASS
    assert (
        compare(baseline, changed, ComparisonPolicy(allowed_extra_output_fields=[])).verdict
        == Verdict.FAIL
    )
    conflicting = evaluate(observed(extra="candidate", outputs=2, conflict=True))
    assert compare(baseline, conflicting, policy).verdict == Verdict.FAIL


def test_invalid_baseline_and_improvement_are_still_failures(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    good = evaluate(observed())
    bad = evaluate(observed(payment_count=2))
    policy = ComparisonPolicy(allowed_extra_output_fields=[])
    assert compare(bad, good, policy).reason == "invalid_baseline_candidate_improved"
    assert compare(bad, bad, policy).reason == "both_versions_failed"
    assert compare(good, bad, policy).reason == "candidate_regression"
