import pytest

from eventgate.runner import ProgressDiagnosis, classify_progress


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (ProgressDiagnosis(True, True, True, False, False), "application_progress"),
        (ProgressDiagnosis(True, True, True, True, False), "observation_error"),
        (ProgressDiagnosis(True, False, True, False, False), "observation_error"),
        (ProgressDiagnosis(True, True, False, False, False), "observation_error"),
        (ProgressDiagnosis(False, True, True, False, False), "observation_error"),
        (ProgressDiagnosis(True, True, True, False, True), "observation_error"),
    ],
)
def test_progress_timeout_requires_independent_health_evidence(
    observed: ProgressDiagnosis, expected: str
) -> None:
    assert classify_progress(observed) == expected
