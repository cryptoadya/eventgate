from collections.abc import Callable

from eventgate.models import ScenarioEvidence
from eventgate.normalize import normalize


def test_normalization_preserves_payment_multiplicity_and_output_counts(
    observed: Callable[..., ScenarioEvidence],
) -> None:
    snapshot = normalize(observed(payment_count=2, outputs=2))
    assert len(snapshot.payments) == 2 and snapshot.payments[0] == snapshot.payments[1]
    assert snapshot.physical_output_count == 2
    assert snapshot.logical_output_count == 1
    assert len(snapshot.completions["payment.completed:evt-1001"]) == 1
