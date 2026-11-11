import pytest

from eventgate.adapters.payment_v1 import PaymentV1Adapter
from eventgate.contracts.control import ArmResponse
from eventgate.scenarios import ScenarioContext, run_scenario


@pytest.mark.parametrize(("variant", "payments"), [("stable", 1), ("buggy", 2)])
def test_first_message_is_replayed_without_resending(
    scenario_environment,
    variant: str,
    payments: int,
) -> None:
    manager, handle = scenario_environment(variant, "crash_after_db_commit")
    evidence = run_scenario(ScenarioContext(manager, handle), handle.scenario_id)
    assert evidence.evidence_complete, evidence.errors
    assert [receipt.offset for receipt in evidence.receipts] == [0]
    assert evidence.input_end_offsets == {0: 1}
    restart = evidence.restart
    assert restart is not None and restart.role == "consumer" and restart.exit_code == 86
    assert restart.failpoint is not None and restart.failpoint.state == "fired"
    assert restart.previous_offsets == {0: None}
    assert restart.previous_database is not None
    assert len(restart.previous_database.payments) == 1
    assert restart.previous_database.outbox[0].published_at is None
    assert evidence.committed_offsets == {0: 1}
    assert evidence.database is not None and len(evidence.database.payments) == payments
    assert evidence.database.outbox[0].published_at is not None
    assert evidence.output_records is not None and len(evidence.output_records) == 1
    events = [item for item in evidence.timeline if item.event == "message_received"]
    assert {event.details["process_instance_id"] for event in events} == {
        restart.old_instance_id,
        restart.new_instance_id,
    }
    assert {event.details["offset"] for event in events} == {0}


def test_unreproduced_failpoint_is_execution_error(
    scenario_environment, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, handle = scenario_environment("stable", "crash_after_db_commit")
    handle.config.timeouts.progress_seconds = 3

    def acknowledge_without_arm(self, handle, role, command, deadline):
        return ArmResponse(token=command.token, state="armed")

    monkeypatch.setattr(PaymentV1Adapter, "arm", acknowledge_without_arm)
    evidence = run_scenario(ScenarioContext(manager, handle), handle.scenario_id)
    assert not evidence.evidence_complete and evidence.restart is None
    assert len(evidence.receipts) == 1
    assert evidence.errors[0].code == "contract_or_scenario"
