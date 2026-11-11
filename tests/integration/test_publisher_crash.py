import pytest

from eventgate.scenarios import ScenarioContext, run_scenario


@pytest.mark.parametrize("variant", ["stable", "buggy"])
def test_acknowledged_outbox_is_replayed_by_publisher_only(
    scenario_environment, variant: str
) -> None:
    manager, handle = scenario_environment(variant, "crash_after_publish_ack")
    evidence = run_scenario(ScenarioContext(manager, handle), handle.scenario_id)
    assert evidence.evidence_complete, evidence.errors
    assert len(evidence.receipts) == 1 and evidence.input_end_offsets == {0: 1}
    restart = evidence.restart
    assert restart is not None and restart.role == "publisher" and restart.exit_code == 86
    assert restart.failpoint is not None and restart.failpoint.state == "fired"
    assert (
        restart.previous_database is not None
        and restart.previous_database.outbox[0].published_at is None
    )
    assert restart.previous_output is not None and len(restart.previous_output) == 1
    assert evidence.database is not None and len(evidence.database.payments) == 1
    assert evidence.database.outbox[0].published_at is not None
    assert evidence.output_records is not None and len(evidence.output_records) >= 2
    first = restart.previous_output[0]
    assert first == evidence.output_records[0]
    assert first.completion is not None
    assert all(item.completion == first.completion for item in evidence.output_records)
    fired = [item for item in evidence.timeline if item.event == "failpoint_fired"]
    assert len(fired) == 1 and fired[0].details["token"] == restart.failpoint.token
    acks = [item for item in evidence.timeline if item.event == "publish_acknowledged"]
    assert {item.details["process_instance_id"] for item in acks} == {
        restart.old_instance_id,
        restart.new_instance_id,
    }
