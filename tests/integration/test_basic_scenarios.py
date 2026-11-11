import pytest

from eventgate.scenarios import ScenarioContext, run_scenario


@pytest.mark.parametrize("variant", ["stable", "buggy"])
@pytest.mark.parametrize("scenario", ["normal", "duplicate_delivery", "consumer_restart"])
def test_basic_scenarios_collect_real_effects(
    scenario_environment, variant: str, scenario: str
) -> None:
    manager, handle = scenario_environment(variant, scenario)
    evidence = run_scenario(ScenarioContext(manager, handle), scenario)
    assert evidence.evidence_complete, evidence.errors
    assert not evidence.errors
    assert evidence.database is not None and evidence.output_records is not None
    count = (
        2
        if scenario == "consumer_restart"
        or (scenario == "duplicate_delivery" and variant == "buggy")
        else 1
    )
    assert len(evidence.database.payments) == count
    events = 2 if scenario == "consumer_restart" else 1
    assert (
        len(evidence.database.inbox)
        == len(evidence.database.outbox)
        == len(evidence.output_records)
        == events
    )
    assert all(row.published_at is not None for row in evidence.database.outbox)
    expected = 1 if scenario == "normal" else 2
    assert [receipt.offset for receipt in evidence.receipts] == list(range(expected))
    assert evidence.committed_offsets == {0: expected}
    timeline = [item.event for item in evidence.timeline]
    assert timeline.index("worker_paused") > max(
        i for i, event in enumerate(timeline) if event == "input_confirmed"
    )
    if scenario == "duplicate_delivery":
        assert evidence.receipts[0].payload == evidence.receipts[1].payload
        assert timeline.index("first_input_processed") < timeline.index(
            "input_confirmed", 1 + timeline.index("input_confirmed")
        )
    if scenario == "consumer_restart":
        assert evidence.restart is not None
        assert evidence.restart.role == "consumer" and evidence.restart.exit_code == 0
        assert evidence.restart.old_instance_id != evidence.restart.new_instance_id
        assert evidence.restart.previous_database is not None
        assert evidence.restart.previous_database.payments == evidence.database.payments[:1]
