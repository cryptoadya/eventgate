from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from eventgate.contracts.control import ArmRequest
from eventgate_demo.failpoints import FailpointConflict, FailpointStore


def test_token_is_durable_and_fires_only_once(tmp_path: Path) -> None:
    token, first, second = (str(uuid4()) for _ in range(3))
    request = ArmRequest(
        token=token, name="after_db_commit_before_offset_commit", target_id="evt-1001"
    )
    path = tmp_path / "failpoint.json"
    store = FailpointStore(path, "demo", "normal", "consumer", first)
    assert store.arm(request).state == "armed"
    assert store.arm(request).state == "armed"
    assert store.fire("evt-1002") is None
    with pytest.raises(FailpointConflict):
        store.arm(request.model_copy(update={"target_id": "evt-1002"}))
    receipt = store.fire("evt-1001")
    assert receipt is not None and receipt.state == "fired" and receipt.process_instance_id == first
    restarted = FailpointStore(path, "demo", "normal", "consumer", second)
    assert restarted.arm(request) == receipt
    assert restarted.fire("evt-1001") is None
    with pytest.raises(FailpointConflict):
        restarted.arm(request.model_copy(update={"token": str(uuid4())}))


def test_wrong_role_or_invalid_token_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="token"):
        ArmRequest(token="wrong", name="after_db_commit_before_offset_commit", target_id="evt-1001")
    store = FailpointStore(tmp_path / "failpoint.json", "run", "normal", "publisher", str(uuid4()))
    with pytest.raises(FailpointConflict):
        store.arm(
            ArmRequest(
                token=str(uuid4()),
                name="after_db_commit_before_offset_commit",
                target_id="evt-1001",
            )
        )


def test_invalid_stored_state_cannot_silently_rearm(tmp_path: Path) -> None:
    path = tmp_path / "failpoint.json"
    path.write_text('{"state":"unknown"}')
    with pytest.raises(ValueError, match="state"):
        FailpointStore(path, "demo", "normal", "consumer", str(uuid4()))
