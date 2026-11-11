"""Typed wire contract for the test-only payment-v1 control API."""

import re
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, model_validator

from eventgate.config import ScenarioID

type Role = Literal["consumer", "publisher"]
type WorkerState = Literal["starting", "ready", "paused", "error"]
type FailpointName = Literal[
    "after_db_commit_before_offset_commit", "after_publish_ack_before_outbox_mark"
]
type FailpointState = Literal["armed", "fired"]

ROLE_FAILPOINT: dict[Role, FailpointName] = {
    "consumer": "after_db_commit_before_offset_commit",
    "publisher": "after_publish_ack_before_outbox_mark",
}


def _uuid(value: str) -> str:
    if str(UUID(value)) != value:
        raise ValueError("Expected a canonical UUID")
    return value


type UUIDString = Annotated[str, AfterValidator(_uuid)]


class _ControlModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class Dependencies(_ControlModel):
    kafka: bool
    postgres: bool


class WorkerStatus(_ControlModel):
    contract_version: Literal["payment-v1"]
    role: Role
    state: WorkerState
    run_id: str
    scenario_id: ScenarioID
    process_instance_id: UUIDString
    dependencies: Dependencies


class ArmRequest(_ControlModel):
    token: UUIDString
    name: FailpointName
    target_id: str

    @model_validator(mode="after")
    def target_matches_stage(self) -> "ArmRequest":
        pattern = "[A-Za-z0-9_-]{1,128}"
        if self.name == ROLE_FAILPOINT["publisher"]:
            pattern = "payment\\.completed:" + pattern
        if not re.fullmatch(pattern, self.target_id):
            raise ValueError("target_id does not match the failpoint")
        return self


class ArmResponse(_ControlModel):
    token: UUIDString
    state: FailpointState


class FailpointReceipt(ArmRequest):
    contract_version: Literal["payment-v1"]
    run_id: str
    scenario_id: ScenarioID
    role: Role
    process_instance_id: UUIDString
    state: FailpointState
