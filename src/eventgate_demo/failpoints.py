"""Durably arm and consume a single failpoint on a dedicated control volume."""

import os
import tempfile
from pathlib import Path
from threading import Lock

from eventgate.config import ScenarioID
from eventgate.contracts.control import ROLE_FAILPOINT, ArmRequest, FailpointReceipt, Role
from eventgate.contracts.payment import parse_json_object


class FailpointConflict(ValueError):
    """An arm request conflicts with this role or its existing token."""


class FailpointStore:
    def __init__(
        self, path: Path, run_id: str, scenario_id: ScenarioID, role: Role, instance: str
    ) -> None:
        self.path = path
        self.run_id = run_id
        self.scenario_id = scenario_id
        self.role = role
        self.instance = instance
        self._lock = Lock()
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            self._receipt: FailpointReceipt | None = None
        else:
            receipt = FailpointReceipt.model_validate(parse_json_object(payload))
            if (
                receipt.run_id != run_id
                or receipt.scenario_id != scenario_id
                or receipt.role != role
                or receipt.name != ROLE_FAILPOINT[role]
            ):
                raise ValueError(
                    "Stored failpoint state does not belong to this environment and role"
                )
            self._receipt = receipt

    def _store(self, receipt: FailpointReceipt) -> None:
        fd, temp_name = tempfile.mkstemp(prefix=".failpoint-", dir=self.path.parent)
        temp = Path(temp_name)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(receipt.model_dump_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temp.unlink(missing_ok=True)
        self._receipt = receipt

    def arm(self, request: ArmRequest) -> FailpointReceipt:
        with self._lock:
            if request.name != ROLE_FAILPOINT[self.role]:
                raise FailpointConflict("Failpoint is not supported by this role")
            current = self._receipt
            if current is not None:
                if (current.token, current.name, current.target_id) != (
                    request.token,
                    request.name,
                    request.target_id,
                ):
                    raise FailpointConflict("A different failpoint is already recorded")
                return current
            receipt = FailpointReceipt(
                **request.model_dump(),
                contract_version="payment-v1",
                role=self.role,
                run_id=self.run_id,
                scenario_id=self.scenario_id,
                process_instance_id=self.instance,
                state="armed",
            )
            self._store(receipt)
            return receipt

    def fire(self, target_id: str) -> FailpointReceipt | None:
        with self._lock:
            current = self._receipt
            if current is None or current.state != "armed" or current.target_id != target_id:
                return None
            fired = current.model_copy(
                update={"state": "fired", "process_instance_id": self.instance}
            )
            self._store(fired)
            return fired
