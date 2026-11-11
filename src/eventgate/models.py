"""Facts collected while observing an isolated application run."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from eventgate.config import ScenarioID
from eventgate.contracts.control import FailpointReceipt, Role, WorkerStatus
from eventgate.contracts.payment import PaymentCompleted, PaymentRequested, SchemaV1
from eventgate.kafka_io import InputReceipt, KafkaRecord


@dataclass(frozen=True)
class PaymentRow:
    payment_id: int
    event_id: str
    order_id: str
    amount_minor: int
    currency: str


@dataclass(frozen=True)
class OutboxRow:
    completion_id: str
    source_event_id: str
    payload: JsonValue
    published_at: datetime | None


@dataclass(frozen=True)
class DatabaseSnapshot:
    inbox: list[str]
    payments: list[PaymentRow]
    outbox: list[OutboxRow]


@dataclass(frozen=True)
class ObservedCompletion:
    record: KafkaRecord
    completion: PaymentCompleted | None
    error: str | None = None


@dataclass(frozen=True)
class TimelineEntry:
    at: datetime
    source: Literal["runner", "application"]
    event: str
    details: dict[str, JsonValue]


@dataclass(frozen=True)
class ScenarioIssue:
    code: str
    stage: str
    message: str


@dataclass(frozen=True)
class RestartEvidence:
    role: Role
    exit_code: int
    old_instance_id: str
    new_instance_id: str
    previous_database: DatabaseSnapshot | None
    previous_offsets: dict[int, int | None] | None
    failpoint: FailpointReceipt | None = None
    previous_output: list[ObservedCompletion] | None = None


@dataclass
class ScenarioEvidence:
    scenario_id: ScenarioID
    inputs: list[PaymentRequested]
    receipts: list[InputReceipt] = field(default_factory=list)
    database: DatabaseSnapshot | None = None
    committed_offsets: dict[int, int | None] | None = None
    input_end_offsets: dict[int, int] | None = None
    output_end_offsets: dict[int, int] | None = None
    output_records: list[ObservedCompletion] | None = None
    paused_workers: dict[Role, WorkerStatus] = field(default_factory=dict)
    timeline: list[TimelineEntry] = field(default_factory=list)
    evidence_complete: bool = False
    progress_observed: bool = False
    restart: RestartEvidence | None = None
    errors: list[ScenarioIssue] = field(default_factory=list)

    def note(
        self,
        event: str,
        *,
        source: Literal["runner", "application"] = "runner",
        **details: JsonValue,
    ) -> None:
        self.timeline.append(TimelineEntry(datetime.now(UTC), source, event, details))


class Verdict(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"

    @property
    def exit_code(self) -> int:
        return {Verdict.PASS: 0, Verdict.FAIL: 1, Verdict.ERROR: 2}[self]


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    NOT_EVALUATED = "NOT_EVALUATED"


class ExecutionState(StrEnum):
    COMPLETED = "COMPLETED"
    ABORTED = "ABORTED"
    NOT_RUN = "NOT_RUN"


class _ReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CheckResult(_ReportModel):
    id: str
    status: CheckStatus
    explanation: str
    expected: JsonValue = None
    actual: JsonValue = None
    evidence: list[str] = Field(default_factory=list)


class BusinessPayment(_ReportModel):
    event_id: str
    order_id: str
    amount_minor: int
    currency: str


class BusinessOutbox(_ReportModel):
    completion_id: str
    source_event_id: str
    payload: JsonValue
    published: bool


class BusinessSnapshot(_ReportModel):
    inbox: list[str]
    payments: list[BusinessPayment]
    outbox: list[BusinessOutbox]
    completions: dict[str, list[dict[str, JsonValue]]]
    physical_output_count: int
    logical_output_count: int


class ScenarioResult(_ReportModel):
    scenario_id: ScenarioID
    execution_state: ExecutionState
    evidence_complete: bool
    verdict: Verdict
    reason_codes: list[str] = Field(default_factory=list)
    checks: list[CheckResult] = Field(default_factory=list)
    errors: list[ScenarioIssue] = Field(default_factory=list)
    business: BusinessSnapshot | None = None
    timeline: list[TimelineEntry] = Field(default_factory=list)
    artifacts: dict[str, str] = Field(default_factory=dict)


class Difference(_ReportModel):
    path: str
    baseline: JsonValue
    candidate: JsonValue


class ComparisonResult(_ReportModel):
    scenario_id: ScenarioID
    verdict: Verdict
    reason: str
    diff_status: CheckStatus
    differences: list[Difference] = Field(default_factory=list)


class ImageInfo(_ReportModel):
    reference: str
    image_id: str
    repo_digests: list[str] = Field(default_factory=list)
    architecture: str
    os: str
    source_revision: str | None = None
    source_state: str = "unknown"


type VersionRole = Literal["baseline", "candidate"]


class CleanupResult(_ReportModel):
    environment_id: str
    project: str
    success: bool
    remaining: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class RunResult(_ReportModel):
    schema_version: SchemaV1 = 1
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    runner_provenance: dict[str, JsonValue] = Field(default_factory=dict)
    mode: Literal["release", "development"] | None = None
    selection_coverage: Literal["full", "partial"] | None = None
    evidence_complete: bool = False
    verdict: Verdict = Verdict.ERROR
    exit_code: int = 2
    release_eligible: bool = False
    interrupted: bool = False
    effective_config: dict[str, JsonValue] | None = None
    config_sha256: str | None = None
    image_metadata: dict[str, ImageInfo] = Field(default_factory=dict)
    selected_scenarios: list[ScenarioID] = Field(default_factory=list)
    input_manifest: dict[str, list[dict[str, JsonValue]]] = Field(default_factory=dict)
    versions: dict[VersionRole, list[ScenarioResult]] = Field(default_factory=dict)
    comparisons: list[ComparisonResult] = Field(default_factory=list)
    cleanup: list[CleanupResult] = Field(default_factory=list)
    errors: list[ScenarioIssue] = Field(default_factory=list)
    artifacts: dict[str, str] = Field(default_factory=dict)
