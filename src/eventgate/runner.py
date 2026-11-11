"""Run isolated version/scenario pairs and keep trustworthy evidence after failures."""

import hashlib
import json
import platform
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Literal
from uuid import uuid4

from eventgate.adapters.payment_v1 import ControlError, PaymentV1Adapter
from eventgate.artifacts import ArtifactError, ArtifactStore
from eventgate.compare import finalize_run
from eventgate.config import ComparisonPolicy, ScenarioID, VerifyConfig
from eventgate.environment import CleanupError, DockerError, EnvironmentHandle, EnvironmentManager
from eventgate.invariants import evaluate
from eventgate.kafka_io import KafkaIO
from eventgate.models import (
    CleanupResult,
    ExecutionState,
    ImageInfo,
    RunResult,
    ScenarioEvidence,
    ScenarioIssue,
    ScenarioResult,
    Verdict,
    VersionRole,
)
from eventgate.normalize import canonical_json
from eventgate.reports import write_reports
from eventgate.scenarios import ScenarioContext, parse_worker_logs, run_scenario, scenario_inputs


@dataclass(frozen=True)
class ProgressDiagnosis:
    process_state_read: bool
    postgres_healthy: bool
    kafka_healthy: bool
    progress_reached: bool
    dependency_error: bool


def classify_progress(
    diagnosis: ProgressDiagnosis,
) -> Literal["application_progress", "observation_error"]:
    if (
        diagnosis.process_state_read
        and diagnosis.postgres_healthy
        and diagnosis.kafka_healthy
        and not diagnosis.progress_reached
        and not diagnosis.dependency_error
    ):
        return "application_progress"
    return "observation_error"


def _error_text(error: BaseException) -> str:
    if isinstance(error, DockerError) and error.stderr:
        return f"{error}: {error.stderr.strip()}"
    return f"{type(error).__name__}: {error}"


def _docker_versions(seconds: int) -> dict[str, str]:
    deadline = time.monotonic() + seconds
    engine = EnvironmentManager._docker(
        "version", "--format", "{{.Server.Version}}", deadline=deadline
    ).stdout.strip()
    compose = EnvironmentManager._docker(
        "compose", "version", "--short", deadline=deadline
    ).stdout.strip()
    if not engine or not compose:
        raise DockerError("Docker Engine or Compose version could not be confirmed")
    return {"docker_engine": engine, "docker_compose": compose}


def _image_info(reference: str, seconds: int) -> ImageInfo:
    try:
        inspected = EnvironmentManager._docker(
            "image",
            "inspect",
            "--format",
            "{{json .}}",
            reference,
            deadline=time.monotonic() + seconds,
        )
    except DockerError:
        raise DockerError(f"Image is not available locally: {reference}") from None
    data = json.loads(inspected.stdout)
    labels = (data.get("Config") or {}).get("Labels") or {}
    revision = labels.get("org.opencontainers.image.revision")
    if not isinstance(revision, str) or not re.fullmatch(
        r"[a-fA-F0-9]{40}|[a-fA-F0-9]{64}", revision
    ):
        revision = None
    state = labels.get("io.eventgate.source-state")
    return ImageInfo(
        reference=reference,
        image_id=data["Id"],
        architecture=data["Architecture"],
        os=data["Os"],
        repo_digests=data.get("RepoDigests") or [],
        source_revision=revision,
        source_state=state if state in ("clean", "dirty") else "unknown",
    )


def _references(config: VerifyConfig) -> dict[str, str]:
    return {
        "baseline": config.baseline.image,
        "candidate": config.candidate.image,
        "kafka": config.infrastructure.kafka_image,
        "postgres": config.infrastructure.postgres_image,
    }


def doctor(config: VerifyConfig | None) -> tuple[dict[str, str], list[str]]:
    info: dict[str, str] = {}
    problems: list[str] = []
    seconds = config.timeouts.control_seconds if config is not None else 10
    try:
        info.update(_docker_versions(seconds))
    except Exception as error:
        problems.append(_error_text(error))
        return info, problems
    if config is not None:
        for key, reference in _references(config).items():
            try:
                info[key] = _image_info(reference, seconds).image_id
            except Exception as error:
                problems.append(_error_text(error))
    return info, problems


def _diagnose_progress(
    manager: EnvironmentManager,
    handle: EnvironmentHandle,
    evidence: ScenarioEvidence,
    logs: str,
) -> None:
    indexes = [
        index for index, error in enumerate(evidence.errors) if error.code == "progress_unresolved"
    ]
    if not indexes:
        return
    adapter = PaymentV1Adapter(manager)
    kafka = KafkaIO(handle.kafka_bootstrap)
    deadline = time.monotonic() + handle.config.timeouts.control_seconds
    state = postgres = broker = progressed = False
    dependency_error = bool(
        re.search(
            r"KafkaException|DeliveryUncertain|OperationalError|connection refused|Connection reset|transport failure",
            logs,
            re.IGNORECASE,
        )
    )
    try:
        for role in ("consumer", "publisher"):
            container = manager.role_state(handle, role, deadline)
            evidence.note(
                "worker_state_observed",
                role=role,
                running=container.running,
                exit_code=container.exit_code,
            )
            if container.running:
                try:
                    observed = adapter.status(handle, role, deadline)
                except ControlError:
                    dependency_error = True
                else:
                    if not (observed.dependencies.kafka and observed.dependencies.postgres):
                        dependency_error = True
        state = True
        snapshot = adapter.read_database(handle, deadline)
        evidence.database = snapshot
        postgres = True
        topics = handle.config.kafka
        offsets = kafka.committed_offsets(handle.consumer_group, topics.input_topic, deadline)
        evidence.committed_offsets = offsets
        evidence.input_end_offsets = kafka.end_offsets(topics.input_topic, deadline)
        evidence.output_end_offsets = kafka.end_offsets(topics.output_topic, deadline)
        broker = True
        progressed = adapter.offsets_cover(offsets, evidence.receipts) and not any(
            row.published_at is None for row in snapshot.outbox
        )
    except Exception as error:
        evidence.note("diagnostic_observation_failed", error_type=type(error).__name__)
    code = classify_progress(
        ProgressDiagnosis(state, postgres, broker, progressed, dependency_error)
    )
    evidence.note(
        "progress_diagnosed",
        outcome=code,
        postgres_healthy=postgres,
        kafka_healthy=broker,
        process_state_read=state,
        dependency_error=dependency_error,
    )
    for index in indexes:
        original = evidence.errors[index]
        evidence.errors[index] = ScenarioIssue(code, original.stage, original.message)


def _partial_observations(
    manager: EnvironmentManager, handle: EnvironmentHandle, evidence: ScenarioEvidence
) -> None:
    if evidence.evidence_complete:
        return
    adapter = PaymentV1Adapter(manager)
    kafka = KafkaIO(handle.kafka_bootstrap)
    deadline = time.monotonic() + handle.config.timeouts.control_seconds
    topics = handle.config.kafka
    try:
        if evidence.database is None:
            evidence.database = adapter.read_database(handle, deadline)
    except Exception as error:
        evidence.errors.append(
            ScenarioIssue("observation_error", "partial_database", _error_text(error))
        )
    try:
        if evidence.output_end_offsets is None:
            evidence.output_end_offsets = kafka.end_offsets(topics.output_topic, deadline)
        if evidence.output_records is None:
            evidence.output_records = adapter.decode_records(
                kafka.read_until(topics.output_topic, evidence.output_end_offsets, deadline)
            )
        if evidence.committed_offsets is None:
            evidence.committed_offsets = kafka.committed_offsets(
                handle.consumer_group, topics.input_topic, deadline
            )
    except Exception as error:
        evidence.errors.append(
            ScenarioIssue("observation_error", "partial_kafka", _error_text(error))
        )


def _not_run(scenario: ScenarioID, reason: str) -> ScenarioResult:
    return ScenarioResult(
        scenario_id=scenario,
        execution_state=ExecutionState.NOT_RUN,
        evidence_complete=False,
        verdict=Verdict.ERROR,
        reason_codes=["not_run"],
        errors=[ScenarioIssue("not_run", "execution", reason)],
    )


def _run_pair(
    result: RunResult,
    config: VerifyConfig,
    manager: EnvironmentManager,
    store: ArtifactStore,
    role: VersionRole,
    scenario: ScenarioID,
) -> tuple[ScenarioResult, str | None]:
    handle = manager.create(config, role, scenario)
    store.redactor.add(handle.password)
    evidence = ScenarioEvidence(scenario_id=scenario, inputs=scenario_inputs(scenario))
    stage = "infrastructure_start"
    artifacts: dict[str, str] = {}
    stop: str | None = None

    def interrupted() -> None:
        nonlocal stop
        result.interrupted = True
        stop = "Interrupted by the user"
        evidence.evidence_complete = False
        if not any(error.code == "interrupted" for error in evidence.errors):
            evidence.errors.append(ScenarioIssue("interrupted", stage, stop))

    try:
        manager.start_infrastructure(handle)
        stage = "application_start"
        manager.start_application(handle, image_id=result.image_metadata[role].image_id)
        stage = "scenario"
        run_scenario(ScenarioContext(manager, handle), scenario, evidence=evidence)
    except KeyboardInterrupt:
        interrupted()
    except Exception as error:
        code = "contract_or_scenario" if stage == "application_start" else "environment_error"
        evidence.errors.append(ScenarioIssue(code, stage, _error_text(error)))
    finally:
        try:
            prefix = f"{role}/{scenario}"
            logs = ""
            try:
                logs = manager.collect_logs(handle)
                existing = set(
                    (item.at, item.event, repr(item.details))
                    for item in evidence.timeline
                    if item.source == "application"
                )
                evidence.timeline.extend(
                    item
                    for item in parse_worker_logs(logs)
                    if (item.at, item.event, repr(item.details)) not in existing
                )
            except Exception as error:
                evidence.errors.append(
                    ScenarioIssue("observation_error", "logs", _error_text(error))
                )
            if not result.interrupted:
                _diagnose_progress(manager, handle, evidence, logs)
            if stage == "scenario":
                _partial_observations(manager, handle, evidence)
            artifacts["logs"] = store.write_text(f"{prefix}/containers.log", logs)
            artifacts["evidence"] = store.write_json(f"{prefix}/evidence.json", evidence)
            ports: dict[str, int] = {"kafka": handle.kafka_port, "postgres": handle.postgres_port}
            for worker, port in handle.control_ports.items():
                ports[worker] = port
            artifacts["environment"] = store.write_json(
                f"{prefix}/environment.json",
                {
                    "environment_id": handle.environment_id,
                    "project": handle.project,
                    "consumer_group": handle.consumer_group,
                    "volumes": handle.volumes,
                    "ports": ports,
                    "startup_attempted": handle.attempted,
                },
            )
        except KeyboardInterrupt:
            interrupted()
        except Exception as error:
            stop = _error_text(error)
            result.errors.append(ScenarioIssue("artifact_error", "artifact_write", stop))
        finally:
            cleanup = CleanupResult(
                environment_id=handle.environment_id, project=handle.project, success=False
            )
            try:
                try:
                    manager.destroy(handle)
                except KeyboardInterrupt:
                    interrupted()
                    manager.destroy(handle)
                cleanup.success = True
            except CleanupError as error:
                cleanup.remaining = list(error.remaining)
                cleanup.errors = list(error.errors)
            except Exception as error:
                cleanup.errors = [_error_text(error)]
            result.cleanup.append(cleanup)
            if not cleanup.success:
                stop = "Environment cleanup failed"

    pair = evaluate(evidence)
    pair.artifacts = artifacts
    for check in pair.checks:
        if "evidence" in artifacts:
            check.evidence.append(artifacts["evidence"])
    if pair.verdict == Verdict.ERROR and stop is None:
        try:
            _docker_versions(config.timeouts.control_seconds)
        except Exception as error:
            stop = "Shared Docker Engine or Compose became unavailable"
            result.errors.append(
                ScenarioIssue("docker_unavailable", "preflight", _error_text(error))
            )
    return pair, stop


def _new_run(directory: Path) -> tuple[RunResult, ArtifactStore]:
    result = RunResult(
        run_id=uuid4().hex,
        started_at=datetime.now(UTC),
        runner_provenance={
            "version": version("eventgate"),
            "contract": "payment-v1",
            "system": platform.system(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
        },
    )
    return result, ArtifactStore(directory, result.run_id)


def _publish(result: RunResult, store: ArtifactStore, started: float) -> RunResult:
    result.finished_at = datetime.now(UTC)
    result.duration_seconds = time.monotonic() - started
    safe_result = RunResult.model_validate(store.redactor.json_value(result))
    try:
        write_reports(safe_result, store.directory)
    except ArtifactError as error:
        if safe_result.interrupted:
            raise InterruptedError(str(error)) from None
        raise
    return safe_result


def configuration_error(message: str, output: Path = Path("results")) -> RunResult:
    started = time.monotonic()
    result, store = _new_run(output)
    result.errors.append(ScenarioIssue("configuration_error", "config", message))
    return _publish(result, store, started)


def verify(config: VerifyConfig) -> RunResult:
    started = time.monotonic()
    result, store = _new_run(config.artifacts.directory)
    result.mode = config.mode
    result.selection_coverage = config.selection_coverage
    result.selected_scenarios = config.scenarios
    effective = config.model_dump(mode="json")
    result.effective_config = effective
    result.config_sha256 = hashlib.sha256(canonical_json(effective).encode()).hexdigest()
    result.input_manifest = {
        scenario: [request.model_dump(mode="json") for request in scenario_inputs(scenario)]
        for scenario in config.scenarios
    }
    manager = EnvironmentManager(result.run_id)
    stop: str | None = None
    try:
        result.runner_provenance.update(_docker_versions(config.timeouts.control_seconds))
        for key, reference in _references(config).items():
            result.image_metadata[key] = _image_info(reference, config.timeouts.control_seconds)
    except KeyboardInterrupt:
        result.interrupted = True
        stop = "Interrupted during preflight"
        result.errors.append(ScenarioIssue("interrupted", "preflight", stop))
    except Exception as error:
        stop = _error_text(error)
        result.errors.append(ScenarioIssue("preflight_error", "preflight", stop))

    for role in ("baseline", "candidate"):
        result.versions[role] = []
        for scenario in config.scenarios:
            if stop is not None:
                result.versions[role].append(_not_run(scenario, stop))
                continue
            try:
                pair, stop = _run_pair(result, config, manager, store, role, scenario)
            except KeyboardInterrupt:
                result.interrupted = True
                stop = "Interrupted by the user"
                pair = _not_run(scenario, stop)
            result.versions[role].append(pair)
    policy: ComparisonPolicy = config.comparison
    finalize_run(result, policy)
    return _publish(result, store, started)
