import os
import shutil
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import pytest

from eventgate.config import ScenarioID, VerifyConfig, load_config
from eventgate.environment import EnvironmentHandle, EnvironmentManager

ROOT = Path(__file__).resolve().parents[1]


def _fixture_config(monkeypatch: pytest.MonkeyPatch) -> VerifyConfig:
    for key, value in (
        ("EVENTGATE_BASELINE_IMAGE", "eventgate-payment:stable"),
        ("EVENTGATE_STABLE_IMAGE", "eventgate-payment:stable"),
        ("EVENTGATE_KAFKA_IMAGE", "apache/kafka:4.3.2@sha256:" + "a" * 64),
        ("EVENTGATE_POSTGRES_IMAGE", "postgres:17.11-bookworm@sha256:" + "b" * 64),
    ):
        if key not in os.environ:
            monkeypatch.setenv(key, value)
    return load_config(ROOT / "examples/stable-vs-stable.yaml")


@pytest.fixture
def verify_config(monkeypatch: pytest.MonkeyPatch) -> VerifyConfig:
    return _fixture_config(monkeypatch)


@pytest.fixture
def docker_ready() -> None:
    if shutil.which("docker") is None:
        pytest.fail("Docker CLI and Engine are required to run integration tests", pytrace=False)
    for args in (["docker", "version"], ["docker", "compose", "version"]):
        result = subprocess.run(args, capture_output=True, text=True, timeout=15, check=False)
        if result.returncode:
            pytest.fail(f"{' '.join(args)} failed: {result.stderr.strip()}", pytrace=False)


@pytest.fixture
def integration_config(docker_ready: None, monkeypatch: pytest.MonkeyPatch) -> VerifyConfig:
    for line in (ROOT / "config/infrastructure.env.example").read_text().splitlines():
        if line and not line.startswith("#"):
            name, value = line.split("=", 1)
            if name not in os.environ:
                monkeypatch.setenv(name, value)
    return _fixture_config(monkeypatch)


@pytest.fixture
def environments(
    integration_config: VerifyConfig, tmp_path: Path
) -> Iterator[tuple[EnvironmentManager, list[EnvironmentHandle]]]:
    manager = EnvironmentManager(f"test-{uuid4().hex}")
    handles: list[EnvironmentHandle] = []
    yield manager, handles
    failures: list[Exception] = []
    for handle in reversed(handles):
        try:
            if handle.attempted:
                logs = manager.collect_logs(handle)
                (tmp_path / f"{handle.project}.log").write_text(
                    logs.replace(handle.password, "[REDACTED]"), encoding="utf-8"
                )
        except Exception as error:
            failures.append(error)
        finally:
            try:
                manager.destroy(handle)
            except Exception as error:
                failures.append(error)
    if failures:
        raise ExceptionGroup("Integration environment teardown failed", failures)


def _demo_image(variant: str) -> Iterator[str]:
    tag = f"eventgate-test-{variant}:{uuid4().hex}"
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    source_revision = revision.stdout.strip() if revision.returncode == 0 else "unknown"
    source_status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    source_state = (
        "unknown" if source_status.returncode else ("dirty" if source_status.stdout else "clean")
    )
    subprocess.run(
        [
            "docker",
            "build",
            "-f",
            "docker/payment.Dockerfile",
            "--build-arg",
            f"PAYMENT_VARIANT={variant}",
            "--build-arg",
            f"SOURCE_REVISION={source_revision}",
            "--build-arg",
            f"SOURCE_STATE={source_state}",
            "-t",
            tag,
            ".",
        ],
        cwd=ROOT,
        check=True,
        timeout=360,
    )
    try:
        yield subprocess.check_output(
            ["docker", "image", "inspect", "--format", "{{.Id}}", tag],
            text=True,
            timeout=10,
        ).strip()
    finally:
        subprocess.run(["docker", "image", "rm", tag], check=True, timeout=20, capture_output=True)


@pytest.fixture(scope="session")
def stable_image() -> Iterator[str]:
    yield from _demo_image("stable")


@pytest.fixture(scope="session")
def buggy_image() -> Iterator[str]:
    yield from _demo_image("buggy")


@pytest.fixture
def running_payment(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    request: pytest.FixtureRequest,
) -> tuple[EnvironmentManager, EnvironmentHandle]:
    image: str = request.getfixturevalue("stable_image")
    manager, handles = environments
    handle = manager.create(integration_config, "baseline", "normal")
    handles.append(handle)
    manager.start_infrastructure(handle)
    manager.start_application(handle, image_id=image)
    return manager, handle


@pytest.fixture
def scenario_environment(
    integration_config: VerifyConfig,
    environments: tuple[EnvironmentManager, list[EnvironmentHandle]],
    request: pytest.FixtureRequest,
) -> Callable[[str, ScenarioID], tuple[EnvironmentManager, EnvironmentHandle]]:
    def start(variant: str, scenario: ScenarioID) -> tuple[EnvironmentManager, EnvironmentHandle]:
        image: str = request.getfixturevalue(f"{variant}_image")
        manager, handles = environments
        handle = manager.create(integration_config, "baseline", scenario)
        handles.append(handle)
        manager.start_infrastructure(handle)
        manager.start_application(handle, image_id=image)
        return manager, handle

    return start
