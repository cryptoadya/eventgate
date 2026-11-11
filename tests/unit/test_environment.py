import socket
import subprocess
from importlib.resources import files
from pathlib import Path

import pytest
import yaml

from eventgate.config import VerifyConfig
from eventgate.environment import CleanupError, DockerError, EnvironmentManager


def test_compose_is_packaged_and_uses_loopback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    spec = yaml.safe_load(files("eventgate").joinpath("resources/compose.yaml").read_text())
    for service in ("kafka", "postgres"):
        assert spec["services"][service]["ports"][0].startswith("127.0.0.1:")
        assert spec["services"][service]["pull_policy"] == "never"
    assert set(spec["volumes"]) == {
        "kafka-data",
        "kafka-secrets",
        "kafka-config",
        "postgres-data",
        "consumer-control",
        "publisher-control",
    }


def test_distinct_environment_identifiers(verify_config: VerifyConfig) -> None:
    manager = EnvironmentManager("example")
    first = manager.create(verify_config, "baseline", "normal")
    second = manager.create(verify_config, "candidate", "normal")
    assert first.project != second.project
    assert first.consumer_group != second.consumer_group
    assert first.volumes.keys() == second.volumes.keys()
    assert set(first.volumes.values()).isdisjoint(second.volumes.values())
    assert first.password != second.password
    assert first.password not in repr(first)
    assert first.health_topic not in {
        verify_config.kafka.input_topic,
        verify_config.kafka.output_topic,
    }


def test_bind_retries_are_bounded_and_cleaned(
    verify_config: VerifyConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = EnvironmentManager("example")
    handle = manager.create(verify_config, "baseline", "normal")
    attempts: list[tuple[int, int]] = []
    cleaned: list[str] = []

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if args[1] == "logs":
            return subprocess.CompletedProcess(args, 0, "test logs", "")
        attempts.append((handle.kafka_port, handle.postgres_port))
        raise DockerError("Docker port bind failed", stderr="port is already allocated")

    def destroy(target: object, **kwargs: object) -> None:
        assert target is handle
        cleaned.append(handle.project)

    monkeypatch.setattr(manager, "_compose", run)
    monkeypatch.setattr(manager, "destroy", destroy)
    with pytest.raises(DockerError, match="bind"):
        manager.start_infrastructure(handle)
    assert len(attempts) == len(cleaned) == 4
    assert all(kafka != postgres for kafka, postgres in attempts)


def test_cleanup_checks_owner_and_reports_remaining_resources(
    verify_config: VerifyConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = EnvironmentManager("example")
    handle = manager.create(verify_config, "baseline", "normal")
    handle.attempted = True
    owned = f"{handle.project}_kafka-data"
    unrelated = "unrelated-volume"
    removed: list[str] = []

    def docker(*args: str, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if args[:2] == ("volume", "ls"):
            return subprocess.CompletedProcess(
                args, 0, f"{owned}\t{handle.run_id}\n{unrelated}\tforeign\n", ""
            )
        if args[:2] == ("volume", "rm"):
            removed.extend(args[2:])
            raise DockerError("Volume is still in use")
        assert args[:2] in {("container", "ls"), ("network", "ls")}
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(manager, "_docker", docker)
    with pytest.raises(CleanupError) as exc:
        manager.destroy(handle)
    assert removed == [owned]
    assert exc.value.remaining == (f"volume:{owned}", f"volume:{unrelated}")


def test_ports_are_bound_on_loopback_and_distinct() -> None:
    kafka, postgres = EnvironmentManager._allocate_ports()
    with socket.socket() as first, socket.socket() as second:
        first.bind(("127.0.0.1", kafka))
        second.bind(("127.0.0.1", postgres))
