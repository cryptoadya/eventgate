import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CLI = Path(sys.executable).with_name("eventgate")


def make_config(tmp_path, integration_config, baseline, candidate, mode="release", selected=None):
    data = integration_config.model_dump(mode="json")
    data["baseline"]["image"] = baseline
    data["candidate"]["image"] = candidate
    data["artifacts"]["directory"] = str(tmp_path / "output")
    data["mode"] = mode
    if selected is not None:
        data["scenarios"] = selected
    path = tmp_path / "verify.yaml"
    path.write_text(yaml.safe_dump(data))
    return path, tmp_path / "output"


def run_verify(path: Path, cwd: Path, *, env: dict[str, str] | None = None):
    return subprocess.run(
        [str(CLI), "verify", "--config", str(path)],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=360,
        check=False,
        env=env,
    )


def read_report(output: Path):
    [report] = output.glob("*/report.json")
    return json.loads(report.read_text())


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [("stable", "stable", 0), ("stable", "buggy", 1), ("buggy", "stable", 1)],
)
def test_release_cli_gate(integration_config, request, tmp_path, left, right, expected):
    path, output = make_config(
        tmp_path,
        integration_config,
        request.getfixturevalue(f"{left}_image"),
        request.getfixturevalue(f"{right}_image"),
    )
    process = run_verify(path, tmp_path)
    assert process.returncode == expected, (
        process.stdout,
        process.stderr,
        read_report(output).get("errors"),
    )
    report = read_report(output)
    assert report["release_eligible"] is (expected == 0)
    assert report["evidence_complete"] is True
    assert len(report["cleanup"]) == 10 and all(item["success"] for item in report["cleanup"])
    assert len(report["versions"]["candidate"]) == 5
    failing = {item["scenario_id"] for item in report["comparisons"] if item["verdict"] == "FAIL"}
    assert failing == (set() if expected == 0 else {"duplicate_delivery", "crash_after_db_commit"})
    if left == "buggy":
        assert all(
            item["reason"] == "invalid_baseline_candidate_improved"
            for item in report["comparisons"]
            if item["verdict"] == "FAIL"
        )


def test_development_subset_does_not_pass_release_gate(integration_config, stable_image, tmp_path):
    path, output = make_config(
        tmp_path, integration_config, stable_image, stable_image, "development", ["normal"]
    )
    release_subset = yaml.safe_load(path.read_text())
    release_subset["mode"] = "release"
    rejected = tmp_path / "release-subset.yaml"
    rejected.write_text(yaml.safe_dump(release_subset))
    refused = run_verify(rejected, tmp_path)
    assert refused.returncode == 2
    assert "Release mode requires all five scenarios" in refused.stderr
    assert not output.exists()
    process = run_verify(path, tmp_path)
    assert process.returncode == 0, process.stderr
    report = read_report(output)
    assert report["mode"] == "development" and report["selection_coverage"] == "partial"
    assert report["release_eligible"] is False
    assert "NOT A RELEASE GATE" in process.stdout


@pytest.mark.parametrize("content", ["mode: release\nscenarios: [normal]\n", "invalid: ["])
def test_invalid_config_writes_diagnostic(tmp_path, content):
    path = tmp_path / "config.yaml"
    path.write_text(content)
    process = run_verify(path, tmp_path)
    assert process.returncode == 2
    report = read_report(tmp_path / "results")
    assert report["verdict"] == "ERROR" and report["errors"][0]["code"] == "configuration_error"


def test_unavailable_engine_and_unwritable_output_have_no_side_effects(
    integration_config, tmp_path
):
    path, output = make_config(
        tmp_path,
        integration_config,
        "eventgate:stable",
        "eventgate:stable",
        "development",
        ["normal"],
    )
    env = {**os.environ, "DOCKER_HOST": f"unix://{tmp_path}/absent.sock"}
    process = run_verify(path, tmp_path, env=env)
    assert process.returncode == 2
    report = read_report(output)
    assert report["cleanup"] == []
    assert report["versions"]["baseline"][0]["execution_state"] == "NOT_RUN"
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("keep")
    data = yaml.safe_load(path.read_text())
    data["artifacts"]["directory"] = str(blocked)
    path.write_text(yaml.safe_dump(data))
    process = run_verify(path, tmp_path, env=env)
    assert process.returncode == 2
    assert "output" in process.stderr.lower()
    assert blocked.read_text() == "keep"


def test_cleanup_failure_stops_new_runs_and_returns_two(
    integration_config, stable_image, tmp_path, monkeypatch
):
    from eventgate.environment import CleanupError, EnvironmentManager
    from eventgate.runner import verify

    config = integration_config.model_copy(deep=True)
    config.baseline.image = config.candidate.image = stable_image
    config.artifacts.directory = tmp_path / "output"
    config.mode = "development"
    config.scenarios = ["normal"]
    original = EnvironmentManager.destroy

    def fail(self, handle):
        original(self, handle)
        raise CleanupError(("volume:test-remaining",), ("Injected cleanup failure",))

    monkeypatch.setattr(EnvironmentManager, "destroy", fail)
    report = verify(config)
    assert report.exit_code == 2
    assert report.cleanup[0].remaining == ["volume:test-remaining"]
    assert report.versions["candidate"][0].execution_state == "NOT_RUN"


def test_worker_failure_with_healthy_dependencies_is_fail(
    integration_config, stable_image, tmp_path, monkeypatch
):
    import psycopg

    from eventgate.environment import EnvironmentManager
    from eventgate.runner import verify

    config = integration_config.model_copy(deep=True)
    config.baseline.image = config.candidate.image = stable_image
    config.artifacts.directory = tmp_path / "output"
    config.mode = "development"
    config.scenarios = ["normal"]
    config.timeouts.progress_seconds = 5
    original = EnvironmentManager.start_application

    def fail_baseline(self, handle, **kwargs):
        original(self, handle, **kwargs)
        if handle.version == "baseline":
            with psycopg.connect(handle.postgres_dsn) as conn:
                conn.execute("ALTER TABLE payments ADD CHECK (false)")

    monkeypatch.setattr(EnvironmentManager, "start_application", fail_baseline)
    report = verify(config)
    assert report.exit_code == 1, report.model_dump()
    bad = report.versions["baseline"][0]
    assert not bad.evidence_complete
    assert any(error.code == "application_progress" for error in bad.errors)
    assert report.versions["candidate"][0].verdict == "PASS"
