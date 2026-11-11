import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from eventgate.models import RunResult, ScenarioIssue, Verdict
from eventgate.reports import write_reports


@pytest.fixture
def offline_run(tmp_path: Path) -> Path:
    data = tmp_path / "run"
    data.mkdir()
    (data / "evidence.txt").write_text("Saved evidence")
    result = RunResult(
        run_id="example",
        started_at=datetime.now(UTC),
        verdict=Verdict.FAIL,
        exit_code=1,
        artifacts={"evidence": "evidence.txt"},
        errors=[ScenarioIssue("example_failure", "test", "Expected application failure")],
    )
    write_reports(result, data)
    return data


def render(path: Path, cwd: Path):
    return subprocess.run(
        [str(Path(sys.executable).with_name("eventgate")), "report", "--input", str(path)],
        cwd=cwd,
        env={**os.environ, "DOCKER_HOST": "unix:///absent.sock"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_offline_preserves_original_failure_and_renders_new_files(
    tmp_path: Path, offline_run: Path
) -> None:
    original = (offline_run / "report.json").read_bytes()
    assert render(offline_run, tmp_path).returncode == 0
    assert render(offline_run, tmp_path).returncode == 0
    folders = list((offline_run / "rendered").iterdir())
    assert len(folders) == 2
    for folder in folders:
        assert "FAIL" in (folder / "report.html").read_text()
        assert (folder / "junit.xml").is_file()
    assert (offline_run / "report.json").read_bytes() == original


@pytest.mark.parametrize("fault", ["unsupported", "corrupt", "missing", "blocked"])
def test_offline_reports_errors(tmp_path: Path, offline_run: Path, fault: str) -> None:
    path = offline_run / "report.json"
    if fault == "unsupported":
        payload = json.loads(path.read_text())
        payload["schema_version"] = 9
        path.write_text(json.dumps(payload))
    elif fault == "corrupt":
        path.write_text("{")
    elif fault == "missing":
        (offline_run / "evidence.txt").unlink()
    else:
        (offline_run / "rendered").write_text("not a directory")
    process = render(offline_run, tmp_path)
    assert process.returncode == 2 and process.stderr
