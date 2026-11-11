import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree

import pytest

from eventgate import reports
from eventgate.models import (
    CheckResult,
    CheckStatus,
    ExecutionState,
    RunResult,
    ScenarioIssue,
    ScenarioResult,
    Verdict,
)


@pytest.mark.parametrize(
    ("verdict", "element"), [(Verdict.FAIL, "failure"), (Verdict.ERROR, "error")]
)
def test_formats_share_verdict_and_escape_payload(
    tmp_path: Path, verdict: Verdict, element: str
) -> None:
    payload = '<script>alert("data")</script>&\x01'
    run = RunResult(
        run_id="run",
        started_at=datetime.now(UTC),
        verdict=verdict,
        exit_code=verdict.exit_code,
        versions={
            "candidate": [
                ScenarioResult(
                    scenario_id="normal",
                    execution_state=ExecutionState.COMPLETED,
                    evidence_complete=True,
                    verdict=verdict,
                    checks=[
                        CheckResult(
                            id="check", status=CheckStatus(verdict.value), explanation=payload
                        )
                    ],
                )
            ]
        },
    )
    result = reports.write_reports(run, tmp_path)
    assert result.html is not None and result.junit is not None and result.archive is not None
    assert "<script>" not in result.html.read_text() and "&lt;script&gt;" in result.html.read_text()
    assert ElementTree.parse(result.junit).find(f".//{element}") is not None
    with zipfile.ZipFile(result.archive) as archive:
        assert archive.read("report.json") == result.json.read_bytes()
        assert "diagnostics.zip" not in archive.namelist()
        assert all(not item.startswith("rendered/") for item in archive.namelist())
    assert json.loads(result.json.read_text())["verdict"] == verdict


@pytest.mark.parametrize("failed", ["junit.xml", "diagnostics.zip", "report.json"])
def test_failure_during_publication_never_leaves_a_final_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed: str,
) -> None:
    initial = Verdict.FAIL if failed == "diagnostics.zip" else Verdict.PASS
    run = RunResult(
        run_id="run",
        started_at=datetime.now(UTC),
        verdict=initial,
        exit_code=initial.exit_code,
        release_eligible=initial == Verdict.PASS,
    )
    original = reports.atomic_write
    writes = []

    def inject(path: Path, content: bytes) -> None:
        writes.append(path.name)
        if path.name == failed and writes.count(failed) == 1:
            assert (tmp_path / "report.html").is_file()
            assert not (tmp_path / "report.json").exists()
            raise OSError("Injected write error")
        original(path, content)

    monkeypatch.setattr(reports, "atomic_write", inject)
    paths = reports.write_reports(run, tmp_path)
    saved = json.loads(paths.json.read_text())
    assert saved["verdict"] == "ERROR" and saved["exit_code"] == 2 and not saved["release_eligible"]
    assert saved["errors"][-1]["code"] == "report_error"
    assert paths.html is not None and paths.junit is not None and paths.archive is not None
    html = paths.html.read_text()
    assert "<title>EventGate report — ERROR</title>" in html
    assert "Release eligible: <strong>no</strong>" in html
    assert ElementTree.parse(paths.junit).find(".//error") is not None
    with zipfile.ZipFile(paths.archive) as archive:
        assert archive.read("report.json") == paths.json.read_bytes()
        assert archive.read("report.html") == paths.html.read_bytes()
        assert archive.read("junit.xml") == paths.junit.read_bytes()
    assert writes[-1] == "report.json"


@pytest.mark.parametrize("blocked", ["junit.xml", "report.html"])
def test_recovery_omits_views_that_still_cannot_be_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked: str
) -> None:
    run = RunResult(
        run_id="run",
        started_at=datetime.now(UTC),
        verdict=Verdict.PASS,
        exit_code=0,
        release_eligible=True,
    )
    original = reports.atomic_write
    writes: list[str] = []

    def inject(path: Path, content: bytes) -> None:
        writes.append(path.name)
        if path.name == "junit.xml" and writes.count("junit.xml") == 1:
            raise OSError("Initial report publication failure")
        if path.name == blocked and writes.count(blocked) > 1:
            raise OSError("Report remains unavailable")
        original(path, content)

    monkeypatch.setattr(reports, "atomic_write", inject)
    paths = reports.write_reports(run, tmp_path)
    saved = json.loads(paths.json.read_text())
    assert saved["verdict"] == "ERROR"
    assert blocked not in saved["artifacts"].values()
    assert not (tmp_path / blocked).exists()
    if blocked == "report.html":
        assert paths.html is None
    else:
        assert paths.junit is None
        assert paths.html is not None
        assert "Release eligible: <strong>no</strong>" in paths.html.read_text()


def test_junit_not_run_is_skipped_with_global_error(tmp_path: Path) -> None:
    run = RunResult(
        run_id="run",
        started_at=datetime.now(UTC),
        interrupted=True,
        exit_code=130,
        versions={
            "baseline": [
                ScenarioResult(
                    scenario_id="normal",
                    execution_state=ExecutionState.NOT_RUN,
                    evidence_complete=False,
                    verdict=Verdict.ERROR,
                    errors=[ScenarioIssue("not_run", "execution", "Interrupted")],
                )
            ]
        },
    )
    paths = reports.write_reports(run, tmp_path)
    assert paths.junit is not None
    root = ElementTree.parse(paths.junit)
    assert root.find(".//skipped") is not None and root.find(".//error") is not None
