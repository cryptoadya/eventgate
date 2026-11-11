"""Write a consistent report set and regenerate views from saved evidence."""

import io
import re
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path, PurePosixPath
from uuid import uuid4
from xml.etree import ElementTree as ET

from jinja2 import Environment, StrictUndefined

from eventgate.artifacts import ArtifactError, Redactor, atomic_write, json_bytes
from eventgate.contracts.payment import parse_json_object
from eventgate.models import CheckStatus, ExecutionState, RunResult, ScenarioIssue, Verdict

_REPORT = "report.json"
_HTML = "report.html"
_JUNIT = "junit.xml"
_ARCHIVE = "diagnostics.zip"
_XML_INVALID = re.compile(r"[^\x09\x0a\x0d\x20-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]")


@dataclass(frozen=True)
class ReportPaths:
    json: Path
    html: Path | None = None
    junit: Path | None = None
    archive: Path | None = None


def _xml_text(value: str) -> str:
    return _XML_INVALID.sub("\ufffd", value)


def render_html(result: RunResult, *, link_prefix: str = "") -> bytes:
    template = (
        files("eventgate").joinpath("templates", "report.html.j2").read_text(encoding="utf-8")
    )
    env = Environment(autoescape=True, undefined=StrictUndefined)
    text = env.from_string(template).render(run=result, link_prefix=link_prefix)
    return _xml_text(text).encode("utf-8")


def render_junit(result: RunResult) -> bytes:
    suite = ET.Element("testsuite", name="EventGate")
    for role, scenarios in result.versions.items():
        for scenario in scenarios:
            case = ET.SubElement(
                suite, "testcase", classname=f"eventgate.{role}", name=scenario.scenario_id
            )
            details = "\n".join(
                [
                    f"{check.status}: {check.id}: {check.explanation}"
                    for check in scenario.checks
                    if check.status != CheckStatus.PASS
                ]
                + [f"{item.code}: {item.message}" for item in scenario.errors]
            )
            if scenario.execution_state == ExecutionState.NOT_RUN:
                ET.SubElement(case, "skipped", message="Scenario was not run").text = _xml_text(
                    details
                )
            elif scenario.verdict != Verdict.PASS:
                element = "error" if scenario.verdict == Verdict.ERROR else "failure"
                ET.SubElement(case, element, message=scenario.verdict.value).text = _xml_text(
                    details
                )

    for comparison in result.comparisons:
        if not comparison.differences:
            continue
        case = ET.SubElement(
            suite, "testcase", classname="eventgate.comparison", name=comparison.scenario_id
        )
        if comparison.verdict != Verdict.PASS:
            kind = "error" if comparison.verdict == Verdict.ERROR else "failure"
            ET.SubElement(case, kind, message=comparison.reason).text = _xml_text(
                "\n".join(
                    f"{diff.path}: baseline={diff.baseline!r}, candidate={diff.candidate!r}"
                    for diff in comparison.differences
                )
            )

    for error in result.errors:
        case = ET.SubElement(
            suite, "testcase", classname="eventgate.run", name=_xml_text(error.code)
        )
        ET.SubElement(case, "error", message=_xml_text(error.stage)).text = _xml_text(error.message)
    for cleanup in result.cleanup:
        if not cleanup.success:
            case = ET.SubElement(
                suite, "testcase", classname="eventgate.cleanup", name=cleanup.project
            )
            ET.SubElement(case, "error", message="Environment cleanup failed").text = _xml_text(
                "\n".join([*cleanup.remaining, *cleanup.errors])
            )
    if result.interrupted or (result.verdict == Verdict.ERROR and not suite.findall(".//error")):
        case = ET.SubElement(suite, "testcase", classname="eventgate.run", name="execution")
        ET.SubElement(
            case,
            "error",
            message="Run interrupted" if result.interrupted else "Run did not complete",
        )
    if result.verdict == Verdict.FAIL and not suite.findall(".//failure"):
        case = ET.SubElement(suite, "testcase", classname="eventgate.run", name="gate")
        ET.SubElement(case, "failure", message="Release gate failed")
    for count, elements in (
        ("tests", "./testcase"),
        ("failures", ".//failure"),
        ("errors", ".//error"),
        ("skipped", ".//skipped"),
    ):
        suite.set(count, str(len(suite.findall(elements))))
    if result.duration_seconds is not None:
        suite.set("time", f"{result.duration_seconds:.3f}")
    ET.indent(suite)
    output = io.BytesIO()
    ET.ElementTree(suite).write(output, encoding="utf-8", xml_declaration=True)
    return output.getvalue()


def _artifact_paths(result: RunResult) -> set[str]:
    paths = set(result.artifacts.values())
    for scenarios in result.versions.values():
        for scenario in scenarios:
            paths.update(scenario.artifacts.values())
            for check in scenario.checks:
                paths.update(check.evidence)
    return paths


def _safe_artifact(root: Path, name: str) -> Path:
    relative = PurePosixPath(name)
    if (
        relative.is_absolute()
        or not relative.parts
        or ".." in relative.parts
        or "\\" in name
        or ":" in name
        or relative.parts[0] == "rendered"
    ):
        raise ArtifactError(f"Invalid artifact path: {name!r}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ArtifactError(f"Required artifact is unavailable: {name}")
    return path


def _archive(result: RunResult, directory: Path, report_json: bytes) -> bytes:
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        output.writestr(_REPORT, report_json)
        for path in sorted(_artifact_paths(result) - {_ARCHIVE, _REPORT}):
            output.write(_safe_artifact(directory, path), path)
    return archive.getvalue()


def _json(result: RunResult) -> bytes:
    return json_bytes(Redactor().json_value(result))


def _recover_report(result: RunResult, path: Path, render: Callable[[], bytes]) -> Path | None:
    try:
        atomic_write(path, render())
    except Exception:
        # A previous write may have published a view of the superseded verdict.
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
        except OSError:
            result.errors.append(
                ScenarioIssue("report_error", "report_write", f"Could not remove {path.name}")
            )
        return None
    return path


def write_reports(result: RunResult, output_dir: Path) -> ReportPaths:
    json_path = output_dir / _REPORT
    html_path, junit_path = output_dir / _HTML, output_dir / _JUNIT
    archive_path = output_dir / _ARCHIVE if result.verdict != Verdict.PASS else None
    result.artifacts.update(report=_REPORT, html=_HTML, junit=_JUNIT)
    if archive_path is not None:
        result.artifacts["diagnostics"] = _ARCHIVE
    try:
        payload = _json(result)
        atomic_write(html_path, render_html(result))
        atomic_write(junit_path, render_junit(result))
        if archive_path is not None:
            atomic_write(archive_path, _archive(result, output_dir, payload))
        atomic_write(json_path, payload)
    except (Exception, KeyboardInterrupt) as error:
        if isinstance(error, KeyboardInterrupt):
            result.interrupted = True
        result.verdict = Verdict.ERROR
        result.exit_code = 130 if result.interrupted else 2
        result.release_eligible = False
        result.errors.append(
            ScenarioIssue(
                "report_error",
                "report_write",
                f"Could not publish every report format: {type(error).__name__}",
            )
        )
        for key in ("html", "junit", "diagnostics"):
            result.artifacts.pop(key, None)

        recovered_junit = _recover_report(result, junit_path, lambda: render_junit(result))
        if recovered_junit is not None:
            result.artifacts["junit"] = _JUNIT
        recovered_html = _recover_report(result, html_path, lambda: render_html(result))
        if recovered_html is not None:
            result.artifacts["html"] = _HTML

        result.artifacts["diagnostics"] = _ARCHIVE
        recovered_archive = _recover_report(
            result,
            output_dir / _ARCHIVE,
            lambda: _archive(result, output_dir, _json(result)),
        )
        if recovered_archive is None:
            result.artifacts.pop("diagnostics")
        try:
            atomic_write(json_path, _json(result))
        except (Exception, KeyboardInterrupt):
            raise ArtifactError("REPORT INCOMPLETE: could not publish final report.json") from None
        return ReportPaths(json_path, recovered_html, recovered_junit, recovered_archive)
    return ReportPaths(json_path, html_path, junit_path, archive_path)


def render_offline(input_dir: Path) -> ReportPaths:
    try:
        root = input_dir.resolve()
        path = _safe_artifact(root, _REPORT)
        value = parse_json_object(path.read_bytes())
        if set(RunResult.model_fields) - value.keys():
            raise ArtifactError("Saved report is missing required fields")
        result = RunResult.model_validate(value)
        if result.exit_code != (130 if result.interrupted else result.verdict.exit_code):
            raise ArtifactError("Saved verdict and exit code disagree")
        if result.release_eligible and not (
            result.verdict == Verdict.PASS
            and result.mode == "release"
            and result.selection_coverage == "full"
            and result.evidence_complete
        ):
            raise ArtifactError("Saved release eligibility is inconsistent")
        for relative in _artifact_paths(result):
            _safe_artifact(root, relative)
        parent = root / "rendered"
        if parent.is_symlink():
            raise ArtifactError("The rendered output directory must not be a symbolic link")
        directory = parent / uuid4().hex
        directory.mkdir(parents=True, exist_ok=False)
        html_path, junit_path = directory / _HTML, directory / _JUNIT
        atomic_write(html_path, render_html(result, link_prefix="../../"))
        atomic_write(junit_path, render_junit(result))
        return ReportPaths(path, html_path, junit_path)
    except ArtifactError:
        raise
    except Exception as error:
        raise ArtifactError(f"Cannot render the saved report: {type(error).__name__}") from None
