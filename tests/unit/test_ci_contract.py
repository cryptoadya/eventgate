import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def load_workflow(name: str):
    path = ROOT / ".github/workflows" / name
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


def test_demo_uses_distinct_trusted_and_candidate_revisions() -> None:
    workflow = load_workflow("eventgate-demo.yml")
    assert set(workflow["on"]) == {"pull_request"}
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["demo"]
    assert job["name"] == "EventGate demo gate"
    assert "needs" not in job and "continue-on-error" not in job
    steps = job["steps"]
    checkouts = [step for step in steps if step.get("uses", "").startswith("actions/checkout@")]
    assert [(step["with"]["path"], step["with"]["ref"]) for step in checkouts] == [
        ("trusted", "${{ github.event.pull_request.base.sha }}"),
        ("candidate", "${{ github.event.pull_request.head.sha }}"),
    ]
    assert all(step["with"]["persist-credentials"] == "false" for step in checkouts)
    install = next(step for step in steps if step.get("name") == "Install trusted EventGate")
    assert install["working-directory"] == "trusted"
    build = next(step for step in steps if step.get("name") == "Build Baseline and Candidate")
    assert build["run"].count("PAYMENT_VARIANT=stable") == 2
    assert "git -C trusted rev-parse HEAD" in build["run"]
    assert "git -C candidate rev-parse HEAD" in build["run"]
    gate = next(step for step in steps if step.get("name") == "Verify Candidate")
    assert gate["working-directory"] == "trusted"
    assert "eventgate verify --config examples/stable-vs-stable.yaml" in gate["run"]
    assert "continue-on-error" not in gate and "||" not in gate["run"]
    config = yaml.safe_load((ROOT / "examples/stable-vs-stable.yaml").read_text())
    assert config["mode"] == "release" and len(config["scenarios"]) == 5
    upload = next(
        step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert upload["if"] == "always()" and "trusted/results" in upload["with"]["path"]
    assert steps.index(upload) > steps.index(gate)


def test_main_ci_checks_working_tree_and_preserves_reports() -> None:
    workflow = load_workflow("ci.yml")
    assert set(workflow["on"]) == {"push", "pull_request"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["quality"]
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["env"]["EXPECTED_SHA"] == "${{ github.event.pull_request.head.sha || github.sha }}"
    steps = job["steps"]
    assert steps[0]["with"]["ref"] == "${{ env.EXPECTED_SHA }}"
    platform = next(step for step in steps if step.get("name") == "Record native Linux environment")
    assert 'test "$(uname -sm)" = "Linux x86_64"' in platform["run"]
    assert 'test "$docker_platform" = "linux/amd64"' in platform["run"]
    assert "results/platform.txt" in platform["run"]
    commands = "\n".join(step.get("run", "") for step in steps)
    assert "requirements-dev.lock" in commands
    for command in (
        "ruff check .",
        "ruff format --check .",
        "mypy src",
        "pytest tests/unit",
        "pytest tests/integration tests/e2e",
    ):
        assert command in commands
    assert "git rev-parse HEAD" in commands and "SOURCE_REVISION" in commands
    assert "docker build" in commands and "docker pull" in commands
    assert "continue-on-error" not in workflow["jobs"]["quality"]
    for step in steps:
        assert "continue-on-error" not in step
        if "uses" in step:
            assert re.fullmatch(r"actions/[a-z-]+@[0-9a-f]{40}", step["uses"])
    assert steps[-1]["if"] == "always()"
    assert "results/" in steps[-1]["with"]["path"]
    assert "report.json" in steps[-1]["with"]["path"]
