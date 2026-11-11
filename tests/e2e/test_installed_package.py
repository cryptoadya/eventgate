import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_installed_wheel_has_resources_and_cli_outside_checkout(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    installed = tmp_path / "installed"
    outside = tmp_path / "outside"
    outside.mkdir()
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(dist)],
        cwd=ROOT,
        check=True,
        timeout=45,
    )
    [wheel] = dist.glob("*.whl")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--target",
            str(installed),
            str(wheel),
        ],
        cwd=outside,
        check=True,
        timeout=25,
    )
    env = {**os.environ, "PYTHONPATH": str(installed)}
    check = """
from pathlib import Path
from importlib.resources import files
import sys
import eventgate
import eventgate_demo
root = Path(sys.argv[1]).resolve()
assert Path(eventgate.__file__).resolve().is_relative_to(root)
assert Path(eventgate_demo.__file__).resolve().is_relative_to(root)
for relative in ('resources/compose.yaml', 'templates/report.html.j2'):
    assert files('eventgate').joinpath(relative).read_text().strip()
"""
    subprocess.run(
        [sys.executable, "-c", check, str(installed)], cwd=outside, env=env, check=True, timeout=10
    )
    for args in (["--help"], ["scenarios", "list"]):
        process = subprocess.run(
            [str(installed / "bin/eventgate"), *args],
            cwd=outside,
            env=env,
            check=True,
            text=True,
            capture_output=True,
            timeout=10,
        )
        assert process.stdout.strip()
