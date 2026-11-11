import json
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml


def test_interrupt_saves_partial_report_and_removes_owned_resources(
    integration_config, stable_image, tmp_path
):
    config = integration_config.model_dump(mode="json")
    config["baseline"]["image"] = config["candidate"]["image"] = stable_image
    config["artifacts"]["directory"] = str(tmp_path / "output")
    path = tmp_path / "verify.yaml"
    path.write_text(yaml.safe_dump(config))
    cli = Path(sys.executable).with_name("eventgate")
    with subprocess.Popen(
        [str(cli), "verify", "--config", str(path)],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                runs = list((tmp_path / "output").glob("*/"))
                if runs:
                    run_id = runs[0].name
                    ids = subprocess.check_output(
                        [
                            "docker",
                            "ps",
                            "-a",
                            "-q",
                            "--filter",
                            f"label=eventgate.run_id={run_id}",
                        ],
                        text=True,
                        timeout=5,
                    )
                    if ids.strip():
                        break
                assert child.poll() is None
                time.sleep(0.1)
            else:
                raise AssertionError("No owned container was observed during startup")
            child.send_signal(signal.SIGINT)
            out, err = child.communicate(timeout=45)
            assert child.returncode == 130, (out, err)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)

    report = json.loads((runs[0] / "report.json").read_text())
    assert report["interrupted"] and not report["release_eligible"]
    assert report["verdict"] == "ERROR" and report["exit_code"] == 130
    for kind in ("container", "network", "volume"):
        args = ["docker", kind, "ls", "-q"]
        if kind == "container":
            args.append("-a")
        assert not subprocess.check_output(
            [*args, "--filter", f"label=eventgate.run_id={run_id}"], text=True, timeout=5
        ).strip()
