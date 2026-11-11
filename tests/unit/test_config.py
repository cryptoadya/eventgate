from pathlib import Path

import pytest
import yaml

from eventgate.config import SCENARIO_ORDER, ConfigError, load_config

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def image_refs(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, tag in (
        ("BASELINE", "stable"),
        ("STABLE", "stable"),
        ("BUGGY", "buggy"),
    ):
        monkeypatch.setenv(f"EVENTGATE_{name}_IMAGE", f"eventgate-payment:{tag}")
    monkeypatch.setenv("EVENTGATE_KAFKA_IMAGE", f"apache/kafka:4.0.0@sha256:{'a' * 64}")
    monkeypatch.setenv("EVENTGATE_POSTGRES_IMAGE", f"postgres:17.4@sha256:{'b' * 64}")


@pytest.fixture
def config_text() -> str:
    return (ROOT / "examples/stable-vs-stable.yaml").read_text(encoding="utf-8")


def write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "verify.yaml"
    path.write_text(content, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("example", "variant"), [("stable-vs-stable", "stable"), ("stable-vs-buggy", "buggy")]
)
def test_checked_in_examples(example: str, variant: str) -> None:
    config = load_config(ROOT / f"examples/{example}.yaml")
    assert config.baseline.image == "eventgate-payment:stable"
    assert config.candidate.image == f"eventgate-payment:{variant}"
    assert config.scenarios == list(SCENARIO_ORDER)
    assert config.mode == "release"
    assert config.artifacts.directory == ROOT / "results"


@pytest.mark.parametrize("value", [None, ""])
def test_missing_or_empty_env_reports_yaml_field(
    tmp_path: Path, config_text: str, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    if value is None:
        monkeypatch.delenv("EVENTGATE_BASELINE_IMAGE")
    else:
        monkeypatch.setenv("EVENTGATE_BASELINE_IMAGE", value)
    with pytest.raises(ConfigError, match=r"baseline\.image.*EVENTGATE_BASELINE_IMAGE"):
        load_config(write_config(tmp_path, config_text))


@pytest.mark.parametrize(
    ("old", "new", "location"),
    [
        ("schema_version: 1", "schema_version: true", "schema_version"),
        ("adapter: payment-v1", "adapter: custom", "adapter"),
        ("mode: release", "mode: preview", "mode"),
        ("- normal", "- missing", r"scenarios\.0"),
        ("- normal", "- duplicate_delivery", "scenarios"),
        (
            "output_topic: payments.completed",
            "output_topic: payments.requested",
            r"kafka\.output_topic",
        ),
        ("partitions: 1", "partitions: true", r"kafka\.partitions"),
        ("control_seconds: 10", "control_seconds: '10'", r"timeouts\.control_seconds"),
        ("startup_seconds: 60", "startup_seconds: 0", r"timeouts\.startup_seconds"),
        (
            "allowed_extra_output_fields: []",
            "allowed_extra_output_fields: [amount_minor]",
            r"comparison\.allowed_extra_output_fields\.0",
        ),
        (
            "allowed_extra_output_fields: []",
            "allowed_extra_output_fields: [metadata.trace]",
            r"comparison\.allowed_extra_output_fields\.0",
        ),
        (
            "allowed_extra_output_fields: []",
            "allowed_extra_output_fields: ['*']",
            r"comparison\.allowed_extra_output_fields\.0",
        ),
        ("adapter: payment-v1", "adapter: payment-v1\nhook: arbitrary", "hook"),
        ("partitions: 1", "partitions: 1\n  partitions: 1", r"kafka\.partitions"),
    ],
)
def test_config_validation_reports_field(
    tmp_path: Path, config_text: str, old: str, new: str, location: str
) -> None:
    assert old in config_text
    with pytest.raises(ConfigError, match=location):
        load_config(write_config(tmp_path, config_text.replace(old, new)))


def test_release_requires_full_set_but_development_accepts_subset(
    tmp_path: Path, config_text: str
) -> None:
    source = yaml.safe_load(config_text)
    source.pop("mode")
    source["scenarios"] = ["consumer_restart", "normal"]
    path = write_config(tmp_path, yaml.safe_dump(source))
    with pytest.raises(ConfigError, match="scenarios"):
        load_config(path)
    source["mode"] = "development"
    config = load_config(write_config(tmp_path, yaml.safe_dump(source)))
    assert config.scenarios == ["normal", "consumer_restart"]
    assert config.selection_coverage == "partial"
    assert config.artifacts.directory == tmp_path.parent / "results"
    source["scenarios"] = []
    with pytest.raises(ConfigError, match="scenarios"):
        load_config(write_config(tmp_path, yaml.safe_dump(source)))


def test_substitution_is_literal_and_does_not_reparse_yaml(
    tmp_path: Path, config_text: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RESULT_PATH", "false: $(touch should-not-exist)")
    path = write_config(tmp_path, config_text.replace("../results", "${RESULT_PATH}"))
    assert load_config(path).artifacts.directory == tmp_path / "false: $(touch should-not-exist)"
    assert not (tmp_path / "should-not-exist").exists()


def test_yaml_cannot_construct_python_objects(tmp_path: Path, config_text: str) -> None:
    with pytest.raises(ConfigError, match="YAML"):
        load_config(
            write_config(tmp_path, config_text.replace("../results", "!!python/name:os.system"))
        )
