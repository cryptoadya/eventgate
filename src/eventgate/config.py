"""Load strict, inert configuration for a payment-v1 verification run."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, get_args

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
)
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

from eventgate.contracts.payment import PaymentCompleted, SchemaV1

type ScenarioID = Literal[
    "normal",
    "duplicate_delivery",
    "consumer_restart",
    "crash_after_db_commit",
    "crash_after_publish_ack",
]
SCENARIO_ORDER: tuple[ScenarioID, ...] = get_args(ScenarioID.__value__)
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_DIGEST = re.compile(r"(?:[^@\s]+@)?sha256:[a-f0-9]{64}\Z")
_REQUIRED_FIELDS = frozenset(PaymentCompleted.model_fields)


def _image_ref(value: str) -> str:
    if _DIGEST.fullmatch(value) and not value.startswith("-"):
        return value
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Expected an image with an explicit tag or sha256 digest")
    if value.rsplit(":", 1)[-1] == "latest":
        raise ValueError("The latest tag is not supported")
    return value


def _pinned_image(value: str) -> str:
    if not _DIGEST.fullmatch(value) or not _image_ref(value):
        raise ValueError("Infrastructure images require a sha256 digest")
    return value


def _topic_name(value: str) -> str:
    if value in {".", ".."} or not re.fullmatch(r"[a-zA-Z0-9._-]{1,249}", value):
        raise ValueError("Expected a valid Kafka topic name")
    return value


def _extra_field(value: str) -> str:
    if value in _REQUIRED_FIELDS:
        raise ValueError("Required completion fields cannot be ignored")
    if not re.fullmatch(r"[^\s.$\[\]*?]+", value):
        raise ValueError("Use a top-level field name without paths or wildcards")
    return value


class _ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class AppImage(_ConfigModel):
    image: Annotated[str, AfterValidator(_image_ref)]


class InfrastructureConfig(_ConfigModel):
    kafka_image: Annotated[str, AfterValidator(_pinned_image)]
    postgres_image: Annotated[str, AfterValidator(_pinned_image)]


class KafkaConfig(_ConfigModel):
    input_topic: Annotated[str, AfterValidator(_topic_name)]
    output_topic: Annotated[str, AfterValidator(_topic_name)]
    partitions: SchemaV1

    @field_validator("output_topic")
    @classmethod
    def distinct_topics(cls, value: str, info: ValidationInfo) -> str:
        if value == info.data.get("input_topic"):
            raise ValueError("Input and output topics must differ")
        return value


type PositiveSeconds = Annotated[int, Field(gt=0)]


class Timeouts(_ConfigModel):
    startup_seconds: PositiveSeconds
    progress_seconds: PositiveSeconds
    control_seconds: PositiveSeconds
    cleanup_seconds: PositiveSeconds


class ComparisonPolicy(_ConfigModel):
    allowed_extra_output_fields: list[Annotated[str, AfterValidator(_extra_field)]]


class ArtifactsConfig(_ConfigModel):
    directory: Path

    @field_validator("directory", mode="before")
    @classmethod
    def absolute_directory(cls, value: object, info: ValidationInfo) -> Path:
        if not isinstance(value, str) or not value.strip() or "\0" in value:
            raise ValueError("Expected a non-empty directory path")
        context = info.context
        base = context["config_dir"] if context else Path.cwd()
        try:
            return (base / Path(value)).resolve()
        except (OSError, RuntimeError):
            raise ValueError("Cannot resolve the artifact directory") from None


class VerifyConfig(_ConfigModel):
    schema_version: SchemaV1
    adapter: Literal["payment-v1"]
    mode: Literal["release", "development"] = "release"
    baseline: AppImage
    candidate: AppImage
    infrastructure: InfrastructureConfig
    kafka: KafkaConfig
    scenarios: Annotated[list[ScenarioID], Field(min_length=1)]
    timeouts: Timeouts
    comparison: ComparisonPolicy
    artifacts: ArtifactsConfig

    @field_validator("scenarios")
    @classmethod
    def validate_selection(cls, value: list[ScenarioID], info: ValidationInfo) -> list[ScenarioID]:
        selected = set(value)
        if len(selected) != len(value):
            raise ValueError("Scenarios must not repeat")
        if info.data.get("mode") == "release" and selected != set(SCENARIO_ORDER):
            raise ValueError("Release mode requires all five scenarios")
        return [scenario for scenario in SCENARIO_ORDER if scenario in selected]

    @property
    def selection_coverage(self) -> Literal["full", "partial"]:
        return "full" if len(self.scenarios) == len(SCENARIO_ORDER) else "partial"


@dataclass(frozen=True)
class ConfigIssue:
    path: str
    message: str


class ConfigError(ValueError):
    def __init__(self, *issues: ConfigIssue) -> None:
        self.issues = issues
        super().__init__("; ".join(f"{issue.path}: {issue.message}" for issue in issues))


type Location = tuple[str | int, ...]


def _path(location: Location) -> str:
    return ".".join(map(str, location)) or "config"


def _check_yaml(node: Node, location: Location, active: set[Node], checked: set[Node]) -> None:
    if node in active:
        raise ConfigError(ConfigIssue(_path(location), "Recursive YAML aliases are not supported"))
    if node in checked:
        return
    active.add(node)
    if isinstance(node, MappingNode):
        keys: set[str] = set()
        for key, value in node.value:
            if not isinstance(key, ScalarNode) or key.tag != "tag:yaml.org,2002:str":
                raise ConfigError(ConfigIssue(_path(location), "YAML mapping keys must be strings"))
            child = (*location, key.value)
            if key.value in keys:
                raise ConfigError(ConfigIssue(_path(child), "Duplicate YAML key"))
            keys.add(key.value)
            _check_yaml(value, child, active, checked)
    elif isinstance(node, SequenceNode):
        for index, value in enumerate(node.value):
            _check_yaml(value, (*location, index), active, checked)
    active.remove(node)
    checked.add(node)


def _substitute(value: object, location: Location, expanded: dict[int, object]) -> object:
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name = match[1]
            resolved = os.environ.get(name)
            if not resolved:
                raise ConfigError(
                    ConfigIssue(_path(location), f"Environment variable {name} is unset or empty")
                )
            return resolved

        return _ENV.sub(replace, value)

    identity = id(value)
    if identity in expanded:
        return expanded[identity]
    if isinstance(value, dict):
        expanded[identity] = {
            key: _substitute(item, (*location, key), expanded) for key, item in value.items()
        }
        return expanded[identity]
    if isinstance(value, list):
        expanded[identity] = [
            _substitute(item, (*location, index), expanded) for index, item in enumerate(value)
        ]
        return expanded[identity]
    return value


def load_config(path: Path) -> VerifyConfig:
    """Validate YAML and resolve value substitutions without invoking a shell."""
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise ConfigError(
            ConfigIssue("config", "Cannot read the UTF-8 configuration file")
        ) from None

    loader = yaml.SafeLoader(source)
    try:
        node = loader.get_single_node()
        if node is None:
            raise ConfigError(ConfigIssue("config", "Configuration is empty"))
        _check_yaml(node, (), set(), set())
        data = _substitute(loader.construct_document(node), (), {})
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
        raise ConfigError(ConfigIssue("config", f"Unsupported or invalid YAML{where}")) from None
    except RecursionError:
        raise ConfigError(ConfigIssue("config", "YAML nesting is too deep")) from None
    finally:
        loader.dispose()

    try:
        return VerifyConfig.model_validate(data, context={"config_dir": path.absolute().parent})
    except ValidationError as error:
        raise ConfigError(
            *(
                ConfigIssue(_path(item["loc"]), item["msg"])
                for item in error.errors(include_input=False)
            )
        ) from None
