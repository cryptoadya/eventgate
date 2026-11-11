"""Store run evidence without disclosing generated credentials."""

import base64
import json
import os
import re
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from pydantic import BaseModel, JsonValue

_DSN = re.compile(rb"postgres(?:ql)?(?:\+[A-Za-z0-9_]+)?://[^\s'\"<>]+", re.IGNORECASE)
_ASSIGNMENT = re.compile(
    rb"(\b(?:POSTGRES_PASSWORD|DATABASE_URL)\s*=\s*)[^\s'\"<>]+", re.IGNORECASE
)
_REPLACEMENT = b"[REDACTED]"


class ArtifactError(RuntimeError):
    """A required run artifact could not be stored."""


class Redactor:
    def __init__(self, secrets: list[str] | None = None) -> None:
        self._secrets: set[bytes] = set()
        for secret in secrets or []:
            self.add(secret)

    def add(self, secret: str) -> None:
        if secret:
            self._secrets.add(secret.encode("utf-8"))

    def raw(self, value: bytes) -> bytes:
        value = _ASSIGNMENT.sub(rb"\g<1>" + _REPLACEMENT, _DSN.sub(_REPLACEMENT, value))
        for secret in sorted(self._secrets, key=len, reverse=True):
            value = value.replace(secret, _REPLACEMENT)
        return value

    def text(self, value: str) -> str:
        return self.raw(value.encode("utf-8", errors="backslashreplace")).decode("utf-8")

    def json_value(self, value: object) -> JsonValue:
        if isinstance(value, bytes):
            return {
                "encoding": "base64",
                "value": base64.b64encode(self.raw(value)).decode("ascii"),
            }
        if isinstance(value, str):
            return self.text(value)
        if value is None or isinstance(value, (int, bool, float)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, BaseModel):
            return self.json_value(value.model_dump(mode="json"))
        if is_dataclass(value) and not isinstance(value, type):
            return {
                field.name: self.json_value(getattr(value, field.name)) for field in fields(value)
            }
        if isinstance(value, (list, tuple)):
            return [self.json_value(item) for item in value]
        if isinstance(value, dict):
            return {self.text(str(key)): self.json_value(item) for key, item in value.items()}
        raise TypeError(f"Unsupported artifact value: {type(value).__name__}")


def json_bytes(value: JsonValue) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def atomic_write(path: Path, data: bytes) -> None:
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(prefix=".eventgate-", dir=path.parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class ArtifactStore:
    def __init__(self, base: Path, run_id: str) -> None:
        self.directory = base / run_id
        self.redactor = Redactor()
        try:
            self.directory.mkdir(parents=True, exist_ok=False, mode=0o700)
            probe = self.directory / ".write-probe"
            atomic_write(probe, b"eventgate")
            probe.unlink()
        except OSError:
            raise ArtifactError(f"Cannot create a writable output directory under {base}") from None

    def write_json(self, relative: str, value: object) -> str:
        return self.write(relative, json_bytes(self.redactor.json_value(value)))

    def write_text(self, relative: str, value: str) -> str:
        return self.write(relative, self.redactor.text(value).encode("utf-8"))

    def write(self, relative: str, data: bytes) -> str:
        path = self.directory / relative
        try:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            atomic_write(path, data)
        except OSError:
            raise ArtifactError(f"Cannot write output artifact {relative}") from None
        return relative
