import base64

from eventgate.artifacts import Redactor


def test_redaction_applies_before_encoding_binary_and_nested_data() -> None:
    secret = "test-secret-value"
    dsn = f"postgresql://run:{secret}@127.0.0.1/eventgate"
    redactor = Redactor([secret])
    assert redactor.text(f"connection to {dsn}") == "connection to [REDACTED]"
    result = redactor.json_value(
        {"rows": [{"data": b"\xff" + secret.encode(), "password": secret}]}
    )
    assert isinstance(result, dict) and isinstance(result["rows"], list)
    row = result["rows"][0]
    assert isinstance(row, dict) and row["password"] == "[REDACTED]"
    encoded = row["data"]
    assert isinstance(encoded, dict) and isinstance(encoded["value"], str)
    assert base64.b64decode(encoded["value"]) == b"\xff[REDACTED]"
