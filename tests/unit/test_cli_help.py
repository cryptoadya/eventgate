from importlib.metadata import version

from typer.testing import CliRunner

from eventgate.cli import app


def test_help_advertises_the_available_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Check event-driven applications" in result.output
    assert all(command in result.output for command in ("verify", "doctor", "scenarios"))


def test_version_comes_from_installed_package() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout == f"EventGate {version('eventgate')}\n"
