"""EventGate's command line interface."""

from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import typer

from eventgate.artifacts import ArtifactError, Redactor
from eventgate.config import SCENARIO_ORDER, ConfigError, load_config
from eventgate.reports import render_offline
from eventgate.runner import configuration_error, doctor, verify

app = typer.Typer(
    help="Check event-driven applications for business regressions.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


@app.callback(invoke_without_command=True)
def root(
    ctx: typer.Context,
    show_version: Annotated[
        bool, typer.Option("--version", help="Show the installed version and exit.")
    ] = False,
) -> None:
    if show_version:
        typer.echo(f"EventGate {version('eventgate')}")
        raise typer.Exit()
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@app.command("verify")
def verify_command(
    config: Annotated[Path, typer.Option("--config", help="Path to a verify configuration.")],
) -> None:
    """Compare two application versions across the selected payment scenarios."""
    try:
        settings = load_config(config)
    except ConfigError as error:
        try:
            result = configuration_error(str(error))
            typer.echo(f"Report: {Path('results') / result.run_id / 'report.json'}")
        except ArtifactError as report_error:
            typer.echo(str(report_error), err=True)
        typer.echo(f"Configuration error: {Redactor().text(str(error))}", err=True)
        raise typer.Exit(2) from None

    if settings.mode == "development":
        typer.echo("DEVELOPMENT RUN — NOT A RELEASE GATE")
    try:
        result = verify(settings)
    except InterruptedError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(130) from None
    except ArtifactError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(2) from None
    except KeyboardInterrupt:
        typer.echo("Interrupted before the result could be published", err=True)
        raise typer.Exit(130) from None
    typer.echo(f"{result.verdict} (exit {result.exit_code})")
    typer.echo(f"Report: {settings.artifacts.directory / result.run_id / 'report.json'}")
    for issue in result.errors:
        typer.echo(f"{issue.code}: {issue.message}", err=True)
    raise typer.Exit(result.exit_code)


@app.command("doctor")
def doctor_command(
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Also check configuration and image availability."),
    ] = None,
) -> None:
    """Check Docker Engine, Compose, and optionally the local image references."""
    try:
        settings = load_config(config) if config is not None else None
    except ConfigError as error:
        typer.echo(f"Configuration error: {Redactor().text(str(error))}", err=True)
        raise typer.Exit(2) from None
    values, problems = doctor(settings)
    for label, value in values.items():
        typer.echo(f"{label}: {value}")
    for problem in problems:
        typer.echo(Redactor().text(problem), err=True)
    raise typer.Exit(2 if problems else 0)


scenario_commands = typer.Typer(help="List available verification scenarios.")
app.add_typer(scenario_commands, name="scenarios")


@scenario_commands.command("list")
def list_scenarios() -> None:
    """Print supported scenario IDs in their execution order."""
    for scenario in SCENARIO_ORDER:
        typer.echo(scenario)


@app.command("report")
def report_command(
    input_dir: Annotated[
        Path, typer.Option("--input", help="Directory containing a saved report.json.")
    ],
) -> None:
    """Regenerate HTML and JUnit without running Docker or changing the saved verdict."""
    try:
        paths = render_offline(input_dir)
    except ArtifactError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(2) from None
    typer.echo(f"HTML: {paths.html}")
    typer.echo(f"JUnit: {paths.junit}")


def main() -> None:
    app()
