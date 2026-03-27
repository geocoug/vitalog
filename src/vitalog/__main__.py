#!/usr/bin/env python

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import typer

from vitalog.console import get_console
from vitalog.db import connect, init_schema
from vitalog.logging import configure_logging

__version__ = "0.2.2"

app = typer.Typer(add_completion=False, help="Vitalog: health data CLI.")
load_app = typer.Typer(help="Load health data into DuckDB.")
app.add_typer(load_app, name="load")

logger = logging.getLogger("vitalog")
console = get_console()

DEFAULT_DB = Path("vitalog.duckdb")


def _validate_apple_export(path: Path) -> None:
    """Validate that path is an existing ZIP file."""
    import zipfile

    if not path.exists():
        console.print(f"[bold red]Error:[/bold red] {path} not found")
        raise typer.Exit(1)
    if not zipfile.is_zipfile(path):
        console.print(f"[bold red]Error:[/bold red] {path} is not a valid ZIP file")
        raise typer.Exit(1)


def _validate_sleep_csv(path: Path) -> None:
    """Validate that path is an existing CSV file."""
    if not path.exists():
        console.print(f"[bold red]Error:[/bold red] {path} not found")
        raise typer.Exit(1)
    if path.suffix.lower() != ".csv":
        console.print(f"[bold red]Error:[/bold red] {path} is not a CSV file")
        raise typer.Exit(1)


@app.callback(invoke_without_command=True)
def app_callback(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show version and exit.",
        is_eager=True,
    ),
) -> None:
    configure_logging()
    if version:
        script_name = os.path.basename(sys.argv[0])
        console.print(f"[bold cyan]{script_name}[/bold cyan] v{__version__}")
        raise typer.Exit()
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(1)


@load_app.command("all")
def load_all(
    export: Path = typer.Option(..., "--apple", "-a", help="Apple Health export ZIP file"),
    sleep_file: Path | None = typer.Option(None, "--sleep", "-s", help="SleepCycle CSV file (optional)"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Load Apple Health export (and optionally SleepCycle CSV) into DuckDB."""
    from vitalog.etl.apple import load_apple_health

    _validate_apple_export(export)
    if sleep_file is not None:
        _validate_sleep_csv(sleep_file)

    with connect(db) as conn:
        init_schema(conn)
        load_apple_health(export, conn)
        if sleep_file is not None:
            from vitalog.etl.sleep import load_sleep_cycle

            load_sleep_cycle(sleep_file, conn)

    console.print(f"\n[bold green]Done.[/bold green] Database: {db}")


@load_app.command("apple")
def load_apple(
    export: Path = typer.Option(..., "--file", "-f", help="Apple Health export ZIP file"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Load Apple Health export data into DuckDB."""
    from vitalog.etl.apple import load_apple_health

    _validate_apple_export(export)

    with connect(db) as conn:
        init_schema(conn)
        load_apple_health(export, conn)
    console.print(f"\n[bold green]Done.[/bold green] Database: {db}")


@load_app.command("sleep")
def load_sleep(
    file: Path = typer.Option(..., "--file", "-f", help="SleepCycle CSV file"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Load SleepCycle CSV data into DuckDB."""
    from vitalog.etl.sleep import load_sleep_cycle

    _validate_sleep_csv(file)

    with connect(db) as conn:
        init_schema(conn)
        load_sleep_cycle(file, conn)
    console.print(f"\n[bold green]Done.[/bold green] Database: {db}")


@app.command()
def narrative(
    period: str = typer.Option("last-week", "--period", "-p", help="Time period: last-week, last-month, last-quarter"),
    start: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD) for custom period"),
    end: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for custom period"),
    question: str | None = typer.Option(
        None,
        "--question",
        "-q",
        help="Ask a specific question about your health data instead of generating a journal entry",
    ),
    output: Path | None = typer.Option(None, "--output", "-o", help="Write narrative to file instead of terminal"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Generate an AI-powered health narrative for a time period."""
    from vitalog.narrative.generate import generate_narrative
    from vitalog.narrative.queries import get_period_stats, resolve_date_range

    if not db.exists():
        console.print("[bold red]Error:[/bold red] Database not found. Run 'vitalog load' first.")
        raise typer.Exit(1)

    try:
        start_date, end_date = resolve_date_range(period, start, end)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1) from e

    if question:
        console.print(f"[cyan]Answering question[/cyan] for {start_date} to {end_date} ...")
    else:
        console.print(f"[cyan]Generating narrative[/cyan] for {start_date} to {end_date} ...")

    with connect(db) as conn:
        stats = get_period_stats(conn, start_date, end_date)

    generate_narrative(stats, output, question=question)


@app.command()
def dashboard(
    period: str = typer.Option(
        "last-year",
        "--period",
        "-p",
        help="Time period: last-month, last-quarter, last-year, all",
    ),
    start: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD) for custom period"),
    end: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for custom period"),
    output: Path = typer.Option(Path("dashboard.html"), "--output", "-o", help="Output HTML file path"),
    no_open: bool = typer.Option(False, "--no-open", help="Don't open the dashboard in the browser"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Generate a static HTML health dashboard."""
    from vitalog.dashboard.render import render_dashboard
    from vitalog.narrative.queries import resolve_date_range

    if not db.exists():
        console.print("[bold red]Error:[/bold red] Database not found. Run 'vitalog load' first.")
        raise typer.Exit(1)

    try:
        start_date, end_date = resolve_date_range(period, start, end)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1) from e
    console.print(f"[cyan]Generating dashboard[/cyan] for {start_date} to {end_date} ...")

    with connect(db) as conn:
        render_dashboard(conn, start_date, end_date, output)

    console.print(f"[bold green]Dashboard written to[/bold green] {output}")

    if not no_open:
        import webbrowser

        webbrowser.open(f"file://{output.resolve()}")


@app.command()
def profile(
    age: int | None = typer.Option(None, "--age", help="Age in years"),
    weight: float | None = typer.Option(None, "--weight", help="Weight in lbs"),
    height: float | None = typer.Option(None, "--height", help="Height in inches"),
    sex: str | None = typer.Option(None, "--sex", help="Biological sex (male/female)"),
    show: bool = typer.Option(False, "--show", help="Show current profile"),
    db: Path = typer.Option(DEFAULT_DB, "--db", help="DuckDB database file path"),
) -> None:
    """Set or view user demographics for personalized health context."""
    with connect(db) as conn:
        init_schema(conn)

        if show:
            rows = conn.execute("SELECT key, value FROM user_profile ORDER BY key").fetchall()
            if rows:
                for k, v in rows:
                    console.print(f"  [bold]{k}:[/bold] {v}")
            else:
                console.print("[dim]No profile set. Use --age, --weight, --height, --sex to set.[/dim]")
            return

        updates = {}
        if age is not None:
            updates["age"] = str(age)
        if weight is not None:
            updates["weight_lbs"] = str(weight)
        if height is not None:
            updates["height_in"] = str(height)
        if sex is not None:
            if sex.lower() not in ("male", "female"):
                console.print("[bold red]Error:[/bold red] --sex must be 'male' or 'female'")
                raise typer.Exit(1)
            updates["sex"] = sex.lower()

        if not updates:
            console.print("[dim]No profile fields provided. Use --age, --weight, --height, --sex.[/dim]")
            raise typer.Exit(1)

        for k, v in updates.items():
            conn.execute(
                """
                INSERT INTO user_profile (key, value) VALUES (?, ?)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value
                """,
                [k, v],
            )
        console.print(f"[green]Profile updated:[/green] {', '.join(f'{k}={v}' for k, v in updates.items())}")


if __name__ == "__main__":
    app()
