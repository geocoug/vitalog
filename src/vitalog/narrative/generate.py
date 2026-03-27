from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from rich.markdown import Markdown

from vitalog.console import get_console
from vitalog.narrative.prompt import (
    QUESTION_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    build_prompt,
    build_question_prompt,
)

console = get_console()


def generate_narrative(stats: dict, output: Path | None = None, question: str | None = None) -> str:
    load_dotenv()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        console.print(
            "[bold red]Error:[/bold red] ANTHROPIC_API_KEY not set. "
            "Add it to your .env file or set the environment variable.",
        )
        msg = "ANTHROPIC_API_KEY not set"
        raise ValueError(msg)

    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    if question:
        prompt = build_question_prompt(stats, question)
        system = QUESTION_SYSTEM_PROMPT
    else:
        prompt = build_prompt(stats)
        system = SYSTEM_PROMPT

    console.print("[cyan]Calling Claude API ...[/cyan]")

    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

    try:
        message = client.messages.create(
            model=model,
            max_tokens=2048,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError as e:
        console.print(f"[bold red]Error:[/bold red] Claude API request failed: {e}")
        raise

    narrative = message.content[0].text

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(narrative)
        console.print(f"[green]Narrative written to[/green] {output}")
    else:
        console.print()
        console.print(Markdown(narrative))

    return narrative
