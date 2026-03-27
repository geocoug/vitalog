from __future__ import annotations

import logging

from rich.logging import RichHandler

from .console import get_console


def configure_logging(level: int = logging.INFO) -> None:
    console = get_console()

    handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_time=False,
        show_level=True,
        show_path=False,
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)
