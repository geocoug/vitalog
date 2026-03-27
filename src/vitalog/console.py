from __future__ import annotations

from functools import lru_cache

from rich.console import Console


@lru_cache(maxsize=1)
def get_console() -> Console:
    return Console()
