"""Tests for vitalog.console — Rich console singleton."""

from __future__ import annotations

from rich.console import Console

from vitalog.console import get_console


def test_returns_console() -> None:
    c = get_console()
    assert isinstance(c, Console)


def test_singleton() -> None:
    c1 = get_console()
    c2 = get_console()
    assert c1 is c2
