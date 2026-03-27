"""Tests for package-level imports and metadata."""

from __future__ import annotations


def test_version_importable() -> None:
    from vitalog import __version__

    assert isinstance(__version__, str)
    assert "." in __version__


def test_main_importable() -> None:
    from vitalog.__main__ import app

    assert app is not None
