"""Tests for vitalog.logging — logging configuration."""

from __future__ import annotations

import logging

from vitalog.logging import configure_logging


def test_configure_logging_sets_handler() -> None:
    configure_logging()
    root = logging.getLogger()
    assert len(root.handlers) >= 1
    assert root.level == logging.INFO


def test_configure_logging_custom_level() -> None:
    configure_logging(level=logging.DEBUG)
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    # Reset
    configure_logging(level=logging.INFO)
