"""Tests for vitalog.etl.apple — Apple Health XML parsing."""

from __future__ import annotations

from vitalog.etl.apple import _parse_ts, _safe_float, abbreviate


class TestAbbreviate:
    def test_quantity_type(self) -> None:
        assert abbreviate("HKQuantityTypeIdentifierStepCount") == "StepCount"

    def test_category_type(self) -> None:
        assert abbreviate("HKCategoryTypeIdentifierSleepAnalysis") == "SleepAnalysis"

    def test_data_type(self) -> None:
        assert abbreviate("HKDataTypeSleepDurationGoal") == "SleepDurationGoal"

    def test_no_prefix(self) -> None:
        assert abbreviate("CustomType") == "CustomType"

    def test_empty_string(self) -> None:
        assert abbreviate("") == ""


class TestParseTs:
    def test_standard_timestamp(self) -> None:
        assert _parse_ts("2025-06-01 08:30:00 -0700") == "2025-06-01 08:30:00"

    def test_positive_offset(self) -> None:
        assert _parse_ts("2025-01-15 14:00:00 +0530") == "2025-01-15 14:00:00"

    def test_no_timezone(self) -> None:
        assert _parse_ts("2025-06-01 08:30:00") == "2025-06-01 08:30:00"

    def test_none(self) -> None:
        assert _parse_ts(None) is None


class TestSafeFloat:
    def test_valid_float(self) -> None:
        assert _safe_float("3.14") == "3.14"

    def test_valid_integer(self) -> None:
        assert _safe_float("42") == "42"

    def test_invalid(self) -> None:
        assert _safe_float("not-a-number") is None

    def test_none(self) -> None:
        assert _safe_float(None) is None

    def test_empty_string(self) -> None:
        assert _safe_float("") is None
