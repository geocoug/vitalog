"""Tests for vitalog.narrative.queries — date range resolution and stats."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from vitalog.narrative.queries import (
    _heart_rate_stats,
    _ring_stats,
    _sleep_stats,
    _step_stats,
    _workout_stats,
    get_period_stats,
    resolve_date_range,
)


class TestResolveDateRange:
    def test_explicit_start_and_end(self) -> None:
        s, e = resolve_date_range("ignored", "2025-01-01", "2025-06-30")
        assert s == date(2025, 1, 1)
        assert e == date(2025, 6, 30)

    def test_start_only(self) -> None:
        s, e = resolve_date_range("ignored", "2025-01-01", None)
        assert s == date(2025, 1, 1)
        assert e == date.today()

    def test_last_week(self) -> None:
        s, e = resolve_date_range("last-week", None, None)
        assert e == date.today()
        assert s == date.today() - timedelta(weeks=1)

    def test_last_month(self) -> None:
        s, e = resolve_date_range("last-month", None, None)
        assert (e - s).days == 30

    def test_last_quarter(self) -> None:
        s, e = resolve_date_range("last-quarter", None, None)
        assert (e - s).days == 90

    def test_last_year(self) -> None:
        s, e = resolve_date_range("last-year", None, None)
        assert (e - s).days == 365

    def test_all(self) -> None:
        s, e = resolve_date_range("all", None, None)
        assert (e - s).days == 365 * 10

    def test_unknown_period(self) -> None:
        with pytest.raises(ValueError, match="Unknown period"):
            resolve_date_range("invalid-period", None, None)


class TestStepStats:
    def test_basic_stats(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        ps = date(2025, 5, 28)
        pe = date(2025, 5, 31)
        result = _step_stats(seeded_db, s, e, ps, pe)
        assert result["total"] > 0
        assert result["daily_avg"] > 0
        assert result["max_day"] >= result["min_day"]


class TestHeartRateStats:
    def test_basic_stats(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        ps = date(2025, 5, 28)
        pe = date(2025, 5, 31)
        result = _heart_rate_stats(seeded_db, s, e, ps, pe)
        assert result["avg_resting"] is not None
        assert result["min_resting"] <= result["max_resting"]


class TestSleepStats:
    def test_basic_stats(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        ps = date(2025, 5, 28)
        pe = date(2025, 5, 31)
        result = _sleep_stats(seeded_db, s, e, ps, pe)
        assert result["nights_tracked"] > 0
        assert result["avg_quality_pct"] is not None


class TestWorkoutStats:
    def test_basic_stats(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        result = _workout_stats(seeded_db, s, e)
        assert result["count"] == 3
        assert len(result["by_type"]) > 0


class TestRingStats:
    def test_basic_stats(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        result = _ring_stats(seeded_db, s, e)
        assert result["days_tracked"] == 3
        assert result["move_close_pct"] is not None


class TestGetPeriodStats:
    def test_returns_all_sections(self, seeded_db) -> None:
        s = date(2025, 6, 1)
        e = date(2025, 6, 3)
        stats = get_period_stats(seeded_db, s, e)
        assert "period" in stats
        assert "profile" in stats
        assert "steps" in stats
        assert "heart_rate" in stats
        assert "sleep" in stats
        assert "workouts" in stats
        assert "activity_rings" in stats
        assert stats["period"]["days"] == 2
