"""Tests for vitalog.etl.sleep — SleepCycle CSV loading."""

from __future__ import annotations

from pathlib import Path

from vitalog.etl.sleep import load_sleep_cycle

_CSV_HEADER = (
    "Start;End;Sleep Quality;Regularity;Awake (seconds);Dream (seconds);"
    "Light (seconds);Deep (seconds);Mood;Heart rate (bpm);Steps;Alarm mode;"
    "Air Pressure (Pa);City;Movements per hour;Time in bed (seconds);"
    "Time asleep (seconds);Time before sleep (seconds);Window start;Window stop;"
    "Snore time (seconds);Weather temperature (\u00b0F);Weather type;Notes;"
    "Body temperature deviation (degrees Celsius)"
)


def _write_csv(path: Path, row: str) -> None:
    """Write a SleepCycle CSV with the standard header + one data row (UTF-8)."""
    path.write_text(f"{_CSV_HEADER}\n{row}\n", encoding="utf-8")


class TestLoadSleepCycle:
    def test_loads_csv(self, tmp_path: Path, tmp_db) -> None:
        csv = tmp_path / "sleepdata.csv"
        _write_csv(
            csv,
            "2025-06-01 22:30:00;2025-06-02 06:30:00;82%;75%;1200;3600;10800;7200;"
            "good;58;0;normal;;;4.2;28800;25200;600;;;300;68;clear;;",
        )

        load_sleep_cycle(csv, tmp_db)

        count = tmp_db.execute("SELECT COUNT(*) FROM stg_sleep_cycle").fetchone()[0]
        assert count == 1

        row = tmp_db.execute("SELECT sleep_quality, deep_seconds FROM stg_sleep_cycle").fetchone()
        assert row[0] == 0.82  # Converted from 82%
        assert row[1] == 7200

    def test_clears_existing_data(self, tmp_path: Path, tmp_db) -> None:
        csv = tmp_path / "sleepdata.csv"
        _write_csv(
            csv,
            "2025-06-01 22:30:00;2025-06-02 06:30:00;80%;70%;1200;3600;10800;7200;"
            "good;58;0;normal;;;4.2;28800;25200;600;;;300;68;clear;;",
        )

        load_sleep_cycle(csv, tmp_db)
        load_sleep_cycle(csv, tmp_db)

        count = tmp_db.execute("SELECT COUNT(*) FROM stg_sleep_cycle").fetchone()[0]
        assert count == 1  # Not doubled
