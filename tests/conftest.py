"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from vitalog.db import STAGING_DDL, VIEWS_DDL


@pytest.fixture
def tmp_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with schema initialized."""
    conn = duckdb.connect(":memory:")
    conn.execute(STAGING_DDL)
    conn.execute(VIEWS_DDL)
    yield conn
    conn.close()


@pytest.fixture
def seeded_db(tmp_db: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection pre-loaded with sample health data."""
    # Records — steps
    tmp_db.executemany(
        "INSERT INTO stg_records VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (
                "iPhone",
                "17.0",
                None,
                "StepCount",
                "count",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:30:00",
                3000,
            ),
            (
                "iPhone",
                "17.0",
                None,
                "StepCount",
                "count",
                "2025-06-01 12:00:00",
                "2025-06-01 12:00:00",
                "2025-06-01 12:30:00",
                5000,
            ),
            (
                "iPhone",
                "17.0",
                None,
                "StepCount",
                "count",
                "2025-06-02 08:00:00",
                "2025-06-02 08:00:00",
                "2025-06-02 08:30:00",
                7000,
            ),
            (
                "iPhone",
                "17.0",
                None,
                "StepCount",
                "count",
                "2025-06-03 08:00:00",
                "2025-06-03 08:00:00",
                "2025-06-03 08:30:00",
                6000,
            ),
        ],
    )

    # Records — heart rate
    tmp_db.executemany(
        "INSERT INTO stg_records VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (
                "Watch",
                "10.0",
                None,
                "HeartRate",
                "count/min",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:01:00",
                72,
            ),
            (
                "Watch",
                "10.0",
                None,
                "HeartRate",
                "count/min",
                "2025-06-01 12:00:00",
                "2025-06-01 12:00:00",
                "2025-06-01 12:01:00",
                85,
            ),
            (
                "Watch",
                "10.0",
                None,
                "HeartRate",
                "count/min",
                "2025-06-02 08:00:00",
                "2025-06-02 08:00:00",
                "2025-06-02 08:01:00",
                68,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RestingHeartRate",
                "count/min",
                "2025-06-01 00:00:00",
                "2025-06-01 00:00:00",
                "2025-06-01 00:01:00",
                55,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RestingHeartRate",
                "count/min",
                "2025-06-02 00:00:00",
                "2025-06-02 00:00:00",
                "2025-06-02 00:01:00",
                53,
            ),
        ],
    )

    # Records — VO2Max, HRV, respiratory rate, SpO2, walking speed, flights
    tmp_db.executemany(
        "INSERT INTO stg_records VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (
                "Watch",
                "10.0",
                None,
                "VO2Max",
                "mL/min·kg",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:01:00",
                42.5,
            ),
            (
                "Watch",
                "10.0",
                None,
                "VO2Max",
                "mL/min·kg",
                "2025-06-02 08:00:00",
                "2025-06-02 08:00:00",
                "2025-06-02 08:01:00",
                43.0,
            ),
            (
                "Watch",
                "10.0",
                None,
                "HeartRateVariabilitySDNN",
                "ms",
                "2025-06-01 00:00:00",
                "2025-06-01 00:00:00",
                "2025-06-01 00:01:00",
                45,
            ),
            (
                "Watch",
                "10.0",
                None,
                "HeartRateVariabilitySDNN",
                "ms",
                "2025-06-02 00:00:00",
                "2025-06-02 00:00:00",
                "2025-06-02 00:01:00",
                50,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RespiratoryRate",
                "count/min",
                "2025-06-01 00:00:00",
                "2025-06-01 00:00:00",
                "2025-06-01 00:01:00",
                14.5,
            ),
            (
                "Watch",
                "10.0",
                None,
                "OxygenSaturation",
                "%",
                "2025-06-01 00:00:00",
                "2025-06-01 00:00:00",
                "2025-06-01 00:01:00",
                0.97,
            ),
            (
                "Watch",
                "10.0",
                None,
                "WalkingSpeed",
                "m/s",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:30:00",
                1.4,
            ),
            (
                "Watch",
                "10.0",
                None,
                "FlightsClimbed",
                "count",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:30:00",
                5,
            ),
            (
                "Watch",
                "10.0",
                None,
                "FlightsClimbed",
                "count",
                "2025-06-02 08:00:00",
                "2025-06-02 08:00:00",
                "2025-06-02 08:30:00",
                8,
            ),
            # Distance records for running
            (
                "Watch",
                "10.0",
                None,
                "DistanceWalkingRunning",
                "mi",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                3.1,
            ),
            (
                "Watch",
                "10.0",
                None,
                "DistanceWalkingRunning",
                "mi",
                "2025-06-02 07:00:00",
                "2025-06-02 07:00:00",
                "2025-06-02 07:45:00",
                5.2,
            ),
            # Running speed / power
            (
                "Watch",
                "10.0",
                None,
                "RunningSpeed",
                "m/s",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                3.0,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RunningPower",
                "W",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                250,
            ),
            # Running mechanics
            (
                "Watch",
                "10.0",
                None,
                "RunningStrideLength",
                "m",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                1.2,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RunningVerticalOscillation",
                "cm",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                8.5,
            ),
            (
                "Watch",
                "10.0",
                None,
                "RunningGroundContactTime",
                "ms",
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                240,
            ),
            # Sleep analysis
            (
                "Watch",
                "10.0",
                None,
                "SleepAnalysis",
                None,
                "2025-06-01 22:00:00",
                "2025-06-01 22:00:00",
                "2025-06-02 06:00:00",
                None,
            ),
        ],
    )

    # Workouts
    tmp_db.executemany(
        "INSERT INTO stg_workouts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "Watch",
                "10.0",
                None,
                "2025-06-01 07:00:00",
                "2025-06-01 07:00:00",
                "2025-06-01 07:30:00",
                "HKWorkoutActivityTypeRunning",
                30.0,
                "min",
                None,
                None,
                None,
                None,
            ),
            (
                "Watch",
                "10.0",
                None,
                "2025-06-02 07:00:00",
                "2025-06-02 07:00:00",
                "2025-06-02 07:45:00",
                "HKWorkoutActivityTypeRunning",
                45.0,
                "min",
                None,
                None,
                None,
                None,
            ),
            (
                "Watch",
                "10.0",
                None,
                "2025-06-03 08:00:00",
                "2025-06-03 08:00:00",
                "2025-06-03 09:00:00",
                "HKWorkoutActivityTypeCycling",
                60.0,
                "min",
                None,
                None,
                None,
                None,
            ),
        ],
    )

    # Activity summary
    tmp_db.executemany(
        "INSERT INTO stg_activity_summary VALUES (?,?,?,?,?,?,?,?)",
        [
            ("2025-06-01", 450, 400, "kcal", 35, 30, 12, 12),
            ("2025-06-02", 500, 400, "kcal", 40, 30, 14, 12),
            ("2025-06-03", 350, 400, "kcal", 25, 30, 10, 12),
        ],
    )

    # Sleep cycle
    tmp_db.executemany(
        """INSERT INTO stg_sleep_cycle (start_time, end_time, sleep_quality, regularity,
           awake_seconds, dream_seconds, light_seconds, deep_seconds, mood, heart_rate_bpm,
           steps, alarm_mode, air_pressure_pa, city, movements_per_hour, time_in_bed_seconds,
           time_asleep_seconds, time_before_sleep_seconds, window_start, window_stop,
           snore_time_seconds, weather_temperature_f, weather_type, notes, body_temp_deviation_c)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                "2025-06-01 22:30:00",
                "2025-06-02 06:30:00",
                0.82,
                0.75,
                1200,
                3600,
                10800,
                7200,
                "good",
                58,
                0,
                "normal",
                None,
                None,
                4.2,
                28800,
                25200,
                600,
                None,
                None,
                300,
                68,
                "clear",
                None,
                None,
            ),
            (
                "2025-06-02 23:00:00",
                "2025-06-03 07:00:00",
                0.78,
                0.70,
                1800,
                3000,
                11400,
                6600,
                "ok",
                60,
                0,
                "normal",
                None,
                None,
                5.1,
                28800,
                24000,
                900,
                None,
                None,
                450,
                70,
                "cloudy",
                None,
                None,
            ),
        ],
    )

    # User profile
    tmp_db.executemany(
        "INSERT INTO user_profile (key, value) VALUES (?, ?)",
        [
            ("age", "35"),
            ("sex", "male"),
            ("weight_lbs", "175"),
            ("height_in", "70"),
        ],
    )

    return tmp_db
