from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

STAGING_DDL = """
CREATE TABLE IF NOT EXISTS stg_records (
    source_name    VARCHAR,
    source_version VARCHAR,
    device         VARCHAR,
    record_type    VARCHAR,
    unit           VARCHAR,
    creation_date  TIMESTAMP,
    start_date     TIMESTAMP,
    end_date       TIMESTAMP,
    value          DOUBLE
);

CREATE TABLE IF NOT EXISTS stg_workouts (
    source_name              VARCHAR,
    source_version           VARCHAR,
    device                   VARCHAR,
    creation_date            TIMESTAMP,
    start_date               TIMESTAMP,
    end_date                 TIMESTAMP,
    workout_activity_type    VARCHAR,
    duration                 DOUBLE,
    duration_unit            VARCHAR,
    total_distance           DOUBLE,
    total_distance_unit      VARCHAR,
    total_energy_burned      DOUBLE,
    total_energy_burned_unit VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_activity_summary (
    date_components           DATE,
    active_energy_burned      DOUBLE,
    active_energy_burned_goal DOUBLE,
    active_energy_burned_unit VARCHAR,
    apple_exercise_time       DOUBLE,
    apple_exercise_time_goal  DOUBLE,
    apple_stand_hours         DOUBLE,
    apple_stand_hours_goal    DOUBLE
);

CREATE TABLE IF NOT EXISTS stg_workout_routes (
    gpx_file       VARCHAR,
    workout_name   VARCHAR,
    recorded_at    TIMESTAMP,
    latitude       DOUBLE,
    longitude      DOUBLE,
    elevation      DOUBLE,
    speed          DOUBLE,
    course         DOUBLE,
    horiz_accuracy DOUBLE,
    vert_accuracy  DOUBLE
);

CREATE TABLE IF NOT EXISTS stg_sleep_cycle (
    start_time                TIMESTAMP,
    end_time                  TIMESTAMP,
    sleep_quality             DOUBLE,
    regularity                DOUBLE,
    awake_seconds             DOUBLE,
    dream_seconds             DOUBLE,
    light_seconds             DOUBLE,
    deep_seconds              DOUBLE,
    mood                      VARCHAR,
    heart_rate_bpm            DOUBLE,
    steps                     INTEGER,
    alarm_mode                VARCHAR,
    air_pressure_pa           DOUBLE,
    city                      VARCHAR,
    movements_per_hour        DOUBLE,
    time_in_bed_seconds       DOUBLE,
    time_asleep_seconds       DOUBLE,
    time_before_sleep_seconds DOUBLE,
    window_start              TIMESTAMP,
    window_stop               TIMESTAMP,
    snore_time_seconds        DOUBLE,
    weather_temperature_f     DOUBLE,
    weather_type              VARCHAR,
    notes                     VARCHAR,
    body_temp_deviation_c     DOUBLE
);

CREATE TABLE IF NOT EXISTS user_profile (
    key   VARCHAR PRIMARY KEY,
    value VARCHAR
);
"""

VIEWS_DDL = """
CREATE OR REPLACE VIEW daily_steps AS
SELECT start_date::DATE AS date, SUM(value)::BIGINT AS steps
FROM stg_records WHERE record_type = 'StepCount'
GROUP BY start_date::DATE;

CREATE OR REPLACE VIEW daily_heart_rate AS
SELECT start_date::DATE AS date,
       ROUND(AVG(value), 1) AS avg_hr,
       ROUND(MIN(value), 1) AS min_hr,
       ROUND(MAX(value), 1) AS max_hr
FROM stg_records WHERE record_type = 'HeartRate'
GROUP BY start_date::DATE;

CREATE OR REPLACE VIEW daily_resting_hr AS
SELECT start_date::DATE AS date, ROUND(AVG(value), 1) AS resting_hr
FROM stg_records WHERE record_type = 'RestingHeartRate'
GROUP BY start_date::DATE;

CREATE OR REPLACE VIEW daily_summary AS
SELECT
    a.date_components AS date,
    a.active_energy_burned,
    a.active_energy_burned_goal,
    a.apple_exercise_time,
    a.apple_exercise_time_goal,
    a.apple_stand_hours,
    a.apple_stand_hours_goal,
    s.steps,
    d.distance,
    hr.resting_hr
FROM stg_activity_summary a
LEFT JOIN daily_steps s ON a.date_components = s.date
LEFT JOIN (
    SELECT start_date::DATE AS date, ROUND(SUM(value), 2) AS distance
    FROM stg_records WHERE record_type = 'DistanceWalkingRunning'
    GROUP BY start_date::DATE
) d ON a.date_components = d.date
LEFT JOIN daily_resting_hr hr ON a.date_components = hr.date;

CREATE OR REPLACE VIEW workout_summary AS
SELECT
    w.start_date,
    w.end_date,
    w.workout_activity_type,
    w.duration,
    w.duration_unit,
    w.total_distance,
    w.total_distance_unit,
    w.total_energy_burned,
    w.total_energy_burned_unit,
    ROUND(AVG(CASE WHEN r.record_type = 'HeartRate' THEN r.value END), 1) AS avg_heart_rate,
    ROUND(MIN(CASE WHEN r.record_type = 'HeartRate' THEN r.value END), 1) AS min_heart_rate,
    ROUND(MAX(CASE WHEN r.record_type = 'HeartRate' THEN r.value END), 1) AS max_heart_rate,
    ROUND(AVG(CASE WHEN r.record_type = 'RunningSpeed' THEN r.value END), 2) AS avg_speed
FROM stg_workouts w
LEFT JOIN stg_records r
    ON r.record_type IN ('HeartRate', 'RunningSpeed')
    AND r.start_date BETWEEN w.start_date AND w.end_date
    AND r.end_date BETWEEN w.start_date AND w.end_date
GROUP BY ALL;

CREATE OR REPLACE VIEW sleep_combined AS
WITH apple AS (
    SELECT
        start_date::DATE AS date,
        'apple_health' AS source,
        NULL::DOUBLE AS sleep_quality,
        MAX(EXTRACT(EPOCH FROM end_date - start_date)) AS duration_seconds,
        NULL::DOUBLE AS deep_seconds,
        NULL::DOUBLE AS light_seconds,
        NULL::DOUBLE AS dream_seconds,
        NULL::DOUBLE AS awake_seconds,
        NULL::DOUBLE AS heart_rate_bpm
    FROM stg_records
    WHERE record_type = 'SleepAnalysis'
    GROUP BY start_date::DATE
    HAVING MAX(EXTRACT(EPOCH FROM end_date - start_date)) > 3600
),
sleepcycle AS (
    SELECT
        start_time::DATE AS date,
        'sleep_cycle' AS source,
        sleep_quality,
        time_asleep_seconds AS duration_seconds,
        deep_seconds,
        light_seconds,
        dream_seconds,
        awake_seconds,
        heart_rate_bpm
    FROM stg_sleep_cycle
)
-- Prefer SleepCycle data when both sources have data for the same date
SELECT * FROM sleepcycle
UNION ALL
SELECT * FROM apple WHERE date NOT IN (SELECT date FROM sleepcycle);
"""


@contextmanager
def connect(db_path: Path) -> Generator[duckdb.DuckDBPyConnection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        yield conn
    finally:
        conn.close()


def get_user_profile(conn: duckdb.DuckDBPyConnection) -> dict:
    """Return user profile as a dict of key-value pairs, or empty dict if none set."""
    try:
        rows = conn.execute("SELECT key, value FROM user_profile").fetchall()
        return dict(rows)
    except Exception:  # noqa: BLE001 — table may not exist in older databases
        return {}


def format_height(inches_str: str | None) -> str | None:
    """Convert height in inches to feet and inches display format (e.g., '70' -> '5\\'10\"')."""
    if not inches_str:
        return None
    try:
        total = float(inches_str)
        feet = int(total // 12)
        remaining = int(round(total % 12))
        return f"{feet}'{remaining}\""
    except (ValueError, TypeError):
        return inches_str


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(STAGING_DDL)
    conn.execute(VIEWS_DDL)
