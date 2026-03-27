from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from vitalog.console import get_console

logger = logging.getLogger("vitalog")
console = get_console()


def load_sleep_cycle(csv_path: Path, conn: duckdb.DuckDBPyConnection) -> None:
    console.print(f"[cyan]Loading[/cyan] {csv_path.name} ...")

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM stg_sleep_cycle")

        conn.execute(
            """
            INSERT INTO stg_sleep_cycle
            SELECT
                TRY_CAST("Start" AS TIMESTAMP) AS start_time,
                TRY_CAST("End" AS TIMESTAMP) AS end_time,
                TRY_CAST(REPLACE(CAST("Sleep Quality" AS VARCHAR), '%', '') AS DOUBLE) / 100.0 AS sleep_quality,
                TRY_CAST(REPLACE(CAST("Regularity" AS VARCHAR), '%', '') AS DOUBLE) / 100.0 AS regularity,
                TRY_CAST("Awake (seconds)" AS DOUBLE) AS awake_seconds,
                TRY_CAST("Dream (seconds)" AS DOUBLE) AS dream_seconds,
                TRY_CAST("Light (seconds)" AS DOUBLE) AS light_seconds,
                TRY_CAST("Deep (seconds)" AS DOUBLE) AS deep_seconds,
                CAST("Mood" AS VARCHAR) AS mood,
                TRY_CAST("Heart rate (bpm)" AS DOUBLE) AS heart_rate_bpm,
                TRY_CAST("Steps" AS INTEGER) AS steps,
                CAST("Alarm mode" AS VARCHAR) AS alarm_mode,
                TRY_CAST("Air Pressure (Pa)" AS DOUBLE) AS air_pressure_pa,
                CAST("City" AS VARCHAR) AS city,
                TRY_CAST("Movements per hour" AS DOUBLE) AS movements_per_hour,
                TRY_CAST("Time in bed (seconds)" AS DOUBLE) AS time_in_bed_seconds,
                TRY_CAST("Time asleep (seconds)" AS DOUBLE) AS time_asleep_seconds,
                TRY_CAST("Time before sleep (seconds)" AS DOUBLE) AS time_before_sleep_seconds,
                TRY_CAST("Window start" AS TIMESTAMP) AS window_start,
                TRY_CAST("Window stop" AS TIMESTAMP) AS window_stop,
                TRY_CAST("Snore time (seconds)" AS DOUBLE) AS snore_time_seconds,
                TRY_CAST("Weather temperature (°F)" AS DOUBLE) AS weather_temperature_f,
                CAST("Weather type" AS VARCHAR) AS weather_type,
                CAST("Notes" AS VARCHAR) AS notes,
                TRY_CAST("Body temperature deviation (degrees Celsius)" AS DOUBLE) AS body_temp_deviation_c
            FROM read_csv($csv_path, delim=';', header=true, auto_detect=true)
            """,
            {"csv_path": str(csv_path)},
        )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    count = conn.execute("SELECT COUNT(*) FROM stg_sleep_cycle").fetchone()[0]
    console.print(f"  [green]Loaded[/green] {count:,} sleep records")
