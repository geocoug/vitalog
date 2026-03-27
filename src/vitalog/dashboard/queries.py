from __future__ import annotations

from datetime import date, timedelta

import duckdb


def daily_steps_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            date,
            steps,
            AVG(steps) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::BIGINT AS steps_7d_avg
        FROM daily_steps
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "steps": r[1], "steps_7d_avg": r[2]} for r in rows]


def daily_hr_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            dhr.date,
            dhr.avg_hr,
            dhr.min_hr,
            dhr.max_hr,
            drhr.resting_hr
        FROM daily_heart_rate dhr
        LEFT JOIN daily_resting_hr drhr ON dhr.date = drhr.date
        WHERE dhr.date BETWEEN ? AND ?
        ORDER BY dhr.date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "avg_hr": r[1], "min_hr": r[2], "max_hr": r[3], "resting_hr": r[4]} for r in rows]


def activity_rings_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            date_components AS date,
            active_energy_burned,
            active_energy_burned_goal,
            apple_exercise_time,
            apple_exercise_time_goal,
            apple_stand_hours,
            apple_stand_hours_goal
        FROM stg_activity_summary
        WHERE date_components BETWEEN ? AND ?
        ORDER BY date_components
        """,
        [s, e],
    ).fetchall()
    return [
        {
            "date": str(r[0]),
            "energy": r[1],
            "energy_goal": r[2],
            "exercise": r[3],
            "exercise_goal": r[4],
            "stand": r[5],
            "stand_goal": r[6],
        }
        for r in rows
    ]


def sleep_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            date,
            source,
            ROUND(duration_seconds / 3600.0, 2) AS hours,
            sleep_quality
        FROM sleep_combined
        WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "source": r[1], "hours": r[2], "quality": r[3]} for r in rows]


def workout_type_counts(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT workout_activity_type, COUNT(*) AS count
        FROM stg_workouts
        WHERE start_date::DATE BETWEEN ? AND ?
        GROUP BY workout_activity_type
        ORDER BY count DESC
        """,
        [s, e],
    ).fetchall()
    return [{"type": r[0], "count": r[1]} for r in rows]


def workout_routes_data(
    conn: duckdb.DuckDBPyConnection,
    s: date,
    e: date,
    limit: int = 50,
    max_pts: int = 200,
) -> list[dict]:
    meta = conn.execute(
        """
        WITH rf AS (
            SELECT gpx_file,
                   MIN(recorded_at) - INTERVAL 7 HOUR AS rstart,
                   MAX(recorded_at) - INTERVAL 7 HOUR AS rend,
                   COUNT(*) AS pt_count
            FROM stg_workout_routes
            WHERE recorded_at::DATE BETWEEN ? AND ?
            GROUP BY gpx_file
            ORDER BY MIN(recorded_at) DESC
            LIMIT ?
        ),
        matched AS (
            SELECT rf.gpx_file,
                   w.workout_activity_type,
                   w.start_date::DATE AS dt,
                   ROUND(w.duration, 1) AS dur_min,
                   ROW_NUMBER() OVER (
                       PARTITION BY rf.gpx_file
                       ORDER BY ABS(EPOCH(w.start_date) - EPOCH(rf.rstart))
                   ) AS rn
            FROM rf
            LEFT JOIN stg_workouts w
                ON w.start_date <= rf.rend AND w.end_date >= rf.rstart
        ),
        gps_dist AS (
            SELECT gpx_file,
                   ROUND(SUM(3959 * 2 * ASIN(SQRT(
                       SIN(RADIANS(lat2 - lat) / 2) * SIN(RADIANS(lat2 - lat) / 2)
                       + COS(RADIANS(lat)) * COS(RADIANS(lat2))
                       * SIN(RADIANS(lon2 - lon) / 2) * SIN(RADIANS(lon2 - lon) / 2)
                   ))), 2) AS miles
            FROM (
                SELECT gpx_file,
                       latitude AS lat, longitude AS lon,
                       LEAD(latitude) OVER (PARTITION BY gpx_file ORDER BY recorded_at) AS lat2,
                       LEAD(longitude) OVER (PARTITION BY gpx_file ORDER BY recorded_at) AS lon2
                FROM stg_workout_routes
                WHERE gpx_file IN (SELECT gpx_file FROM rf)
            ) sub
            WHERE lat2 IS NOT NULL
            GROUP BY gpx_file
        )
        SELECT rf.gpx_file,
               m.workout_activity_type,
               m.dt,
               m.dur_min,
               gd.miles,
               rf.pt_count
        FROM rf
        LEFT JOIN matched m ON m.gpx_file = rf.gpx_file AND m.rn = 1
        LEFT JOIN gps_dist gd ON gd.gpx_file = rf.gpx_file
        ORDER BY rf.rstart DESC
        """,
        [s, e, limit],
    ).fetchall()

    routes = []
    for gpx_file, wtype, dt, dur, miles, pt_count in meta:
        # Downsample: keep every Nth point to stay under max_pts
        step = max(1, (pt_count or 1) // max_pts)
        points = conn.execute(
            """
            SELECT latitude, longitude, elevation, speed
            FROM (
                SELECT latitude, longitude, elevation, speed,
                       ROW_NUMBER() OVER (ORDER BY recorded_at) AS rn,
                       COUNT(*) OVER () AS total
                FROM stg_workout_routes
                WHERE gpx_file = ?
            )
            WHERE rn = 1 OR rn % ? = 0 OR rn = total
            ORDER BY rn
            """,
            [gpx_file, step],
        ).fetchall()
        if points:
            clean_type = (wtype or "").replace("HKWorkoutActivityType", "") if wtype else None
            routes.append(
                {
                    "name": gpx_file,
                    "workout_type": clean_type,
                    "date": str(dt) if dt else None,
                    "duration_min": float(dur) if dur else None,
                    "distance_mi": float(miles) if miles else None,
                    "points": [{"lat": p[0], "lon": p[1], "ele": p[2], "speed": p[3]} for p in points],
                },
            )
    return routes


def running_pace_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            w.start_date::DATE AS date,
            ROUND(w.duration, 1) AS duration_min,
            ROUND(w.total_distance, 2) AS distance,
            w.total_distance_unit,
            ROUND(AVG(r.value), 2) AS avg_speed_mph
        FROM stg_workouts w
        LEFT JOIN stg_records r
            ON r.record_type = 'RunningSpeed'
            AND r.start_date BETWEEN w.start_date AND w.end_date
            AND r.end_date BETWEEN w.start_date AND w.end_date
        WHERE w.workout_activity_type LIKE '%Running%'
            AND w.start_date::DATE BETWEEN ? AND ?
            AND w.duration > 5
        GROUP BY w.start_date, w.duration, w.total_distance, w.total_distance_unit
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    result = []
    for r in rows:
        pace = None
        # RunningSpeed is stored in m/s; convert to mph (1 m/s = 2.23694 mph)
        speed_mph = round(r[4] * 2.23694, 2) if r[4] else None
        if speed_mph and speed_mph > 0:
            pace = round(60.0 / speed_mph, 2)  # min/mile
        result.append(
            {
                "date": str(r[0]),
                "duration_min": r[1],
                "distance": r[2],
                "avg_speed_mph": speed_mph,
                "pace_min_per_mi": pace,
            },
        )
    return result


def running_distance_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            w.start_date::DATE AS date,
            ROUND(SUM(r.value), 2) AS distance,
            ROUND(w.duration, 1) AS duration_min
        FROM stg_workouts w
        JOIN stg_records r
            ON r.record_type = 'DistanceWalkingRunning'
            AND r.start_date BETWEEN w.start_date AND w.end_date
            AND r.end_date BETWEEN w.start_date AND w.end_date
        WHERE w.workout_activity_type LIKE '%Running%'
            AND w.start_date::DATE BETWEEN ? AND ?
            AND w.duration > 5
        GROUP BY w.start_date, w.duration
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "distance": r[1], "duration_min": r[2]} for r in rows]


def cycling_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            w.start_date::DATE AS date,
            dist.miles AS distance,
            ROUND(w.duration, 1) AS duration_min,
            cal.kcal AS calories
        FROM stg_workouts w
        LEFT JOIN (
            SELECT w2.start_date AS ws, ROUND(SUM(r.value), 2) AS miles
            FROM stg_workouts w2
            JOIN stg_records r
                ON r.record_type = 'DistanceCycling'
                AND r.start_date BETWEEN w2.start_date AND w2.end_date
                AND r.end_date BETWEEN w2.start_date AND w2.end_date
            WHERE w2.workout_activity_type LIKE '%Cycling%'
                AND w2.start_date::DATE BETWEEN ? AND ?
            GROUP BY w2.start_date
        ) dist ON dist.ws = w.start_date
        LEFT JOIN (
            SELECT w2.start_date AS ws, ROUND(SUM(r.value), 0) AS kcal
            FROM stg_workouts w2
            JOIN stg_records r
                ON r.record_type = 'ActiveEnergyBurned'
                AND r.start_date BETWEEN w2.start_date AND w2.end_date
                AND r.end_date BETWEEN w2.start_date AND w2.end_date
            WHERE w2.workout_activity_type LIKE '%Cycling%'
                AND w2.start_date::DATE BETWEEN ? AND ?
            GROUP BY w2.start_date
        ) cal ON cal.ws = w.start_date
        WHERE w.workout_activity_type LIKE '%Cycling%'
            AND w.start_date::DATE BETWEEN ? AND ?
            AND w.duration > 5
        ORDER BY date
        """,
        [s, e, s, e, s, e],
    ).fetchall()
    return [
        {
            "date": str(r[0]),
            "distance": r[1],
            "duration_min": r[2],
            "calories": r[3],
        }
        for r in rows
    ]


def weekly_workout_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        WITH top_types AS (
            SELECT workout_activity_type
            FROM stg_workouts
            WHERE start_date::DATE BETWEEN ? AND ?
            GROUP BY workout_activity_type
            ORDER BY COUNT(*) DESC
            LIMIT 6
        )
        SELECT
            DATE_TRUNC('week', start_date)::DATE AS week,
            CASE WHEN t.workout_activity_type IS NOT NULL
                 THEN w.workout_activity_type ELSE 'Other' END AS wtype,
            COUNT(*) AS count
        FROM stg_workouts w
        LEFT JOIN top_types t ON w.workout_activity_type = t.workout_activity_type
        WHERE w.start_date::DATE BETWEEN ? AND ?
        GROUP BY week, wtype
        ORDER BY week, wtype
        """,
        [s, e, s, e],
    ).fetchall()
    return [{"week": str(r[0]), "type": r[1], "count": r[2]} for r in rows]


def weekly_volume_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Weekly training volume in minutes, broken down by workout type."""
    rows = conn.execute(
        """
        WITH top_types AS (
            SELECT workout_activity_type
            FROM stg_workouts
            WHERE start_date::DATE BETWEEN ? AND ?
            GROUP BY workout_activity_type
            ORDER BY COUNT(*) DESC
            LIMIT 6
        )
        SELECT
            DATE_TRUNC('week', start_date)::DATE AS week,
            CASE WHEN t.workout_activity_type IS NOT NULL
                 THEN w.workout_activity_type ELSE 'Other' END AS wtype,
            ROUND(SUM(w.duration), 0)::INT AS minutes
        FROM stg_workouts w
        LEFT JOIN top_types t ON w.workout_activity_type = t.workout_activity_type
        WHERE w.start_date::DATE BETWEEN ? AND ?
        GROUP BY week, wtype
        ORDER BY week, wtype
        """,
        [s, e, s, e],
    ).fetchall()
    return [{"week": str(r[0]), "type": r[1], "minutes": r[2]} for r in rows]


def workout_heatmap_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_date::DATE AS date,
            DAYOFWEEK(start_date::DATE) AS dow,
            ROUND(SUM(duration), 0) AS total_min
        FROM stg_workouts
        WHERE start_date::DATE BETWEEN ? AND ?
        GROUP BY start_date::DATE
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "dow": r[1], "total_min": r[2]} for r in rows]


def day_of_week_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        WITH dow_steps AS (
            SELECT DAYOFWEEK(date) AS dow, AVG(steps) AS avg_steps
            FROM daily_steps WHERE date BETWEEN ? AND ? GROUP BY 1
        ),
        dow_workout AS (
            SELECT DAYOFWEEK(start_date::DATE) AS dow,
                   SUM(duration) / COUNT(DISTINCT start_date::DATE) AS avg_workout_min
            FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ? GROUP BY 1
        ),
        dow_sleep AS (
            SELECT DAYOFWEEK(date) AS dow, AVG(duration_seconds) / 3600.0 AS avg_sleep_hrs
            FROM sleep_combined WHERE date BETWEEN ? AND ? AND duration_seconds > 0 GROUP BY 1
        )
        SELECT
            s.dow,
            ROUND(COALESCE(s.avg_steps, 0))::BIGINT AS avg_steps,
            ROUND(COALESCE(w.avg_workout_min, 0), 1) AS avg_workout_min,
            ROUND(COALESCE(sl.avg_sleep_hrs, 0), 1) AS avg_sleep_hrs
        FROM dow_steps s
        LEFT JOIN dow_workout w ON s.dow = w.dow
        LEFT JOIN dow_sleep sl ON s.dow = sl.dow
        ORDER BY s.dow
        """,
        [s, e, s, e, s, e],
    ).fetchall()
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    return [
        {
            "day": day_names[r[0]] if r[0] < len(day_names) else str(r[0]),
            "avg_steps": r[1],
            "avg_workout_min": r[2],
            "avg_sleep_hrs": r[3],
        }
        for r in rows
    ]


def monthly_summary_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        WITH months AS (
            SELECT DATE_TRUNC('month', UNNEST(GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL 1 DAY)))::DATE AS month
        ),
        distinct_months AS (SELECT DISTINCT month FROM months ORDER BY month),
        m_steps AS (
            SELECT DATE_TRUNC('month', date)::DATE AS month,
                   SUM(steps)::BIGINT AS total_steps,
                   ROUND(AVG(steps))::BIGINT AS avg_daily_steps
            FROM daily_steps WHERE date BETWEEN ? AND ? GROUP BY 1
        ),
        m_workouts AS (
            SELECT DATE_TRUNC('month', start_date)::DATE AS month,
                   COUNT(*) AS workouts,
                   ROUND(SUM(duration)) AS total_workout_min
            FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ? GROUP BY 1
        ),
        m_sleep AS (
            SELECT DATE_TRUNC('month', date)::DATE AS month,
                   ROUND(AVG(duration_seconds) / 3600.0, 1) AS avg_sleep_hrs
            FROM sleep_combined WHERE date BETWEEN ? AND ? AND duration_seconds > 0 GROUP BY 1
        ),
        m_hr AS (
            SELECT DATE_TRUNC('month', date)::DATE AS month,
                   ROUND(AVG(resting_hr), 1) AS avg_resting_hr
            FROM daily_resting_hr WHERE date BETWEEN ? AND ? GROUP BY 1
        ),
        m_vo2 AS (
            SELECT DATE_TRUNC('month', start_date)::DATE AS month,
                   ROUND(AVG(value), 1) AS avg_vo2max
            FROM stg_records
            WHERE record_type = 'VO2Max'
              AND start_date::DATE BETWEEN ? AND ?
            GROUP BY 1
        ),
        m_hrv AS (
            SELECT DATE_TRUNC('month', start_date)::DATE AS month,
                   ROUND(AVG(value), 1) AS avg_hrv
            FROM stg_records
            WHERE record_type = 'HeartRateVariabilitySDNN'
              AND start_date::DATE BETWEEN ? AND ?
            GROUP BY 1
        )
        SELECT
            dm.month,
            s.total_steps,
            s.avg_daily_steps,
            w.workouts,
            w.total_workout_min,
            sl.avg_sleep_hrs,
            hr.avg_resting_hr,
            v.avg_vo2max,
            h.avg_hrv
        FROM distinct_months dm
        LEFT JOIN m_steps s ON dm.month = s.month
        LEFT JOIN m_workouts w ON dm.month = w.month
        LEFT JOIN m_sleep sl ON dm.month = sl.month
        LEFT JOIN m_hr hr ON dm.month = hr.month
        LEFT JOIN m_vo2 v ON dm.month = v.month
        LEFT JOIN m_hrv h ON dm.month = h.month
        ORDER BY dm.month
        """,
        [s, e, s, e, s, e, s, e, s, e, s, e, s, e],
    ).fetchall()
    return [
        {
            "month": r[0].strftime("%Y-%m") if r[0] else "",
            "total_steps": r[1],
            "avg_daily_steps": r[2],
            "workouts": r[3],
            "total_workout_min": r[4],
            "avg_sleep_hrs": r[5],
            "avg_resting_hr": r[6],
            "avg_vo2max": r[7],
            "avg_hrv": r[8],
        }
        for r in rows
    ]


def correlation_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    rows = conn.execute(
        """
        WITH daily AS (
            SELECT
                ds.date,
                ds.steps,
                drhr.resting_hr,
                sc.duration_seconds / 3600.0 AS sleep_hrs,
                sc.sleep_quality,
                w.workout_min
            FROM daily_steps ds
            LEFT JOIN daily_resting_hr drhr ON ds.date = drhr.date
            LEFT JOIN (
                SELECT date, MAX(duration_seconds) AS duration_seconds, MAX(sleep_quality) AS sleep_quality
                FROM sleep_combined WHERE duration_seconds > 0
                GROUP BY date
            ) sc ON ds.date = sc.date
            LEFT JOIN (
                SELECT start_date::DATE AS date, SUM(duration) AS workout_min
                FROM stg_workouts GROUP BY 1
            ) w ON ds.date = w.date
            WHERE ds.date BETWEEN ? AND ?
        )
        SELECT date, steps, resting_hr, sleep_hrs, sleep_quality, workout_min
        FROM daily
        WHERE steps IS NOT NULL
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return {
        "dates": [str(r[0]) for r in rows],
        "steps": [r[1] for r in rows],
        "resting_hr": [r[2] for r in rows],
        "sleep_hrs": [r[3] for r in rows],
        "sleep_quality": [r[4] for r in rows],
        "workout_min": [r[5] for r in rows],
    }


_ALLOWED_AGGS = {"AVG", "SUM", "MIN", "MAX"}


def _daily_record_data(
    conn: duckdb.DuckDBPyConnection,
    s: date,
    e: date,
    record_type: str,
    agg: str = "AVG",
    ma_window: int = 7,
) -> list[dict]:
    """Generic daily aggregate for a record type with rolling average."""
    if agg not in _ALLOWED_AGGS:
        msg = f"Invalid aggregate function: {agg}"
        raise ValueError(msg)
    if not isinstance(ma_window, int) or ma_window < 1:
        msg = f"Invalid moving average window: {ma_window}"
        raise ValueError(msg)
    rows = conn.execute(
        f"""
        SELECT
            start_date::DATE AS date,
            ROUND({agg}(value), 2) AS val,
            ROUND(AVG({agg}(value)) OVER (
                ORDER BY start_date::DATE ROWS BETWEEN {ma_window - 1} PRECEDING AND CURRENT ROW
            ), 2) AS ma
        FROM stg_records
        WHERE record_type = ? AND start_date::DATE BETWEEN ? AND ?
        GROUP BY start_date::DATE
        ORDER BY date
        """,  # noqa: S608 — agg and ma_window are validated above
        [record_type, s, e],
    ).fetchall()
    return [{"date": str(r[0]), "val": r[1], "ma": r[2]} for r in rows]


def vo2max_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    return _daily_record_data(conn, s, e, "VO2Max", ma_window=30)


def hrv_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    return _daily_record_data(conn, s, e, "HeartRateVariabilitySDNN", ma_window=7)


def respiratory_rate_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    return _daily_record_data(conn, s, e, "RespiratoryRate", ma_window=7)


def spo2_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_date::DATE AS date,
            -- Apple Health stores SpO2 as 0-1 fraction; guard against already-percentage values
            ROUND(AVG(CASE WHEN value <= 1 THEN value * 100 ELSE value END), 1) AS avg_pct,
            ROUND(MIN(CASE WHEN value <= 1 THEN value * 100 ELSE value END), 1) AS min_pct
        FROM stg_records
        WHERE record_type = 'OxygenSaturation' AND start_date::DATE BETWEEN ? AND ?
        GROUP BY start_date::DATE
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "avg_pct": r[1], "min_pct": r[2]} for r in rows]


def sleep_stages_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            ROUND(deep_seconds / 3600.0, 2) AS deep_hrs,
            ROUND(light_seconds / 3600.0, 2) AS light_hrs,
            ROUND(dream_seconds / 3600.0, 2) AS rem_hrs,
            ROUND(awake_seconds / 3600.0, 2) AS awake_hrs
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND (deep_seconds + light_seconds + dream_seconds + awake_seconds) > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "deep": r[1], "light": r[2], "rem": r[3], "awake": r[4]} for r in rows]


def sleep_efficiency_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            ROUND(time_asleep_seconds / NULLIF(time_in_bed_seconds, 0) * 100, 1) AS efficiency_pct,
            ROUND(AVG(time_asleep_seconds / NULLIF(time_in_bed_seconds, 0) * 100)
                OVER (ORDER BY start_time::DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS efficiency_7d_avg
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND time_in_bed_seconds > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "val": r[1], "ma": r[2]} for r in rows]


def sleep_latency_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            ROUND(time_before_sleep_seconds / 60.0, 1) AS latency_min,
            ROUND(AVG(time_before_sleep_seconds / 60.0)
                OVER (ORDER BY start_time::DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS latency_7d_avg
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND time_before_sleep_seconds IS NOT NULL AND time_before_sleep_seconds > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "val": r[1], "ma": r[2]} for r in rows]


def snore_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            ROUND(snore_time_seconds / 60.0, 1) AS snore_min,
            ROUND(AVG(snore_time_seconds / 60.0)
                OVER (ORDER BY start_time::DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS snore_7d_avg
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND snore_time_seconds IS NOT NULL AND snore_time_seconds > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "val": r[1], "ma": r[2]} for r in rows]


def sleep_hr_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            sc.start_time::DATE AS date,
            sc.heart_rate_bpm AS sleep_hr,
            drhr.resting_hr
        FROM stg_sleep_cycle sc
        LEFT JOIN daily_resting_hr drhr ON sc.start_time::DATE = drhr.date
        WHERE sc.start_time::DATE BETWEEN ? AND ?
            AND sc.heart_rate_bpm IS NOT NULL
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "sleep_hr": r[1], "resting_hr": r[2]} for r in rows]


def sleep_regularity_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            ROUND(regularity * 100, 1) AS regularity_pct,
            ROUND(AVG(regularity * 100)
                OVER (ORDER BY start_time::DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS regularity_7d_avg
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND regularity IS NOT NULL AND regularity > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "val": r[1], "ma": r[2]} for r in rows]


def sleep_environment_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        WITH bounds AS (
            SELECT
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY weather_temperature_f) AS temp_lo,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY weather_temperature_f) AS temp_hi,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY movements_per_hour) AS mov_lo,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY movements_per_hour) AS mov_hi
            FROM stg_sleep_cycle
            WHERE start_time::DATE BETWEEN ? AND ?
                AND weather_temperature_f IS NOT NULL
                AND movements_per_hour IS NOT NULL
        )
        SELECT
            start_time::DATE AS date,
            weather_temperature_f AS temp_f,
            air_pressure_pa,
            movements_per_hour,
            ROUND(sleep_quality * 100, 1) AS quality_pct
        FROM stg_sleep_cycle, bounds
        WHERE start_time::DATE BETWEEN ? AND ?
            AND (weather_temperature_f IS NULL
                 OR (weather_temperature_f BETWEEN bounds.temp_lo AND bounds.temp_hi))
            AND (movements_per_hour IS NULL
                 OR (movements_per_hour BETWEEN bounds.mov_lo AND bounds.mov_hi))
        ORDER BY date
        """,
        [s, e, s, e],
    ).fetchall()
    return [
        {
            "date": str(r[0]),
            "temp_f": r[1],
            "pressure": r[2],
            "movements": r[3],
            "quality": r[4],
        }
        for r in rows
    ]


def sleep_impact_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        WITH sleep_prev AS (
            SELECT date + INTERVAL 1 DAY AS next_day,
                   duration_seconds / 3600.0 AS sleep_hrs,
                   sleep_quality
            FROM sleep_combined
            WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        )
        SELECT
            sp.next_day::DATE AS date,
            ROUND(sp.sleep_hrs, 1) AS sleep_hrs,
            ROUND(sp.sleep_quality * 100, 1) AS sleep_quality_pct,
            ds.steps,
            drhr.resting_hr,
            ROUND(w.workout_min, 0) AS workout_min
        FROM sleep_prev sp
        LEFT JOIN daily_steps ds ON sp.next_day = ds.date
        LEFT JOIN daily_resting_hr drhr ON sp.next_day = drhr.date
        LEFT JOIN (
            SELECT start_date::DATE AS date, SUM(duration) AS workout_min
            FROM stg_workouts GROUP BY 1
        ) w ON sp.next_day = w.date
        WHERE ds.steps IS NOT NULL
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [
        {
            "date": str(r[0]),
            "sleep_hrs": r[1],
            "sleep_quality": r[2],
            "steps": r[3],
            "resting_hr": r[4],
            "workout_min": r[5],
        }
        for r in rows
    ]


def walking_speed_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    return _daily_record_data(conn, s, e, "WalkingSpeed", ma_window=14)


def flights_climbed_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    return _daily_record_data(conn, s, e, "FlightsClimbed", agg="SUM", ma_window=7)


def running_mechanics_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            w.start_date::DATE AS date,
            ROUND(AVG(CASE WHEN r.record_type = 'RunningPower' THEN r.value END), 1) AS power_w,
            ROUND(AVG(CASE WHEN r.record_type = 'RunningGroundContactTime' THEN r.value END), 1) AS gct_ms,
            ROUND(AVG(CASE WHEN r.record_type = 'RunningVerticalOscillation' THEN r.value END), 2) AS osc_cm,
            ROUND(AVG(CASE WHEN r.record_type = 'RunningSpeed' THEN r.value END), 2) AS speed_mph
        FROM stg_workouts w
        JOIN stg_records r
            ON r.record_type IN ('RunningPower', 'RunningGroundContactTime',
                                 'RunningVerticalOscillation', 'RunningSpeed')
            AND r.start_date BETWEEN w.start_date AND w.end_date
        WHERE w.workout_activity_type LIKE '%Running%'
            AND w.start_date::DATE BETWEEN ? AND ?
            AND w.duration > 5
        GROUP BY w.start_date::DATE
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "power": r[1], "gct": r[2], "osc": r[3], "speed": r[4]} for r in rows]


def summary_cards_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    steps = conn.execute(
        "SELECT COALESCE(ROUND(AVG(steps)), 0)::BIGINT FROM daily_steps WHERE date BETWEEN ? AND ?",
        [s, e],
    ).fetchone()[0]

    resting_hr = conn.execute(
        "SELECT ROUND(AVG(resting_hr), 1) FROM daily_resting_hr WHERE date BETWEEN ? AND ?",
        [s, e],
    ).fetchone()[0]

    workouts = conn.execute(
        "SELECT COUNT(*) FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?",
        [s, e],
    ).fetchone()[0]

    avg_sleep = conn.execute(
        """
        SELECT ROUND(AVG(duration_seconds) / 3600.0, 1)
        FROM sleep_combined WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        """,
        [s, e],
    ).fetchone()[0]

    ring_pct = conn.execute(
        """
        SELECT ROUND(100.0 * SUM(CASE WHEN
            active_energy_burned >= active_energy_burned_goal
            AND apple_exercise_time >= apple_exercise_time_goal
            AND apple_stand_hours >= apple_stand_hours_goal
            AND active_energy_burned_goal > 0
            AND apple_exercise_time_goal > 0
            AND apple_stand_hours_goal > 0
            THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0), 1)
        FROM stg_activity_summary WHERE date_components BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Distance-based stats (join workouts with distance records)
    race_stats = conn.execute(
        """
        WITH run_dist AS (
            SELECT w.start_date, SUM(r.value) AS miles
            FROM stg_workouts w
            JOIN stg_records r ON r.record_type = 'DistanceWalkingRunning'
                AND r.start_date BETWEEN w.start_date AND w.end_date
            WHERE w.workout_activity_type LIKE '%Running%'
                AND w.start_date::DATE BETWEEN ? AND ?
            GROUP BY w.start_date
        )
        SELECT
            COALESCE(SUM(CASE WHEN miles >= 26.0 THEN 1 ELSE 0 END), 0) AS marathons,
            COALESCE(SUM(CASE WHEN miles >= 13.0 AND miles < 26.0 THEN 1 ELSE 0 END), 0) AS halves,
            ROUND(COALESCE(MAX(miles), 0), 1) AS longest_run_mi,
            ROUND(COALESCE(SUM(miles), 0), 0)::BIGINT AS total_run_mi
        FROM run_dist
        """,
        [s, e],
    ).fetchone()

    longest_walk = conn.execute(
        """
        SELECT ROUND(COALESCE(MAX(walk_mi), 0), 1)
        FROM (
            SELECT SUM(r.value) AS walk_mi
            FROM stg_workouts w
            JOIN stg_records r ON r.record_type = 'DistanceWalkingRunning'
                AND r.start_date BETWEEN w.start_date AND w.end_date
            WHERE w.workout_activity_type LIKE '%Walking%'
                AND w.start_date::DATE BETWEEN ? AND ?
            GROUP BY w.start_date
        )
        """,
        [s, e],
    ).fetchone()[0]

    # Cycling stats
    cycling = conn.execute(
        """
        WITH ride_dist AS (
            SELECT w.start_date, SUM(r.value) AS miles
            FROM stg_workouts w
            JOIN stg_records r ON r.record_type = 'DistanceCycling'
                AND r.start_date BETWEEN w.start_date AND w.end_date
            WHERE w.workout_activity_type LIKE '%Cycling%'
                AND w.start_date::DATE BETWEEN ? AND ?
            GROUP BY w.start_date
        )
        SELECT
            ROUND(COALESCE(SUM(miles), 0), 0)::BIGINT AS total_mi,
            ROUND(COALESCE(MAX(miles), 0), 1) AS longest_mi
        FROM ride_dist
        """,
        [s, e],
    ).fetchone()

    cycling_count = conn.execute(
        """
        SELECT COUNT(*) FROM stg_workouts
        WHERE workout_activity_type LIKE '%Cycling%' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Total workout hours
    total_workout_hrs = conn.execute(
        """
        SELECT ROUND(COALESCE(SUM(duration), 0) / 60.0, 0)::BIGINT
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Longest consecutive workout day streak
    streak = conn.execute(
        """
        WITH dates AS (
            SELECT DISTINCT start_date::DATE AS d FROM stg_workouts
            WHERE start_date::DATE BETWEEN ? AND ?
        ),
        gaps AS (
            SELECT d, d - ROW_NUMBER() OVER (ORDER BY d) * INTERVAL 1 DAY AS grp FROM dates
        )
        SELECT COALESCE(MAX(cnt), 0) FROM (SELECT COUNT(*) AS cnt FROM gaps GROUP BY grp)
        """,
        [s, e],
    ).fetchone()[0]

    active_days = conn.execute(
        "SELECT COUNT(DISTINCT start_date::DATE) FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?",
        [s, e],
    ).fetchone()[0]

    # VO2Max (latest + peak)
    vo2_latest = conn.execute(
        """
        SELECT ROUND(value, 1) FROM stg_records
        WHERE record_type = 'VO2Max' AND start_date::DATE BETWEEN ? AND ?
        ORDER BY start_date DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    vo2_peak = conn.execute(
        """
        SELECT ROUND(MAX(value), 1) FROM stg_records
        WHERE record_type = 'VO2Max' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    # HRV average
    avg_hrv = conn.execute(
        """
        SELECT ROUND(AVG(value), 0)::BIGINT FROM stg_records
        WHERE record_type = 'HeartRateVariabilitySDNN' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Flights climbed total
    total_flights = conn.execute(
        """
        SELECT COALESCE(ROUND(SUM(value)), 0)::BIGINT FROM stg_records
        WHERE record_type = 'FlightsClimbed' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Walking speed avg
    avg_walk_speed = conn.execute(
        """
        SELECT ROUND(AVG(value), 1) FROM stg_records
        WHERE record_type = 'WalkingSpeed' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Sleep stage averages (from SleepCycle)
    sleep_stages = conn.execute(
        """
        SELECT
            ROUND(AVG(deep_seconds) / 3600.0, 1),
            ROUND(AVG(dream_seconds) / 3600.0, 1),
            ROUND(MAX(sleep_quality) * 100, 0)::BIGINT
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND (deep_seconds + light_seconds + dream_seconds + awake_seconds) > 0
        """,
        [s, e],
    ).fetchone()

    # Running power avg
    avg_run_power = conn.execute(
        """
        SELECT ROUND(AVG(r.value), 0)::BIGINT
        FROM stg_workouts w
        JOIN stg_records r ON r.record_type = 'RunningPower'
            AND r.start_date BETWEEN w.start_date AND w.end_date
        WHERE w.workout_activity_type LIKE '%Running%'
            AND w.start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Run count and avg run duration
    run_stats = conn.execute(
        """
        SELECT COUNT(*), ROUND(AVG(duration), 0)::BIGINT
        FROM stg_workouts
        WHERE workout_activity_type LIKE '%Running%' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    # Total workout calories
    total_calories = conn.execute(
        """
        SELECT COALESCE(ROUND(SUM(total_energy_burned)), 0)::BIGINT
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Avg workout duration
    avg_workout_dur = conn.execute(
        """
        SELECT ROUND(AVG(duration), 0)::BIGINT
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Strength training stats (HIIT, functional strength, core)
    strength = conn.execute(
        """
        SELECT COUNT(*) AS cnt,
               ROUND(COALESCE(SUM(duration), 0))::BIGINT AS total_min,
               ROUND(COALESCE(AVG(duration), 0))::BIGINT AS avg_min,
               COALESCE(ROUND(SUM(total_energy_burned)), 0)::BIGINT AS total_cal
        FROM stg_workouts
        WHERE (workout_activity_type LIKE '%HIIT%'
            OR workout_activity_type LIKE '%FunctionalStrengthTraining%'
            OR workout_activity_type LIKE '%CoreTraining%'
            OR workout_activity_type LIKE '%TraditionalStrengthTraining%')
            AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    # Avg SpO2
    avg_spo2 = conn.execute(
        """
        SELECT ROUND(AVG(CASE WHEN value <= 1 THEN value * 100 ELSE value END), 1)
        FROM stg_records
        WHERE record_type = 'OxygenSaturation' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Avg respiratory rate
    avg_resp = conn.execute(
        """
        SELECT ROUND(AVG(value), 1) FROM stg_records
        WHERE record_type = 'RespiratoryRate' AND start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()[0]

    # Avg daily walking/running distance
    avg_daily_dist = conn.execute(
        """
        SELECT ROUND(AVG(dist), 1) FROM (
            SELECT start_date::DATE AS d, SUM(value) AS dist
            FROM stg_records WHERE record_type = 'DistanceWalkingRunning'
                AND start_date::DATE BETWEEN ? AND ?
            GROUP BY d
        )
        """,
        [s, e],
    ).fetchone()[0]

    return {
        "avg_steps": steps or 0,
        "avg_resting_hr": resting_hr,
        "workouts": workouts or 0,
        "avg_sleep_hours": avg_sleep,
        "ring_close_pct": ring_pct,
        "marathons": race_stats[0],
        "half_marathons": race_stats[1],
        "longest_run_mi": race_stats[2],
        "total_run_mi": race_stats[3],
        "longest_walk_mi": longest_walk,
        "workout_streak": streak,
        "active_days": active_days or 0,
        "cycling_count": cycling_count or 0,
        "total_cycling_mi": cycling[0],
        "longest_ride_mi": cycling[1],
        "total_workout_hrs": total_workout_hrs or 0,
        "vo2max_latest": vo2_latest[0] if vo2_latest else None,
        "vo2max_peak": vo2_peak[0] if vo2_peak else None,
        "avg_hrv": avg_hrv,
        "total_flights": total_flights or 0,
        "avg_walking_speed": avg_walk_speed,
        "avg_deep_sleep": sleep_stages[0] if sleep_stages else None,
        "avg_rem_sleep": sleep_stages[1] if sleep_stages else None,
        "best_sleep_quality": sleep_stages[2] if sleep_stages else None,
        "avg_run_power": avg_run_power,
        "run_count": run_stats[0] if run_stats else 0,
        "avg_run_dur": run_stats[1] if run_stats else None,
        "total_calories": total_calories or 0,
        "avg_workout_dur": avg_workout_dur,
        "avg_spo2": avg_spo2,
        "avg_resp_rate": avg_resp,
        "avg_daily_distance": avg_daily_dist,
        "strength_count": strength[0] if strength else 0,
        "strength_total_min": strength[1] if strength else 0,
        "strength_avg_min": strength[2] if strength else None,
        "strength_total_cal": strength[3] if strength else 0,
    }


def personal_records_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Find personal records (bests) within the date range."""
    records = []

    # Most steps in a day
    row = conn.execute(
        "SELECT date, steps FROM daily_steps WHERE date BETWEEN ? AND ? ORDER BY steps DESC LIMIT 1",
        [s, e],
    ).fetchone()
    if row:
        records.append({"label": "Most Steps", "value": f"{row[1]:,}", "date": str(row[0]), "icon": "\U0001f463"})

    # Longest run (convert km to miles if needed)
    row = conn.execute(
        """
        SELECT start_date::DATE, ROUND(
            CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                 ELSE total_distance END, 1)
        FROM stg_workouts
        WHERE workout_activity_type LIKE '%Running%'
            AND start_date::DATE BETWEEN ? AND ?
            AND total_distance IS NOT NULL
        ORDER BY CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                      ELSE total_distance END DESC
        LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row:
        records.append({"label": "Longest Run", "value": f"{row[1]} mi", "date": str(row[0]), "icon": "\U0001f3c3"})

    # Lowest resting HR
    row = conn.execute(
        """
        SELECT date, resting_hr FROM daily_resting_hr
        WHERE date BETWEEN ? AND ? AND resting_hr IS NOT NULL
        ORDER BY resting_hr ASC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row:
        records.append(
            {
                "label": "Lowest Resting HR",
                "value": f"{row[1]} bpm",
                "date": str(row[0]),
                "icon": "\u2764\ufe0f",
            },
        )

    # Best VO2Max
    row = conn.execute(
        """
        SELECT start_date::DATE, ROUND(value, 1) FROM stg_records
        WHERE record_type = 'VO2Max'
            AND start_date::DATE BETWEEN ? AND ?
        ORDER BY value DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row:
        records.append({"label": "Peak VO2Max", "value": str(row[1]), "date": str(row[0]), "icon": "\U0001f4aa"})

    # Best sleep quality (SleepCycle)
    row = conn.execute(
        """
        SELECT start_time::DATE, ROUND(sleep_quality * 100, 0)::INT
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ? AND sleep_quality IS NOT NULL
        ORDER BY sleep_quality DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row:
        records.append(
            {
                "label": "Best Sleep Quality",
                "value": f"{row[1]}%",
                "date": str(row[0]),
                "icon": "\U0001f31f",
            },
        )

    # Most flights in a day
    row = conn.execute(
        """
        SELECT start_date::DATE AS d, SUM(value)::INT AS v
        FROM stg_records
        WHERE record_type = 'FlightsClimbed'
            AND start_date::DATE BETWEEN ? AND ?
        GROUP BY d ORDER BY v DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row:
        records.append(
            {
                "label": "Most Flights",
                "value": f"{row[1]:,}",
                "date": str(row[0]),
                "icon": "\u26f0\ufe0f",
            },
        )

    return records


def training_records_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Training-specific personal records."""
    records = []

    # Longest workout (any type)
    row = conn.execute(
        """
        SELECT start_date::DATE, workout_activity_type, ROUND(duration, 0)::INT
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
        ORDER BY duration DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[2]:
        wtype = (row[1] or "").replace("HKWorkoutActivityType", "")
        records.append({"label": "Longest Workout", "value": f"{row[2]} min ({wtype})", "date": str(row[0])})

    # Most calories in a single workout
    row = conn.execute(
        """
        SELECT start_date::DATE, workout_activity_type, ROUND(total_energy_burned)::INT
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
            AND total_energy_burned IS NOT NULL AND total_energy_burned > 0
        ORDER BY total_energy_burned DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[2]:
        wtype = (row[1] or "").replace("HKWorkoutActivityType", "")
        records.append({"label": "Most Calories", "value": f"{row[2]:,} kcal ({wtype})", "date": str(row[0])})

    # Longest run (miles)
    row = conn.execute(
        """
        SELECT start_date::DATE, ROUND(
            CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                 ELSE total_distance END, 1)
        FROM stg_workouts
        WHERE workout_activity_type LIKE '%Running%'
            AND start_date::DATE BETWEEN ? AND ? AND total_distance IS NOT NULL
        ORDER BY CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                      ELSE total_distance END DESC
        LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[1]:
        records.append({"label": "Longest Run", "value": f"{row[1]} mi", "date": str(row[0])})

    # Longest ride (miles)
    row = conn.execute(
        """
        SELECT start_date::DATE, ROUND(
            CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                 ELSE total_distance END, 1)
        FROM stg_workouts
        WHERE workout_activity_type LIKE '%Cycling%'
            AND start_date::DATE BETWEEN ? AND ? AND total_distance IS NOT NULL
        ORDER BY CASE WHEN total_distance_unit = 'km' THEN total_distance * 0.621371
                      ELSE total_distance END DESC
        LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[1]:
        records.append({"label": "Longest Ride", "value": f"{row[1]} mi", "date": str(row[0])})

    # Highest running power
    row = conn.execute(
        """
        SELECT w.start_date::DATE, ROUND(AVG(r.value))::INT
        FROM stg_workouts w
        JOIN stg_records r ON r.record_type = 'RunningPower'
            AND r.start_date BETWEEN w.start_date AND w.end_date
        WHERE w.workout_activity_type LIKE '%Running%'
            AND w.start_date::DATE BETWEEN ? AND ?
        GROUP BY w.start_date
        ORDER BY AVG(r.value) DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[1]:
        records.append({"label": "Peak Run Power", "value": f"{row[1]} W", "date": str(row[0])})

    # Most workouts in a single week
    row = conn.execute(
        """
        SELECT DATE_TRUNC('week', start_date)::DATE AS week, COUNT(*) AS cnt
        FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
        GROUP BY week ORDER BY cnt DESC LIMIT 1
        """,
        [s, e],
    ).fetchone()
    if row and row[1] and row[1] > 1:
        records.append({"label": "Busiest Week", "value": f"{row[1]} workouts", "date": f"Week of {row[0]}"})

    # Most steps in a single day (training context)
    row = conn.execute(
        "SELECT date, steps FROM daily_steps WHERE date BETWEEN ? AND ? ORDER BY steps DESC LIMIT 1",
        [s, e],
    ).fetchone()
    if row and row[1]:
        records.append({"label": "Most Steps (Day)", "value": f"{row[1]:,}", "date": str(row[0])})

    return records


def weight_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Return body weight time series (BodyMass records)."""
    rows = conn.execute(
        """
        SELECT start_date::DATE AS date,
               ROUND(AVG(value), 1) AS weight
        FROM stg_records
        WHERE record_type = 'BodyMass'
            AND start_date::DATE BETWEEN ? AND ?
        GROUP BY start_date::DATE
        ORDER BY date
        """,
        [s, e],
    ).fetchall()
    return [{"date": str(r[0]), "weight": r[1]} for r in rows]


def prior_period_summary(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    """Query the same summary metrics for the equivalent prior period."""
    days = (e - s).days or 1
    ps = s - timedelta(days=days)
    pe = s - timedelta(days=1)

    prior_steps = conn.execute(
        "SELECT COALESCE(ROUND(AVG(steps)), 0)::BIGINT FROM daily_steps WHERE date BETWEEN ? AND ?",
        [ps, pe],
    ).fetchone()[0]

    prior_hr = conn.execute(
        "SELECT ROUND(AVG(resting_hr), 1) FROM daily_resting_hr WHERE date BETWEEN ? AND ?",
        [ps, pe],
    ).fetchone()[0]

    prior_sleep = conn.execute(
        """
        SELECT ROUND(AVG(duration_seconds) / 3600.0, 1)
        FROM sleep_combined WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        """,
        [ps, pe],
    ).fetchone()[0]

    prior_hrv = conn.execute(
        """
        SELECT ROUND(AVG(value), 0)::BIGINT FROM stg_records
        WHERE record_type = 'HeartRateVariabilitySDNN' AND start_date::DATE BETWEEN ? AND ?
        """,
        [ps, pe],
    ).fetchone()[0]

    return {
        "avg_steps": prior_steps or 0,
        "avg_resting_hr": prior_hr,
        "avg_sleep_hours": prior_sleep,
        "avg_hrv": prior_hrv,
    }


def sleep_score_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Composite 0-100 sleep score per night from SleepCycle data."""
    rows = conn.execute(
        """
        SELECT
            start_time::DATE AS date,
            time_asleep_seconds / 3600.0 AS hrs,
            CASE WHEN time_in_bed_seconds > 0
                 THEN time_asleep_seconds / time_in_bed_seconds ELSE NULL END AS eff,
            regularity,
            time_before_sleep_seconds / 60.0 AS latency_min,
            CASE WHEN (deep_seconds + light_seconds + dream_seconds + awake_seconds) > 0
                 THEN deep_seconds::DOUBLE / (deep_seconds + light_seconds + dream_seconds + awake_seconds)
                 ELSE NULL END AS deep_pct
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
            AND time_asleep_seconds > 0
        ORDER BY date
        """,
        [s, e],
    ).fetchall()

    results = []
    for r in rows:
        hrs, eff, reg, lat, deep = r[1], r[2], r[3], r[4], r[5]
        # Duration score (40%): 100 if 7-8 hrs, scaled down otherwise
        dur_score = 0
        if hrs is not None:
            if 7 <= hrs <= 8:
                dur_score = 100
            elif hrs < 7:
                dur_score = max(0, hrs / 7 * 100)
            else:
                dur_score = max(0, 100 - (hrs - 8) * 25)
        # Efficiency score (25%): direct percentage
        eff_score = (eff * 100) if eff is not None else 0
        # Regularity score (15%): direct percentage
        reg_score = (reg * 100) if reg is not None else 0
        # Latency score (10%): 100 if <10 min, 0 if >60 min
        lat_score = 0
        if lat is not None:
            lat_score = max(0, min(100, (60 - lat) / 50 * 100))
        # Deep sleep score (10%): 100 if >=20%, scaled
        deep_score = min(100, (deep * 100 / 20) * 100) if deep is not None else 0

        score = round(dur_score * 0.4 + eff_score * 0.25 + reg_score * 0.15 + lat_score * 0.1 + deep_score * 0.1)
        score = max(0, min(100, score))
        results.append({"date": str(r[0]), "score": score})

    # Add 7-day rolling average
    for i, row in enumerate(results):
        window = results[max(0, i - 6) : i + 1]
        row["score_7d_avg"] = round(sum(r["score"] for r in window) / len(window))

    return results


def hr_zone_data(conn: duckdb.DuckDBPyConnection, s: date, e: date, max_hr: int = 190) -> list[dict]:
    """Heart rate zone distribution by week. Zones based on % of max HR."""
    z1 = max_hr * 0.5  # Rest boundary
    z2 = max_hr * 0.6  # Fat Burn
    z3 = max_hr * 0.7  # Cardio
    z4 = max_hr * 0.85  # Peak
    rows = conn.execute(
        """
        WITH hr_dur AS (
            SELECT start_date, value,
                   GREATEST(EXTRACT(EPOCH FROM end_date - start_date) / 60.0, 1.0) AS dur_min
            FROM stg_records
            WHERE record_type = 'HeartRate' AND start_date::DATE BETWEEN ? AND ?
        )
        SELECT
            DATE_TRUNC('week', start_date)::DATE AS week,
            SUM(CASE WHEN value < ? THEN dur_min ELSE 0 END)::INT AS rest,
            SUM(CASE WHEN value >= ? AND value < ? THEN dur_min ELSE 0 END)::INT AS fat_burn,
            SUM(CASE WHEN value >= ? AND value < ? THEN dur_min ELSE 0 END)::INT AS cardio,
            SUM(CASE WHEN value >= ? AND value < ? THEN dur_min ELSE 0 END)::INT AS peak,
            SUM(CASE WHEN value >= ? THEN dur_min ELSE 0 END)::INT AS max_zone
        FROM hr_dur
        GROUP BY week
        ORDER BY week
        """,
        [s, e, z1, z1, z2, z2, z3, z3, z4, z4],
    ).fetchall()
    return [
        {"week": str(r[0]), "rest": r[1], "fat_burn": r[2], "cardio": r[3], "peak": r[4], "max_zone": r[5]}
        for r in rows
    ]


def bmi_data(conn: duckdb.DuckDBPyConnection, s: date, e: date, height_in: float | None = None) -> list[dict]:
    """BMI time series from BodyMass records + height."""
    if not height_in:
        return []
    height_m = height_in * 0.0254
    rows = conn.execute(
        """
        SELECT start_date::DATE AS date,
               ROUND(AVG(value) * 0.453592 / (? * ?), 1) AS bmi
        FROM stg_records
        WHERE record_type = 'BodyMass'
            AND start_date::DATE BETWEEN ? AND ?
        GROUP BY start_date::DATE
        ORDER BY date
        """,
        [height_m, height_m, s, e],
    ).fetchall()
    results = [{"date": str(r[0]), "bmi": r[1]} for r in rows]
    # Add 7-day rolling average
    for i, row in enumerate(results):
        window = results[max(0, i - 6) : i + 1]
        vals = [r["bmi"] for r in window if r["bmi"] is not None]
        row["bmi_7d_avg"] = round(sum(vals) / len(vals), 1) if vals else None
    return results


def daily_activity_calendar(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> list[dict]:
    """Every day in range with rings-closed count (0-3) for a calendar heatmap."""
    rows = conn.execute(
        """
        WITH all_days AS (
            SELECT UNNEST(GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL 1 DAY))::DATE AS date
        ),
        ring_days AS (
            SELECT date_components AS date,
                (CASE WHEN active_energy_burned >= active_energy_burned_goal AND active_energy_burned_goal > 0 THEN 1 ELSE 0 END
                + CASE WHEN apple_exercise_time >= apple_exercise_time_goal AND apple_exercise_time_goal > 0 THEN 1 ELSE 0 END
                + CASE WHEN apple_stand_hours >= apple_stand_hours_goal AND apple_stand_hours_goal > 0 THEN 1 ELSE 0 END) AS rings,
                ROUND(active_energy_burned)::INT AS move_cal,
                ROUND(apple_exercise_time)::INT AS exercise_min,
                apple_stand_hours::INT AS stand_hrs
            FROM stg_activity_summary
            WHERE date_components BETWEEN ? AND ?
        )
        SELECT d.date, COALESCE(r.rings, 0), r.move_cal, r.exercise_min, r.stand_hrs
        FROM all_days d
        LEFT JOIN ring_days r ON d.date = r.date
        ORDER BY d.date
        """,
        [s, e, s, e],
    ).fetchall()
    return [
        {
            "date": str(r[0]),
            "rings": r[1],
            "move_cal": r[2],
            "exercise_min": r[3],
            "stand_hrs": r[4],
        }
        for r in rows
    ]


def consistency_data(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    """Workout consistency metrics."""
    # Current streak (consecutive workout days ending at or near today)
    streak = conn.execute(
        """
        WITH dates AS (
            SELECT DISTINCT start_date::DATE AS d FROM stg_workouts
            WHERE start_date::DATE <= ? ORDER BY d DESC
        ),
        numbered AS (
            SELECT d, d - ROW_NUMBER() OVER (ORDER BY d DESC) * INTERVAL 1 DAY AS grp FROM dates
        ),
        streaks AS (
            SELECT grp, COUNT(*) AS len, MAX(d) AS end_date FROM numbered GROUP BY grp
        )
        SELECT len FROM streaks ORDER BY end_date DESC LIMIT 1
        """,
        [e],
    ).fetchone()

    # Weekly consistency: % of weeks with 3+ workouts
    weekly = conn.execute(
        """
        WITH weeks AS (
            SELECT DATE_TRUNC('week', start_date)::DATE AS week, COUNT(*) AS cnt
            FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
            GROUP BY week
        )
        SELECT
            COUNT(*) AS total_weeks,
            SUM(CASE WHEN cnt >= 3 THEN 1 ELSE 0 END) AS consistent_weeks
        FROM weeks
        """,
        [s, e],
    ).fetchone()

    # Longest rest gap
    gap = conn.execute(
        """
        WITH dates AS (
            SELECT DISTINCT start_date::DATE AS d FROM stg_workouts
            WHERE start_date::DATE BETWEEN ? AND ? ORDER BY d
        ),
        gaps AS (
            SELECT (EPOCH(d) - EPOCH(LAG(d) OVER (ORDER BY d)))::INT / 86400 AS gap_days FROM dates
        )
        SELECT COALESCE(MAX(gap_days), 0)
        FROM gaps
        """,
        [s, e],
    ).fetchone()

    total_weeks = weekly[0] if weekly else 0
    consistent_weeks = weekly[1] if weekly else 0

    return {
        "current_streak": streak[0] if streak else 0,
        "consistency_pct": round(consistent_weeks / total_weeks * 100, 1) if total_weeks > 0 else None,
        "longest_rest_gap": gap[0] if gap else None,
    }


def week_review_data(conn: duckdb.DuckDBPyConnection, e: date) -> list[dict]:
    """Last 7 days: workout status and ring closure for a mini review grid."""
    s = e - timedelta(days=6)
    rows = conn.execute(
        """
        WITH days AS (
            SELECT UNNEST(GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL 1 DAY))::DATE AS date
        ),
        workouts AS (
            SELECT start_date::DATE AS date, 1 AS had_workout
            FROM stg_workouts WHERE start_date::DATE BETWEEN ? AND ?
            GROUP BY start_date::DATE
        ),
        rings AS (
            SELECT date_components AS date,
                CASE WHEN active_energy_burned >= active_energy_burned_goal AND active_energy_burned_goal > 0 THEN 1 ELSE 0 END
                + CASE WHEN apple_exercise_time >= apple_exercise_time_goal AND apple_exercise_time_goal > 0 THEN 1 ELSE 0 END
                + CASE WHEN apple_stand_hours >= apple_stand_hours_goal AND apple_stand_hours_goal > 0 THEN 1 ELSE 0 END AS rings_closed
            FROM stg_activity_summary
            WHERE date_components BETWEEN ? AND ?
        )
        SELECT d.date, COALESCE(w.had_workout, 0), COALESCE(r.rings_closed, 0)
        FROM days d
        LEFT JOIN workouts w ON d.date = w.date
        LEFT JOIN rings r ON d.date = r.date
        ORDER BY d.date
        """,
        [s, e, s, e, s, e],
    ).fetchall()
    day_abbr = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    return [
        {
            "date": str(r[0]),
            "day": day_abbr[r[0].weekday() + 1 if r[0].weekday() < 6 else 0],  # Python weekday to Sun=0
            "had_workout": bool(r[1]),
            "rings_closed": r[2],
        }
        for r in rows
    ]
