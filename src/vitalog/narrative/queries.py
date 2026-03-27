from __future__ import annotations

from datetime import date, timedelta

import duckdb


def resolve_date_range(period: str, start: str | None, end: str | None) -> tuple[date, date]:
    today = date.today()

    if start and end:
        return date.fromisoformat(start), date.fromisoformat(end)

    if start:
        return date.fromisoformat(start), today

    periods = {
        "last-week": timedelta(weeks=1),
        "last-month": timedelta(days=30),
        "last-quarter": timedelta(days=90),
        "last-year": timedelta(days=365),
        "all": timedelta(days=365 * 10),
    }

    delta = periods.get(period)
    if delta is None:
        msg = f"Unknown period '{period}'. Use: {', '.join(periods)}"
        raise ValueError(msg)

    return today - delta, today


def get_period_stats(conn: duckdb.DuckDBPyConnection, start_date: date, end_date: date) -> dict:
    days = (end_date - start_date).days or 1
    prior_start = start_date - timedelta(days=days)
    prior_end = start_date - timedelta(days=1)

    from vitalog.db import get_user_profile

    profile = get_user_profile(conn)

    return {
        "period": {"start": start_date.isoformat(), "end": end_date.isoformat(), "days": days},
        "profile": profile,
        "steps": _step_stats(conn, start_date, end_date, prior_start, prior_end),
        "heart_rate": _heart_rate_stats(conn, start_date, end_date, prior_start, prior_end),
        "sleep": _sleep_stats(conn, start_date, end_date, prior_start, prior_end),
        "workouts": _workout_stats(conn, start_date, end_date),
        "activity_rings": _ring_stats(conn, start_date, end_date),
    }


def _step_stats(conn: duckdb.DuckDBPyConnection, s: date, e: date, ps: date, pe: date) -> dict:
    current = conn.execute(
        """
        SELECT
            COALESCE(SUM(steps), 0)::BIGINT AS total,
            COALESCE(ROUND(AVG(steps)), 0)::BIGINT AS daily_avg,
            COALESCE(MIN(steps), 0)::BIGINT AS min_day,
            COALESCE(MAX(steps), 0)::BIGINT AS max_day
        FROM daily_steps WHERE date BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    prior_avg = conn.execute(
        "SELECT COALESCE(ROUND(AVG(steps)), 0)::BIGINT FROM daily_steps WHERE date BETWEEN ? AND ?",
        [ps, pe],
    ).fetchone()[0]

    return {
        "total": current[0],
        "daily_avg": current[1],
        "min_day": current[2],
        "max_day": current[3],
        "prior_daily_avg": prior_avg,
    }


def _heart_rate_stats(conn: duckdb.DuckDBPyConnection, s: date, e: date, ps: date, pe: date) -> dict:
    current = conn.execute(
        """
        SELECT
            ROUND(AVG(resting_hr), 1) AS avg_resting,
            ROUND(MIN(resting_hr), 1) AS min_resting,
            ROUND(MAX(resting_hr), 1) AS max_resting
        FROM daily_resting_hr WHERE date BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    overall = conn.execute(
        """
        SELECT
            ROUND(AVG(avg_hr), 1) AS avg_hr,
            ROUND(MIN(min_hr), 1) AS min_hr,
            ROUND(MAX(max_hr), 1) AS max_hr
        FROM daily_heart_rate WHERE date BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    prior_resting = conn.execute(
        "SELECT ROUND(AVG(resting_hr), 1) FROM daily_resting_hr WHERE date BETWEEN ? AND ?",
        [ps, pe],
    ).fetchone()[0]

    return {
        "avg_resting": current[0],
        "min_resting": current[1],
        "max_resting": current[2],
        "avg_overall": overall[0],
        "min_overall": overall[1],
        "max_overall": overall[2],
        "prior_avg_resting": prior_resting,
    }


def _sleep_stats(conn: duckdb.DuckDBPyConnection, s: date, e: date, ps: date, pe: date) -> dict:
    current = conn.execute(
        """
        SELECT
            ROUND(AVG(duration_seconds) / 3600.0, 1) AS avg_hours,
            ROUND(MIN(duration_seconds) / 3600.0, 1) AS min_hours,
            ROUND(MAX(duration_seconds) / 3600.0, 1) AS max_hours,
            COUNT(*) AS nights
        FROM sleep_combined
        WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        """,
        [s, e],
    ).fetchone()

    quality = conn.execute(
        """
        SELECT ROUND(AVG(sleep_quality) * 100, 1)
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ? AND sleep_quality > 0
        """,
        [s, e],
    ).fetchone()[0]

    prior_avg = conn.execute(
        """
        SELECT ROUND(AVG(duration_seconds) / 3600.0, 1)
        FROM sleep_combined
        WHERE date BETWEEN ? AND ? AND duration_seconds > 0
        """,
        [ps, pe],
    ).fetchone()[0]

    sleep_breakdown = conn.execute(
        """
        SELECT
            ROUND(AVG(deep_seconds) / 60.0, 0) AS avg_deep_min,
            ROUND(AVG(light_seconds) / 60.0, 0) AS avg_light_min,
            ROUND(AVG(dream_seconds) / 60.0, 0) AS avg_dream_min,
            ROUND(AVG(awake_seconds) / 60.0, 0) AS avg_awake_min
        FROM stg_sleep_cycle
        WHERE start_time::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    return {
        "avg_hours": current[0],
        "min_hours": current[1],
        "max_hours": current[2],
        "nights_tracked": current[3],
        "avg_quality_pct": quality,
        "prior_avg_hours": prior_avg,
        "avg_deep_min": sleep_breakdown[0] if sleep_breakdown else None,
        "avg_light_min": sleep_breakdown[1] if sleep_breakdown else None,
        "avg_dream_min": sleep_breakdown[2] if sleep_breakdown else None,
        "avg_awake_min": sleep_breakdown[3] if sleep_breakdown else None,
    }


def _workout_stats(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    total = conn.execute(
        """
        SELECT
            COUNT(*) AS count,
            COALESCE(ROUND(SUM(duration), 0), 0) AS total_duration_min,
            COALESCE(ROUND(SUM(total_distance), 1), 0) AS total_distance,
            COALESCE(ROUND(AVG(total_energy_burned), 0), 0) AS avg_calories
        FROM stg_workouts
        WHERE start_date::DATE BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    by_type = conn.execute(
        """
        SELECT
            workout_activity_type,
            COUNT(*) AS count,
            ROUND(SUM(duration), 0) AS total_min,
            ROUND(SUM(total_distance), 1) AS total_dist
        FROM stg_workouts
        WHERE start_date::DATE BETWEEN ? AND ?
        GROUP BY workout_activity_type
        ORDER BY count DESC
        LIMIT 10
        """,
        [s, e],
    ).fetchall()

    return {
        "count": total[0],
        "total_duration_min": total[1],
        "total_distance": total[2],
        "avg_calories": total[3],
        "by_type": [{"type": row[0], "count": row[1], "total_min": row[2], "total_dist": row[3]} for row in by_type],
    }


def _ring_stats(conn: duckdb.DuckDBPyConnection, s: date, e: date) -> dict:
    result = conn.execute(
        """
        SELECT
            COUNT(*) AS days,
            ROUND(100.0 * SUM(CASE WHEN active_energy_burned >= active_energy_burned_goal
                AND active_energy_burned_goal > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS move_pct,
            ROUND(100.0 * SUM(CASE WHEN apple_exercise_time >= apple_exercise_time_goal
                AND apple_exercise_time_goal > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS exercise_pct,
            ROUND(100.0 * SUM(CASE WHEN apple_stand_hours >= apple_stand_hours_goal
                AND apple_stand_hours_goal > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS stand_pct,
            SUM(CASE WHEN active_energy_burned >= active_energy_burned_goal
                AND apple_exercise_time >= apple_exercise_time_goal
                AND apple_stand_hours >= apple_stand_hours_goal
                AND active_energy_burned_goal > 0
                AND apple_exercise_time_goal > 0
                AND apple_stand_hours_goal > 0
                THEN 1 ELSE 0 END) AS all_rings_closed
        FROM stg_activity_summary
        WHERE date_components BETWEEN ? AND ?
        """,
        [s, e],
    ).fetchone()

    return {
        "days_tracked": result[0],
        "move_close_pct": result[1],
        "exercise_close_pct": result[2],
        "stand_close_pct": result[3],
        "all_rings_closed_days": result[4],
    }
