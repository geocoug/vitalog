from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import jinja2
from markupsafe import Markup

from vitalog.console import get_console
from vitalog.dashboard.maps import render_route_map
from vitalog.dashboard.queries import (
    activity_rings_data,
    bmi_data,
    consistency_data,
    correlation_data,
    cycling_data,
    daily_hr_data,
    daily_steps_data,
    day_of_week_data,
    flights_climbed_data,
    hr_zone_data,
    hrv_data,
    monthly_summary_data,
    personal_records_data,
    prior_period_summary,
    respiratory_rate_data,
    running_distance_data,
    running_mechanics_data,
    running_pace_data,
    sleep_data,
    sleep_efficiency_data,
    sleep_environment_data,
    sleep_hr_data,
    sleep_impact_data,
    sleep_latency_data,
    sleep_regularity_data,
    sleep_score_data,
    sleep_stages_data,
    snore_data,
    spo2_data,
    summary_cards_data,
    training_records_data,
    vo2max_data,
    walking_speed_data,
    weekly_volume_data,
    weekly_workout_data,
    weight_data,
    workout_heatmap_data,
    workout_routes_data,
    workout_type_counts,
)

console = get_console()


def _fmt(value, suffix: str = "", default: str = "N/A") -> str:
    if value is None:
        return default
    return f"{value:,}{suffix}" if isinstance(value, int) else f"{value}{suffix}"


def _td(val, fmt: str = "") -> str:
    if val is None:
        return "<td>&mdash;</td>"
    if fmt == ",":
        return f"<td>{val:,}</td>"
    if fmt == ",.0f":
        return f"<td>{val:,.0f}</td>"
    return f"<td>{val}</td>"


def _build_monthly_table(data: list[dict]) -> str | None:
    if not data:
        return None

    # Compute period averages for conditional coloring
    def avg_of(key):
        vals = [m[key] for m in data if m[key] is not None]
        return sum(vals) / len(vals) if vals else None

    avg_steps = avg_of("avg_daily_steps")
    avg_sleep = avg_of("avg_sleep_hrs")
    avg_hr = avg_of("avg_resting_hr")
    avg_vo2 = avg_of("avg_vo2max")
    avg_hrv_val = avg_of("avg_hrv")

    def _color_td(val, avg, fmt="", invert=False):
        if val is None or avg is None:
            return _td(val, fmt)
        css = ""
        if invert:
            css = "above-avg" if val < avg else "below-avg" if val > avg else ""
        else:
            css = "above-avg" if val > avg else "below-avg" if val < avg else ""
        raw = _td(val, fmt)
        if css:
            return raw.replace("<td>", f'<td class="{css}">')
        return raw

    rows = []
    for m in data:
        rows.append(
            "<tr>"
            + f"<td>{m['month']}</td>"
            + _td(m["total_steps"], ",")
            + _color_td(m["avg_daily_steps"], avg_steps, ",")
            + _td(m["workouts"])
            + _td(m["total_workout_min"], ",.0f")
            + _color_td(m["avg_sleep_hrs"], avg_sleep)
            + _color_td(m["avg_resting_hr"], avg_hr, invert=True)
            + _color_td(m["avg_vo2max"], avg_vo2)
            + _color_td(m["avg_hrv"], avg_hrv_val)
            + "</tr>",
        )
    return (
        '<table class="monthly-table">'
        "<thead><tr>"
        "<th>Month</th><th>Total Steps</th><th>Avg Steps/Day</th>"
        "<th>Workouts</th><th>Workout Min</th><th>Avg Sleep</th>"
        "<th>Resting HR</th><th>VO2Max</th><th>HRV</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        '<div style="display:flex;gap:16px;padding:10px 14px;font-size:0.78rem;color:#6b7280;">'
        '<span><span style="color:#16a34a;font-weight:700;">&#9632;</span> Above period average</span>'
        '<span><span style="color:#dc2626;font-weight:700;">&#9632;</span> Below period average</span>'
        '<span style="color:#9ca3af;font-style:italic;">(Resting HR: lower is better)</span>'
        "</div>"
    )


def render_dashboard(
    conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    output_path: Path,
) -> None:
    console.print("  Querying data ...")

    s = summary_cards_data(conn, start_date, end_date)
    steps_d = daily_steps_data(conn, start_date, end_date)
    hr_d = daily_hr_data(conn, start_date, end_date)
    rings_d = activity_rings_data(conn, start_date, end_date)
    sl_d = sleep_data(conn, start_date, end_date)
    wk_types = workout_type_counts(conn, start_date, end_date)
    routes = workout_routes_data(conn, start_date, end_date)
    rp_d = running_pace_data(conn, start_date, end_date)
    ww_d = weekly_workout_data(conn, start_date, end_date)
    wv_d = weekly_volume_data(conn, start_date, end_date)
    hm_d = workout_heatmap_data(conn, start_date, end_date)
    dow_d = day_of_week_data(conn, start_date, end_date)
    monthly_d = monthly_summary_data(conn, start_date, end_date)
    corr_d = correlation_data(conn, start_date, end_date)
    vo2_d = vo2max_data(conn, start_date, end_date)
    hrv_d = hrv_data(conn, start_date, end_date)
    resp_d = respiratory_rate_data(conn, start_date, end_date)
    sp_d = spo2_data(conn, start_date, end_date)
    stages_d = sleep_stages_data(conn, start_date, end_date)
    sleff_d = sleep_efficiency_data(conn, start_date, end_date)
    sllat_d = sleep_latency_data(conn, start_date, end_date)
    snore_d = snore_data(conn, start_date, end_date)
    slhr_d = sleep_hr_data(conn, start_date, end_date)
    slreg_d = sleep_regularity_data(conn, start_date, end_date)
    slenv_d = sleep_environment_data(conn, start_date, end_date)
    slimpact_d = sleep_impact_data(conn, start_date, end_date)
    walk_d = walking_speed_data(conn, start_date, end_date)
    flights_d = flights_climbed_data(conn, start_date, end_date)
    mech_d = running_mechanics_data(conn, start_date, end_date)
    run_dist_d = running_distance_data(conn, start_date, end_date)
    cycling_d = cycling_data(conn, start_date, end_date)
    pr_d = personal_records_data(conn, start_date, end_date)
    tr_d = training_records_data(conn, start_date, end_date)
    weight_d = weight_data(conn, start_date, end_date)
    prior_d = prior_period_summary(conn, start_date, end_date)
    sleepscore_d = sleep_score_data(conn, start_date, end_date)
    consist_d = consistency_data(conn, start_date, end_date)
    from vitalog.db import format_height, get_user_profile

    user_profile = get_user_profile(conn)

    # Compute max HR from profile age (default 190 if no age)
    max_hr = 190
    with contextlib.suppress(ValueError, TypeError):
        if user_profile.get("age"):
            max_hr = 220 - int(user_profile["age"])
        elif user_profile.get("date_of_birth"):
            dob = date.fromisoformat(user_profile["date_of_birth"])
            max_hr = 220 - (end_date - dob).days // 365

    hrz_d = hr_zone_data(conn, start_date, end_date, max_hr)

    height_in = None
    with contextlib.suppress(ValueError, TypeError):
        if user_profile.get("height_in"):
            height_in = float(user_profile["height_in"])
    bmi_d = bmi_data(conn, start_date, end_date, height_in)

    # Format profile for display — convert height to ft/in
    display_profile = dict(user_profile)
    if display_profile.get("height_in"):
        display_profile["height_display"] = format_height(display_profile["height_in"])

    console.print("  Building dashboard ...")

    # Chart data — serialized as JSON for D3 client-side rendering
    chart_data = {
        "steps": steps_d,
        "heart_rate": hr_d,
        "activity_rings": rings_d,
        "sleep": sl_d,
        "workout_types": wk_types,
        "running_pace": rp_d,
        "weekly_workouts": ww_d,
        "heatmap": hm_d,
        "day_of_week": dow_d,
        "correlation": corr_d,
        "vo2max": vo2_d,
        "hrv": hrv_d,
        "respiratory": resp_d,
        "spo2": sp_d,
        "sleep_stages": stages_d,
        "walking_speed": walk_d,
        "flights": flights_d,
        "running_mechanics": mech_d,
        "running_distance": run_dist_d,
        "cycling": cycling_d,
        "weekly_volume": wv_d,
        "sleep_efficiency": sleff_d,
        "sleep_latency": sllat_d,
        "snore": snore_d,
        "sleep_hr": slhr_d,
        "sleep_regularity": slreg_d,
        "sleep_environment": slenv_d,
        "sleep_impact": slimpact_d,
        "weight": weight_d,
        "sleep_score": sleepscore_d,
        "hr_zones": hrz_d,
        "bmi": bmi_d,
    }

    def _delta(current, prior, invert=False):
        """Compute % delta between current and prior. Returns str like '+12%' or '-3%', or None."""
        if current is None or prior is None or prior == 0:
            return None
        pct = round((current - prior) / abs(prior) * 100)
        if pct == 0:
            return None
        # For inverted metrics (resting HR), negative is good
        sign = "+" if pct > 0 else ""
        return f"{sign}{pct}%"

    def _delta_dir(current, prior, invert=False):
        """Return 'up', 'down', or None for arrow direction + good/bad coloring."""
        if current is None or prior is None or prior == 0:
            return None
        diff = current - prior
        if diff == 0:
            return None
        if invert:
            return "good" if diff < 0 else "bad"
        return "good" if diff > 0 else "bad"

    # Build sparkline data (last 30 values from existing datasets)
    def _spark(dataset, key, n=30):
        if not dataset:
            return []
        vals = [d.get(key) for d in dataset if d.get(key) is not None]
        return vals[-n:] if vals else []

    # Prior period label for delta context
    days = (end_date - start_date).days or 1
    prior_label = f"vs prior {days} days"

    # Build per-tab static content (cards, tables)
    overview = {
        "hero_cards": [
            {
                "label": "Avg Daily Steps",
                "value": _fmt(s["avg_steps"]),
                "color": "#4CAF50",
                "spark": _spark(steps_d, "steps"),
                "delta": _delta(s["avg_steps"], prior_d["avg_steps"]),
                "delta_dir": _delta_dir(s["avg_steps"], prior_d["avg_steps"]),
                "tip": f"Average steps per day in this period. Change shown {prior_label}.",
            },
            {
                "label": "Avg Resting HR",
                "value": _fmt(s["avg_resting_hr"], " bpm"),
                "color": "#E91E63",
                "spark": _spark(hr_d, "resting_hr"),
                "delta": _delta(s["avg_resting_hr"], prior_d["avg_resting_hr"], invert=True),
                "delta_dir": _delta_dir(s["avg_resting_hr"], prior_d["avg_resting_hr"], invert=True),
                "tip": f"Average resting heart rate. Lower is generally better. Change shown {prior_label}.",
            },
            {
                "label": "Avg Sleep",
                "value": _fmt(s["avg_sleep_hours"], " hrs"),
                "color": "#673AB7",
                "spark": _spark(sl_d, "hours"),
                "delta": _delta(s["avg_sleep_hours"], prior_d["avg_sleep_hours"]),
                "delta_dir": _delta_dir(s["avg_sleep_hours"], prior_d["avg_sleep_hours"]),
                "tip": f"Average hours of sleep per night. Change shown {prior_label}.",
            },
            {
                "label": "Rings Closed",
                "value": _fmt(s["ring_close_pct"], "%"),
                "color": "#FF9800",
                "spark": _spark(rings_d, "energy"),
                "tip": "Percentage of days where all 3 Apple Watch rings (Move, Exercise, Stand) were closed.",
            },
            {
                "label": "VO2Max",
                "value": _fmt(s["vo2max_latest"]),
                "color": "#00BCD4",
                "spark": _spark(vo2_d, "val"),
                "tip": "Most recent VO2Max estimate (mL/kg/min). Higher values indicate better cardiovascular fitness.",
            },
            {
                "label": "Avg HRV",
                "value": _fmt(s["avg_hrv"], " ms"),
                "color": "#8BC34A",
                "spark": _spark(hrv_d, "val"),
                "delta": _delta(s["avg_hrv"], prior_d["avg_hrv"]),
                "delta_dir": _delta_dir(s["avg_hrv"], prior_d["avg_hrv"]),
                "tip": f"Average heart rate variability (SDNN). Higher is generally better. Change shown {prior_label}.",
            },
        ],
        "stat_cards": [
            {
                "label": "Total Workouts",
                "value": _fmt(s["workouts"]),
                "color": "#2196F3",
                "tip": "Total number of recorded workout sessions.",
            },
            {
                "label": "Workout Days",
                "value": _fmt(s["active_days"], " days"),
                "color": "#7CB342",
                "tip": "Number of distinct days with at least one workout.",
            },
            {
                "label": "Best Streak",
                "value": _fmt(s["workout_streak"], " days"),
                "color": "#FF5722",
                "tip": "Longest run of consecutive days with a workout.",
            },
            {
                "label": "Workout Hours",
                "value": _fmt(s["total_workout_hrs"], " hrs"),
                "color": "#009688",
                "tip": "Total workout duration across all sessions.",
            },
            {
                "label": "Miles Run",
                "value": _fmt(s["total_run_mi"], " mi"),
                "color": "#26A69A",
                "tip": "Total running distance (all running workouts).",
            },
            {
                "label": "Miles Cycled",
                "value": _fmt(s["total_cycling_mi"], " mi"),
                "color": "#7E57C2",
                "tip": "Total cycling distance (all cycling workouts).",
            },
            {
                "label": "Flights Climbed",
                "value": _fmt(s["total_flights"]),
                "color": "#FF8A65",
                "tip": "Total flights of stairs climbed (1 flight = ~10 feet elevation).",
            },
            {
                "label": "Avg Daily Distance",
                "value": _fmt(s.get("avg_daily_distance"), " mi"),
                "color": "#78909C",
                "tip": "Average walking + running distance per day.",
            },
        ],
        "monthly_table": Markup(mt) if (mt := _build_monthly_table(monthly_d)) else None,  # noqa: S704
        "personal_records": pr_d,
    }

    # Compute sleep stat averages from query data
    avg_efficiency = None
    if sleff_d:
        eff_vals = [d["val"] for d in sleff_d if d.get("val") is not None]
        if eff_vals:
            avg_efficiency = round(sum(eff_vals) / len(eff_vals), 1)

    avg_latency = None
    if sllat_d:
        lat_vals = [d["val"] for d in sllat_d if d.get("val") is not None]
        if lat_vals:
            avg_latency = round(sum(lat_vals) / len(lat_vals), 0)

    avg_snore = None
    if snore_d:
        snr_vals = [d["val"] for d in snore_d if d.get("val") is not None]
        if snr_vals:
            avg_snore = round(sum(snr_vals) / len(snr_vals), 0)

    avg_sleep_score = None
    if sleepscore_d:
        sc_vals = [d["score"] for d in sleepscore_d if d.get("score") is not None]
        if sc_vals:
            avg_sleep_score = round(sum(sc_vals) / len(sc_vals))

    sleep_tab = {
        "hero_cards": [
            {
                "label": "Sleep Score",
                "value": _fmt(avg_sleep_score, "/100"),
                "color": "#673AB7",
                "spark": _spark(sleepscore_d, "score"),
                "tip": "Composite 0-100 score: duration (40%), efficiency (25%), regularity (15%), time to sleep (10%), deep sleep (10%). 80+ is excellent.",
            },
        ],
        "stat_cards": [
            {
                "label": "Avg Time Asleep",
                "value": _fmt(s["avg_sleep_hours"], " hrs"),
                "color": "#673AB7",
                "tip": "Average hours actually asleep per night (excludes time awake in bed).",
            },
            {
                "label": "Avg Deep Sleep",
                "value": _fmt(s["avg_deep_sleep"], " hrs"),
                "color": "#1A237E",
                "tip": "Average deep sleep per night. Deep sleep is critical for physical recovery.",
            },
            {
                "label": "Avg REM Sleep",
                "value": _fmt(s["avg_rem_sleep"], " hrs"),
                "color": "#7E57C2",
                "tip": "Average REM sleep per night. REM is important for memory and learning.",
            },
            {
                "label": "Best Quality",
                "value": _fmt(s["best_sleep_quality"], "%"),
                "color": "#FF9800",
                "tip": "Highest single-night sleep quality score from SleepCycle.",
            },
            {
                "label": "Sleep Efficiency",
                "value": _fmt(avg_efficiency, "%"),
                "color": "#4CAF50",
                "tip": "Time asleep / time in bed. 85%+ is considered good.",
            },
            {
                "label": "Avg Time to Sleep",
                "value": _fmt(avg_latency, " min"),
                "color": "#42A5F5",
                "tip": "Average minutes to fall asleep after getting into bed.",
            },
            {
                "label": "Avg Snore Time",
                "value": _fmt(avg_snore, " min"),
                "color": "#FF7043",
                "tip": "Average nightly snoring duration in minutes.",
            },
        ],
    }

    # Compute run averages from run distance data
    avg_run_dist = None
    if run_dist_d:
        rdists = [r["distance"] for r in run_dist_d if r.get("distance")]
        if rdists:
            avg_run_dist = round(sum(rdists) / len(rdists), 1)

    # Compute cycling averages from ride data
    avg_ride_dist = None
    avg_ride_dur = None
    total_ride_cal = None
    if cycling_d:
        dists = [r["distance"] for r in cycling_d if r.get("distance")]
        durs = [r["duration_min"] for r in cycling_d if r.get("duration_min")]
        cals = [r["calories"] for r in cycling_d if r.get("calories")]
        if dists:
            avg_ride_dist = round(sum(dists) / len(dists), 1)
        if durs:
            avg_ride_dur = round(sum(durs) / len(durs), 0)
        if cals:
            total_ride_cal = round(sum(cals), 0)

    training = {
        "hero_cards": [
            {
                "label": "Total Workouts",
                "value": _fmt(s["workouts"]),
                "color": "#2196F3",
                "spark": _spark(ww_d, "count") if ww_d else [],
            },
            {
                "label": "Workout Hours",
                "value": _fmt(s["total_workout_hrs"], " hrs"),
                "color": "#009688",
                "spark": _spark(wv_d, "minutes") if wv_d else [],
            },
            {
                "label": "Total Calories",
                "value": _fmt(s["total_calories"], " kcal"),
                "color": "#E91E63",
                "spark": [],
            },
            {
                "label": "Peak VO2Max",
                "value": _fmt(s["vo2max_peak"]),
                "color": "#00BCD4",
                "spark": _spark(vo2_d, "val"),
            },
        ],
        "records": tr_d,
        "card_groups": [
            {
                "label": "Running",
                "cards": [
                    {"label": "Runs", "value": _fmt(s["run_count"]), "color": "#43A047"},
                    {"label": "Miles Run", "value": _fmt(s["total_run_mi"], " mi"), "color": "#26A69A"},
                    {"label": "Marathons", "value": _fmt(s["marathons"]), "color": "#D32F2F"},
                    {"label": "Half Marathons", "value": _fmt(s["half_marathons"]), "color": "#F57C00"},
                    {"label": "Longest Run", "value": _fmt(s["longest_run_mi"], " mi"), "color": "#00897B"},
                    {"label": "Avg Distance", "value": _fmt(avg_run_dist, " mi"), "color": "#2E7D32"},
                    {"label": "Avg Duration", "value": _fmt(s["avg_run_dur"], " min"), "color": "#388E3C"},
                    {"label": "Avg Power", "value": _fmt(s["avg_run_power"], " W"), "color": "#FF7043"},
                ],
            },
            {
                "label": "Cycling",
                "cards": [
                    {"label": "Rides", "value": _fmt(s["cycling_count"]), "color": "#AB47BC"},
                    {"label": "Miles Cycled", "value": _fmt(s["total_cycling_mi"], " mi"), "color": "#7E57C2"},
                    {"label": "Longest Ride", "value": _fmt(s["longest_ride_mi"], " mi"), "color": "#8D6E63"},
                    {"label": "Avg Distance", "value": _fmt(avg_ride_dist, " mi"), "color": "#9C27B0"},
                    {"label": "Avg Duration", "value": _fmt(avg_ride_dur, " min"), "color": "#6A1B9A"},
                    {"label": "Calories", "value": _fmt(total_ride_cal, " kcal"), "color": "#CE93D8"},
                ],
            },
            {
                "label": "Strength",
                "cards": [
                    {"label": "Sessions", "value": _fmt(s["strength_count"]), "color": "#795548"},
                    {"label": "Total Time", "value": _fmt(s["strength_total_min"], " min"), "color": "#8D6E63"},
                    {"label": "Avg Duration", "value": _fmt(s["strength_avg_min"], " min"), "color": "#A1887F"},
                    {"label": "Calories", "value": _fmt(s["strength_total_cal"], " kcal"), "color": "#D7CCC8"},
                ],
            },
        ],
    }

    vitals_tab = {
        "stat_cards": [
            {"label": "Avg Resting HR", "value": _fmt(s["avg_resting_hr"], " bpm"), "color": "#E91E63"},
            {"label": "VO2Max", "value": _fmt(s["vo2max_latest"]), "color": "#00BCD4"},
            {"label": "Avg HRV", "value": _fmt(s["avg_hrv"], " ms"), "color": "#8BC34A"},
            {"label": "Avg SpO2", "value": _fmt(s.get("avg_spo2"), "%"), "color": "#9C27B0"},
            {"label": "Avg Respiratory", "value": _fmt(s.get("avg_resp_rate"), " br/min"), "color": "#607D8B"},
        ],
    }

    activity_tab = {
        "stat_cards": [
            {
                "label": "All 3 Rings Closed",
                "value": _fmt(s["ring_close_pct"], "% of days"),
                "color": "#FF9800",
                "tip": "Percentage of days where Move, Exercise, and Stand goals were all met.",
            },
            {
                "label": "Total Flights",
                "value": _fmt(s["total_flights"]),
                "color": "#FF8A65",
                "tip": "Total flights of stairs climbed (1 flight = ~10 feet elevation gain).",
            },
            {
                "label": "Avg Walk Speed",
                "value": _fmt(s["avg_walking_speed"], " mph"),
                "color": "#78909C",
                "tip": "Average walking speed during daily movement.",
            },
            {
                "label": "Days with Workouts",
                "value": _fmt(s["active_days"]),
                "color": "#7CB342",
                "tip": "Number of distinct days with at least one recorded workout session.",
            },
            {
                "label": "Current Streak",
                "value": _fmt(consist_d["current_streak"], " days"),
                "color": "#FF5722",
                "tip": "Consecutive days with at least one workout, ending at or near the end of this period.",
            },
            {
                "label": "Weekly Consistency",
                "value": _fmt(consist_d["consistency_pct"], "%"),
                "color": "#4CAF50",
                "tip": "Percentage of weeks in this period that had 3 or more workouts.",
            },
        ],
    }

    routes_tab = {
        "stat_cards": [],
    }

    console.print("  Rendering map ...")
    routes_html = render_route_map(routes)

    # Render with Jinja2
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent),
        autoescape=jinja2.select_autoescape(default_for_string=True, default=True),
    )
    template = env.get_template("template.html")
    html = template.render(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        chart_data_json=Markup(json.dumps(chart_data, default=str)),  # noqa: S704
        overview=overview,
        vitals=vitals_tab,
        sleep=sleep_tab,
        training=training,
        activity=activity_tab,
        routes_tab=routes_tab,
        routes_html=Markup(routes_html),  # noqa: S704
        profile=display_profile,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
