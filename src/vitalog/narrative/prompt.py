from __future__ import annotations

SYSTEM_PROMPT = """You are a personal health analyst writing a weekly/monthly health narrative journal entry.

Guidelines:
- Write in second person ("you")
- Be encouraging but honest about both achievements and areas for improvement
- Use specific numbers from the data provided
- Note meaningful trends, anomalies, and correlations
- Keep it to 3-5 paragraphs
- Format as markdown with a date-range header
- If data for a metric is missing or zero, skip it gracefully
- Compare to the prior period when available to highlight trends
- If user demographics (age, weight, height, sex) are provided, use them to contextualize metrics
  (e.g., compare resting HR to age-appropriate norms, calculate BMI, assess VO2Max percentile)"""

QUESTION_SYSTEM_PROMPT = """You are a personal health analyst answering questions about a user's health data.

Guidelines:
- Answer the user's specific question using only the health data provided
- Write in second person ("you")
- Use specific numbers from the data to support your answer
- Be encouraging but honest
- Format as markdown
- If the data is insufficient to fully answer the question, say so clearly
- Keep your answer focused and concise — don't volunteer unrelated metrics
- If user demographics (age, weight, height, sex) are provided, use them to contextualize metrics
  (e.g., compare resting HR to age-appropriate norms, calculate BMI, assess VO2Max percentile)"""


def build_prompt(stats: dict) -> str:
    sections = [f"# Health data for {stats['period']['start']} to {stats['period']['end']}"]
    sections.append(f"Period length: {stats['period']['days']} days\n")

    # User profile (demographics)
    profile = stats.get("profile", {})
    if profile:
        sections.append("## User Profile")
        if profile.get("age"):
            sections.append(f"- Age: {profile['age']} years")
        if profile.get("sex"):
            sections.append(f"- Sex: {profile['sex']}")
        if profile.get("weight_lbs"):
            sections.append(f"- Weight: {profile['weight_lbs']} lbs")
        if profile.get("height_in"):
            sections.append(f"- Height: {profile['height_in']} inches")
        sections.append("")

    # Steps
    s = stats["steps"]
    sections.append("## Steps")
    sections.append(f"- Daily average: {s['daily_avg']:,}")
    sections.append(f"- Total: {s['total']:,}")
    sections.append(f"- Best day: {s['max_day']:,} steps")
    sections.append(f"- Lowest day: {s['min_day']:,} steps")
    if s["prior_daily_avg"]:
        sections.append(f"- Prior period daily average: {s['prior_daily_avg']:,}")

    # Heart rate
    hr = stats["heart_rate"]
    sections.append("\n## Heart Rate")
    if hr["avg_resting"]:
        sections.append(f"- Average resting HR: {hr['avg_resting']} bpm")
        sections.append(f"- Resting HR range: {hr['min_resting']} - {hr['max_resting']} bpm")
    if hr["avg_overall"]:
        sections.append(f"- Overall HR range: {hr['min_overall']} - {hr['max_overall']} bpm")
    if hr["prior_avg_resting"]:
        sections.append(f"- Prior period avg resting HR: {hr['prior_avg_resting']} bpm")

    # Sleep
    sl = stats["sleep"]
    sections.append("\n## Sleep")
    if sl["avg_hours"]:
        sections.append(f"- Average sleep: {sl['avg_hours']} hours/night")
        sections.append(f"- Range: {sl['min_hours']} - {sl['max_hours']} hours")
        sections.append(f"- Nights tracked: {sl['nights_tracked']}")
    if sl["avg_quality_pct"]:
        sections.append(f"- Average sleep quality (SleepCycle): {sl['avg_quality_pct']}%")
    if sl["avg_deep_min"]:
        sections.append(
            f"- Sleep stages avg: {sl['avg_deep_min']:.0f}min deep, "
            f"{sl['avg_light_min']:.0f}min light, "
            f"{sl['avg_dream_min']:.0f}min dream, "
            f"{sl['avg_awake_min']:.0f}min awake",
        )
    if sl["prior_avg_hours"]:
        sections.append(f"- Prior period avg: {sl['prior_avg_hours']} hours/night")

    # Workouts
    w = stats["workouts"]
    sections.append("\n## Workouts")
    sections.append(f"- Total workouts: {w['count']}")
    if w["total_duration_min"]:
        sections.append(f"- Total duration: {w['total_duration_min']:.0f} minutes")
    if w["total_distance"]:
        sections.append(f"- Total distance: {w['total_distance']:.1f}")
    if w["avg_calories"]:
        sections.append(f"- Average calories burned: {w['avg_calories']:.0f} kcal")
    if w["by_type"]:
        sections.append("- Breakdown by type:")
        for wt in w["by_type"]:
            name = wt["type"].replace("HKWorkoutActivityType", "")
            sections.append(f"  - {name}: {wt['count']}x ({wt['total_min']:.0f} min)")

    # Activity rings
    r = stats["activity_rings"]
    sections.append("\n## Activity Rings")
    sections.append(f"- Days tracked: {r['days_tracked']}")
    if r["days_tracked"]:
        sections.append(f"- Move ring closed: {r['move_close_pct']}% of days")
        sections.append(f"- Exercise ring closed: {r['exercise_close_pct']}% of days")
        sections.append(f"- Stand ring closed: {r['stand_close_pct']}% of days")
        sections.append(f"- All three rings closed: {r['all_rings_closed_days']} days")

    return "\n".join(sections)


def build_question_prompt(stats: dict, question: str) -> str:
    """Build a prompt that includes the user's question and their health data as context."""
    data_context = build_prompt(stats)
    return f"## My question\n\n{question}\n\n---\n\n## My health data\n\n{data_context}"
