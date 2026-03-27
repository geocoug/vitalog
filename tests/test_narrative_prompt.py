"""Tests for vitalog.narrative.prompt — prompt building."""

from __future__ import annotations

from vitalog.narrative.prompt import (
    QUESTION_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    build_prompt,
    build_question_prompt,
)


def _sample_stats() -> dict:
    return {
        "period": {"start": "2025-06-01", "end": "2025-06-07", "days": 7},
        "profile": {},
        "steps": {
            "total": 50000,
            "daily_avg": 7143,
            "min_day": 3000,
            "max_day": 12000,
            "prior_daily_avg": 6500,
        },
        "heart_rate": {
            "avg_resting": 55.0,
            "min_resting": 50.0,
            "max_resting": 60.0,
            "avg_overall": 75.0,
            "min_overall": 45.0,
            "max_overall": 165.0,
            "prior_avg_resting": 57.0,
        },
        "sleep": {
            "avg_hours": 7.2,
            "min_hours": 5.8,
            "max_hours": 8.5,
            "nights_tracked": 7,
            "avg_quality_pct": 82.0,
            "prior_avg_hours": 6.9,
            "avg_deep_min": 90,
            "avg_light_min": 180,
            "avg_dream_min": 60,
            "avg_awake_min": 30,
        },
        "workouts": {
            "count": 5,
            "total_duration_min": 250,
            "total_distance": 15.0,
            "avg_calories": 350,
            "by_type": [
                {"type": "HKWorkoutActivityTypeRunning", "count": 3, "total_min": 150, "total_dist": 12.0},
                {"type": "HKWorkoutActivityTypeCycling", "count": 2, "total_min": 100, "total_dist": 3.0},
            ],
        },
        "activity_rings": {
            "days_tracked": 7,
            "move_close_pct": 85.7,
            "exercise_close_pct": 71.4,
            "stand_close_pct": 100.0,
            "all_rings_closed_days": 5,
        },
    }


class TestSystemPrompt:
    def test_not_empty(self) -> None:
        assert len(SYSTEM_PROMPT) > 50

    def test_contains_guidelines(self) -> None:
        assert "second person" in SYSTEM_PROMPT


class TestBuildPrompt:
    def test_includes_date_range(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "2025-06-01" in prompt
        assert "2025-06-07" in prompt

    def test_includes_steps(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "7,143" in prompt
        assert "50,000" in prompt

    def test_includes_heart_rate(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "55.0 bpm" in prompt

    def test_includes_sleep(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "7.2 hours" in prompt
        assert "82.0%" in prompt

    def test_includes_workouts(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "Total workouts: 5" in prompt
        assert "Running" in prompt

    def test_includes_activity_rings(self) -> None:
        prompt = build_prompt(_sample_stats())
        assert "85.7%" in prompt
        assert "5 days" in prompt

    def test_includes_profile(self) -> None:
        stats = _sample_stats()
        stats["profile"] = {"age": "35", "sex": "male", "weight_lbs": "175", "height_in": "70"}
        prompt = build_prompt(stats)
        assert "Age: 35" in prompt
        assert "Sex: male" in prompt
        assert "Weight: 175" in prompt

    def test_no_profile(self) -> None:
        stats = _sample_stats()
        stats["profile"] = {}
        prompt = build_prompt(stats)
        assert "User Profile" not in prompt

    def test_handles_missing_data(self) -> None:
        stats = _sample_stats()
        stats["heart_rate"]["avg_resting"] = None
        stats["heart_rate"]["avg_overall"] = None
        stats["sleep"]["avg_hours"] = None
        stats["sleep"]["avg_quality_pct"] = None
        stats["sleep"]["avg_deep_min"] = None
        stats["workouts"]["total_duration_min"] = None
        stats["workouts"]["total_distance"] = None
        stats["workouts"]["avg_calories"] = None
        stats["workouts"]["by_type"] = []
        stats["activity_rings"]["days_tracked"] = 0
        stats["steps"]["prior_daily_avg"] = None
        stats["heart_rate"]["prior_avg_resting"] = None
        stats["sleep"]["prior_avg_hours"] = None
        prompt = build_prompt(stats)
        assert "2025-06-01" in prompt


class TestQuestionSystemPrompt:
    def test_not_empty(self) -> None:
        assert len(QUESTION_SYSTEM_PROMPT) > 50

    def test_contains_guidelines(self) -> None:
        assert "question" in QUESTION_SYSTEM_PROMPT.lower()


class TestBuildQuestionPrompt:
    def test_includes_question(self) -> None:
        prompt = build_question_prompt(_sample_stats(), "How is my sleep quality?")
        assert "How is my sleep quality?" in prompt

    def test_includes_health_data(self) -> None:
        prompt = build_question_prompt(_sample_stats(), "Am I exercising enough?")
        assert "2025-06-01" in prompt
        assert "7,143" in prompt

    def test_has_both_sections(self) -> None:
        prompt = build_question_prompt(_sample_stats(), "Test question")
        assert "My question" in prompt
        assert "My health data" in prompt
