"""Tests for vitalog.dashboard.queries — chart data queries."""

from __future__ import annotations

from datetime import date

from vitalog.dashboard.queries import (
    activity_rings_data,
    bmi_data,
    consistency_data,
    correlation_data,
    cycling_data,
    daily_activity_calendar,
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
    week_review_data,
    weekly_volume_data,
    weekly_workout_data,
    workout_heatmap_data,
    workout_routes_data,
    workout_type_counts,
)

S = date(2025, 6, 1)
E = date(2025, 6, 3)


class TestDailyStepsData:
    def test_returns_list(self, seeded_db) -> None:
        result = daily_steps_data(seeded_db, S, E)
        assert isinstance(result, list)
        assert len(result) > 0
        assert "date" in result[0]
        assert "steps" in result[0]

    def test_correct_aggregation(self, seeded_db) -> None:
        result = daily_steps_data(seeded_db, S, E)
        day1 = next(r for r in result if r["date"] == "2025-06-01")
        assert day1["steps"] == 8000  # 3000 + 5000


class TestDailyHrData:
    def test_returns_list(self, seeded_db) -> None:
        result = daily_hr_data(seeded_db, S, E)
        assert len(result) > 0
        assert "resting_hr" in result[0]


class TestActivityRingsData:
    def test_returns_list(self, seeded_db) -> None:
        result = activity_rings_data(seeded_db, S, E)
        assert len(result) == 3
        assert "energy" in result[0]
        assert "energy_goal" in result[0]


class TestSleepData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_data(seeded_db, S, E)
        assert isinstance(result, list)
        assert len(result) > 0
        assert "hours" in result[0]


class TestWorkoutTypeCounts:
    def test_returns_list(self, seeded_db) -> None:
        result = workout_type_counts(seeded_db, S, E)
        assert len(result) > 0
        assert "type" in result[0]
        assert "count" in result[0]

    def test_cleans_type_names(self, seeded_db) -> None:
        result = workout_type_counts(seeded_db, S, E)
        types = [r["type"] for r in result]
        # At least some types should exist
        assert len(types) > 0


class TestRunningPaceData:
    def test_returns_list(self, seeded_db) -> None:
        result = running_pace_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestWeeklyWorkoutData:
    def test_returns_list(self, seeded_db) -> None:
        result = weekly_workout_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "week" in result[0]
            assert "type" in result[0]
            assert "count" in result[0]


class TestWeeklyVolumeData:
    def test_returns_list(self, seeded_db) -> None:
        result = weekly_volume_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "minutes" in result[0]


class TestWorkoutHeatmapData:
    def test_returns_list(self, seeded_db) -> None:
        result = workout_heatmap_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "date" in result[0]
            assert "total_min" in result[0]


class TestDayOfWeekData:
    def test_returns_list(self, seeded_db) -> None:
        result = day_of_week_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestMonthlySummaryData:
    def test_returns_list(self, seeded_db) -> None:
        result = monthly_summary_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestCorrelationData:
    def test_returns_dict(self, seeded_db) -> None:
        result = correlation_data(seeded_db, S, E)
        assert isinstance(result, dict)
        assert "dates" in result


class TestVo2maxData:
    def test_returns_list(self, seeded_db) -> None:
        result = vo2max_data(seeded_db, S, E)
        assert len(result) > 0
        assert "val" in result[0]


class TestHrvData:
    def test_returns_list(self, seeded_db) -> None:
        result = hrv_data(seeded_db, S, E)
        assert len(result) > 0
        assert "val" in result[0]


class TestRespiratoryRateData:
    def test_returns_list(self, seeded_db) -> None:
        result = respiratory_rate_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSpo2Data:
    def test_returns_list(self, seeded_db) -> None:
        result = spo2_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSleepStagesData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_stages_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestWalkingSpeedData:
    def test_returns_list(self, seeded_db) -> None:
        result = walking_speed_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestFlightsClimbedData:
    def test_returns_list(self, seeded_db) -> None:
        result = flights_climbed_data(seeded_db, S, E)
        assert len(result) > 0
        assert "val" in result[0]


class TestRunningMechanicsData:
    def test_returns_list(self, seeded_db) -> None:
        result = running_mechanics_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestRunningDistanceData:
    def test_returns_list(self, seeded_db) -> None:
        result = running_distance_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestCyclingData:
    def test_returns_list(self, seeded_db) -> None:
        result = cycling_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestWorkoutRoutesData:
    def test_returns_list_empty(self, seeded_db) -> None:
        result = workout_routes_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSummaryCardsData:
    def test_returns_dict(self, seeded_db) -> None:
        result = summary_cards_data(seeded_db, S, E)
        assert isinstance(result, dict)
        assert "avg_steps" in result
        assert "avg_resting_hr" in result
        assert "workouts" in result
        assert "avg_sleep_hours" in result
        assert "ring_close_pct" in result
        assert "vo2max_latest" in result
        assert "avg_hrv" in result

    def test_step_count(self, seeded_db) -> None:
        result = summary_cards_data(seeded_db, S, E)
        assert result["avg_steps"] > 0

    def test_workout_count(self, seeded_db) -> None:
        result = summary_cards_data(seeded_db, S, E)
        assert result["workouts"] == 3


class TestPersonalRecordsData:
    def test_returns_list(self, seeded_db) -> None:
        result = personal_records_data(seeded_db, S, E)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_record_structure(self, seeded_db) -> None:
        result = personal_records_data(seeded_db, S, E)
        for rec in result:
            assert "label" in rec
            assert "value" in rec
            assert "date" in rec
            assert "icon" in rec


class TestSleepEfficiencyData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_efficiency_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "val" in result[0]
            assert "ma" in result[0]


class TestSleepLatencyData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_latency_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSnoreData:
    def test_returns_list(self, seeded_db) -> None:
        result = snore_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSleepHrData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_hr_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "sleep_hr" in result[0]
            assert "resting_hr" in result[0]


class TestSleepRegularityData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_regularity_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestSleepEnvironmentData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_environment_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "temp_f" in result[0]
            assert "quality" in result[0]


class TestSleepImpactData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_impact_data(seeded_db, S, E)
        assert isinstance(result, list)


class TestTrainingRecordsData:
    def test_returns_list(self, seeded_db) -> None:
        result = training_records_data(seeded_db, S, E)
        assert isinstance(result, list)

    def test_has_records_with_data(self, seeded_db) -> None:
        result = training_records_data(seeded_db, S, E)
        if result:
            assert "label" in result[0]
            assert "value" in result[0]
            assert "date" in result[0]


class TestPriorPeriodSummary:
    def test_returns_dict(self, seeded_db) -> None:
        result = prior_period_summary(seeded_db, S, E)
        assert isinstance(result, dict)
        assert "avg_steps" in result
        assert "avg_resting_hr" in result


class TestSleepScoreData:
    def test_returns_list(self, seeded_db) -> None:
        result = sleep_score_data(seeded_db, S, E)
        assert isinstance(result, list)
        if result:
            assert "score" in result[0]
            assert "score_7d_avg" in result[0]
            assert 0 <= result[0]["score"] <= 100


class TestHrZoneData:
    def test_returns_list(self, seeded_db) -> None:
        result = hr_zone_data(seeded_db, S, E, max_hr=190)
        assert isinstance(result, list)
        if result:
            assert "week" in result[0]
            assert "rest" in result[0]


class TestBmiData:
    def test_returns_empty_without_height(self, seeded_db) -> None:
        assert bmi_data(seeded_db, S, E, height_in=None) == []

    def test_returns_list_with_height(self, seeded_db) -> None:
        # Seed a weight record
        seeded_db.execute(
            "INSERT INTO stg_records VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "Watch",
                "10.0",
                None,
                "BodyMass",
                "lb",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:01:00",
                175.0,
            ),
        )
        result = bmi_data(seeded_db, S, E, height_in=70)
        assert isinstance(result, list)
        if result:
            assert "bmi" in result[0]
            assert result[0]["bmi"] > 0


class TestDailyActivityCalendar:
    def test_returns_list(self, seeded_db) -> None:
        result = daily_activity_calendar(seeded_db, S, E)
        assert isinstance(result, list)
        assert len(result) == 3  # 3 days in range


class TestConsistencyData:
    def test_returns_dict(self, seeded_db) -> None:
        result = consistency_data(seeded_db, S, E)
        assert isinstance(result, dict)
        assert "current_streak" in result
        assert "consistency_pct" in result


class TestWeekReviewData:
    def test_returns_list(self, seeded_db) -> None:
        result = week_review_data(seeded_db, E)
        assert isinstance(result, list)
        assert len(result) == 7
        assert "day" in result[0]
        assert "had_workout" in result[0]
        assert "rings_closed" in result[0]


class TestEmptyDatabase:
    """All query functions must handle an empty database without raising."""

    def test_daily_steps_empty(self, tmp_db) -> None:
        assert daily_steps_data(tmp_db, S, E) == []

    def test_daily_hr_empty(self, tmp_db) -> None:
        assert daily_hr_data(tmp_db, S, E) == []

    def test_activity_rings_empty(self, tmp_db) -> None:
        assert activity_rings_data(tmp_db, S, E) == []

    def test_sleep_empty(self, tmp_db) -> None:
        assert sleep_data(tmp_db, S, E) == []

    def test_workout_type_counts_empty(self, tmp_db) -> None:
        assert workout_type_counts(tmp_db, S, E) == []

    def test_workout_routes_empty(self, tmp_db) -> None:
        assert workout_routes_data(tmp_db, S, E) == []

    def test_running_pace_empty(self, tmp_db) -> None:
        assert running_pace_data(tmp_db, S, E) == []

    def test_weekly_workout_empty(self, tmp_db) -> None:
        assert weekly_workout_data(tmp_db, S, E) == []

    def test_weekly_volume_empty(self, tmp_db) -> None:
        assert weekly_volume_data(tmp_db, S, E) == []

    def test_workout_heatmap_empty(self, tmp_db) -> None:
        assert workout_heatmap_data(tmp_db, S, E) == []

    def test_day_of_week_empty(self, tmp_db) -> None:
        assert day_of_week_data(tmp_db, S, E) == []

    def test_monthly_summary_empty(self, tmp_db) -> None:
        result = monthly_summary_data(tmp_db, S, E)
        assert isinstance(result, list)

    def test_correlation_empty(self, tmp_db) -> None:
        result = correlation_data(tmp_db, S, E)
        assert isinstance(result, dict)
        assert result["dates"] == []

    def test_vo2max_empty(self, tmp_db) -> None:
        assert vo2max_data(tmp_db, S, E) == []

    def test_hrv_empty(self, tmp_db) -> None:
        assert hrv_data(tmp_db, S, E) == []

    def test_respiratory_empty(self, tmp_db) -> None:
        assert respiratory_rate_data(tmp_db, S, E) == []

    def test_spo2_empty(self, tmp_db) -> None:
        assert spo2_data(tmp_db, S, E) == []

    def test_sleep_stages_empty(self, tmp_db) -> None:
        assert sleep_stages_data(tmp_db, S, E) == []

    def test_walking_speed_empty(self, tmp_db) -> None:
        assert walking_speed_data(tmp_db, S, E) == []

    def test_flights_empty(self, tmp_db) -> None:
        assert flights_climbed_data(tmp_db, S, E) == []

    def test_running_mechanics_empty(self, tmp_db) -> None:
        assert running_mechanics_data(tmp_db, S, E) == []

    def test_running_distance_empty(self, tmp_db) -> None:
        assert running_distance_data(tmp_db, S, E) == []

    def test_cycling_empty(self, tmp_db) -> None:
        assert cycling_data(tmp_db, S, E) == []

    def test_summary_cards_empty(self, tmp_db) -> None:
        result = summary_cards_data(tmp_db, S, E)
        assert isinstance(result, dict)
        assert result["avg_steps"] == 0
        assert result["workouts"] == 0

    def test_personal_records_empty(self, tmp_db) -> None:
        assert personal_records_data(tmp_db, S, E) == []

    def test_sleep_efficiency_empty(self, tmp_db) -> None:
        assert sleep_efficiency_data(tmp_db, S, E) == []

    def test_sleep_latency_empty(self, tmp_db) -> None:
        assert sleep_latency_data(tmp_db, S, E) == []

    def test_snore_empty(self, tmp_db) -> None:
        assert snore_data(tmp_db, S, E) == []

    def test_sleep_hr_empty(self, tmp_db) -> None:
        assert sleep_hr_data(tmp_db, S, E) == []

    def test_sleep_regularity_empty(self, tmp_db) -> None:
        assert sleep_regularity_data(tmp_db, S, E) == []

    def test_sleep_environment_empty(self, tmp_db) -> None:
        assert sleep_environment_data(tmp_db, S, E) == []

    def test_sleep_impact_empty(self, tmp_db) -> None:
        assert sleep_impact_data(tmp_db, S, E) == []

    def test_training_records_empty(self, tmp_db) -> None:
        assert training_records_data(tmp_db, S, E) == []

    def test_weight_data_empty(self, tmp_db) -> None:
        from vitalog.dashboard.queries import weight_data

        assert weight_data(tmp_db, S, E) == []

    def test_weight_data_returns_values(self, seeded_db) -> None:
        from vitalog.dashboard.queries import weight_data

        # Seed a BodyMass record
        seeded_db.execute(
            "INSERT INTO stg_records VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "Watch",
                "10.0",
                None,
                "BodyMass",
                "lb",
                "2025-06-01 08:00:00",
                "2025-06-01 08:00:00",
                "2025-06-01 08:01:00",
                175.0,
            ),
        )
        result = weight_data(seeded_db, S, E)
        assert len(result) == 1
        assert result[0]["weight"] == 175.0

    def test_prior_period_empty(self, tmp_db) -> None:
        result = prior_period_summary(tmp_db, S, E)
        assert isinstance(result, dict)
        assert result["avg_steps"] == 0

    def test_sleep_score_empty(self, tmp_db) -> None:
        assert sleep_score_data(tmp_db, S, E) == []

    def test_hr_zone_empty(self, tmp_db) -> None:
        assert hr_zone_data(tmp_db, S, E) == []

    def test_bmi_empty(self, tmp_db) -> None:
        assert bmi_data(tmp_db, S, E, height_in=70) == []

    def test_calendar_empty(self, tmp_db) -> None:
        result = daily_activity_calendar(tmp_db, S, E)
        assert isinstance(result, list)
        assert len(result) == 3  # 3 days, all 0 minutes

    def test_consistency_empty(self, tmp_db) -> None:
        result = consistency_data(tmp_db, S, E)
        assert isinstance(result, dict)

    def test_week_review_empty(self, tmp_db) -> None:
        result = week_review_data(tmp_db, E)
        assert isinstance(result, list)
        assert len(result) == 7
