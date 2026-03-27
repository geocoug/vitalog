"""Integration tests for vitalog.etl.apple — full XML load pipeline."""

from __future__ import annotations

import zipfile
from pathlib import Path
from xml.etree.ElementTree import Element

from vitalog.etl.apple import (
    _parse_activity,
    _parse_record,
    _parse_workout,
    load_apple_health,
)


class TestParseRecord:
    def test_standard_record(self) -> None:
        elem = Element(
            "Record",
            attrib={
                "sourceName": "iPhone",
                "sourceVersion": "17.0",
                "device": "iPhone 15",
                "type": "HKQuantityTypeIdentifierStepCount",
                "unit": "count",
                "creationDate": "2025-06-01 08:00:00 -0700",
                "startDate": "2025-06-01 08:00:00 -0700",
                "endDate": "2025-06-01 08:30:00 -0700",
                "value": "3000",
            },
        )
        row = _parse_record(elem)
        assert row[0] == "iPhone"
        assert row[3] == "StepCount"
        assert row[4] == "count"
        assert row[5] == "2025-06-01 08:00:00"
        assert row[8] == "3000"

    def test_missing_optional_fields(self) -> None:
        elem = Element(
            "Record",
            attrib={
                "type": "HKQuantityTypeIdentifierHeartRate",
                "startDate": "2025-06-01 08:00:00 -0700",
                "endDate": "2025-06-01 08:01:00 -0700",
                "value": "72",
            },
        )
        row = _parse_record(elem)
        assert row[0] is None  # sourceName
        assert row[2] is None  # device
        assert row[3] == "HeartRate"


class TestParseWorkout:
    def test_standard_workout(self) -> None:
        elem = Element(
            "Workout",
            attrib={
                "sourceName": "Watch",
                "sourceVersion": "10.0",
                "creationDate": "2025-06-01 07:00:00 -0700",
                "startDate": "2025-06-01 07:00:00 -0700",
                "endDate": "2025-06-01 07:30:00 -0700",
                "workoutActivityType": "HKWorkoutActivityTypeRunning",
                "duration": "30.5",
                "durationUnit": "min",
                "totalDistance": "3.1",
                "totalDistanceUnit": "mi",
                "totalEnergyBurned": "350",
                "totalEnergyBurnedUnit": "kcal",
            },
        )
        row = _parse_workout(elem)
        assert row[0] == "Watch"
        assert row[6] == "HKWorkoutActivityTypeRunning"
        assert row[7] == "30.5"
        assert row[9] == "3.1"

    def test_missing_distance(self) -> None:
        elem = Element(
            "Workout",
            attrib={
                "workoutActivityType": "HKWorkoutActivityTypeCycling",
                "startDate": "2025-06-01 07:00:00 -0700",
                "endDate": "2025-06-01 08:00:00 -0700",
                "duration": "60",
                "durationUnit": "min",
            },
        )
        row = _parse_workout(elem)
        assert row[9] is None  # totalDistance


class TestParseActivity:
    def test_standard_activity(self) -> None:
        elem = Element(
            "ActivitySummary",
            attrib={
                "dateComponents": "2025-06-01",
                "activeEnergyBurned": "450.5",
                "activeEnergyBurnedGoal": "400",
                "activeEnergyBurnedUnit": "kcal",
                "appleExerciseTime": "35",
                "appleExerciseTimeGoal": "30",
                "appleStandHours": "12",
                "appleStandHoursGoal": "12",
            },
        )
        row = _parse_activity(elem)
        assert row[0] == "2025-06-01"
        assert row[1] == "450.5"
        assert row[3] == "kcal"


def _make_export_zip(tmp_path: Path) -> Path:
    """Build a minimal Apple Health export ZIP."""
    export_dir = tmp_path / "apple_health_export"
    export_dir.mkdir()

    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
    <Record type="HKQuantityTypeIdentifierStepCount"
            sourceName="iPhone" sourceVersion="17.0"
            unit="count"
            creationDate="2025-06-01 08:00:00 -0700"
            startDate="2025-06-01 08:00:00 -0700"
            endDate="2025-06-01 08:30:00 -0700"
            value="5000"/>
    <Record type="HKQuantityTypeIdentifierStepCount"
            sourceName="iPhone" sourceVersion="17.0"
            unit="count"
            creationDate="2025-06-01 12:00:00 -0700"
            startDate="2025-06-01 12:00:00 -0700"
            endDate="2025-06-01 12:30:00 -0700"
            value="3000"/>
    <Record type="HKQuantityTypeIdentifierHeartRate"
            sourceName="Watch" sourceVersion="10.0"
            unit="count/min"
            creationDate="2025-06-01 08:00:00 -0700"
            startDate="2025-06-01 08:00:00 -0700"
            endDate="2025-06-01 08:01:00 -0700"
            value="72"/>
    <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
             sourceName="Watch" sourceVersion="10.0"
             duration="30" durationUnit="min"
             startDate="2025-06-01 07:00:00 -0700"
             endDate="2025-06-01 07:30:00 -0700"
             creationDate="2025-06-01 07:00:00 -0700"/>
    <ActivitySummary dateComponents="2025-06-01"
                     activeEnergyBurned="450"
                     activeEnergyBurnedGoal="400"
                     activeEnergyBurnedUnit="kcal"
                     appleExerciseTime="35"
                     appleExerciseTimeGoal="30"
                     appleStandHours="12"
                     appleStandHoursGoal="12"/>
</HealthData>"""

    (export_dir / "export.xml").write_text(xml_content)

    zip_path = tmp_path / "export.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(export_dir / "export.xml", "apple_health_export/export.xml")
    return zip_path


class TestLoadAppleHealth:
    def test_full_pipeline(self, tmp_path: Path, tmp_db) -> None:
        zip_path = _make_export_zip(tmp_path)
        load_apple_health(zip_path, tmp_db)

        records = tmp_db.execute("SELECT COUNT(*) FROM stg_records").fetchone()[0]
        assert records == 3  # 2 steps + 1 HR

        workouts = tmp_db.execute("SELECT COUNT(*) FROM stg_workouts").fetchone()[0]
        assert workouts == 1

        activity = tmp_db.execute("SELECT COUNT(*) FROM stg_activity_summary").fetchone()[0]
        assert activity == 1

    def test_clears_existing_data(self, tmp_path: Path, tmp_db) -> None:
        zip_path = _make_export_zip(tmp_path)
        load_apple_health(zip_path, tmp_db)
        load_apple_health(zip_path, tmp_db)

        records = tmp_db.execute("SELECT COUNT(*) FROM stg_records").fetchone()[0]
        assert records == 3  # Not doubled

    def test_missing_xml_raises(self, tmp_path: Path, tmp_db) -> None:
        import pytest

        zip_path = tmp_path / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("apple_health_export/readme.txt", "no xml here")

        with pytest.raises(FileNotFoundError, match="export.xml"):
            load_apple_health(zip_path, tmp_db)
