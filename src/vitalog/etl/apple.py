from __future__ import annotations

import csv
import logging
import re
import tempfile
import zipfile
from pathlib import Path
from xml.etree import ElementTree

import duckdb

from vitalog.console import get_console
from vitalog.etl.gpx import load_gpx_routes

logger = logging.getLogger("vitalog")
console = get_console()

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) [+-]\d{4}")

RECORD_COLS = [
    "source_name",
    "source_version",
    "device",
    "record_type",
    "unit",
    "creation_date",
    "start_date",
    "end_date",
    "value",
]
WORKOUT_COLS = [
    "source_name",
    "source_version",
    "device",
    "creation_date",
    "start_date",
    "end_date",
    "workout_activity_type",
    "duration",
    "duration_unit",
    "total_distance",
    "total_distance_unit",
    "total_energy_burned",
    "total_energy_burned_unit",
]
ACTIVITY_COLS = [
    "date_components",
    "active_energy_burned",
    "active_energy_burned_goal",
    "active_energy_burned_unit",
    "apple_exercise_time",
    "apple_exercise_time_goal",
    "apple_stand_hours",
    "apple_stand_hours_goal",
]


_SEX_MAP = {
    "HKBiologicalSexMale": "male",
    "HKBiologicalSexFemale": "female",
    "HKBiologicalSexOther": "other",
}


def _parse_me(elem: ElementTree.Element) -> dict:
    """Extract date of birth and biological sex from the <Me> XML element."""
    a = elem.attrib
    result = {}
    dob = a.get("HKCharacteristicTypeIdentifierDateOfBirth")
    if dob:
        result["date_of_birth"] = dob
    sex_raw = a.get("HKCharacteristicTypeIdentifierBiologicalSex", "")
    sex = _SEX_MAP.get(sex_raw)
    if sex:
        result["sex"] = sex
    return result


def _upsert_profile(conn, me_data: dict) -> None:
    """Insert or update user_profile rows from <Me> element data."""
    mapping = {
        "date_of_birth": "date_of_birth",
        "sex": "sex",
    }
    for src_key, profile_key in mapping.items():
        if me_data.get(src_key):
            conn.execute(
                """
                INSERT INTO user_profile (key, value) VALUES (?, ?)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value
                """,
                [profile_key, me_data[src_key]],
            )


def abbreviate(type_name: str) -> str:
    for prefix in ("HKQuantityTypeIdentifier", "HKCategoryTypeIdentifier", "HKDataType"):
        if type_name.startswith(prefix):
            return type_name[len(prefix) :]
    return type_name


def _parse_ts(value: str | None) -> str | None:
    if value is None:
        return None
    m = DATE_RE.match(value)
    return m.group(1) if m else value


def _safe_float(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        float(value)
        return value
    except (ValueError, TypeError):
        return None


def _parse_record(elem: ElementTree.Element) -> list[str | None]:
    a = elem.attrib
    return [
        a.get("sourceName"),
        a.get("sourceVersion"),
        a.get("device"),
        abbreviate(a.get("type", "")),
        a.get("unit"),
        _parse_ts(a.get("creationDate")),
        _parse_ts(a.get("startDate")),
        _parse_ts(a.get("endDate")),
        _safe_float(a.get("value")),
    ]


def _parse_workout(elem: ElementTree.Element) -> list[str | None]:
    a = elem.attrib
    return [
        a.get("sourceName"),
        a.get("sourceVersion"),
        a.get("device"),
        _parse_ts(a.get("creationDate")),
        _parse_ts(a.get("startDate")),
        _parse_ts(a.get("endDate")),
        abbreviate(a.get("workoutActivityType", "")),
        _safe_float(a.get("duration")),
        a.get("durationUnit"),
        _safe_float(a.get("totalDistance")),
        a.get("totalDistanceUnit"),
        _safe_float(a.get("totalEnergyBurned")),
        a.get("totalEnergyBurnedUnit"),
    ]


def _parse_activity(elem: ElementTree.Element) -> list[str | None]:
    a = elem.attrib
    return [
        a.get("dateComponents"),
        _safe_float(a.get("activeEnergyBurned")),
        _safe_float(a.get("activeEnergyBurnedGoal")),
        a.get("activeEnergyBurnedUnit"),
        _safe_float(a.get("appleExerciseTime")),
        _safe_float(a.get("appleExerciseTimeGoal")),
        _safe_float(a.get("appleStandHours")),
        _safe_float(a.get("appleStandHoursGoal")),
    ]


def _extract_zip(zip_path: Path, dest: Path) -> Path:
    console.print(f"[cyan]Extracting[/cyan] {zip_path.name} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Validate members to prevent zip-slip path traversal
        for member in zf.namelist():
            member_path = (dest / member).resolve()
            if not str(member_path).startswith(str(dest.resolve())):
                msg = f"Unsafe path in ZIP: {member}"
                raise ValueError(msg)
        zf.extractall(dest)
    export_dir = dest / "apple_health_export"
    if not export_dir.exists():
        export_dir = dest
    return export_dir


def _load_xml(xml_path: Path, conn: duckdb.DuckDBPyConnection, work_dir: Path) -> None:
    console.print(f"[cyan]Parsing[/cyan] {xml_path.name} → temp CSVs ...")

    records_csv = work_dir / "records.csv"
    workouts_csv = work_dir / "workouts.csv"
    activity_csv = work_dir / "activity.csv"

    record_count = 0
    workout_count = 0
    activity_count = 0

    with (
        open(records_csv, "w", newline="") as rf,
        open(workouts_csv, "w", newline="") as wf,
        open(activity_csv, "w", newline="") as af,
    ):
        rw = csv.writer(rf)
        rw.writerow(RECORD_COLS)
        ww = csv.writer(wf)
        ww.writerow(WORKOUT_COLS)
        aw = csv.writer(af)
        aw.writerow(ACTIVITY_COLS)

        for _event, elem in ElementTree.iterparse(str(xml_path), events=("end",)):
            if elem.tag == "Record":
                rw.writerow(_parse_record(elem))
                record_count += 1
                if record_count % 500_000 == 0:
                    console.print(f"  {record_count:,} records parsed ...")
                elem.clear()
            elif elem.tag == "Workout":
                ww.writerow(_parse_workout(elem))
                workout_count += 1
                elem.clear()
            elif elem.tag == "ActivitySummary":
                aw.writerow(_parse_activity(elem))
                activity_count += 1
                elem.clear()
            elif elem.tag == "Me":
                me_data = _parse_me(elem)
                if me_data:
                    _upsert_profile(conn, me_data)
                    console.print(
                        f"  [green]User profile extracted:[/green] {', '.join(f'{k}={v}' for k, v in me_data.items())}",
                    )
                elem.clear()

    console.print(
        f"  Parsed {record_count:,} records, {workout_count:,} workouts, {activity_count:,} activity summaries",
    )

    # Bulk load CSVs into DuckDB (much faster than executemany)
    console.print("[cyan]Loading[/cyan] records into DuckDB ...")
    conn.execute(
        """
        INSERT INTO stg_records
        SELECT * FROM read_csv($csv_path,
            header=true,
            columns={
                'source_name': 'VARCHAR',
                'source_version': 'VARCHAR',
                'device': 'VARCHAR',
                'record_type': 'VARCHAR',
                'unit': 'VARCHAR',
                'creation_date': 'TIMESTAMP',
                'start_date': 'TIMESTAMP',
                'end_date': 'TIMESTAMP',
                'value': 'DOUBLE'
            }
        )
        """,
        {"csv_path": str(records_csv)},
    )

    console.print("[cyan]Loading[/cyan] workouts into DuckDB ...")
    conn.execute(
        """
        INSERT INTO stg_workouts
        SELECT * FROM read_csv($csv_path,
            header=true,
            columns={
                'source_name': 'VARCHAR',
                'source_version': 'VARCHAR',
                'device': 'VARCHAR',
                'creation_date': 'TIMESTAMP',
                'start_date': 'TIMESTAMP',
                'end_date': 'TIMESTAMP',
                'workout_activity_type': 'VARCHAR',
                'duration': 'DOUBLE',
                'duration_unit': 'VARCHAR',
                'total_distance': 'DOUBLE',
                'total_distance_unit': 'VARCHAR',
                'total_energy_burned': 'DOUBLE',
                'total_energy_burned_unit': 'VARCHAR'
            }
        )
        """,
        {"csv_path": str(workouts_csv)},
    )

    console.print("[cyan]Loading[/cyan] activity summaries into DuckDB ...")
    conn.execute(
        """
        INSERT INTO stg_activity_summary
        SELECT * FROM read_csv($csv_path,
            header=true,
            columns={
                'date_components': 'DATE',
                'active_energy_burned': 'DOUBLE',
                'active_energy_burned_goal': 'DOUBLE',
                'active_energy_burned_unit': 'VARCHAR',
                'apple_exercise_time': 'DOUBLE',
                'apple_exercise_time_goal': 'DOUBLE',
                'apple_stand_hours': 'DOUBLE',
                'apple_stand_hours_goal': 'DOUBLE'
            }
        )
        """,
        {"csv_path": str(activity_csv)},
    )

    console.print(
        f"  [green]Loaded[/green] {record_count:,} records, "
        f"{workout_count:,} workouts, "
        f"{activity_count:,} activity summaries",
    )


def load_apple_health(export_zip: Path, conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("BEGIN TRANSACTION")
    try:
        # Clear existing data
        for table in ("stg_records", "stg_workouts", "stg_activity_summary", "stg_workout_routes"):
            conn.execute(f"DELETE FROM {table}")  # noqa: S608 — table names are hardcoded constants

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            export_dir = _extract_zip(export_zip, tmpdir_path)
            xml_path = export_dir / "export.xml"

            if not xml_path.exists():
                console.print("[bold red]Error:[/bold red] export.xml not found in ZIP")
                msg = "export.xml not found in ZIP"
                raise FileNotFoundError(msg)

            _load_xml(xml_path, conn, tmpdir_path)

            gpx_dir = export_dir / "workout-routes"
            if gpx_dir.exists():
                load_gpx_routes(gpx_dir, conn)

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Report record type counts
    result = conn.execute(
        "SELECT record_type, COUNT(*) AS cnt FROM stg_records GROUP BY record_type ORDER BY cnt DESC LIMIT 15",
    ).fetchall()
    if result:
        console.print("\n[bold]Top record types:[/bold]")
        for rtype, cnt in result:
            console.print(f"  {rtype:40s} {cnt:>10,}")
