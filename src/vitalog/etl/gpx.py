from __future__ import annotations

import csv
import logging
import multiprocessing
import tempfile
from pathlib import Path

import duckdb
import gpxpy

from vitalog.console import get_console

logger = logging.getLogger("vitalog")
console = get_console()


def _parse_gpx_file(gpx_file: Path) -> list[tuple]:
    rows = []
    with open(gpx_file) as f:
        gpx = gpxpy.parse(f)

    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                ext = point.extensions
                speed = None
                course = None
                h_acc = None
                v_acc = None
                if ext:
                    for child in ext:
                        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        if tag == "speed":
                            speed = float(child.text) if child.text else None
                        elif tag == "course":
                            course = float(child.text) if child.text else None
                        elif tag == "hAcc":
                            h_acc = float(child.text) if child.text else None
                        elif tag == "vAcc":
                            v_acc = float(child.text) if child.text else None

                rows.append(
                    (
                        gpx_file.name,
                        track.name,
                        point.time.isoformat() if point.time else None,
                        point.latitude,
                        point.longitude,
                        point.elevation,
                        speed,
                        course,
                        h_acc,
                        v_acc,
                    ),
                )
    return rows


def _parse_gpx_file_safe(gpx_file_str: str) -> list[tuple]:
    """Wrapper for multiprocessing — accepts a string path and catches errors."""
    try:
        return _parse_gpx_file(Path(gpx_file_str))
    except Exception as e:
        logger.warning("Failed to parse %s: %s", gpx_file_str, e)
        return []


_CSV_COLS = [
    "gpx_file",
    "workout_name",
    "recorded_at",
    "latitude",
    "longitude",
    "elevation",
    "speed",
    "course",
    "horiz_accuracy",
    "vert_accuracy",
]


def load_gpx_routes(gpx_dir: Path, conn: duckdb.DuckDBPyConnection) -> None:
    gpx_files = sorted(gpx_dir.rglob("*.gpx"))
    total = len(gpx_files)
    console.print(f"[cyan]Loading[/cyan] {total:,} GPX workout routes ...")

    # Parallel parse across CPU cores
    file_paths = [str(f) for f in gpx_files]
    workers = min(multiprocessing.cpu_count() or 4, total)
    console.print(f"  Parsing GPX files with {workers} workers ...")

    with multiprocessing.Pool(processes=workers) as pool:
        results = pool.map(_parse_gpx_file_safe, file_paths, chunksize=32)

    # Write all rows to a temp CSV, then bulk-load into DuckDB
    route_count = 0
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as tmp:
        writer = csv.writer(tmp)
        writer.writerow(_CSV_COLS)
        for rows in results:
            for row in rows:
                writer.writerow(row)
                route_count += 1
        tmp_path = tmp.name

    try:
        if route_count > 0:
            console.print(f"  [cyan]Bulk loading[/cyan] {route_count:,} route points ...")
            conn.execute(
                """
                INSERT INTO stg_workout_routes
                SELECT * FROM read_csv($csv_path,
                    header=true,
                    columns={
                        'gpx_file': 'VARCHAR',
                        'workout_name': 'VARCHAR',
                        'recorded_at': 'TIMESTAMP',
                        'latitude': 'DOUBLE',
                        'longitude': 'DOUBLE',
                        'elevation': 'DOUBLE',
                        'speed': 'DOUBLE',
                        'course': 'DOUBLE',
                        'horiz_accuracy': 'DOUBLE',
                        'vert_accuracy': 'DOUBLE'
                    }
                )
                """,
                {"csv_path": tmp_path},
            )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    failed = sum(1 for r in results if not r)
    if failed:
        console.print(f"  [yellow]Warning:[/yellow] {failed} GPX files failed to parse")

    console.print(f"  [green]Loaded[/green] {route_count:,} route points from {total:,} GPX files")
