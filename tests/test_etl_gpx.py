"""Tests for vitalog.etl.gpx — GPX file parsing."""

from __future__ import annotations

from pathlib import Path

from vitalog.etl.gpx import _parse_gpx_file, load_gpx_routes


def _make_gpx(path: Path, name: str = "Test Track") -> Path:
    """Write a minimal GPX file."""
    path.write_text(f"""<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1">
  <trk>
    <name>{name}</name>
    <trkseg>
      <trkpt lat="47.6062" lon="-122.3321">
        <ele>10.5</ele>
        <time>2025-06-01T07:00:00Z</time>
      </trkpt>
      <trkpt lat="47.6065" lon="-122.3325">
        <ele>11.0</ele>
        <time>2025-06-01T07:01:00Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>
""")
    return path


class TestParseGpxFile:
    def test_parses_trackpoints(self, tmp_path: Path) -> None:
        gpx = _make_gpx(tmp_path / "route.gpx")
        rows = _parse_gpx_file(gpx)
        assert len(rows) == 2
        assert rows[0][0] == "route.gpx"  # filename
        assert rows[0][1] == "Test Track"  # track name
        assert rows[0][3] == 47.6062  # lat
        assert rows[0][4] == -122.3321  # lon
        assert rows[0][5] == 10.5  # elevation

    def test_empty_gpx(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.gpx"
        path.write_text("""<?xml version="1.0"?>
<gpx version="1.1"><trk><trkseg></trkseg></trk></gpx>""")
        rows = _parse_gpx_file(path)
        assert rows == []

    def test_parses_extensions(self, tmp_path: Path) -> None:
        path = tmp_path / "ext.gpx"
        path.write_text("""<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1"
     xmlns="http://www.topografix.com/GPX/1/1"
     xmlns:ns3="http://www.garmin.com/xmlschemas/TrackPointExtension/v1">
  <trk>
    <name>Extended</name>
    <trkseg>
      <trkpt lat="47.6062" lon="-122.3321">
        <ele>10.5</ele>
        <time>2025-06-01T07:00:00Z</time>
        <extensions>
          <ns3:speed>3.5</ns3:speed>
          <ns3:course>180.0</ns3:course>
          <ns3:hAcc>5.0</ns3:hAcc>
          <ns3:vAcc>3.0</ns3:vAcc>
        </extensions>
      </trkpt>
    </trkseg>
  </trk>
</gpx>""")
        rows = _parse_gpx_file(path)
        assert len(rows) == 1
        assert rows[0][6] == 3.5  # speed
        assert rows[0][7] == 180.0  # course
        assert rows[0][8] == 5.0  # hAcc
        assert rows[0][9] == 3.0  # vAcc


class TestLoadGpxRoutes:
    def test_loads_multiple_files(self, tmp_path: Path, tmp_db) -> None:
        gpx_dir = tmp_path / "routes"
        gpx_dir.mkdir()
        _make_gpx(gpx_dir / "route1.gpx", "Route 1")
        _make_gpx(gpx_dir / "route2.gpx", "Route 2")

        load_gpx_routes(gpx_dir, tmp_db)

        count = tmp_db.execute("SELECT COUNT(*) FROM stg_workout_routes").fetchone()[0]
        assert count == 4  # 2 files × 2 points each

    def test_handles_many_files(self, tmp_path: Path, tmp_db) -> None:
        gpx_dir = tmp_path / "routes"
        gpx_dir.mkdir()
        for i in range(10):
            _make_gpx(gpx_dir / f"route_{i:03d}.gpx", f"Route {i}")

        load_gpx_routes(gpx_dir, tmp_db)

        count = tmp_db.execute("SELECT COUNT(*) FROM stg_workout_routes").fetchone()[0]
        assert count == 20  # 10 files × 2 points each

    def test_skips_invalid_gpx(self, tmp_path: Path, tmp_db) -> None:
        gpx_dir = tmp_path / "routes"
        gpx_dir.mkdir()
        _make_gpx(gpx_dir / "good.gpx")
        (gpx_dir / "bad.gpx").write_text("not valid xml!!!")

        load_gpx_routes(gpx_dir, tmp_db)

        count = tmp_db.execute("SELECT COUNT(*) FROM stg_workout_routes").fetchone()[0]
        assert count == 2  # Only the good file
