"""Tests for vitalog.__main__ — CLI commands."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from vitalog.__main__ import __version__, app

runner = CliRunner()


class TestVersionFlag:
    def test_version_output(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output

    def test_short_flag(self) -> None:
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestNoCommand:
    def test_shows_help(self) -> None:
        result = runner.invoke(app, [])
        assert result.exit_code == 1


class TestLoadApple:
    def test_missing_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.zip"
        result = runner.invoke(app, ["load", "apple", "--file", str(missing)])
        assert result.exit_code == 1
        assert "not found" in " ".join(result.output.split())


class TestLoadAll:
    def test_missing_apple_file(self, tmp_path: Path) -> None:
        csv = tmp_path / "sleep.csv"
        csv.write_text("Start;End\n")
        result = runner.invoke(
            app,
            ["load", "all", "--apple", str(tmp_path / "missing.zip"), "--sleep", str(csv)],
        )
        assert result.exit_code == 1
        assert "not found" in " ".join(result.output.split())

    def test_missing_sleep_file(self, tmp_path: Path) -> None:
        import zipfile

        zip_path = tmp_path / "export.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "x")
        result = runner.invoke(
            app,
            ["load", "all", "--apple", str(zip_path), "--sleep", str(tmp_path / "missing.csv")],
        )
        assert result.exit_code == 1
        assert "not found" in " ".join(result.output.split())

    def test_invalid_zip(self, tmp_path: Path) -> None:
        fake_zip = tmp_path / "bad.zip"
        fake_zip.write_text("not a zip")
        csv = tmp_path / "sleep.csv"
        csv.write_text("Start;End\n")
        result = runner.invoke(
            app,
            ["load", "all", "--apple", str(fake_zip), "--sleep", str(csv)],
        )
        assert result.exit_code == 1
        assert "not a valid ZIP" in " ".join(result.output.split())

    def test_non_csv_sleep(self, tmp_path: Path) -> None:
        import zipfile

        zip_path = tmp_path / "export.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("dummy.txt", "x")
        txt = tmp_path / "sleep.txt"
        txt.write_text("data")
        result = runner.invoke(
            app,
            ["load", "all", "--apple", str(zip_path), "--sleep", str(txt)],
        )
        assert result.exit_code == 1
        assert "not a CSV" in result.output

    def test_apple_only_without_sleep(self, tmp_path: Path) -> None:
        """load all should work with only --apple (sleep is optional)."""
        missing = tmp_path / "nonexistent.zip"
        result = runner.invoke(app, ["load", "all", "--apple", str(missing)])
        assert result.exit_code == 1
        assert "not found" in " ".join(result.output.split())  # fails on missing file, not missing --sleep flag


class TestLoadSleep:
    def test_missing_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.csv"
        result = runner.invoke(app, ["load", "sleep", "--file", str(missing)])
        assert result.exit_code == 1
        assert "not found" in " ".join(result.output.split())


class TestNarrative:
    def test_missing_db(self, tmp_path: Path) -> None:
        db = tmp_path / "nonexistent.duckdb"
        result = runner.invoke(app, ["narrative", "--db", str(db)])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_invalid_period(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        db.write_bytes(b"")  # Create empty file so it passes existence check
        result = runner.invoke(app, ["narrative", "--period", "bogus", "--db", str(db)])
        assert result.exit_code == 1
        assert "Unknown period" in result.output


class TestDashboard:
    def test_missing_db(self, tmp_path: Path) -> None:
        db = tmp_path / "nonexistent.duckdb"
        result = runner.invoke(app, ["dashboard", "--db", str(db)])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_invalid_period(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        db.write_bytes(b"")
        result = runner.invoke(app, ["dashboard", "--period", "bogus", "--db", str(db)])
        assert result.exit_code == 1
        assert "Unknown period" in result.output


class TestProfile:
    def test_set_profile(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        result = runner.invoke(app, ["profile", "--age", "35", "--db", str(db)])
        assert result.exit_code == 0
        assert "age=35" in result.output

    def test_set_multiple(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        result = runner.invoke(
            app,
            ["profile", "--age", "30", "--weight", "175", "--height", "70", "--sex", "male", "--db", str(db)],
        )
        assert result.exit_code == 0
        assert "Profile updated" in result.output

    def test_show_empty(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        result = runner.invoke(app, ["profile", "--show", "--db", str(db)])
        assert result.exit_code == 0
        assert "No profile set" in result.output

    def test_show_after_set(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        runner.invoke(app, ["profile", "--age", "28", "--db", str(db)])
        result = runner.invoke(app, ["profile", "--show", "--db", str(db)])
        assert result.exit_code == 0
        assert "28" in result.output

    def test_invalid_sex(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        result = runner.invoke(app, ["profile", "--sex", "other", "--db", str(db)])
        assert result.exit_code == 1
        assert "must be" in result.output

    def test_no_fields(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        result = runner.invoke(app, ["profile", "--db", str(db)])
        assert result.exit_code == 1

    def test_upsert(self, tmp_path: Path) -> None:
        db = tmp_path / "test.duckdb"
        runner.invoke(app, ["profile", "--age", "30", "--db", str(db)])
        runner.invoke(app, ["profile", "--age", "31", "--db", str(db)])
        result = runner.invoke(app, ["profile", "--show", "--db", str(db)])
        assert "31" in result.output
