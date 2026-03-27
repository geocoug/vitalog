"""Tests for vitalog.db — schema DDL and connection management."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from vitalog.db import STAGING_DDL, VIEWS_DDL, connect, format_height, init_schema


class TestConnect:
    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        db_path = tmp_path / "subdir" / "nested" / "test.duckdb"
        with connect(db_path) as conn:
            conn.execute("SELECT 1")
        assert db_path.parent.exists()

    def test_yields_connection(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with connect(db_path) as conn:
            assert isinstance(conn, duckdb.DuckDBPyConnection)
            result = conn.execute("SELECT 42").fetchone()
            assert result[0] == 42

    def test_closes_on_exit(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with connect(db_path) as conn:
            pass
        with pytest.raises(duckdb.ConnectionException):
            conn.execute("SELECT 1")


class TestInitSchema:
    def test_creates_staging_tables(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with connect(db_path) as conn:
            init_schema(conn)
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            assert "stg_records" in tables
            assert "stg_workouts" in tables
            assert "stg_activity_summary" in tables
            assert "stg_workout_routes" in tables
            assert "stg_sleep_cycle" in tables

    def test_creates_views(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with connect(db_path) as conn:
            init_schema(conn)
            # Views should exist and be queryable
            conn.execute("SELECT * FROM daily_steps LIMIT 1")
            conn.execute("SELECT * FROM daily_heart_rate LIMIT 1")
            conn.execute("SELECT * FROM daily_resting_hr LIMIT 1")
            conn.execute("SELECT * FROM daily_summary LIMIT 1")
            conn.execute("SELECT * FROM workout_summary LIMIT 1")
            conn.execute("SELECT * FROM sleep_combined LIMIT 1")

    def test_idempotent(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.duckdb"
        with connect(db_path) as conn:
            init_schema(conn)
            init_schema(conn)
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            assert "stg_records" in tables


class TestFormatHeight:
    def test_standard(self) -> None:
        assert format_height("70") == "5'10\""

    def test_exact_feet(self) -> None:
        assert format_height("72") == "6'0\""

    def test_none(self) -> None:
        assert format_height(None) is None

    def test_empty(self) -> None:
        assert format_height("") is None

    def test_non_numeric(self) -> None:
        assert format_height("abc") == "abc"


class TestDDLSyntax:
    def test_staging_ddl_valid(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute(STAGING_DDL)
        conn.close()

    def test_views_ddl_valid(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute(STAGING_DDL)
        conn.execute(VIEWS_DDL)
        conn.close()
