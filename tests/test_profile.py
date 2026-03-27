"""Tests for user profile — db helper, ETL parsing, and CLI."""

from __future__ import annotations

from xml.etree import ElementTree

import duckdb
import pytest

from vitalog.db import STAGING_DDL, VIEWS_DDL, get_user_profile
from vitalog.etl.apple import _parse_me, _upsert_profile


@pytest.fixture
def profile_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(STAGING_DDL)
    conn.execute(VIEWS_DDL)
    yield conn
    conn.close()


class TestGetUserProfile:
    def test_no_table(self) -> None:
        """get_user_profile returns {} when user_profile table doesn't exist."""
        conn = duckdb.connect(":memory:")
        result = get_user_profile(conn)
        assert result == {}
        conn.close()

    def test_empty_table(self, profile_db) -> None:
        result = get_user_profile(profile_db)
        assert result == {}

    def test_with_data(self, profile_db) -> None:
        profile_db.execute("INSERT INTO user_profile VALUES ('age', '35')")
        profile_db.execute("INSERT INTO user_profile VALUES ('sex', 'male')")
        result = get_user_profile(profile_db)
        assert result == {"age": "35", "sex": "male"}

    def test_seeded_db(self, seeded_db) -> None:
        result = get_user_profile(seeded_db)
        assert result["age"] == "35"
        assert result["sex"] == "male"
        assert result["weight_lbs"] == "175"
        assert result["height_in"] == "70"


class TestParseMe:
    def test_extracts_dob_and_sex(self) -> None:
        xml = '<Me HKCharacteristicTypeIdentifierDateOfBirth="1990-06-15" HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexMale"/>'
        elem = ElementTree.fromstring(xml)
        result = _parse_me(elem)
        assert result["date_of_birth"] == "1990-06-15"
        assert result["sex"] == "male"

    def test_female(self) -> None:
        xml = '<Me HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexFemale"/>'
        elem = ElementTree.fromstring(xml)
        result = _parse_me(elem)
        assert result["sex"] == "female"

    def test_no_relevant_attrs(self) -> None:
        xml = '<Me HKCharacteristicTypeIdentifierBloodType="HKBloodTypeABPositive"/>'
        elem = ElementTree.fromstring(xml)
        result = _parse_me(elem)
        assert result == {}

    def test_not_set_sex_excluded(self) -> None:
        xml = '<Me HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexNotSet"/>'
        elem = ElementTree.fromstring(xml)
        result = _parse_me(elem)
        assert "sex" not in result


class TestUpsertProfile:
    def test_creates_row(self, profile_db) -> None:
        _upsert_profile(profile_db, {"date_of_birth": "1990-06-15", "sex": "male"})
        rows = profile_db.execute("SELECT key, value FROM user_profile ORDER BY key").fetchall()
        assert dict(rows) == {"date_of_birth": "1990-06-15", "sex": "male"}

    def test_updates_existing(self, profile_db) -> None:
        profile_db.execute("INSERT INTO user_profile VALUES ('sex', 'female')")
        _upsert_profile(profile_db, {"sex": "male"})
        result = profile_db.execute("SELECT value FROM user_profile WHERE key = 'sex'").fetchone()
        assert result[0] == "male"

    def test_does_not_overwrite_with_empty(self, profile_db) -> None:
        profile_db.execute("INSERT INTO user_profile VALUES ('sex', 'male')")
        _upsert_profile(profile_db, {"date_of_birth": "1990-01-01"})
        rows = profile_db.execute("SELECT key, value FROM user_profile ORDER BY key").fetchall()
        result = dict(rows)
        assert result["sex"] == "male"
        assert result["date_of_birth"] == "1990-01-01"
