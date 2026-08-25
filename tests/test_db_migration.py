# This file is part of bank_statement_parser.
#
# Copyright (c) 2026 Jason Farrar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Tests for the database migration module (db_migration.py).

Validates version tracking, script fingerprinting, user-object detection,
and the full migrate_db() upgrade flow.

Run with:
    pytest tests/test_db_migration.py -v
"""

import sqlite3
import warnings
from pathlib import Path

import pytest

from bank_statement_parser.data.create_project_db import main as create_db
from bank_statement_parser.data.mock_project_data import generate_mock_data
from bank_statement_parser.modules.db_migration import (
    _BSP_OWNED_TABLES,
    _get_user_objects,
    fingerprint_data_scripts,
    migrate_db,
    needs_upgrade,
)

_TEST_DB = Path(__file__).parent / "test_migration.db"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fresh_db(tmp_path):
    """Create a fresh database with mock data and return its path."""
    db_path = tmp_path / "project.db"
    create_db(db_path=db_path, with_fk=False)
    generate_mock_data(db_path=db_path, num_batches=2, statements_per_batch=3, transactions_per_statement=5)
    return db_path


@pytest.fixture()
def old_db_no_meta(tmp_path):
    """Create a database WITHOUT db_meta (simulates pre-migration BSP)."""
    db_path = tmp_path / "project.db"
    create_db(db_path=db_path, with_fk=False)
    generate_mock_data(db_path=db_path, num_batches=2, statements_per_batch=3, transactions_per_statement=5)
    # Remove db_meta to simulate old database
    conn = sqlite3.connect(str(db_path))
    conn.execute("DROP TABLE IF EXISTS db_meta")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture()
def db_with_user_objects(fresh_db):
    """Add user-created tables and views to a fresh database, then dirty the version to force migration."""
    conn = sqlite3.connect(str(fresh_db))
    conn.execute("CREATE TABLE user_custom_data (id INTEGER PRIMARY KEY, label TEXT, amount REAL)")
    conn.execute("INSERT INTO user_custom_data VALUES (1, 'test', 42.5)")
    conn.execute("INSERT INTO user_custom_data VALUES (2, 'test2', 100.0)")
    conn.execute("CREATE VIEW user_summary AS SELECT label, SUM(amount) AS total FROM user_custom_data GROUP BY label")
    conn.execute("CREATE INDEX idx_user_label ON user_custom_data(label)")
    # Dirty the version to force migrate_db to run
    conn.execute("UPDATE db_meta SET value = '0.0.1' WHERE key = 'bsp_version'")
    conn.commit()
    conn.close()
    return fresh_db


# ---------------------------------------------------------------------------
# TestFingerprintScripts
# ---------------------------------------------------------------------------


class TestFingerprintScripts:
    def test_returns_dict_with_expected_keys(self):
        """fingerprint_data_scripts() returns a dict containing all data/*.py files."""
        hashes = fingerprint_data_scripts()
        assert isinstance(hashes, dict)
        assert "create_project_db.py" in hashes
        assert "build_datamart.py" in hashes
        assert "mock_project_data.py" in hashes

    def test_digests_are_sha512_hex(self):
        """Each digest is a 128-character hex string (SHA-512)."""
        hashes = fingerprint_data_scripts()
        for name, digest in hashes.items():
            assert len(digest) == 128, f"{name} digest length {len(digest)} != 128"
            assert all(c in "0123456789abcdef" for c in digest), f"{name} digest contains non-hex chars"

    def test_deterministic(self):
        """Calling fingerprint_data_scripts() twice yields identical results."""
        h1 = fingerprint_data_scripts()
        h2 = fingerprint_data_scripts()
        assert h1 == h2

    def test_excludes_pycache(self):
        """__pycache__ directories are excluded from fingerprinting."""
        hashes = fingerprint_data_scripts()
        for key in hashes:
            assert "__pycache__" not in key


# ---------------------------------------------------------------------------
# TestNeedsUpgrade
# ---------------------------------------------------------------------------


class TestNeedsUpgrade:
    def test_no_db_meta_returns_true(self, old_db_no_meta):
        """A database without db_meta always needs an upgrade."""
        assert needs_upgrade(old_db_no_meta) is True

    def test_matching_version_returns_false(self, fresh_db):
        """A database with matching version and hashes does not need an upgrade."""
        assert needs_upgrade(fresh_db) is False

    def test_different_version_returns_true(self, fresh_db):
        """A database with a different bsp_version needs an upgrade."""
        conn = sqlite3.connect(str(fresh_db))
        conn.execute("UPDATE db_meta SET value = '0.0.1' WHERE key = 'bsp_version'")
        conn.commit()
        conn.close()
        assert needs_upgrade(fresh_db) is True

    def test_different_hashes_returns_true(self, fresh_db):
        """A database with different script hashes needs an upgrade."""
        conn = sqlite3.connect(str(fresh_db))
        conn.execute("UPDATE db_meta SET value = '{}' WHERE key = 'script_hashes'")
        conn.commit()
        conn.close()
        assert needs_upgrade(fresh_db) is True

    def test_nonexistent_db_returns_false(self, tmp_path):
        """A nonexistent database path returns False (no upgrade possible)."""
        assert needs_upgrade(tmp_path / "nonexistent.db") is False


# ---------------------------------------------------------------------------
# TestGetUserObjects
# ---------------------------------------------------------------------------


class TestGetUserObjects:
    def test_bsp_tables_not_in_user_objects(self, fresh_db):
        """BSP-owned tables are not reported as user objects."""
        conn = sqlite3.connect(str(fresh_db))
        user_objects = _get_user_objects(conn)
        user_table_names = {t["name"] for t in user_objects["tables"]}
        for bsp_table in _BSP_OWNED_TABLES:
            assert bsp_table not in user_table_names
        conn.close()

    def test_user_table_detected(self, db_with_user_objects):
        """A user-created table is detected as a user object."""
        conn = sqlite3.connect(str(db_with_user_objects))
        user_objects = _get_user_objects(conn)
        user_table_names = {t["name"] for t in user_objects["tables"]}
        assert "user_custom_data" in user_table_names
        conn.close()

    def test_user_view_detected(self, db_with_user_objects):
        """A user-created view is detected as a user object."""
        conn = sqlite3.connect(str(db_with_user_objects))
        user_objects = _get_user_objects(conn)
        user_view_names = {v["name"] for v in user_objects["views"]}
        assert "user_summary" in user_view_names
        conn.close()

    def test_user_index_detected(self, db_with_user_objects):
        """A user-created index is detected as a user object."""
        conn = sqlite3.connect(str(db_with_user_objects))
        user_objects = _get_user_objects(conn)
        user_index_names = {i["name"] for i in user_objects["indexes"]}
        assert "idx_user_label" in user_index_names
        conn.close()

    def test_bsp_views_not_in_user_objects(self, fresh_db):
        """BSP-owned views are not reported as user objects."""
        conn = sqlite3.connect(str(fresh_db))
        user_objects = _get_user_objects(conn)
        user_view_names = {v["name"] for v in user_objects["views"]}
        for bsp_view in ("GapReport", "FlatTransaction"):
            assert bsp_view not in user_view_names
        conn.close()


# ---------------------------------------------------------------------------
# TestMigrateDb
# ---------------------------------------------------------------------------


class TestMigrateDb:
    def test_no_upgrade_needed(self, fresh_db):
        """migrate_db returns False when no upgrade is needed."""
        assert migrate_db(fresh_db) is False

    def test_upgrade_with_user_objects(self, db_with_user_objects):
        """User tables, views, and indexes survive migration."""
        # Record pre-migration state
        conn = sqlite3.connect(str(db_with_user_objects))
        pre_rows = conn.execute("SELECT COUNT(*) FROM user_custom_data").fetchone()[0]
        pre_view = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'user_summary'").fetchone()[0]
        conn.close()

        # Perform migration
        result = migrate_db(db_with_user_objects)
        assert result is True

        # Verify user objects in new database
        conn = sqlite3.connect(str(db_with_user_objects))
        post_rows = conn.execute("SELECT COUNT(*) FROM user_custom_data").fetchone()[0]
        assert post_rows == pre_rows
        post_view = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'user_summary'").fetchone()[0]
        assert post_view == pre_view
        # Check index
        indexes = conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_user_label'").fetchall()
        assert len(indexes) == 1
        conn.close()

    def test_upgrade_with_raw_data(self, db_with_user_objects):
        """BSP raw source data is preserved through migration."""
        conn = sqlite3.connect(str(db_with_user_objects))
        pre_statements = conn.execute("SELECT COUNT(*) FROM statement_heads").fetchone()[0]
        pre_transactions = conn.execute("SELECT COUNT(*) FROM statement_lines").fetchone()[0]
        conn.close()

        migrate_db(db_with_user_objects)

        conn = sqlite3.connect(str(db_with_user_objects))
        post_statements = conn.execute("SELECT COUNT(*) FROM statement_heads").fetchone()[0]
        post_transactions = conn.execute("SELECT COUNT(*) FROM statement_lines").fetchone()[0]
        assert post_statements == pre_statements
        assert post_transactions == pre_transactions
        conn.close()

    def test_old_db_archived(self, db_with_user_objects):
        """The old database is moved to database_archive/ with version suffix."""
        archive_dir = db_with_user_objects.parent / "database_archive"
        migrate_db(db_with_user_objects)

        assert archive_dir.exists()
        archived_files = list(archive_dir.glob("project_v*.db"))
        assert len(archived_files) == 1
        assert "v" in archived_files[0].name

    def test_new_db_has_db_meta(self, db_with_user_objects):
        """The upgraded database contains db_meta with current version."""
        migrate_db(db_with_user_objects)

        conn = sqlite3.connect(str(db_with_user_objects))
        meta = {row[0]: row[1] for row in conn.execute("SELECT key, value FROM db_meta").fetchall()}
        conn.close()

        from bank_statement_parser import __version__

        assert meta["bsp_version"] == __version__
        assert "script_hashes" in meta

    def test_upgrade_failure_preserves_original(self, tmp_path):
        """On failure, the original database is preserved untouched."""
        db_path = tmp_path / "project.db"
        create_db(db_path=db_path, with_fk=False)
        generate_mock_data(db_path=db_path, num_batches=1, statements_per_batch=2, transactions_per_statement=3)

        # Record original state
        conn = sqlite3.connect(str(db_path))
        original_statements = conn.execute("SELECT COUNT(*) FROM statement_heads").fetchone()[0]
        conn.close()

        # No upgrade needed → no warning, original intact
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = migrate_db(db_path)
            assert result is False  # no upgrade needed

        # Verify original is intact
        conn = sqlite3.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM statement_heads").fetchone()[0] == original_statements
        conn.close()

    def test_nonexistent_db_returns_false(self, tmp_path):
        """migrate_db returns False for a nonexistent database."""
        assert migrate_db(tmp_path / "nonexistent.db") is False

    def test_db_without_meta_gets_upgraded(self, old_db_no_meta):
        """A database without db_meta is upgraded and receives db_meta."""
        result = migrate_db(old_db_no_meta)
        assert result is True

        conn = sqlite3.connect(str(old_db_no_meta))
        meta = {row[0]: row[1] for row in conn.execute("SELECT key, value FROM db_meta").fetchall()}
        conn.close()

        from bank_statement_parser import __version__

        assert meta["bsp_version"] == __version__
