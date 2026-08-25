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
Database migration module — version tracking and automatic upgrade.

Provides database-agnostic functions for detecting schema drift between
the installed BSP version and an existing user database, and for
migrating user data to a fresh database when required.

This module is designed to be reusable by external projects (e.g.
*openstan*) that share the same raw SQLite schema.
"""

import hashlib
import json
import sqlite3
import warnings
from pathlib import Path

# ---------------------------------------------------------------------------
# BSP-owned object names — anything NOT in these sets is a user object
# and will be preserved during migration.
# ---------------------------------------------------------------------------

_BSP_OWNED_TABLES: frozenset[str] = frozenset(
    {
        "batch_heads",
        "batch_lines",
        "statement_heads",
        "statement_lines",
        "checks_and_balances",
        "exchange_rates",
        "DimDate",
        "DimAccount",
        "DimStatement",
        "FactTransaction",
        "FactBalance",
        "db_meta",
    }
)

_BSP_OWNED_VIEWS: frozenset[str] = frozenset(
    {
        "GapReport",
        "FlatTransaction",
        "DimStatementBatch",
        "FactTransactionBatch",
        "DimAccountBatch",
        "DimDateBatch",
        "FactBalanceBatch",
        "FlatTransactionBatch",
    }
)

# Raw source tables whose data is migrated.  Mart tables (DimDate, DimAccount,
# etc.) are rebuilt by build_datamart() and are never copied directly.
_RAW_TABLES: tuple[str, ...] = (
    "batch_heads",
    "batch_lines",
    "statement_heads",
    "statement_lines",
    "checks_and_balances",
    "exchange_rates",
)

_DDL_DB_META = """
CREATE TABLE IF NOT EXISTS db_meta (
    "key"   TEXT NOT NULL PRIMARY KEY,
    "value" TEXT NOT NULL
)
"""


# ---------------------------------------------------------------------------
# Script fingerprinting
# ---------------------------------------------------------------------------


def fingerprint_data_scripts(data_dir: Path | None = None) -> dict[str, str]:
    """Compute SHA-512 hex digests for all Python files in the ``data/`` package.

    Args:
        data_dir: Directory containing the data scripts.  When ``None``,
            resolves to the ``data/`` directory inside the installed
            ``bank_statement_parser`` package.

    Returns:
        A dict mapping ``filename.py`` to its SHA-512 hex digest.
    """
    if data_dir is None:
        from bank_statement_parser.modules.paths import DATA  # noqa: PLC0415

        data_dir = DATA

    hashes: dict[str, str] = {}
    for py_file in sorted(data_dir.glob("*.py")):
        if py_file.name.startswith("__pycache__"):
            continue
        digest = hashlib.sha512(py_file.read_bytes(), usedforsecurity=False).hexdigest()
        hashes[py_file.name] = digest
    return hashes


# ---------------------------------------------------------------------------
# db_meta read / write
# ---------------------------------------------------------------------------


def _read_db_meta(conn: sqlite3.Connection) -> dict[str, str] | None:
    """Read all key-value pairs from the ``db_meta`` table.

    Returns ``None`` if the ``db_meta`` table does not exist (i.e. the
    database was created by an older BSP version).
    """
    try:
        rows = conn.execute("SELECT key, value FROM db_meta").fetchall()
    except sqlite3.OperationalError:
        return None
    return {row[0]: row[1] for row in rows}


def _write_db_meta(conn: sqlite3.Connection, version: str, hashes: dict[str, str]) -> None:
    """Write or update the ``bsp_version`` and ``script_hashes`` rows in ``db_meta``.

    Args:
        conn: Open SQLite connection with write access.
        version: The BSP version string (PEP 440).
        hashes: Output of :func:`fingerprint_data_scripts`.
    """
    conn.execute(_DDL_DB_META)
    conn.execute("INSERT OR REPLACE INTO db_meta (key, value) VALUES ('bsp_version', ?)", (version,))
    conn.execute("INSERT OR REPLACE INTO db_meta (key, value) VALUES ('script_hashes', ?)", (json.dumps(hashes),))
    conn.commit()


# ---------------------------------------------------------------------------
# Upgrade detection
# ---------------------------------------------------------------------------


def needs_upgrade(db_path: Path) -> bool:
    """Check whether *db_path* requires a migration to the current BSP version.

    The database needs an upgrade when:

    * The ``db_meta`` table is absent (database created by an older BSP), or
    * The stored ``bsp_version`` differs from the installed BSP version, or
    * The stored ``script_hashes`` differ from the current data scripts.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        ``True`` if the database needs to be upgraded, ``False`` otherwise.
    """
    if not db_path.exists():
        return False

    conn = sqlite3.connect(str(db_path))
    try:
        meta = _read_db_meta(conn)
        if meta is None:
            return True

        from bank_statement_parser import __version__  # noqa: PLC0415

        stored_version = meta.get("bsp_version", "")
        if stored_version != __version__:
            return True

        stored_hashes = json.loads(meta.get("script_hashes", "{}"))
        current_hashes = fingerprint_data_scripts()
        if stored_hashes != current_hashes:
            return True

        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# User object detection
# ---------------------------------------------------------------------------


def _get_user_objects(conn: sqlite3.Connection) -> dict[str, list[dict[str, str]]]:
    """Detect database objects that are not part of the BSP schema.

    Returns a dict with keys ``"tables"``, ``"views"``, ``"triggers"``,
    and ``"indexes"``, each mapping to a list of dicts with ``"name"``
    and ``"sql"`` entries.
    """
    user_objects: dict[str, list[dict[str, str]]] = {"tables": [], "views": [], "triggers": [], "indexes": []}

    rows = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL").fetchall()

    for obj_type, name, tbl_name, sql in rows:
        if obj_type == "table" and name not in _BSP_OWNED_TABLES:
            user_objects["tables"].append({"name": name, "sql": sql})
        elif obj_type == "view" and name not in _BSP_OWNED_VIEWS:
            user_objects["views"].append({"name": name, "sql": sql})
        elif obj_type == "trigger":
            user_objects["triggers"].append({"name": name, "sql": sql})
        elif obj_type == "index":
            # An index is BSP-owned if it sits on a BSP-owned table and
            # follows the BSP naming convention (starts with ``idx_``).
            if tbl_name in _BSP_OWNED_TABLES and name.startswith("idx_"):
                continue
            user_objects["indexes"].append({"name": name, "sql": sql})

    return user_objects


def _copy_user_objects(old_conn: sqlite3.Connection, new_conn: sqlite3.Connection, user_objects: dict[str, list[dict[str, str]]]) -> None:
    """Copy user-added objects from *old_conn* to *new_conn*.

    For user tables, both the schema and data are copied.  For views,
    triggers, and indexes, only the SQL definition is replayed.

    Args:
        old_conn: Read connection to the existing (old) database.
        new_conn: Write connection to the new (upgraded) database.
        user_objects: Output of :func:`_get_user_objects`.
    """
    for table_info in user_objects["tables"]:
        new_conn.execute(table_info["sql"])
        rows = old_conn.execute(f'SELECT * FROM "{table_info["name"]}"').fetchall()  # noqa: S608
        if rows:
            cols = [desc[0] for desc in old_conn.execute(f'SELECT * FROM "{table_info["name"]}" LIMIT 0').fetchall() or []]  # noqa: S608
            # Re-fetch column names via PRAGMA for robustness
            pragma_rows = old_conn.execute(f'PRAGMA table_info("{table_info["name"]}")').fetchall()  # noqa: S608
            cols = [r[1] for r in pragma_rows]
            placeholders = ", ".join(["?"] * len(cols))
            col_str = ", ".join([f'"{c}"' for c in cols])
            new_conn.executemany(f'INSERT OR REPLACE INTO "{table_info["name"]}" ({col_str}) VALUES ({placeholders})', rows)  # noqa: S608

    for view_info in user_objects["views"]:
        new_conn.execute(view_info["sql"])

    for trigger_info in user_objects["triggers"]:
        new_conn.execute(trigger_info["sql"])

    for index_info in user_objects["indexes"]:
        new_conn.execute(index_info["sql"])


# ---------------------------------------------------------------------------
# Core migration
# ---------------------------------------------------------------------------


def migrate_db(db_path: Path) -> bool:
    """Upgrade *db_path* to the current BSP version if needed.

    The upgrade flow:

    1. Check if the database needs an upgrade via :func:`needs_upgrade`.
    2. Create a fresh database with the current BSP schema.
    3. Copy all raw source data from the old database.
    4. Detect and copy any user-added tables, views, triggers, and indexes.
    5. Archive the old database to ``database_archive/`` with its version suffix.
    6. Replace the old database with the upgraded version.

    If any step fails, the original database is left untouched and a
    :exc:`UserWarning` is emitted.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        ``True`` if an upgrade was performed, ``False`` if no upgrade was
        needed or the upgrade failed.
    """
    if not db_path.exists():
        return False

    if not needs_upgrade(db_path):
        return False

    # Resolve current version for the archive name
    from bank_statement_parser import __version__  # noqa: PLC0415

    conn = sqlite3.connect(str(db_path))
    meta = _read_db_meta(conn)
    old_version = meta.get("bsp_version", "unknown") if meta else "unknown"
    conn.close()

    print(f"[upgrade] database upgrade required (v{old_version} → v{__version__})")

    # 1. Create fresh temp DB with current schema
    temp_db_path = db_path.with_name(f"{db_path.stem}_upgrade{db_path.suffix}")
    try:
        from bank_statement_parser.data.create_project_db import main as create_db  # noqa: PLC0415

        create_db(db_path=temp_db_path, with_fk=False)
    except Exception as exc:  # noqa: BLE001
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to create upgrade database: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 2. Copy raw source data
    try:
        old_conn = sqlite3.connect(str(db_path))
        new_conn = sqlite3.connect(str(temp_db_path))

        for table in _RAW_TABLES:
            try:
                rows = old_conn.execute(f'SELECT * FROM "{table}"').fetchall()  # noqa: S608
            except sqlite3.OperationalError:
                continue
            if not rows:
                continue
            pragma_rows = old_conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # noqa: S608
            cols = [r[1] for r in pragma_rows]
            placeholders = ", ".join(["?"] * len(cols))
            col_str = ", ".join([f'"{c}"' for c in cols])
            new_conn.executemany(f'INSERT OR REPLACE INTO "{table}" ({col_str}) VALUES ({placeholders})', rows)  # noqa: S608

        new_conn.commit()

        # 3. Detect and copy user objects
        user_objects = _get_user_objects(old_conn)
        _copy_user_objects(old_conn, new_conn, user_objects)
        new_conn.commit()

        old_conn.close()
        new_conn.close()
    except Exception as exc:  # noqa: BLE001
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to migrate data: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 4. Archive old DB
    try:
        archive_dir = db_path.parent / "database_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"project_v{old_version}{db_path.suffix}"
        db_path.rename(archive_path)
        print(f"[upgrade] archived old database to {archive_path}")
    except Exception as exc:  # noqa: BLE001
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to archive old database: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 5. Promote temp DB to canonical name
    try:
        temp_db_path.rename(db_path)
    except Exception as exc:  # noqa: BLE001
        # Attempt to restore from archive
        try:
            archive_path.rename(db_path)
        except OSError:
            pass
        warnings.warn(f"[upgrade] failed to replace database: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 6. Report user objects preserved
    n_tables = len(user_objects.get("tables", []))
    n_views = len(user_objects.get("views", []))
    n_triggers = len(user_objects.get("triggers", []))
    n_indexes = len(user_objects.get("indexes", []))
    total_user = n_tables + n_views + n_triggers + n_indexes
    if total_user > 0:
        print(f"[upgrade] user objects preserved: {n_tables} table(s), {n_views} view(s), {n_triggers} trigger(s), {n_indexes} index(es)")

    print(f"[upgrade] database upgraded successfully to v{__version__}")
    return True


def _cleanup_temp(temp_db_path: Path) -> None:
    """Remove a partial temp database and its WAL/SHM sidecar files."""
    for suffix in ("", "-wal", "-shm"):
        p = temp_db_path.with_name(temp_db_path.name + suffix)
        if p.exists():
            p.unlink(missing_ok=True)
