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

# Column renames across BSP versions.  Keys are table names, values are
# dicts mapping old column name → new column name.  During migration, a
# renamed column's data is preserved under the new name instead of being
# silently dropped.
_COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "statement_lines": {
        "STD_PAYMENTS_IN": "STD_TRANSACTION_PAYMENTS_IN",
        "STD_PAYMENTS_OUT": "STD_TRANSACTION_PAYMENTS_OUT",
    },
}


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
        from bank_statement_parser.modules.paths import DATA

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

        from bank_statement_parser import __version__

        stored_version = meta.get("bsp_version", "")
        if stored_version != __version__:
            return True

        stored_hashes = json.loads(meta.get("script_hashes", "{}"))
        current_hashes = fingerprint_data_scripts()
        return stored_hashes != current_hashes
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


def _resolve_column_map(
    old_conn: sqlite3.Connection,
    new_conn: sqlite3.Connection,
    table: str,
) -> tuple[list[str], list[str], list[str]]:
    """Build a column mapping from old schema to new schema for *table*.

    Returns a 3-tuple of equal-length lists:

    * ``old_select`` — column names to ``SELECT`` from the old table,
    * ``new_insert`` — corresponding names to ``INSERT INTO`` the new table
      (after applying any renames from :data:`_COLUMN_RENAMES`),
    * ``dropped`` — old columns that have no counterpart in the new schema
      (these are dropped with a :class:`UserWarning`).
    """
    old_pragma = old_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    new_pragma = new_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    old_cols = [r[1] for r in old_pragma]
    new_cols = {r[1] for r in new_pragma}
    renames = _COLUMN_RENAMES.get(table, {})

    old_select: list[str] = []
    new_insert: list[str] = []
    dropped: list[str] = []

    for col in old_cols:
        if col in renames:
            new_name = renames[col]
            if new_name in new_cols:
                old_select.append(col)
                new_insert.append(new_name)
            else:
                dropped.append(col)
        elif col in new_cols:
            old_select.append(col)
            new_insert.append(col)
        else:
            dropped.append(col)

    return old_select, new_insert, dropped


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
        table_name = table_info["name"]
        new_conn.execute(table_info["sql"])
        old_select, new_insert, dropped = _resolve_column_map(old_conn, new_conn, table_name)
        if dropped:
            warnings.warn(
                f"[upgrade] dropping columns from user table {table_name} not in new schema: {', '.join(dropped)}",
                UserWarning,
                stacklevel=2,
            )
        if not new_insert:
            continue
        select_str = ", ".join([f'"{c}"' for c in old_select])
        rows = old_conn.execute(f'SELECT {select_str} FROM "{table_name}"').fetchall()
        if not rows:
            continue
        placeholders = ", ".join(["?"] * len(new_insert))
        col_str = ", ".join([f'"{c}"' for c in new_insert])
        new_conn.executemany(f'INSERT OR REPLACE INTO "{table_name}" ({col_str}) VALUES ({placeholders})', rows)

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
    from bank_statement_parser import __version__

    conn = sqlite3.connect(str(db_path))
    meta = _read_db_meta(conn)
    old_version = meta.get("bsp_version", "unknown") if meta else "unknown"
    conn.close()

    print(f"[upgrade] database upgrade required (v{old_version} → v{__version__})")

    # 1. Create fresh temp DB with current schema
    temp_db_path = db_path.with_name(f"{db_path.stem}_upgrade{db_path.suffix}")
    try:
        from bank_statement_parser.data.create_project_db import main as create_db

        create_db(db_path=temp_db_path, with_fk=False)
    except Exception as exc:  # noqa: BLE001
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to create upgrade database: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 2. Copy raw source data
    old_conn = None
    new_conn = None
    try:
        old_conn = sqlite3.connect(str(db_path))
        new_conn = sqlite3.connect(str(temp_db_path))

        for table in _RAW_TABLES:
            try:
                old_cols_for_select, new_cols_for_insert, dropped = _resolve_column_map(old_conn, new_conn, table)
            except sqlite3.OperationalError:
                continue
            if dropped:
                warnings.warn(
                    f"[upgrade] dropping columns from {table} not in new schema: {', '.join(dropped)}",
                    UserWarning,
                    stacklevel=2,
                )
            if not new_cols_for_insert:
                continue
            select_str = ", ".join([f'"{c}"' for c in old_cols_for_select])
            rows = old_conn.execute(f'SELECT {select_str} FROM "{table}"').fetchall()
            if not rows:
                continue
            placeholders = ", ".join(["?"] * len(new_cols_for_insert))
            col_str = ", ".join([f'"{c}"' for c in new_cols_for_insert])
            new_conn.executemany(f'INSERT OR REPLACE INTO "{table}" ({col_str}) VALUES ({placeholders})', rows)

        new_conn.commit()

        # 3. Detect and copy user objects
        user_objects = _get_user_objects(old_conn)
        _copy_user_objects(old_conn, new_conn, user_objects)
        new_conn.commit()
    except Exception as exc:  # noqa: BLE001
        if old_conn is not None:
            old_conn.close()
        if new_conn is not None:
            new_conn.close()
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to migrate data: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    old_conn.close()
    new_conn.close()

    # 4. Archive old DB
    try:
        archive_dir = db_path.parent / "database_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"project_v{old_version}{db_path.suffix}"
        if archive_path.exists():
            from datetime import datetime

            ts = datetime.now().strftime("%Y%m%d%H%M%S")  # noqa: DTZ005
            archive_path = archive_dir / f"project_v{old_version}_{ts}{db_path.suffix}"
        db_path.rename(archive_path)
        # Move WAL/SHM sidecar files alongside the archived DB so stale
        # sidecars don't corrupt the promoted replacement.  Each move is
        # best-effort: failure here should not abort the migration.
        for suffix in ("-wal", "-shm"):
            try:
                sidecar = db_path.parent / f"{db_path.name}{suffix}"
                if sidecar.exists():
                    sidecar.rename(archive_dir / f"{archive_path.name}{suffix}")
            except OSError:
                pass
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
        _cleanup_temp(temp_db_path)
        warnings.warn(f"[upgrade] failed to replace database: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)
        return False

    # 6. Rebuild mart tables from raw data
    try:
        from bank_statement_parser.data.build_datamart import build_datamart

        print("[upgrade] rebuilding data mart tables …")
        build_datamart(db_path=db_path, verbose=False)
    except Exception as exc:  # noqa: BLE001
        warnings.warn(f"[upgrade] failed to rebuild data mart: {type(exc).__name__}: {exc}", UserWarning, stacklevel=2)

    # 7. Report user objects preserved
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
