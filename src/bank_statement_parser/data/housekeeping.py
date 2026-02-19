import argparse
import sqlite3
from pathlib import Path


class Housekeeping:
    FK_RELATIONSHIPS = [
        ("checks_and_balances", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
        ("checks_and_balances", "ID_BATCH", "batch_heads", "ID_BATCH"),
        ("statement_heads", "ID_BATCH", "batch_heads", "ID_BATCH"),
        ("statement_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
        ("batch_lines", "ID_BATCH", "batch_heads", "ID_BATCH"),
        ("batch_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
    ]

    DELETE_ORDER = [
        "checks_and_balances",
        "statement_lines",
        "batch_lines",
        "statement_heads",
        "batch_heads",
    ]

    # All table names and column names that are permitted in dynamically-constructed SQL.
    # Any identifier not in this set will raise ValueError, preventing SQL injection.
    _ALLOWED_TABLES: frozenset[str] = frozenset(
        [rel[0] for rel in [
            ("checks_and_balances", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
            ("checks_and_balances", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("statement_heads", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("statement_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
            ("batch_lines", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("batch_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
        ]]
        + [rel[2] for rel in [
            ("checks_and_balances", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
            ("checks_and_balances", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("statement_heads", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("statement_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
            ("batch_lines", "ID_BATCH", "batch_heads", "ID_BATCH"),
            ("batch_lines", "ID_STATEMENT", "statement_heads", "ID_STATEMENT"),
        ]]
    )
    _ALLOWED_COLUMNS: frozenset[str] = frozenset(["ID_STATEMENT", "ID_BATCH"])

    def __init__(self, db_path: Path):
        self.db_path = db_path

    @staticmethod
    def _validate_identifier(value: str, allowed: frozenset[str], label: str) -> None:
        """Raise ValueError if *value* is not in the *allowed* whitelist."""
        if value not in allowed:
            raise ValueError(f"Unsafe SQL identifier for {label}: {value!r}")

    def find_orphans(self, conn: sqlite3.Connection, table: str, fk_column: str, parent_table: str, parent_key: str) -> list:
        self._validate_identifier(table, self._ALLOWED_TABLES, "table")
        self._validate_identifier(fk_column, self._ALLOWED_COLUMNS, "fk_column")
        self._validate_identifier(parent_table, self._ALLOWED_TABLES, "parent_table")
        self._validate_identifier(parent_key, self._ALLOWED_COLUMNS, "parent_key")
        query = f"""
            SELECT t.{fk_column}
            FROM {table} t
            LEFT JOIN {parent_table} p ON t.{fk_column} = p.{parent_key}
            WHERE t.{fk_column} IS NOT NULL AND p.{parent_key} IS NULL
        """
        cursor = conn.execute(query)
        return [row[0] for row in cursor.fetchall()]

    def delete_orphans_cascade(self, conn: sqlite3.Connection, table: str, fk_column: str, parent_key: str, orphan_ids: list) -> int:
        self._validate_identifier(table, self._ALLOWED_TABLES, "table")
        self._validate_identifier(fk_column, self._ALLOWED_COLUMNS, "fk_column")
        self._validate_identifier(parent_key, self._ALLOWED_COLUMNS, "parent_key")
        if not orphan_ids:
            return 0

        placeholders = ", ".join(["?" for _ in orphan_ids])
        delete_query = f"""
            DELETE FROM {table}
            WHERE {fk_column} IN ({placeholders})
        """
        cursor = conn.execute(delete_query, orphan_ids)
        return cursor.rowcount

    def get_children_for_parent(self, conn: sqlite3.Connection, table: str, parent_key: str, parent_id: str) -> dict:
        self._validate_identifier(table, self._ALLOWED_TABLES, "table")
        self._validate_identifier(parent_key, self._ALLOWED_COLUMNS, "parent_key")
        children = {}
        for child_table, fk_column, _, _ in self.FK_RELATIONSHIPS:
            if child_table == table:
                query = f"""
                    SELECT {fk_column}
                    FROM {child_table}
                    WHERE {parent_key} = ?
                """
                cursor = conn.execute(query, (parent_id,))
                children[child_table] = [row[0] for row in cursor.fetchall()]
        return children

    def delete_orphans_cascade_for_parent(self, conn: sqlite3.Connection, table: str, parent_key: str, parent_id: str) -> int:
        self._validate_identifier(table, self._ALLOWED_TABLES, "table")
        self._validate_identifier(parent_key, self._ALLOWED_COLUMNS, "parent_key")
        deleted_count = 0

        for child_table, fk_column, _, _ in self.FK_RELATIONSHIPS:
            if child_table == table:
                query = f"""
                    SELECT DISTINCT {fk_column}
                    FROM {child_table}
                    WHERE {parent_key} = ?
                """
                cursor = conn.execute(query, (parent_id,))
                child_ids = [row[0] for row in cursor.fetchall()]

                for child_id in child_ids:
                    deleted_count += self.delete_orphans_cascade_for_parent(conn, child_table, fk_column, child_id)

        self._validate_identifier(table, self._ALLOWED_TABLES, "table")
        self._validate_identifier(parent_key, self._ALLOWED_COLUMNS, "parent_key")
        delete_query = f"DELETE FROM {table} WHERE {parent_key} = ?"
        cursor = conn.execute(delete_query, (parent_id,))
        deleted_count += cursor.rowcount

        return deleted_count

    def check_integrity(self) -> dict[str, dict]:
        conn = sqlite3.connect(self.db_path)
        results = {}

        for table, fk_column, parent_table, parent_key in self.FK_RELATIONSHIPS:
            orphans = self.find_orphans(conn, table, fk_column, parent_table, parent_key)
            results[f"{table}.{fk_column}"] = {
                "parent_table": parent_table,
                "orphan_count": len(orphans),
                "orphan_ids": orphans[:10],
            }

        conn.close()
        return results

    def cleanup(self, delete: bool = False) -> dict[str, dict]:
        results = self.check_integrity()

        total_orphans = sum(r["orphan_count"] for r in results.values())
        if total_orphans == 0:
            print("No orphans found. Database integrity is good.")
            return results

        print(f"Found {total_orphans} orphaned records:")
        for relationship, info in results.items():
            if info["orphan_count"] > 0:
                print(f"  {relationship}: {info['orphan_count']} orphans")

        if not delete:
            print("\nRun with --delete to remove orphaned records.")
            return results

        conn = sqlite3.connect(self.db_path)
        total_deleted = 0

        for parent_table in self.DELETE_ORDER:
            for table, fk_column, parent_table_check, parent_key in self.FK_RELATIONSHIPS:
                if parent_table_check == parent_table:
                    orphans = self.find_orphans(conn, table, fk_column, parent_table_check, parent_key)
                    if orphans:
                        deleted = self.delete_orphans_cascade(conn, table, fk_column, parent_key, orphans)
                        total_deleted += deleted
                        print(f"Deleted {deleted} orphans from {table}.{fk_column}")

        conn.commit()
        conn.close()

        print(f"\nTotal deleted: {total_deleted}")
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database integrity housekeeping")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete orphaned records (default is to only report)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).parent.joinpath("project.db"),
        help="Path to database file",
    )
    args = parser.parse_args()

    hk = Housekeeping(args.db)
    hk.cleanup(delete=args.delete)
