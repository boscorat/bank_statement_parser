"""
Database persistence module.

Provides standalone functions for writing processed bank statement data
to a SQLite database.
"""

import sqlite3
from datetime import date, datetime
from pathlib import Path
from time import time

import polars as pl

from bank_statement_parser.data.build_datamart import build_datamart, _ensure_mart_structure
from bank_statement_parser.modules.data import PdfResult, Success
from bank_statement_parser.modules.errors import ProjectDatabaseMissing
from bank_statement_parser.modules.paths import ProjectPaths

# Python 3.12+ deprecates the built-in date/datetime adapters for sqlite3.
# Register explicit ISO-format adapters so that datetime.date and
# datetime.datetime values are stored as TEXT without triggering warnings.
sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())


def _require_db(db_path: Path) -> None:
    """Raise ProjectDatabaseMissing if the database file does not exist.

    Args:
        db_path: Expected path to the SQLite database file.

    Raises:
        ProjectDatabaseMissing: If *db_path* does not exist.
    """
    if not db_path.exists():
        raise ProjectDatabaseMissing(db_path)


# Whitelists for migration identifier validation — prevents any future
# regression if _MIGRATIONS entries are ever made dynamic.
_ALLOWED_MIGRATION_TABLES: frozenset[str] = frozenset({"batch_lines", "batch_heads", "statement_heads"})
_ALLOWED_MIGRATION_COLUMNS: frozenset[str] = frozenset({"ERROR_DATA", "ID_SESSION", "ID_USER", "STD_REVIEW_COUNT", "STD_CURRENCY"})
_ALLOWED_MIGRATION_TYPES: frozenset[str] = frozenset({"INTEGER", "TEXT", "REAL"})


def _validate_migration_identifier(value: str, allowed: frozenset[str], label: str) -> None:
    """Raise ValueError if *value* is not in *allowed*.

    Args:
        value: The identifier to validate.
        allowed: Frozenset of permitted values.
        label: Human-readable label for the error message.

    Raises:
        ValueError: If *value* is not in *allowed*.
    """
    if value not in allowed:
        raise ValueError(f"Unexpected migration {label} {value!r}; not in allowed set.")


# Columns that may have been added after the initial schema was created.
# Each entry is (table_name, column_name, column_type, default_value).
_MIGRATIONS: list[tuple[str, str, str, str]] = [
    ("batch_lines", "ERROR_DATA", "INTEGER", "0"),
    ("batch_heads", "ID_SESSION", "TEXT", "''"),
    ("batch_heads", "ID_USER", "TEXT", "''"),
    ("batch_heads", "STD_REVIEW_COUNT", "INTEGER", "0"),
    ("statement_heads", "STD_CURRENCY", "TEXT", "'GBP'"),
]

# Tables that may be absent from databases created before they were introduced.
# Each entry is (table_name, create_sql).  Only created when missing — the table
# is never dropped — so existing data is preserved.
_TABLE_MIGRATIONS: list[tuple[str, str]] = [
    (
        "exchange_rates",
        """
        CREATE TABLE exchange_rates (
            "id_date"   TEXT NOT NULL,
            "currency"  TEXT NOT NULL,
            "rate_USD"  REAL NOT NULL,
            PRIMARY KEY (id_date, currency)
        )
        """,
    ),
]

# Views that may be absent from databases created before they were introduced.
# Each entry is (view_name, create_sql).  Only created when missing — never
# dropped — so any user-customised views are preserved.
_VIEW_MIGRATIONS: list[tuple[str, str]] = [
    (
        "GapReport",
        """
        CREATE VIEW GapReport AS
        WITH ordered_statements AS (
            SELECT
                STD_ACCOUNT          AS account_type,
                STD_ACCOUNT_NUMBER   AS account_number,
                STD_ACCOUNT_HOLDER   AS account_holder,
                STD_STATEMENT_DATE   AS statement_date,
                CAST(STD_OPENING_BALANCE AS REAL) AS opening_balance,
                CAST(STD_CLOSING_BALANCE AS REAL) AS closing_balance,
                ROW_NUMBER() OVER (
                    PARTITION BY STD_ACCOUNT, STD_ACCOUNT_NUMBER
                    ORDER BY STD_ACCOUNT, STD_ACCOUNT_NUMBER, STD_STATEMENT_DATE
                ) AS row_num
            FROM statement_heads
        ),
        with_prev AS (
            SELECT
                account_type,
                account_number,
                account_holder,
                statement_date,
                opening_balance,
                closing_balance,
                LAG(closing_balance) OVER (
                    PARTITION BY account_type, account_number
                    ORDER BY statement_date
                ) AS prev_closing_balance,
                CASE
                    WHEN account_type || account_number =
                         LAG(account_type || account_number) OVER (
                             PARTITION BY account_type, account_number
                             ORDER BY statement_date
                         )
                    THEN 0 ELSE 1
                END AS account_change
            FROM ordered_statements
        )
        SELECT
            account_type,
            account_number,
            account_holder,
            statement_date,
            opening_balance,
            closing_balance,
            CASE
                WHEN account_change = 1                          THEN ''
                WHEN opening_balance = prev_closing_balance      THEN ''
                ELSE 'GAP'
            END AS gap_flag
        FROM with_prev
        """,
    ),
    (
        "FlatTransaction",
        """
        CREATE VIEW FlatTransaction AS
        SELECT
            ft.id_date          AS transaction_date,
            ds.statement_date,
            ds.filename,
            da.company,
            da.account_type,
            da.account_number,
            da.sortcode,
            da.account_holder,
            ft.transaction_number,
            ft.transaction_credit_or_debit  AS CD,
            ft.transaction_type             AS type,
            ft.transaction_desc,
            SUBSTR(ft.transaction_desc, 1, 25) AS short_desc,
            ft.value_in,
            ft.value_out,
            ft.value
        FROM FactTransaction ft
        INNER JOIN DimStatement ds ON ft.statement_id = ds.statement_id
        INNER JOIN DimAccount   da ON ft.account_id   = da.account_id
        """,
    ),
    (
        "DimStatementBatch",
        """
        CREATE VIEW DimStatementBatch AS
        SELECT DISTINCT
            bl.ID_BATCH          AS batch_id,
            ds.statement_id,
            ds.id_statement,
            ds.account_id,
            ds.company,
            ds.account_type,
            ds.account_number,
            ds.sortcode,
            ds.account_holder,
            ds.statement_date,
            ds.opening_balance,
            ds.payments_in,
            ds.payments_out,
            ds.closing_balance,
            ds.statement_type,
            ds.filename,
            ds.batch_time
        FROM DimStatement ds
        INNER JOIN batch_lines bl ON ds.id_statement = bl.ID_STATEMENT
        """,
    ),
    (
        "FactTransactionBatch",
        """
        CREATE VIEW FactTransactionBatch AS
        SELECT
            dsb.batch_id,
            ft.transaction_id,
            ft.id_transaction,
            ft.statement_id,
            ft.account_id,
            ft.time_id,
            ft.id_date,
            ft.id_account,
            ft.id_statement,
            ft.transaction_number,
            ft.transaction_credit_or_debit,
            ft.transaction_type,
            ft.transaction_type_cd,
            ft.transaction_desc,
            ft.opening_balance,
            ft.value_in,
            ft.value_out,
            ft.value
        FROM FactTransaction ft
        INNER JOIN DimStatementBatch dsb ON ft.statement_id = dsb.statement_id
        """,
    ),
    (
        "DimAccountBatch",
        """
        CREATE VIEW DimAccountBatch AS
        SELECT DISTINCT
            dsb.batch_id,
            da.account_id,
            da.id_account,
            da.company,
            da.account_type,
            da.account_number,
            da.sortcode,
            da.account_holder
        FROM DimAccount da
        INNER JOIN DimStatementBatch dsb ON da.account_id = dsb.account_id
        """,
    ),
    (
        "DimTimeBatch",
        """
        CREATE VIEW DimTimeBatch AS
        SELECT
            dt.*,
            r.batch_id
        FROM DimTime dt
        INNER JOIN (
            SELECT
                ftb.batch_id,
                MIN(ftb.id_date)        AS min_date,
                MAX(dsb.statement_date) AS max_date
            FROM FactTransactionBatch ftb
            INNER JOIN DimStatementBatch dsb
                   ON ftb.statement_id = dsb.statement_id
                  AND ftb.batch_id     = dsb.batch_id
            GROUP BY ftb.batch_id
        ) r ON dt.id_date BETWEEN r.min_date AND r.max_date
        """,
    ),
    (
        "FactBalanceBatch",
        """
        CREATE VIEW FactBalanceBatch AS
        SELECT
            dab.batch_id,
            fb.time_id,
            fb.account_id,
            fb.id_date,
            fb.id_account,
            fb.opening_balance,
            fb.closing_balance,
            fb.movement,
            fb.outside_date
        FROM FactBalance fb
        INNER JOIN DimAccountBatch dab ON fb.account_id = dab.account_id
        INNER JOIN DimTimeBatch    dtb ON fb.time_id    = dtb.time_id
                                     AND dab.batch_id   = dtb.batch_id
        """,
    ),
    (
        "FlatTransactionBatch",
        """
        CREATE VIEW FlatTransactionBatch AS
        SELECT
            ftb.batch_id,
            ftb.id_date             AS transaction_date,
            dsb.statement_date,
            dsb.filename,
            dab.company,
            dab.account_type,
            dab.account_number,
            dab.sortcode,
            dab.account_holder,
            ftb.transaction_number,
            ftb.transaction_credit_or_debit  AS CD,
            ftb.transaction_type             AS type,
            ftb.transaction_desc,
            SUBSTR(ftb.transaction_desc, 1, 25) AS short_desc,
            ftb.value_in,
            ftb.value_out,
            ftb.value
        FROM FactTransactionBatch ftb
        INNER JOIN DimStatementBatch dsb ON ftb.statement_id = dsb.statement_id
                                        AND ftb.batch_id     = dsb.batch_id
        INNER JOIN DimAccountBatch   dab ON ftb.account_id   = dab.account_id
                                        AND ftb.batch_id     = dab.batch_id
        """,
    ),
]


def _migrate_db(conn: sqlite3.Connection) -> None:
    """Apply forward-only schema migrations for missing columns, tables, mart tables, and views.

    Four classes of migration are applied in order:

    1. **Column migrations** — ``ALTER TABLE … ADD COLUMN`` for any column in
       :data:`_MIGRATIONS` not yet present in its table.
    2. **Table migrations** — ``CREATE TABLE`` for any table in
       :data:`_TABLE_MIGRATIONS` not yet present.  Tables are created empty;
       existing data is never dropped.
    3. **Mart table bootstrap** — creates the five mart tables (``DimTime``,
       ``DimAccount``, ``DimStatement``, ``FactTransaction``, ``FactBalance``)
       and their indexes if absent, via :func:`~bank_statement_parser.data.build_datamart._ensure_mart_structure`.
       Tables are created empty; :func:`~bank_statement_parser.data.build_datamart.build_datamart`
       populates them on every ``update_db`` call using the drop-and-recreate
       strategy, which preserves bulk-insert performance.
    4. **View migrations** — ``CREATE VIEW`` for any view in :data:`_VIEW_MIGRATIONS`
       not yet present.  Existing views (including any user-customised ones) are
       never dropped.

    This function is idempotent — running it against an already-migrated
    database is a no-op.

    Args:
        conn: Open SQLite connection with write access.
    """
    for table, column, col_type, default in _MIGRATIONS:
        _validate_migration_identifier(table, _ALLOWED_MIGRATION_TABLES, "table")
        _validate_migration_identifier(column, _ALLOWED_MIGRATION_COLUMNS, "column")
        _validate_migration_identifier(col_type, _ALLOWED_MIGRATION_TYPES, "column type")
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}  # noqa: S608
        if column not in existing:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {col_type} DEFAULT {default}')  # noqa: S608
            print(f"[migrate] added column {column} to {table}")

    existing_tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    for table_name, create_sql in _TABLE_MIGRATIONS:
        if table_name not in existing_tables:
            conn.execute(create_sql)
            print(f"[migrate] created table {table_name}")

    _ensure_mart_structure(conn)

    existing_views = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'view'").fetchall()}
    for view_name, create_sql in _VIEW_MIGRATIONS:
        if view_name not in existing_views:
            conn.execute(create_sql)
            print(f"[migrate] created view {view_name}")

    conn.commit()


def update_db(
    processed_pdfs: list[BaseException | PdfResult],
    batch_id: str,
    session_id: str,
    user_id: str,
    path: str,
    company_key: str | None,
    account_key: str | None,
    pdf_count: int,
    errors: int,
    reviews: int,
    duration_secs: float,
    process_time: datetime,
    project_path: Path | None = None,
) -> float:
    """
    Insert processed batch results into the SQLite database.

    Iterates through processed PDFs, reads each temporary parquet file,
    and inserts its rows into the corresponding database table. Also writes
    batch header metadata. Should be called after all PDFs have been
    processed and before deleting temporary files.

    Args:
        processed_pdfs: List of :class:`~bank_statement_parser.modules.data.PdfResult`
            entries as returned by
            :func:`~bank_statement_parser.modules.statements.process_pdf_statement`,
            or :class:`BaseException` for any entry that raised an unhandled worker error.
        batch_id: Unique identifier for this batch.
        session_id: UUID4 session identifier generated by the parent
            :class:`~bank_statement_parser.modules.statements.StatementBatch`.
        user_id: OS username of the user who initiated the batch.
        path: String representation of parent directories of the processed PDFs.
        company_key: Optional company identifier used for this batch.
        account_key: Optional account identifier used for this batch.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
        reviews: Count of REVIEW (CAB-failed) statement processings.
        duration_secs: Total processing time accumulated so far (seconds).
        process_time: Timestamp when batch processing started.
        project_path: Optional project root directory path.
            If not provided, uses the default project directory.
            The database file must already exist; call
            :func:`~bank_statement_parser.modules.paths.validate_or_initialise_project`
            beforehand (done automatically by
            :class:`~bank_statement_parser.modules.statements.Statement` and
            :class:`~bank_statement_parser.modules.statements.StatementBatch`).

    Returns:
        float: Time spent on database operations (seconds).

    Raises:
        ProjectDatabaseMissing: If ``database/project.db`` does not exist under
            the resolved project root.
    """
    paths = ProjectPaths.resolve(project_path)
    db_path = paths.project_db

    _require_db(db_path)

    conn = sqlite3.connect(db_path)
    _migrate_db(conn)

    def _insert_df(df: pl.DataFrame, table_name: str) -> None:
        if df.is_empty():
            return
        columns = [col for col in df.columns if col != "index"]
        df_to_insert = df.select(columns)
        for col in df_to_insert.columns:
            if df_to_insert[col].dtype == pl.Decimal:
                df_to_insert = df_to_insert.with_columns(pl.col(col).cast(pl.Float64))
        placeholders = ", ".join(["?"] * len(columns))
        cols_str = ", ".join([f'"{col}"' for col in columns])
        sql = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) VALUES ({placeholders})"
        rows = df_to_insert.rows()
        conn.executemany(sql, rows)

    update_start = time()
    for pdf in processed_pdfs:
        if isinstance(pdf, BaseException):
            conn.close()
            return 0.0
        elif isinstance(pdf, PdfResult):
            # batch_lines is always present on PdfResult
            if pdf.batch_lines and pdf.batch_lines.exists():
                df = pl.read_parquet(pdf.batch_lines)
                _insert_df(df, "batch_lines")
                pdf.batch_lines.unlink()
            # checks_and_balances is present for SUCCESS and REVIEW
            if pdf.checks_and_balances and pdf.checks_and_balances.exists():
                df = pl.read_parquet(pdf.checks_and_balances)
                _insert_df(df, "checks_and_balances")
                pdf.checks_and_balances.unlink()
            # statement_heads and statement_lines are only inserted for SUCCESS
            if pdf.result == "SUCCESS" and isinstance(pdf.payload, Success):
                pq_files = pdf.payload.parquet_files
                if pq_files.statement_heads and pq_files.statement_heads.exists():
                    df = pl.read_parquet(pq_files.statement_heads)
                    _insert_df(df, "statement_heads")
                    pq_files.statement_heads.unlink()
                if pq_files.statement_lines and pq_files.statement_lines.exists():
                    df = pl.read_parquet(pq_files.statement_lines)
                    _insert_df(df, "statement_lines")
                    pq_files.statement_lines.unlink()

    db_secs = time() - update_start

    batch_heads_df = pl.DataFrame(
        {
            "ID_BATCH": [batch_id],
            "ID_SESSION": [session_id],
            "ID_USER": [user_id],
            "STD_PATH": [path],
            "STD_COMPANY": [company_key],
            "STD_ACCOUNT": [account_key],
            "STD_PDF_COUNT": [pdf_count],
            "STD_ERROR_COUNT": [errors],
            "STD_REVIEW_COUNT": [reviews],
            "STD_DURATION_SECS": [duration_secs + db_secs],
            "STD_UPDATETIME": [process_time.isoformat()],
        }
    )
    _insert_df(batch_heads_df, "batch_heads")

    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    if pdf_count > (errors + reviews):  # if all pdf statements have failed/are under review no point in re-building the datamart
        try:
            build_datamart(db_path=db_path)
        except Exception as e:
            print(f"[update_db] ** Datamart Rebuild Failed **: {type(e).__name__}: {e}")

    return db_secs
