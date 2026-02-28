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

from bank_statement_parser.data.build_datamart import build_datamart
from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.errors import ProjectDatabaseMissing
from bank_statement_parser.modules.paths import get_paths

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


def update_db(
    processed_pdfs: list[BaseException | PdfResult],
    batch_id: str,
    path: str,
    company_key: str | None,
    account_key: str | None,
    pdf_count: int,
    errors: int,
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
        path: String representation of parent directories of the processed PDFs.
        company_key: Optional company identifier used for this batch.
        account_key: Optional account identifier used for this batch.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
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
    paths = get_paths(project_path)
    db_path = paths.project_db

    _require_db(db_path)

    conn = sqlite3.connect(db_path)

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
            if pdf.batch_lines_stem:
                batch = paths.parquet / f"{pdf.batch_lines_stem}.parquet"
                if batch.exists():
                    df = pl.read_parquet(batch)
                    _insert_df(df, "batch_lines")
                    batch.unlink()
            if pdf.statement_heads_stem:
                head = paths.parquet / f"{pdf.statement_heads_stem}.parquet"
                if head.exists():
                    df = pl.read_parquet(head)
                    _insert_df(df, "statement_heads")
                    head.unlink()
            if pdf.statement_lines_stem:
                lines = paths.parquet / f"{pdf.statement_lines_stem}.parquet"
                if lines.exists():
                    df = pl.read_parquet(lines)
                    _insert_df(df, "statement_lines")
                    lines.unlink()
            if pdf.cab_stem:
                cab = paths.parquet / f"{pdf.cab_stem}.parquet"
                if cab.exists():
                    df = pl.read_parquet(cab)
                    _insert_df(df, "checks_and_balances")
                    cab.unlink()

    db_secs = time() - update_start

    batch_heads_df = pl.DataFrame(
        {
            "ID_BATCH": [batch_id],
            "STD_PATH": [path],
            "STD_COMPANY": [company_key],
            "STD_ACCOUNT": [account_key],
            "STD_PDF_COUNT": [pdf_count],
            "STD_ERROR_COUNT": [errors],
            "STD_DURATION_SECS": [duration_secs + db_secs],
            "STD_UPDATETIME": [process_time.isoformat()],
        }
    )
    _insert_df(batch_heads_df, "batch_heads")

    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    if pdf_count > errors:  # if all pdf statements have failed no point in re-building the datamart
        try:
            build_datamart(db_path=db_path)
        except Exception as e:
            print(f"[update_db] ** Datamart Rebuild Failed **: {type(e).__name__}: {e}")

    return db_secs
