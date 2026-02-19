"""
Database persistence module.

Provides standalone functions for writing processed bank statement data
to a SQLite database.
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from time import time

import polars as pl

import bank_statement_parser.modules.paths as pt
from bank_statement_parser.data.build_datamart import build_datamart


def update_db(
    processed_pdfs: list[BaseException | tuple],
    batch_id: str,
    path: str,
    company_key: str | None,
    account_key: str | None,
    pdf_count: int,
    errors: int,
    duration_secs: float,
    process_time: datetime,
    db_path: Path | None = None,
) -> float:
    """
    Insert processed batch results into the SQLite database.

    Iterates through processed PDFs, reads each temporary parquet file,
    and inserts its rows into the corresponding database table. Also writes
    batch header metadata. Should be called after all PDFs have been
    processed and before deleting temporary files.

    Args:
        processed_pdfs: List of processed PDF results — each entry is either a
            BaseException (fatal worker error) or a 4-element tuple of
            (batch_lines_file, statement_heads_file, statement_lines_file,
            cab_file) Path values that may be None.
        batch_id: Unique identifier for this batch.
        path: String representation of parent directories of the processed PDFs.
        company_key: Optional company identifier used for this batch.
        account_key: Optional account identifier used for this batch.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
        duration_secs: Total processing time accumulated so far (seconds).
        process_time: Timestamp when batch processing started.
        db_path: Optional custom path to the database file.
            If not provided, uses the default project database.

    Returns:
        float: Time spent on database operations (seconds).
    """
    if db_path is None:
        db_path = pt.PROJECT_DB

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
        if type(pdf) is BaseException:
            conn.close()
            return 0.0
        elif type(pdf) is tuple:
            batch, head, lines, cab = pdf
            if batch and batch.exists():
                df = pl.read_parquet(batch)
                _insert_df(df, "batch_lines")
                batch.unlink()
            if head and head.exists():
                df = pl.read_parquet(head)
                _insert_df(df, "statement_heads")
                head.unlink()
            if lines and lines.exists():
                df = pl.read_parquet(lines)
                _insert_df(df, "statement_lines")
                lines.unlink()
            if cab and cab.exists():
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

    build_datamart(db_path=db_path)

    return db_secs
