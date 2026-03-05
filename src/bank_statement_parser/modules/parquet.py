"""
Parquet read/write classes and batch-update helpers.

Classes:
    Parquet: Base class for parquet file operations (create, update, delete, truncate).
    ChecksAndBalances: Parquet wrapper for checks-and-balances records.
    StatementHeads: Parquet wrapper for statement header records.
    StatementLines: Parquet wrapper for statement transaction-line records.
    BatchHeads: Parquet wrapper for batch header metadata.
    BatchLines: Parquet wrapper for per-PDF batch-line records.

Functions:
    update_parquet: Merge per-PDF temp parquet files into permanent files and write
        the batch header.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import time

import polars as pl

from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.paths import ProjectPaths


class Parquet:
    __slots__ = ("file", "schema", "records", "key", "db_records")

    def __init__(self, file: Path, schema: pl.DataFrame, records: pl.DataFrame | None, key: str | None) -> None:
        self.file = file
        self.schema = schema
        self.records = records
        self.key = key
        self.db_records: pl.DataFrame = schema
        try:
            existing = pl.read_parquet(file)
            # Auto-delete stale parquet files whose column layout no longer matches
            # the current schema (e.g. ID_BATCH → ID_BATCHLINE migration).
            if set(existing.drop("index").columns) != set(schema.columns):
                file.unlink()
            else:
                self.db_records = existing.drop("index")
        except FileNotFoundError:
            pass

    def cleanup(self):
        self.db_records = pl.DataFrame()
        self.file = Path()

    def create(self):  # only to be used if we know the record doesn't exist
        if self.records is not None and self.file:
            self.db_records = self.db_records.extend(self.records)
            self.db_records.with_row_index().write_parquet(self.file)
            return True
        return False

    def update(self):  # this will add a new record or update a current record with the same id
        if self.records is not None and self.key is not None and self.file:
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records = self.db_records.extend(self.records)
            self.db_records.with_row_index().write_parquet(self.file)

    def delete(self):  # deletes the records from the database with the matched keys
        if self.records is not None and self.key is not None and self.file:  # delete the specified records
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records.with_row_index().write_parquet(self.file)
            return True
        else:
            return False

    def truncate(self):  # clears all records and replaces with a blank schema
        if self.file:
            self.schema.with_row_index().write_parquet(self.file)

    def delete_file(self):
        if self.file and self.file.is_file():
            self.file.unlink()


def _load_source(source: Path) -> pl.DataFrame:
    """Read a parquet file and drop the ``index`` column.

    Args:
        source: Path to the parquet file to read.

    Returns:
        DataFrame with the ``index`` column removed.
    """
    return pl.read_parquet(source).drop("index")


class ChecksAndBalances(Parquet):
    """Parquet wrapper for checks-and-balances records.

    Args:
        file: Destination parquet file path.
        source: Optional separate source path to read initial records from.
            When provided, records are loaded from *source* rather than built
            from the data arguments.  When omitted, *file* is used as the
            source when reading existing data (via the base class).
        id_statement: Unique statement identifier (required when building
            records from raw data).
        id_batch: Batch identifier (required when building records from raw
            data).
        checks_and_balances: Raw checks-and-balances DataFrame from
            :class:`~bank_statement_parser.modules.statements.Statement`
            (required when building records from raw data).
    """

    __slots__ = ()

    def __init__(
        self,
        file: Path,
        source: Path | None = None,
        id_statement: str | None = None,
        id_batch: str | None = None,
        checks_and_balances: pl.DataFrame | None = None,
    ) -> None:
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_CAB": pl.Utf8,
                "ID_STATEMENT": pl.Utf8,
                "ID_BATCH": pl.Utf8,
                "HAS_TRANSACTIONS": pl.Boolean,
                "STD_OPENING_BALANCE_HEADS": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN_HEADS": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT_HEADS": pl.Decimal(16, 4),
                "STD_MOVEMENT_HEADS": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE_HEADS": pl.Decimal(16, 4),
                "STD_OPENING_BALANCE_LINES": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN_LINES": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT_LINES": pl.Decimal(16, 4),
                "STD_MOVEMENT_LINES": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE_LINES": pl.Decimal(16, 4),
                "CHECK_PAYMENTS_IN": pl.Boolean,
                "CHECK_PAYMENTS_OUT": pl.Boolean,
                "CHECK_MOVEMENT": pl.Boolean,
                "CHECK_CLOSING": pl.Boolean,
            },
        )
        self.key = "ID_CAB"
        self.records: pl.DataFrame | None = None

        if source is not None:
            self.records = _load_source(source)
        elif checks_and_balances is not None and id_statement is not None and id_batch is not None:
            self.records = build_checks_and_balances_records(self.schema, id_statement, id_batch, checks_and_balances)

        super().__init__(file, self.schema, self.records, self.key)


class StatementHeads(Parquet):
    """Parquet wrapper for statement header records.

    Args:
        file: Destination parquet file path.
        source: Optional separate source path to read initial records from.
        id_statement: Unique statement identifier.
        id_batchline: Batch-line identifier.
        id_account: Account identifier.
        company: Company name.
        statement_type: Statement type label.
        account: Account label.
        header_results: LazyFrame of extracted header fields.
    """

    __slots__ = ()

    def __init__(
        self,
        file: Path,
        source: Path | None = None,
        id_statement: str | None = None,
        id_batchline: str | None = None,
        id_account: str | None = None,
        company: str | None = None,
        statement_type: str | None = None,
        account: str | None = None,
        header_results: pl.LazyFrame | None = None,
    ) -> None:
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_STATEMENT": pl.Utf8,
                "ID_BATCHLINE": pl.Utf8,
                "ID_ACCOUNT": pl.Utf8,
                "STD_COMPANY": pl.Utf8,
                "STD_STATEMENT_TYPE": pl.Utf8,
                "STD_ACCOUNT": pl.Utf8,
                "STD_SORTCODE": pl.Utf8,
                "STD_ACCOUNT_NUMBER": pl.Utf8,
                "STD_ACCOUNT_HOLDER": pl.Utf8,
                "STD_STATEMENT_DATE": pl.Date,
                "STD_OPENING_BALANCE": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
            },
        )
        self.key = "ID_STATEMENT"
        self.records: pl.DataFrame | None = None

        if source is not None:
            self.records = _load_source(source)
        elif header_results is not None and id_statement is not None:
            self.records = build_statement_heads_records(
                self.schema, id_statement, id_batchline, id_account, company, statement_type, account, header_results
            )

        super().__init__(file, self.schema, self.records, self.key)


class StatementLines(Parquet):
    """Parquet wrapper for statement transaction-line records.

    Args:
        file: Destination parquet file path.
        source: Optional separate source path to read initial records from.
        id_statement: Unique statement identifier.
        lines_results: LazyFrame of extracted transaction lines.
    """

    __slots__ = ()

    def __init__(
        self,
        file: Path,
        source: Path | None = None,
        id_statement: str | None = None,
        lines_results: pl.LazyFrame | None = None,
    ) -> None:
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_TRANSACTION": pl.Utf8,
                "ID_STATEMENT": pl.Utf8,
                "STD_PAGE_NUMBER": pl.Int32,
                "STD_TRANSACTION_DATE": pl.Date,
                "STD_TRANSACTION_NUMBER": pl.UInt32,
                "STD_CD": pl.Utf8,
                "STD_TRANSACTION_TYPE": pl.Utf8,
                "STD_TRANSACTION_TYPE_CD": pl.Utf8,
                "STD_TRANSACTION_DESC": pl.Utf8,
                "STD_OPENING_BALANCE": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
            },
        )
        self.key = "ID_TRANSACTION"
        self.records: pl.DataFrame | None = None

        if source is not None:
            self.records = _load_source(source)
        elif lines_results is not None and id_statement is not None:
            self.records = build_statement_lines_records(self.schema, id_statement, lines_results)

        super().__init__(file, self.schema, self.records, self.key)


class BatchHeads(Parquet):
    """Parquet wrapper for batch header metadata.

    Args:
        file: Destination parquet file path.
        batch_id: Unique batch identifier.
        session_id: UUID4 session identifier.
        user_id: OS username of the user who initiated the batch.
        path: String representation of PDF source directories.
        company_key: Company identifier.
        account_key: Account identifier.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
        duration_secs: Total processing time (seconds).
        process_time: Timestamp when batch processing started.
    """

    __slots__ = ()

    def __init__(
        self,
        file: Path,
        batch_id: str | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        path: str | None = None,
        company_key: str | None = None,
        account_key: str | None = None,
        pdf_count: int | None = None,
        errors: int | None = None,
        duration_secs: float | None = None,
        process_time: datetime | None = None,
    ) -> None:
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_BATCH": pl.Utf8,
                "ID_SESSION": pl.Utf8,
                "ID_USER": pl.Utf8,
                "STD_PATH": pl.Utf8,
                "STD_COMPANY": pl.Utf8,
                "STD_ACCOUNT": pl.Utf8,
                "STD_PDF_COUNT": pl.Int64,
                "STD_ERROR_COUNT": pl.Int64,
                "STD_DURATION_SECS": pl.Float64,
                "STD_UPDATETIME": pl.Datetime,
            },
        )
        self.records: pl.DataFrame | None = None
        if batch_id is not None:
            self.records = build_batch_heads_records(
                self.schema,
                batch_id,
                session_id or "",
                user_id or "",
                path,
                company_key,
                account_key,
                pdf_count,
                errors,
                duration_secs,
                process_time,
            )
        self.key = "ID_BATCH"
        super().__init__(file, self.schema, self.records, self.key)


class BatchLines(Parquet):
    """Parquet wrapper for per-PDF batch-line records.

    Args:
        file: Destination parquet file path.
        source: Optional separate source path to read initial records from.
        batch_lines: List of dicts, one per PDF, with batch-line metadata.
    """

    __slots__ = ()

    def __init__(
        self,
        file: Path,
        source: Path | None = None,
        batch_lines: list[dict] | None = None,
    ) -> None:
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_BATCH": pl.Utf8,
                "ID_BATCHLINE": pl.Utf8,
                "ID_STATEMENT": pl.Utf8,
                "STD_BATCH_LINE": pl.Int64,
                "STD_FILENAME": pl.Utf8,
                "STD_ACCOUNT": pl.Utf8,
                "STD_DURATION_SECS": pl.Float64,
                "STD_UPDATETIME": pl.Datetime,
                "STD_SUCCESS": pl.Boolean,
                "STD_ERROR_MESSAGE": pl.Utf8,
                "ERROR_CAB": pl.Boolean,
                "ERROR_CONFIG": pl.Boolean,
                "ERROR_DATA": pl.Boolean,
            },
        )
        self.key = "ID_BATCHLINE"
        self.records: pl.DataFrame | None = None

        if source is not None:
            self.records = _load_source(source)
        elif batch_lines:
            self.records = build_batch_lines_records(self.schema, batch_lines)

        super().__init__(file, self.schema, self.records, self.key)


def _build_checks_and_balances_data(
    id_statement: str,
    id_batch: str,
    checks_and_balances: pl.DataFrame,
) -> pl.DataFrame:
    """Build the data DataFrame for ChecksAndBalances (without extending the schema).

    Args:
        id_statement: Unique statement identifier.
        id_batch: Batch identifier.
        checks_and_balances: The raw checks & balances DataFrame from
            :class:`~bank_statement_parser.modules.statements.Statement`.

    Returns:
        A single-row DataFrame with the ChecksAndBalances columns.
    """
    return checks_and_balances.select(
        ID_CAB=pl.lit(id_statement).add(pl.lit(".").add(pl.lit(id_batch))),
        ID_STATEMENT=pl.lit(id_statement),
        ID_BATCH=pl.lit(id_batch),
        HAS_TRANSACTIONS=~pl.col("ZERO_TRANSACTION_STATEMENT"),
        STD_OPENING_BALANCE_HEADS="STD_OPENING_BALANCE",
        STD_PAYMENTS_IN_HEADS="STD_PAYMENTS_IN",
        STD_PAYMENTS_OUT_HEADS="STD_PAYMENTS_OUT",
        STD_MOVEMENT_HEADS="STD_STATEMENT_MOVEMENT",
        STD_CLOSING_BALANCE_HEADS="STD_CLOSING_BALANCE",
        STD_OPENING_BALANCE_LINES=pl.col("STD_RUNNING_BALANCE").sub(pl.col("STD_MOVEMENT")),
        STD_PAYMENTS_IN_LINES="STD_PAYMENT_IN",
        STD_PAYMENTS_OUT_LINES="STD_PAYMENT_OUT",
        STD_MOVEMENT_LINES="STD_MOVEMENT",
        STD_CLOSING_BALANCE_LINES="STD_RUNNING_BALANCE",
        CHECK_PAYMENTS_IN="BAL_PAYMENTS_IN",
        CHECK_PAYMENTS_OUT="BAL_PAYMENTS_OUT",
        CHECK_MOVEMENT="BAL_MOVEMENT",
        CHECK_CLOSING="BAL_CLOSING",
    )


def build_checks_and_balances_records(
    schema: pl.DataFrame,
    id_statement: str,
    id_batch: str,
    checks_and_balances: pl.DataFrame,
) -> pl.DataFrame:
    """Build the records DataFrame for a ChecksAndBalances parquet write.

    Args:
        schema: Empty DataFrame with the correct column types (from
            ``ChecksAndBalances.schema``).
        id_statement: Unique statement identifier.
        id_batch: Batch identifier.
        checks_and_balances: The raw checks & balances DataFrame from
            :class:`~bank_statement_parser.modules.statements.Statement`.

    Returns:
        A single-row DataFrame matching *schema* ready for ``.extend()``.
    """
    return schema.clone().extend(_build_checks_and_balances_data(id_statement, id_batch, checks_and_balances))


def _build_statement_heads_data(
    id_statement: str,
    id_batchline: str | None,
    id_account: str | None,
    company: str | None,
    statement_type: str | None,
    account: str | None,
    header_results: pl.LazyFrame,
) -> pl.DataFrame:
    """Build the data DataFrame for StatementHeads (without extending the schema).

    Args:
        id_statement: Unique statement identifier.
        id_batchline: Batch-line identifier.
        id_account: Account identifier.
        company: Company name.
        statement_type: Statement type label.
        account: Account label.
        header_results: LazyFrame of extracted header fields.

    Returns:
        A single-row DataFrame with the StatementHeads columns.
    """
    return pl.DataFrame(
        data={
            "ID_STATEMENT": id_statement,
            "ID_BATCHLINE": id_batchline,
            "ID_ACCOUNT": id_account,
            "STD_COMPANY": company,
            "STD_STATEMENT_TYPE": statement_type,
            "STD_ACCOUNT": account,
        },
        orient="row",
    ).hstack(
        header_results.select(
            "STD_SORTCODE",
            "STD_ACCOUNT_NUMBER",
            "STD_ACCOUNT_HOLDER",
            "STD_STATEMENT_DATE",
            "STD_OPENING_BALANCE",
            "STD_PAYMENTS_IN",
            "STD_PAYMENTS_OUT",
            "STD_CLOSING_BALANCE",
        ).collect()
    )


def build_statement_heads_records(
    schema: pl.DataFrame,
    id_statement: str,
    id_batchline: str | None,
    id_account: str | None,
    company: str | None,
    statement_type: str | None,
    account: str | None,
    header_results: pl.LazyFrame,
) -> pl.DataFrame:
    """Build the records DataFrame for a StatementHeads parquet write.

    Args:
        schema: Empty DataFrame with the correct column types.
        id_statement: Unique statement identifier.
        id_batchline: Batch-line identifier.
        id_account: Account identifier.
        company: Company name.
        statement_type: Statement type label.
        account: Account label.
        header_results: LazyFrame of extracted header fields.

    Returns:
        A single-row DataFrame matching *schema* ready for ``.extend()``.
    """
    return schema.clone().extend(
        _build_statement_heads_data(id_statement, id_batchline, id_account, company, statement_type, account, header_results)
    )


def _build_statement_lines_data(
    id_statement: str,
    lines_results: pl.LazyFrame,
) -> pl.DataFrame:
    """Build the data DataFrame for StatementLines (without extending the schema).

    Args:
        id_statement: Unique statement identifier.
        lines_results: LazyFrame of extracted transaction lines.

    Returns:
        A multi-row DataFrame with the StatementLines columns.
    """
    return lines_results.collect().select(
        ID_TRANSACTION=pl.lit(id_statement).add(pl.lit(".").add(pl.col("STD_TRANSACTION_NUMBER").cast(str))),
        ID_STATEMENT=pl.lit(id_statement),
        STD_PAGE_NUMBER="STD_PAGE_NUMBER",
        STD_TRANSACTION_DATE="STD_TRANSACTION_DATE",
        STD_TRANSACTION_NUMBER="STD_TRANSACTION_NUMBER",
        STD_CD="STD_CD",
        STD_TRANSACTION_TYPE="STD_TRANSACTION_TYPE",
        STD_TRANSACTION_TYPE_CD=pl.col("STD_TRANSACTION_TYPE").add("-").add(pl.col("STD_CD")),
        STD_TRANSACTION_DESC="STD_TRANSACTION_DESC",
        STD_OPENING_BALANCE=pl.col("STD_RUNNING_BALANCE").sub(pl.col("STD_MOVEMENT")),
        STD_PAYMENTS_IN="STD_PAYMENT_IN",
        STD_PAYMENTS_OUT="STD_PAYMENT_OUT",
        STD_CLOSING_BALANCE="STD_RUNNING_BALANCE",
    )


def build_statement_lines_records(
    schema: pl.DataFrame,
    id_statement: str,
    lines_results: pl.LazyFrame,
) -> pl.DataFrame:
    """Build the records DataFrame for a StatementLines parquet write.

    Args:
        schema: Empty DataFrame with the correct column types.
        id_statement: Unique statement identifier.
        lines_results: LazyFrame of extracted transaction lines.

    Returns:
        A multi-row DataFrame matching *schema* ready for ``.extend()``.
    """
    return schema.clone().extend(_build_statement_lines_data(id_statement, lines_results))


def build_batch_heads_records(
    schema: pl.DataFrame,
    batch_id: str,
    session_id: str,
    user_id: str,
    path: str | None,
    company_key: str | None,
    account_key: str | None,
    pdf_count: int | None,
    errors: int | None,
    duration_secs: float | None,
    process_time: datetime | None,
) -> pl.DataFrame:
    """Build the records DataFrame for a BatchHeads parquet write.

    Args:
        schema: Empty DataFrame with the correct column types.
        batch_id: Unique batch identifier.
        session_id: UUID4 session identifier generated by the parent
            :class:`~bank_statement_parser.modules.statements.StatementBatch`.
        user_id: OS username of the user who initiated the batch.
        path: String representation of PDF source directories.
        company_key: Company identifier.
        account_key: Account identifier.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
        duration_secs: Total processing time (seconds).
        process_time: Timestamp when batch processing started.

    Returns:
        A single-row DataFrame matching *schema* ready for ``.extend()``.
    """
    return schema.clone().extend(
        pl.DataFrame(
            data={
                "ID_BATCH": batch_id,
                "ID_SESSION": session_id,
                "ID_USER": user_id,
                "STD_PATH": str(path),
                "STD_COMPANY": company_key,
                "STD_ACCOUNT": account_key,
                "STD_PDF_COUNT": pdf_count,
                "STD_ERROR_COUNT": errors,
                "STD_DURATION_SECS": duration_secs,
                "STD_UPDATETIME": process_time,
            },
            orient="row",
        )
    )


def build_batch_lines_records(
    schema: pl.DataFrame,
    batch_lines: list[dict],
) -> pl.DataFrame:
    """Build the records DataFrame for a BatchLines parquet write.

    Args:
        schema: Empty DataFrame with the correct column types.
        batch_lines: List of dicts, one per PDF, with batch-line metadata.

    Returns:
        A DataFrame matching *schema* ready for ``.extend()``.
    """
    return schema.clone().extend(pl.DataFrame(batch_lines))


def update_parquet(
    processed_pdfs: list[BaseException | PdfResult],
    batch_id: str,
    session_id: str,
    user_id: str,
    path: str,
    company_key: str | None,
    account_key: str | None,
    pdf_count: int,
    errors: int,
    duration_secs: float,
    process_time: datetime,
    paths: ProjectPaths,
) -> float:
    """
    Update parquet files with processed results from all PDFs in a batch.

    Iterates through processed PDFs, handles any exceptions, and updates
    the permanent parquet files from temporary files created during processing.
    Also writes batch header metadata.  Should be called after all PDFs have
    been processed to finalise the batch.

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
        duration_secs: Total processing time accumulated so far (seconds).
        process_time: Timestamp when batch processing started.
        paths: Resolved :class:`~bank_statement_parser.modules.paths.ProjectPaths`
            instance for this project.

    Returns:
        float: Time spent updating parquet files (seconds).
    """
    update_start = time()
    for pdf in processed_pdfs:
        # Skip any exceptions that occurred during processing
        if isinstance(pdf, BaseException):
            return 0.0
        elif isinstance(pdf, PdfResult):
            if pdf.batch_lines_stem:
                bl = BatchLines(file=paths.batch_lines, source=paths.parquet / f"{pdf.batch_lines_stem}.parquet")
                bl.update()
                bl.cleanup()
                bl = None
            if pdf.statement_heads_stem:
                sh = StatementHeads(file=paths.statement_heads, source=paths.parquet / f"{pdf.statement_heads_stem}.parquet")
                sh.update()
                sh.cleanup()
                sh = None
            if pdf.statement_lines_stem:
                sl = StatementLines(file=paths.statement_lines, source=paths.parquet / f"{pdf.statement_lines_stem}.parquet")
                sl.update()
                sl.cleanup()
                sl = None
            if pdf.cab_stem:
                cb = ChecksAndBalances(file=paths.cab, source=paths.parquet / f"{pdf.cab_stem}.parquet")
                cb.update()
                cb.cleanup()
                cb = None

    parquet_secs = time() - update_start

    # Write batch header metadata to parquet
    pq_heads = BatchHeads(
        file=paths.batch_heads,
        batch_id=batch_id,
        session_id=session_id,
        user_id=user_id,
        path=path,
        company_key=company_key,
        account_key=account_key,
        pdf_count=pdf_count,
        errors=errors,
        duration_secs=duration_secs + parquet_secs,
        process_time=process_time,
    )
    pq_heads.create()
    pq_heads.cleanup()
    pq_heads = None

    return parquet_secs


def main() -> None:
    """Truncate all permanent parquet files (dev utility)."""
    _paths = ProjectPaths.resolve()
    BatchHeads(file=_paths.batch_heads).truncate()
    BatchLines(file=_paths.batch_lines).truncate()
    StatementHeads(file=_paths.statement_heads).truncate()
    StatementLines(file=_paths.statement_lines).truncate()


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
