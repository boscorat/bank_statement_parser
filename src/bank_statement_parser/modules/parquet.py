from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.paths import ProjectPaths, get_paths


class Parquet:
    __slots__ = ("file", "schema", "records", "key", "db_records")

    def __init__(self, file: Path, schema: pl.DataFrame, records: pl.DataFrame | None, key: str | None) -> None:
        self.file = file
        self.schema = schema
        self.records = records
        self.key = key
        self.db_records: pl.DataFrame = schema
        try:
            self.db_records = pl.read_parquet(file).drop("index")
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


def _resolve_parquet_file(
    paths: ProjectPaths,
    default: Path,
    filename: str | None,
) -> Path:
    """
    Return the full Path for a parquet file, validating the parquet directory.

    If *filename* is provided (bare stem, no extension, no directory), the
    full path is ``paths.parquet / f"{filename}.parquet"``.  Otherwise the
    *default* path is used.

    A :exc:`ProjectSubFolderNotFound` is raised if the parquet directory does
    not exist.
    """
    paths.require_subdir_for_read(paths.parquet)
    if filename is not None:
        return paths.parquet / f"{filename}.parquet"
    return default


class ChecksAndBalances(Parquet):
    __slots__ = ("id", "source_filename", "destination_filename", "project_path")

    def __init__(
        self,
        id_statement: str | None = None,
        id_batch: str | None = None,
        checks_and_balances: pl.DataFrame | None = None,
        id: int = -1,
        source_filename: str | None = None,
        destination_filename: str | None = None,
        project_path: Path | None = None,
    ) -> None:
        self.id = id
        self.source_filename = source_filename
        self.destination_filename = destination_filename
        self.project_path = project_path
        paths = get_paths(project_path)
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

        # Determine the source file (temp file written by process_pdf_statement)
        source_default = paths.cab_temp(self.id) if self.id > -1 else paths.cab
        source_file = _resolve_parquet_file(paths, source_default, source_filename)

        if source_filename is not None:
            self.records = pl.read_parquet(source_file).drop("index")
        elif checks_and_balances is not None and id_statement is not None and id_batch is not None:
            self.records = self.schema.clone().extend(
                checks_and_balances.select(
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
            )

        # Determine the destination file
        dest_default = paths.cab_temp(self.id) if self.id > -1 else paths.cab
        destination_file = _resolve_parquet_file(paths, dest_default, destination_filename)

        super().__init__(destination_file, self.schema, self.records, self.key)


class StatementHeads(Parquet):
    __slots__ = ("id", "source_filename", "destination_filename", "project_path")

    def __init__(
        self,
        id_statement: str | None = None,
        id_batch: str | None = None,
        id_account: str | None = None,
        company: str | None = None,
        statement_type: str | None = None,
        account: str | None = None,
        header_results: pl.LazyFrame | None = None,
        id: int = -1,
        source_filename: str | None = None,
        destination_filename: str | None = None,
        project_path: Path | None = None,
    ) -> None:
        self.id = id
        self.source_filename = source_filename
        self.destination_filename = destination_filename
        self.project_path = project_path
        paths = get_paths(project_path)
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_STATEMENT": pl.Utf8,
                "ID_BATCH": pl.Utf8,
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

        source_default = paths.statement_heads_temp(self.id) if self.id > -1 else paths.statement_heads
        source_file = _resolve_parquet_file(paths, source_default, source_filename)

        if source_filename is not None:
            self.records = pl.read_parquet(source_file).drop("index")
        elif header_results is not None and id_statement is not None:
            self.records = self.schema.clone().extend(
                pl.DataFrame(
                    data={
                        "ID_STATEMENT": id_statement,
                        "ID_BATCH": id_batch,
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
            )

        dest_default = paths.statement_heads_temp(self.id) if self.id > -1 else paths.statement_heads
        destination_file = _resolve_parquet_file(paths, dest_default, destination_filename)

        super().__init__(destination_file, self.schema, self.records, self.key)


class StatementLines(Parquet):
    __slots__ = ("id", "source_filename", "destination_filename", "project_path")

    def __init__(
        self,
        id_statement: str | None = None,
        lines_results: pl.LazyFrame | None = None,
        id: int = -1,
        source_filename: str | None = None,
        destination_filename: str | None = None,
        project_path: Path | None = None,
    ) -> None:
        self.id = id
        self.source_filename = source_filename
        self.destination_filename = destination_filename
        self.project_path = project_path
        paths = get_paths(project_path)
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

        source_default = paths.statement_lines_temp(self.id) if self.id > -1 else paths.statement_lines
        source_file = _resolve_parquet_file(paths, source_default, source_filename)

        if source_filename is not None:
            self.records = pl.read_parquet(source_file).drop("index")
        elif lines_results is not None and id_statement is not None:
            self.records = self.schema.clone().extend(
                lines_results.collect().select(
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
            )

        dest_default = paths.statement_lines_temp(self.id) if self.id > -1 else paths.statement_lines
        destination_file = _resolve_parquet_file(paths, dest_default, destination_filename)

        super().__init__(destination_file, self.schema, self.records, self.key)


class BatchHeads(Parquet):
    __slots__ = ("destination_filename", "project_path")

    def __init__(
        self,
        batch_id: str | None = None,
        path: str | None = None,
        company_key: str | None = None,
        account_key: str | None = None,
        pdf_count: int | None = None,
        errors: int | None = None,
        duration_secs: float | None = None,
        process_time: datetime | None = None,
        destination_filename: str | None = None,
        project_path: Path | None = None,
    ) -> None:
        self.destination_filename = destination_filename
        self.project_path = project_path
        paths = get_paths(project_path)
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_BATCH": pl.Utf8,
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
            self.records = self.schema.clone().extend(
                pl.DataFrame(
                    data={
                        "ID_BATCH": batch_id,
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
        self.key = "ID_BATCH"
        destination_file = _resolve_parquet_file(paths, paths.batch_heads, destination_filename)
        super().__init__(destination_file, self.schema, self.records, self.key)


class BatchLines(Parquet):
    __slots__ = ("batch_lines", "id", "source_filename", "destination_filename", "project_path")

    def __init__(
        self,
        batch_lines: list[dict] | None = None,
        id: int = -1,
        source_filename: str | None = None,
        destination_filename: str | None = None,
        project_path: Path | None = None,
    ) -> None:
        self.batch_lines = batch_lines
        self.id = id
        self.source_filename = source_filename
        self.destination_filename = destination_filename
        self.project_path = project_path
        paths = get_paths(project_path)
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
            },
        )
        self.key = "ID_BATCHLINE"
        self.records: pl.DataFrame | None = None

        source_default = paths.batch_lines_temp(self.id) if self.id > -1 else paths.batch_lines
        source_file = _resolve_parquet_file(paths, source_default, source_filename)

        if source_filename is not None:
            self.records = pl.read_parquet(source_file).drop("index")
        elif self.batch_lines:
            self.records = self.schema.clone().extend(pl.DataFrame(self.batch_lines))

        dest_default = paths.batch_lines_temp(self.id) if self.id > -1 else paths.batch_lines
        destination_file = _resolve_parquet_file(paths, dest_default, destination_filename)

        super().__init__(destination_file, self.schema, self.records, self.key)


def main():
    BatchHeads().truncate()
    BatchLines().truncate()
    StatementHeads().truncate()
    StatementLines().truncate()
    ...


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
