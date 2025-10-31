from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bank_statement_parser.modules.classes.statements import Statement, StatementBatch

from pathlib import Path

import polars as pl

from bank_statement_parser.modules.paths import BATCH_HEADS, BATCH_LINES, CAB, STATEMENT_HEADS, STATEMENT_LINES


class Database:
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

    def create(self):  # only to be used if we know the record doesn't exist
        if self.records is not None:
            self.db_records = self.db_records.extend(self.records)
            self.db_records.with_row_index().write_parquet(self.file)
            return True
        return False

    def update(self):  # this will add a new record or update a current record with the same id
        if self.records is not None and self.key is not None:
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records = self.db_records.extend(self.records)
            self.db_records.with_row_index().write_parquet(self.file)

    def delete(self):  # deletes the records from the database with the matched keys
        if self.records is not None and self.key is not None:  # delete the specified records
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records.with_row_index().write_parquet(self.file)
            return True
        else:
            return False

    def truncate(self):  # clears all records and replaces with a blank schema
        self.schema.with_row_index().write_parquet(self.file)


class ChecksAndBalances(Database):
    def __init__(self, statement: Statement | None = None) -> None:
        self.stmt = statement
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
        self.records: pl.DataFrame | None = None
        if self.stmt:
            self.records = self.schema.clone().extend(
                self.stmt.checks_and_balances.select(
                    ID_CAB=pl.lit(self.stmt.ID_STATEMENT).add(pl.lit(".").add(pl.lit(self.stmt.ID_BATCH))),
                    ID_STATEMENT=pl.lit(self.stmt.ID_STATEMENT),
                    ID_BATCH=pl.lit(self.stmt.ID_BATCH),
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
        self.key = "ID_CAB"
        super().__init__(CAB, self.schema, self.records, self.key)


class StatementHeads(Database):
    def __init__(self, statement: Statement | None = None) -> None:
        self.stmt = statement
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_STATEMENT": pl.Utf8,
                "ID_BATCH": pl.Utf8,
                "STD_COMPANY": pl.Utf8,
                "STD_STATEMENT_TYPE": pl.Utf8,
                "STD_ACCOUNT": pl.Utf8,
                "STD_ACCOUNT_NUMBER": pl.Utf8,
                "STD_ACCOUNT_HOLDER": pl.Utf8,
                "STD_STATEMENT_DATE": pl.Date,
                "STD_OPENING_BALANCE": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
            },
        )
        self.records: pl.DataFrame | None = None
        if self.stmt:
            self.records = self.schema.clone().extend(
                pl.DataFrame(
                    data={
                        "ID_STATEMENT": self.stmt.ID_STATEMENT,
                        "ID_BATCH": self.stmt.ID_BATCH,
                        "STD_COMPANY": self.stmt.company,
                        "STD_STATEMENT_TYPE": self.stmt.statement_type,
                        "STD_ACCOUNT": self.stmt.account,
                    },
                    orient="row",
                ).hstack(
                    self.stmt.header_results.select(
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
        self.key = "ID_STATEMENT"
        super().__init__(STATEMENT_HEADS, self.schema, self.records, self.key)


class StatementLines(Database):
    def __init__(self, statement: Statement | None = None) -> None:
        self.stmt = statement
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_TRANSACTION": pl.Utf8,
                "ID_STATEMENT": pl.Utf8,
                "STD_PAGE_NUMBER": pl.Int32,
                "STD_TRANSACTION_DATE": pl.Date,
                "STD_TRANSACTION_NUMBER": pl.UInt32,
                "STD_TRANSACTION_DESC": pl.Utf8,
                "STD_OPENING_BALANCE": pl.Decimal(16, 4),
                "STD_PAYMENTS_IN": pl.Decimal(16, 4),
                "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
                "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
            },
        )
        self.records: pl.DataFrame | None = None
        if self.stmt:
            self.records = self.schema.clone().extend(
                self.stmt.lines_results.collect().select(
                    ID_TRANSACTION=pl.lit(self.stmt.ID_STATEMENT).add(pl.lit(".").add(pl.col("STD_TRANSACTION_NUMBER").cast(str))),
                    ID_STATEMENT=pl.lit(self.stmt.ID_STATEMENT),
                    STD_PAGE_NUMBER="STD_PAGE_NUMBER",
                    STD_TRANSACTION_DATE="STD_TRANSACTION_DATE",
                    STD_TRANSACTION_NUMBER="STD_TRANSACTION_NUMBER",
                    STD_TRANSACTION_DESC="STD_TRANSACTION_DESC",
                    STD_OPENING_BALANCE=pl.col("STD_RUNNING_BALANCE").sub(pl.col("STD_MOVEMENT")),
                    STD_PAYMENTS_IN="STD_PAYMENT_IN",
                    STD_PAYMENTS_OUT="STD_PAYMENT_OUT",
                    STD_CLOSING_BALANCE="STD_RUNNING_BALANCE",
                )
            )
        self.key = "ID_TRANSACTION"
        super().__init__(STATEMENT_LINES, self.schema, self.records, self.key)


class BatchHeads(Database):
    def __init__(self, batch: StatementBatch | None = None) -> None:
        self.batch = batch
        self.schema = pl.DataFrame(
            orient="row",
            schema={
                "ID_BATCH": pl.Utf8,
                "STD_PATH": pl.Utf8,
                "STD_COMPANY": pl.Utf8,
                "STD_ACCOUNT": pl.Utf8,
                "STD_PDF_COUNT": pl.Int64,
                "STD_ERROR_COUNT": pl.Int64,
                "STD_DURATION_SECS": pl.Int64,
                "STD_UPDATETIME": pl.Datetime,
            },
        )
        self.records: pl.DataFrame | None = None
        if self.batch:
            self.records = self.schema.clone().extend(
                pl.DataFrame(
                    data={
                        "ID_BATCH": self.batch.ID_BATCH,
                        "STD_PATH": self.batch.path,
                        "STD_COMPANY": self.batch.company_key,
                        "STD_ACCOUNT": self.batch.account_key,
                        "STD_PDF_COUNT": self.batch.pdf_count,
                        "STD_ERROR_COUNT": self.batch.errors,
                        "STD_DURATION_SECS": self.batch.duration_secs,
                        "STD_UPDATETIME": self.batch.process_time,
                    },
                    orient="row",
                )
            )
        self.key = "ID_BATCH"
        super().__init__(BATCH_HEADS, self.schema, self.records, self.key)


class BatchLines(Database):
    def __init__(self, batch_lines: list[dict] | None = None) -> None:
        self.batch_lines = batch_lines
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
        self.records: pl.DataFrame | None = None
        if self.batch_lines:
            self.records = self.schema.clone().extend(pl.DataFrame(self.batch_lines))
        self.key = "ID_BATCHLINE"
        super().__init__(BATCH_LINES, self.schema, self.records, self.key)


def main(): ...


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
