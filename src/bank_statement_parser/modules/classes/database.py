from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bank_statement_parser.modules.classes.statements import StatementBatch

from datetime import datetime
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.paths import PATH_BATCH_HEADS, PATH_BATCH_LINES


class Database:
    def __init__(self, file: Path, schema: pl.DataFrame, records: pl.DataFrame | None, key: str | None) -> None:
        self.file = file
        self.schema = schema
        self.records = records
        self.key = key
        self.db_records: pl.DataFrame = schema
        try:
            self.db_records = pl.read_parquet(file)
        except FileNotFoundError:
            pass

    def create(self):  # only to be used if we know the record doesn't exist
        if self.records is not None:
            self.db_records = self.db_records.extend(self.records)
            self.db_records.write_parquet(self.file)
            return True
        return False

    def update(self):  # this will add a new record or update a current record with the same id
        if self.records is not None and self.key is not None:
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records = self.db_records.extend(self.records)
            self.db_records.write_parquet(self.file)

    def delete(self):  # deletes the records from the database with the matched keys
        if self.records is not None and self.key is not None:  # delete the specified records
            self.db_records = self.db_records.remove(pl.col(self.key).is_in(self.records[self.key].implode()))
            self.db_records.write_parquet(self.file)
            return True
        else:
            return False

    def truncate(self):  # clears all records and replaces with a blank schema
        self.schema.write_parquet(self.file)


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
        if batch:
            self.records = self.schema.clone().extend(
                pl.DataFrame(
                    data={
                        "ID_BATCH": batch.ID_BATCH,
                        "STD_PATH": batch.path,
                        "STD_COMPANY": batch.company_key,
                        "STD_ACCOUNT": batch.account_key,
                        "STD_PDF_COUNT": batch.pdf_count,
                        "STD_ERROR_COUNT": batch.errors,
                        "STD_DURATION_SECS": batch.duration_secs,
                        "STD_UPDATETIME": batch.process_time,
                    },
                    orient="row",
                )
            )
        self.key = "ID_BATCH"
        super().__init__(PATH_BATCH_HEADS, self.schema, self.records, self.key)


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
        if batch_lines:
            self.records = self.schema.clone().extend(pl.DataFrame(batch_lines))
        self.key = "ID_BATCHLINE"
        super().__init__(PATH_BATCH_LINES, self.schema, self.records, self.key)


def main():
    print(BatchLines().db_records)
    print(BatchHeads().db_records)
    BatchLines().truncate()
    BatchHeads().truncate()
    # batch_lines: list[dict] = []
    # batch_line: dict = {}
    # batch_line["ID_BATCH"] = "ID1"
    # batch_line["ID_BATCHLINE"] = "ID1" + "_" + str(1)
    # batch_line["STD_BATCH_LINE"] = 1
    # batch_line["STD_FILENAME"] = "file 1"
    # batch_line["STD_DURATION_SECS"] = 3.2
    # batch_line["STD_UPDATETIME"] = datetime.now()
    # batch_line["STD_SUCCESS"] = True
    # batch_line["STD_ERROR_MESSAGE"] = ""
    # batch_line["ERROR_CAB"] = False
    # batch_line["ERROR_CONFIG"] = False
    # batch_lines.append(batch_line)
    # batch_line: dict = {}
    # batch_line["ID_BATCH"] = "ID2"
    # batch_line["ID_BATCHLINE"] = "ID2" + "_" + str(2)
    # batch_line["STD_BATCH_LINE"] = 2
    # batch_line["STD_FILENAME"] = "file 2"
    # batch_line["STD_DURATION_SECS"] = 2.3
    # batch_line["STD_UPDATETIME"] = datetime.now()
    # batch_line["STD_SUCCESS"] = False
    # batch_line["STD_ERROR_MESSAGE"] = "** Checks & Balances Failure **"
    # batch_line["ERROR_CAB"] = True
    # batch_line["ERROR_CONFIG"] = False
    # batch_lines.append(batch_line)
    # batch_line: dict = {}
    # batch_line["ID_BATCH"] = "ID3"
    # batch_line["ID_BATCHLINE"] = "ID3" + "_" + str(3)
    # batch_line["STD_BATCH_LINE"] = 3
    # batch_line["STD_FILENAME"] = "file 3"
    # batch_line["STD_DURATION_SECS"] = 1.2
    # batch_line["STD_UPDATETIME"] = datetime.now()
    # batch_line["STD_SUCCESS"] = False
    # batch_line["STD_ERROR_MESSAGE"] = "** Configuration Failure **"
    # batch_line["ERROR_CAB"] = False
    # batch_line["ERROR_CONFIG"] = True
    # batch_lines.append(batch_line)

    # bl = BatchLines(batch_lines)
    # bl.create()
    # print(bl.db_records)

    # batch: StatementBatch = StatementBatch("/home/boscorat/Downloads/2025")
    # BatchHeads().truncate()  # empty the BatchHeads table
    # bh = BatchHeads(batch=batch)  # create a new record
    # print(bh.db_records)
    # bh.update()
    # print(bh.db_records)
    # bh.create()
    # print(bh.db_records)
    # bh.delete()
    # print(bh.db_records)


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
