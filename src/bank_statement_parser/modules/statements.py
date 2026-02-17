import asyncio
import hashlib
import os
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from time import time
from uuid import uuid4

import polars as pl

import bank_statement_parser.modules.database as db
from bank_statement_parser.modules.config import (
    config_standard_fields,
    get_config_from_account,
    get_config_from_company,
    get_config_from_statement,
)
from bank_statement_parser.modules.data import Account
from bank_statement_parser.modules.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.statement_functions import get_results, get_standard_fields

CPU_WORKERS = os.cpu_count()


class Statement:
    __slots__ = (
        "company_key",
        "file",
        "file_renamed",
        "account_key",
        "ID_BATCH",
        "ID_ACCOUNT",
        "checks_and_balances",
        "pdf",
        "ID_STATEMENT",
        "config",
        "company",
        "account",
        "statement_type",
        "config_header",
        "config_lines",
        "header_results",
        "lines_results",
        "success",
    )
    logs: pl.DataFrame = pl.DataFrame(
        schema={
            "file_path": pl.Utf8,
            "function_file": pl.Utf8,
            "function": pl.Utf8,
            "duration": pl.Float64,
            "log_count": pl.Int64,
            "time": pl.Datetime,
            "exception": pl.Utf8,
        }
    )

    def __init__(
        self,
        file: Path,
        company_key: str | None = None,
        account_key: str | None = None,
        ID_BATCH: str | None = None,
        smart_rename: bool = False,
    ):
        self.file = file
        self.file_renamed = None
        self.company_key = company_key
        self.account_key = account_key
        self.ID_BATCH = ID_BATCH
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()
        self.pdf = pdf_open(str(file.absolute()), logs=self.logs)
        self.ID_STATEMENT = self.build_id()
        self.ID_ACCOUNT: str | None = None
        if self.pdf:
            self.config = self.get_config()
            if self.config:
                self.company = self.config.company.company if self.config.company and hasattr(self.config.company, "company") else ""
                self.account = self.config.account if self.config and hasattr(self.config, "account") else ""
                self.statement_type = (
                    self.config.statement_type.statement_type
                    if self.config.statement_type and hasattr(self.config.statement_type, "statement_type")
                    else None
                )
                self.config_header = (
                    self.config.statement_type.header.configs
                    if self.config.statement_type and hasattr(self.config.statement_type, "header")
                    else None
                )
                self.config_lines = (
                    self.config.statement_type.lines.configs
                    if self.config.statement_type and hasattr(self.config.statement_type, "lines")
                    else None
                )

            self.header_results = self.get_results("header")
            self.lines_results = self.get_results("lines")

            # checks and balances
            self.checks_and_balances = self.checks_and_balances.with_columns(
                BAL_PAYMENTS_IN=pl.when(pl.col("STD_PAYMENTS_IN").sub(pl.col("STD_PAYMENT_IN")) == 0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                BAL_PAYMENTS_OUT=pl.when(pl.col("STD_PAYMENTS_OUT").sub(pl.col("STD_PAYMENT_OUT")) == 0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                BAL_MOVEMENT=pl.when(
                    (pl.col("STD_STATEMENT_MOVEMENT").sub(pl.col("STD_MOVEMENT")) == 0)
                    & (pl.col("STD_MOVEMENT").sub(pl.col("STD_BALANCE_OF_PAYMENTS")) == 0)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                BAL_CLOSING=pl.when(
                    (pl.col("STD_CLOSING_BALANCE").sub(pl.col("STD_RUNNING_BALANCE")) == 0)
                    | (
                        pl.col("STD_PAYMENTS_IN")
                        .add(pl.col("STD_PAYMENTS_OUT"))
                        .add(pl.col("STD_PAYMENT_IN").add(pl.col("STD_PAYMENT_OUT")))
                        == 0
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                ZERO_TRANSACTION_STATEMENT=pl.when(
                    pl.col("STD_PAYMENTS_IN").add(pl.col("STD_PAYMENTS_OUT")).add(pl.col("STD_PAYMENT_IN").add(pl.col("STD_PAYMENT_OUT")))
                    == 0
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
            )
        self.logs.rechunk()
        self.success = self.is_successfull()
        if self.success:
            if self.config:
                acct_number = str(self.header_results.select("STD_ACCOUNT_NUMBER").collect().head(1).item()).replace(" ", "")
                self.ID_ACCOUNT = f"{self.config.company_key}_{self.config.account_type_key}_{acct_number}"
                if smart_rename:
                    stmt_date = str(self.header_results.select("STD_STATEMENT_DATE").collect().head(1).item()).replace("-", "")
                    self.file_renamed = f"{self.ID_ACCOUNT}_{str(stmt_date)}.pdf"

    def build_id(self):
        """
        Generates a unique SHA256-based ID for the statement based on the first 765 characters of the PDF text.
        The returned ID is a string in the format "{key1}.{key2}.{key3}", where each key is a SHA256 hash of a slice of the PDF text.
        """
        if not self.pdf:
            return "0"

        text_p1 = "".join([chr["text"] for chr in self.pdf.pages[0].chars])
        bytes_p1 = text_p1.encode("UTF-8")
        id = hashlib.sha512(bytes_p1, usedforsecurity=False).hexdigest()
        text_p1 = None
        bytes_p1 = None
        return id

    def is_successfull(self):
        if (
            self.checks_and_balances.filter(pl.col("ZERO_TRANSACTION_STATEMENT")).height > 0
        ):  # some statments are just a header so there's nothing really to fail
            return True
        if self.header_results.collect().height == 0:
            return False
        elif self.lines_results.collect().height == 0:
            return False
        elif self.checks_and_balances.height == 0:
            return False
        elif self.checks_and_balances.filter(~pl.col("BAL_PAYMENTS_IN")).height > 0:
            return False
        elif self.checks_and_balances.filter(~pl.col("BAL_PAYMENTS_OUT")).height > 0:
            return False
        elif self.checks_and_balances.filter(~pl.col("BAL_MOVEMENT")).height > 0:
            return False
        elif self.checks_and_balances.filter(~pl.col("BAL_CLOSING")).height > 0:
            return False
        return True

    def get_results(self, section: str) -> pl.LazyFrame:
        results: pl.DataFrame = pl.DataFrame()
        if not self.config:
            return results.lazy()
        if section == "header" and self.config_header:
            for config in self.config_header:
                if self.pdf:
                    results.vstack(
                        get_results(
                            self.pdf,
                            section,
                            config,
                            scope="success",
                            logs=self.logs,
                            file_path=str(self.file.absolute()),
                            exclude_last_n_pages=self.config.exclude_last_n_pages,
                        ),
                        in_place=True,
                    )
            if results.height > 0:
                results = results.pivot(values="value", index="section", on="field")
        elif section == "lines" and self.config_lines:
            for config in self.config_lines:
                if self.pdf:
                    results.vstack(
                        get_results(
                            self.pdf,
                            section,
                            config,
                            scope="success",
                            logs=self.logs,
                            file_path=str(self.file.absolute()),
                            exclude_last_n_pages=self.config.exclude_last_n_pages,
                        ),
                        in_place=True,
                    )

        if self.statement_type:
            results = results.pipe(
                get_standard_fields,
                section,
                config_standard_fields,
                self.statement_type,
                self.checks_and_balances,
                # self.logs,
                # str(self.file.absolute()),
            )
        self.logs.rechunk()
        return results.rechunk().lazy()

    def get_config(self) -> Account | None:
        if self.pdf is None:
            return None
        if self.account_key:
            config = get_config_from_account(self.account_key, self.logs, str(self.file.absolute()))
        elif self.company_key:
            config = get_config_from_company(self.company_key, self.pdf, self.logs, str(self.file.absolute()))
        else:
            config = get_config_from_statement(self.pdf, str(self.file.absolute()), self.logs)
        return deepcopy(config) if config else None  # we return a deepcopy in case we need to make statement-specific modifications

    def cleanup(self):
        if self.pdf is not None:
            pdf_close(self.pdf, logs=self.logs, file_path=str(self.file.absolute()))
        self.config = None
        self.config_header = None
        self.config_lines = None
        self.lines_results = pl.LazyFrame()
        self.header_results = pl.LazyFrame()
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()


class StatementBatch:
    __slots__ = (
        "process_time",
        "path",
        "ID_BATCH",
        "__type",
        "company_key",
        "account_key",
        "print_log",
        "pdfs",
        "pdf_count",
        "log",
        "errors",
        "duration_secs",
        "batch_lines",
        "timer_start",
        "statements",
        "turbo",
        "smart_rename",
    )

    def __init__(
        self,
        path: Path,
        company_key: str | None = None,
        account_key: str | None = None,
        print_log: bool = True,
        turbo: bool = False,
        smart_rename: bool = True,
    ):
        print("processing...")
        self.process_time: datetime = datetime.now()
        self.timer_start = time()
        self.path: Path = path
        self.ID_BATCH: str = str(uuid4())
        self.__type = "file" if self.path.is_file() else "folder"
        self.company_key = company_key
        self.account_key = account_key
        self.print_log = print_log
        self.turbo = turbo
        self.smart_rename = smart_rename
        if self.__type == "folder":
            self.pdfs: list[Path] = [file for file in Path(path).iterdir() if file.is_file() and file.suffix == ".pdf"]
        elif self.__type == "file" and Path(path).suffix == ".pdf":
            self.pdfs = [Path(path)]
        else:
            self.pdfs = []
        self.pdf_count: int = len(self.pdfs)
        self.log: list = []
        self.errors: int = 0
        self.duration_secs: float = 0.00
        self.batch_lines = []
        self.statements = []
        if self.turbo:
            asyncio.run(self.process_turbo(), debug=False)
        else:
            self.process()

    def process(self):
        processed_pdfs = self.__process_batch()
        self.db_updates(processed_pdfs)

    def __process_batch(self):
        for id, pdf in enumerate(self.pdfs):
            self.process_single_pdf(id, pdf)

    async def process_turbo(self):
        processed_pdfs = await self.__process_batch_turbo()
        self.db_updates(processed_pdfs)
        return True

    async def __process_batch_turbo(self):
        self.turbo = True
        loop = asyncio.get_running_loop()

        with ProcessPoolExecutor(max_workers=CPU_WORKERS) as executor:
            tasks = [loop.run_in_executor(executor, self.process_single_pdf, id, pdf) for id, pdf in enumerate(self.pdfs)]

            processed_pdfs = await asyncio.gather(*tasks, return_exceptions=True)

        return processed_pdfs

    def process_single_pdf(self, id, pdf):
        batch_lines_file: Path | None = None
        statement_heads_file: Path | None = None
        statement_lines_file: Path | None = None
        cab_file: Path | None = None
        line_start = time()
        batch_line: dict = {}
        batch_line["ID_BATCH"] = self.ID_BATCH
        batch_line["ID_BATCHLINE"] = self.ID_BATCH + "_" + str(id + 1)
        batch_line["ID_STATEMENT"] = ""
        batch_line["STD_BATCH_LINE"] = id + 1
        batch_line["STD_FILENAME"] = pdf.name
        batch_line["STD_ACCOUNT"] = ""
        batch_line["STD_DURATION_SECS"] = 0.00
        batch_line["STD_UPDATETIME"] = datetime.now()
        batch_line["STD_SUCCESS"] = False
        batch_line["STD_ERROR_MESSAGE"] = ""
        batch_line["ERROR_CAB"] = False
        batch_line["ERROR_CONFIG"] = False
        try:
            stmt = Statement(
                file=pdf, company_key=self.company_key, account_key=self.account_key, ID_BATCH=self.ID_BATCH, smart_rename=self.smart_rename
            )
            batch_line["ID_STATEMENT"] = stmt.ID_STATEMENT
            batch_line["STD_ACCOUNT"] = stmt.account
            batch_line["STD_SUCCESS"] = stmt.success
            if not stmt.success:
                self.errors += 1
                batch_line["ERROR_CAB"] = True
                batch_line["STD_ERROR_MESSAGE"] += "** Checks & Balances Failure **"
            else:
                db_statement_heads = db.StatementHeads(statement=stmt, id=id) if self.turbo else db.StatementHeads(statement=stmt)
                if self.turbo:
                    db_statement_heads.create()
                else:
                    db_statement_heads.update()
                statement_heads_file = db_statement_heads.file
                db_statement_heads.cleanup()
                db_statement_heads = None

                db_statement_lines = db.StatementLines(statement=stmt, id=id) if self.turbo else db.StatementLines(statement=stmt)
                if self.turbo:
                    db_statement_lines.create()
                else:
                    db_statement_lines.update()
                statement_lines_file = db_statement_lines.file
                db_statement_lines.cleanup()
                db_statement_lines = None
            db_cab = db.ChecksAndBalances(statement=stmt, id=id) if self.turbo else db.ChecksAndBalances(statement=stmt)
            if self.turbo:
                db_cab.create()
            else:
                db_cab.update()
            cab_file = db_cab.file
            db_cab.cleanup()
            db_cab = None
            file_old = deepcopy(stmt.file)
            file_new = stmt.file_renamed if stmt.file_renamed else str(file_old)
            stmt.cleanup()
            stmt = None
            if self.smart_rename:
                file_old.rename(file_old.with_name(file_new))
        except BaseException as e:
            print(e)
            self.errors += 1
            batch_line["ERROR_CONFIG"] = True
            batch_line["STD_ERROR_MESSAGE"] += "** Configuration Failure **"
        line_end = time()
        batch_line["STD_DURATION_SECS"] = line_end - line_start
        batch_line["STD_UPDATETIME"] = datetime.now()
        if self.turbo:
            db_batch_lines = db.BatchLines(batch_lines=[batch_line], id=id)
            db_batch_lines.create()
            batch_lines_file = db_batch_lines.file
            db_batch_lines.cleanup()
            db_batch_lines = None
        else:
            self.batch_lines.append(batch_line)
        return (batch_lines_file, statement_heads_file, statement_lines_file, cab_file)

    def db_updates(self, processed_pdfs):
        if self.turbo:
            for batch, head, lines, cab in processed_pdfs:
                if batch:
                    bl = db.BatchLines(source_file=batch)
                    bl.update()
                    bl.delete_source_file()
                    bl.cleanup()
                    bl = None
                if head:
                    sh = db.StatementHeads(source_file=head)
                    sh.update()
                    sh.delete_source_file()
                    sh.cleanup()
                    sh = None
                if lines:
                    sl = db.StatementLines(source_file=lines)
                    sl.update()
                    sl.delete_source_file()
                    sl.cleanup()
                    sl = None
                if cab:
                    cb = db.ChecksAndBalances(source_file=cab)
                    cb.update()
                    cb.delete_source_file()
                    cb.cleanup()
                    cb = None
        else:
            db_batch_lines = db.BatchLines(self.batch_lines)
            db_batch_lines.create()
            db_batch_lines.cleanup()
            db_batch_lines = None
        self.duration_secs = time() - self.timer_start
        db_heads = db.BatchHeads(self)
        db_heads.create()
        db_heads.cleanup()
        db_heads = None
