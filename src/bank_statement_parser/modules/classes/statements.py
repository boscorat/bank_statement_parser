import hashlib
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from time import gmtime, strftime, time
from uuid import uuid4

# from typing import Generator
import polars as pl
import polars.selectors as cs

from bank_statement_parser.modules.classes.data import Account
from bank_statement_parser.modules.classes.database import BatchHeads, BatchLines, ChecksAndBalances, StatementHeads, StatementLines
from bank_statement_parser.modules.config import (
    config_standard_fields,
    get_config_from_account,
    get_config_from_company,
    get_config_from_statement,
)
from bank_statement_parser.modules.functions.pdfs import pdf_close, pdf_open
from bank_statement_parser.modules.functions.statements import get_results, get_standard_fields
from bank_statement_parser.modules.paths import CAB


class Statement:
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

    def __init__(self, file: Path, company_key: str | None = None, account_key: str | None = None, ID_BATCH: str | None = None):
        self.file = file
        self.company_key = company_key
        self.account_key = account_key
        self.ID_BATCH = ID_BATCH
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()
        self.pdf = pdf_open(str(file.absolute()), logs=self.logs)
        self._key1 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[:255] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self._key2 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[255:510] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self._key3 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[510:765] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self.ID_STATEMENT = f"{self._key1}.{self._key2}.{self._key3}"
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
        self.export_logs()
        self.success = self.is_successfull()
        if self.success:
            self.db_updates()

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

    def db_updates(self):
        db_heads = StatementHeads(self)
        db_heads.update()
        db_lines = StatementLines(self)
        db_lines.update()
        db_cab = ChecksAndBalances(self)
        db_cab.create()

    # def export_parquet(self):
    #     # Fact_Transactions
    #     record_flags = pl.LazyFrame(
    #         data=[[self.ID_STATEMENT, str(self.file.absolute()), self.file.name, datetime.now()]],
    #         orient="row",
    #         schema={"STD_STATEMENT": pl.Utf8, "STD_FILEPATH": pl.Utf8, "STD_FILENAME": pl.Utf8, "STD_UPDATETIME": pl.Datetime},
    #     )
    #     FACT_Transaction = (
    #         self.lines_results.select(cs.starts_with("STD"))
    #         .join(record_flags, how="cross")
    #         .with_columns(ID_TRANSACTION=pl.col("STD_GUID"), ID_STATEMENT=pl.col("STD_STATEMENT"))
    #     ).select(
    #         cs.starts_with("ID"),
    #         cs.contains("NUMBER"),
    #         "STD_TRANSACTION_DATE",
    #         "STD_TRANSACTION_DESC",
    #         cs.contains("PAYMENT"),
    #         cs.contains("MOVEMENT"),
    #         cs.contains("BALANCE"),
    #         "STD_FILEPATH",
    #         "STD_FILENAME",
    #         "STD_UPDATETIME",
    #     )
    #     # DIM_Statement
    #     DIM_Statement = (
    #         self.header_results.select(cs.starts_with("STD"))
    #         .join(record_flags, how="cross")
    #         .with_columns(
    #             ID_STATEMENT=pl.col("STD_STATEMENT"),
    #             STD_COMPANY=pl.lit(self.company),
    #             STD_ACCOUNT=pl.lit(self.account),
    #             STD_TYPE=pl.lit(self.statement_type),
    #         )
    #         .select(
    #             cs.starts_with("ID"),
    #             "STD_ACCOUNT_NUMBER",
    #             "STD_ACCOUNT_HOLDER",
    #             cs.contains("STATEMENT_DATE"),
    #             cs.contains("PAYMENT"),
    #             cs.contains("MOVEMENT"),
    #             cs.contains("BALANCE"),
    #             "STD_FILEPATH",
    #             "STD_FILENAME",
    #             "STD_UPDATETIME",
    #             "STD_COMPANY",
    #             "STD_ACCOUNT",
    #             "STD_TYPE",
    #         )
    #     )

    #     FACT_Transaction_current = pl.scan_parquet(FACT_TRANSACTION).filter(pl.col("ID_STATEMENT") != self.ID_STATEMENT).drop("index")
    #     try:
    #         export = FACT_Transaction_current.collect().extend(FACT_Transaction.collect())
    #     except FileNotFoundError:
    #         export = FACT_Transaction.collect()
    #     export.sort("STD_UPDATETIME", "STD_TRANSACTION_NUMBER", descending=[False, False]).with_row_index().write_parquet(
    #         FACT_TRANSACTION
    #     )

    #     DIM_Statement_current = pl.scan_parquet(DIM_STATEMENT).filter(pl.col("ID_STATEMENT") != self.ID_STATEMENT).drop("index")
    #     try:
    #         export = DIM_Statement_current.collect().extend(DIM_Statement.collect())
    #     except FileNotFoundError:
    #         export = DIM_Statement.collect()
    #     export.sort("STD_UPDATETIME", descending=False).with_row_index().write_parquet(DIM_STATEMENT)

    def export_logs(self):
        """Export logs to parquet format"""
        # export_log = self.logs.lazy().join(
        #     pl.LazyFrame(
        #         data=[[self.ID_STATEMENT, datetime.now()]], orient="row", schema={"ID_STATEMENT": pl.Utf8, "log_time": pl.Datetime}
        #     ),
        #     how="cross",
        # )

        # # Latest By Statement
        # parquet_path = dir_logs.joinpath("latest_by_statement.parquet")
        # current_log = pl.scan_parquet(parquet_path).filter(pl.col("ID_STATEMENT") != self.ID_STATEMENT)
        # try:
        #     export = export_log.collect().extend(current_log.drop("index").collect())
        # except FileNotFoundError:
        #     export = export_log.collect()
        # export.sort("time").with_row_index().write_parquet(parquet_path)

        # # Latest Run
        # parquet_path = dir_logs.joinpath("latest_run.parquet")
        # current_log = pl.scan_parquet(parquet_path).filter(pl.col("ID_STATMENT") != self.ID_STATEMENT)
        # export_log.sort("time").collect().with_row_index().write_parquet(parquet_path)

        # Checks & Balances
        # export_cab = self.checks_and_balances.lazy().join(
        #     pl.LazyFrame(
        #         data=[[self.ID_STATEMENT, datetime.now()]], orient="row", schema={"ID_STATEMENT": pl.Utf8, "log_time": pl.Datetime}
        #     ),
        #     how="cross",
        # )
        # current_cab = pl.scan_parquet(CAB).filter(pl.col("ID_STATEMENT") != self.ID_STATEMENT)
        # try:
        #     export = export_cab.collect().extend(current_cab.drop("index").collect())
        # except FileNotFoundError:
        #     export = export_cab.collect()
        # export.sort("log_time").with_row_index().write_parquet(CAB)

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

    def close_pdf(self):
        if self.pdf is not None:
            pdf_close(self.pdf, logs=self.logs, file_path=str(self.file.absolute()))
            self.pdf = None


class StatementBatch:
    def __init__(self, path: str, company_key: str | None = None, account_key: str | None = None, print_log: bool = True):
        print("processing...")
        self.process_time: datetime = datetime.now()
        self.path: str = path
        self.ID_BATCH: str = str(uuid4())
        self.__type = "file" if Path(self.path).is_file() else "folder"
        self.company_key = company_key
        self.account_key = account_key
        self.print_log = print_log
        if self.__type == "folder":
            self.pdfs: list[Path] = [file for file in Path(path).iterdir() if file.is_file() and file.suffix == ".pdf"]
        elif self.__type == "file" and Path(path).suffix == ".pdf":
            self.pdfs = [Path(path)]
        else:
            self.pdfs = []
        self.pdf_count: int = len(self.pdfs)
        self.log: list = []
        self.errors: int = 0
        self.duration_secs: int = 0
        self.process_batch()

    def process_batch(self) -> None:
        timer_start: float = time()
        batch_lines: list[dict] = []

        for id, pdf in enumerate(self.pdfs):
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
                stmt = Statement(file=pdf, company_key=self.company_key, account_key=self.account_key, ID_BATCH=self.ID_BATCH)
                batch_line["ID_STATEMENT"] = stmt.ID_STATEMENT
                batch_line["STD_ACCOUNT"] = stmt.account
                batch_line["STD_SUCCESS"] = stmt.success
                if not stmt.success:
                    self.errors += 1
                    batch_line["ERROR_CAB"] = True
                    batch_line["STD_ERROR_MESSAGE"] += "** Checks & Balances Failure **"
            except BaseException as e:
                print(e)
                self.errors += 1
                batch_line["ERROR_CONFIG"] = True
                batch_line["STD_ERROR_MESSAGE"] += "** Configuration Failure **"
            stmt.close_pdf()
            line_end = time()
            batch_line["STD_DURATION_SECS"] = line_end - line_start
            batch_line["STD_UPDATETIME"] = datetime.now()
            batch_lines.append(batch_line)
        timer_end: float = time()
        self.duration_secs = int(timer_end) - int(timer_start)
        self.db_updates(batch_lines)

    def db_updates(self, batch_lines):
        db_heads = BatchHeads(self)
        db_heads.create()
        db_lines = BatchLines(batch_lines)
        db_lines.create()
