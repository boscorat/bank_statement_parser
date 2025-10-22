import hashlib
import os
import pathlib
import time
from datetime import datetime

import polars as pl
import polars.selectors as cs

from bank_statement_parser.modules.classes.data_definitions import Account
from bank_statement_parser.modules.config import (
    config_standard_fields,
    get_config_from_account,
    get_config_from_company,
    get_config_from_statement,
)
from bank_statement_parser.modules.functions.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.functions.statement_functions import get_results, get_standard_fields

dir_exports = os.path.join(pathlib.Path(__file__).parent.parent.parent.parent, "exports")
dir_parquet = os.path.join(dir_exports, "parquet")
dir_csv = os.path.join(dir_exports, "csv")
dir_logs = os.path.join(dir_exports, "logs")
dir_excel = os.path.join(dir_exports, "excel")
dir_json = os.path.join(dir_exports, "json")


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

    def __init__(self, file_path: str, company_key: str | None = None, account_key: str | None = None):
        self.file_path = file_path
        self.company_key = company_key
        self.account_key = account_key
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()
        self.pdf = pdf_open(file_path, logs=self.logs)
        self._key1 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[:255] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self._key2 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[255:510] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self._key3 = hashlib.sha256(
            bytes([ord(char["text"]) for char in self.pdf.chars[510:765] if len(char["text"]) == 1 and 46 <= ord(char["text"]) <= 122])
        ).hexdigest()
        self.key = f"{self._key1}.{self._key2}.{self._key3}"
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
                self.config_pages = (
                    self.config.statement_type.pages.configs
                    if self.config.statement_type and hasattr(self.config.statement_type, "pages")
                    else None
                )
                self.config_lines = (
                    self.config.statement_type.lines.configs
                    if self.config.statement_type and hasattr(self.config.statement_type, "lines")
                    else None
                )

            self.header_results = self.get_results("header")
            self.page_results = self.get_results("pages")
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
                BAL_CLOSING=pl.when(pl.col("STD_CLOSING_BALANCE").sub(pl.col("STD_RUNNING_BALANCE")) == 0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
            )
        self.logs.rechunk()
        self.export_logs()
        self.success = self.is_successfull()
        if self.success:
            self.export_parquet()

    def is_successfull(self):
        if self.header_results.collect().height == 0:
            return False
        elif self.page_results.collect().height == 0:
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

    def export_parquet(self):
        # Fact_Transactions
        record_flags = pl.LazyFrame(
            data=[[self.key, self.file_path, datetime.now()]],
            orient="row",
            schema={"STD_STATEMENT": pl.Utf8, "STD_FILEPATH": pl.Utf8, "STD_UPDATETIME": pl.Datetime},
        )
        FACT_Transaction = (
            self.lines_results.select(cs.starts_with("STD"))
            .join(record_flags, how="cross")
            .with_columns(ID_TRANSACTION=pl.col("STD_GUID"), ID_STATEMENT=pl.col("STD_STATEMENT"))
        ).select(
            cs.starts_with("ID"),
            cs.contains("NUMBER"),
            cs.contains("TRANSACTION_DATE"),
            cs.contains("PAYMENT"),
            cs.contains("MOVEMENT"),
            cs.contains("BALANCE"),
            "STD_FILEPATH",
            "STD_UPDATETIME",
        )
        # DIM_Statement
        DIM_Statement = (
            self.header_results.select(cs.starts_with("STD"))
            .join(record_flags, how="cross")
            .with_columns(
                ID_STATEMENT=pl.col("STD_STATEMENT"),
                STD_COMPANY=pl.lit(self.company),
                STD_ACCOUNT=pl.lit(self.account),
                STD_TYPE=pl.lit(self.statement_type),
            )
            .select(
                cs.starts_with("ID"),
                "STD_ACCOUNT_NUMBER",
                "STD_ACCOUNT_HOLDER",
                cs.contains("STATEMENT_DATE"),
                cs.contains("PAYMENT"),
                cs.contains("MOVEMENT"),
                cs.contains("BALANCE"),
                "STD_FILEPATH",
                "STD_UPDATETIME",
                "STD_COMPANY",
                "STD_ACCOUNT",
                "STD_TYPE",
            )
        )

        filepath_FACT_Transaction = os.path.join(dir_parquet, "FACT_Transaction.parquet")
        FACT_Transaction_current = pl.scan_parquet(filepath_FACT_Transaction).filter(pl.col("ID_STATEMENT") != self.key).drop("index")
        try:
            export = FACT_Transaction_current.collect().extend(FACT_Transaction.collect())
        except FileNotFoundError:
            export = FACT_Transaction.collect()
        export.sort("STD_UPDATETIME", "STD_TRANSACTION_NUMBER", descending=[False, False]).with_row_index().write_parquet(
            filepath_FACT_Transaction
        )

        filepath_DIM_Statement = os.path.join(dir_parquet, "DIM_Statement.parquet")
        DIM_Statement_current = pl.scan_parquet(filepath_DIM_Statement).filter(pl.col("ID_STATEMENT") != self.key).drop("index")
        try:
            export = DIM_Statement_current.collect().extend(DIM_Statement.collect())
        except FileNotFoundError:
            export = DIM_Statement.collect()
        export.sort("STD_UPDATETIME", descending=False).with_row_index().write_parquet(filepath_DIM_Statement)

    def export_logs(self):
        """Export logs to parquet format"""
        export_log = self.logs.lazy().join(
            pl.LazyFrame(data=[[self.key, datetime.now()]], orient="row", schema={"key": pl.Utf8, "log_time": pl.Datetime}), how="cross"
        )

        # Latest By Statement
        parquet_path = os.path.join(dir_logs, "latest_by_statement.parquet")
        current_log = pl.scan_parquet(parquet_path).filter(pl.col("key") != self.key)
        try:
            export = export_log.collect().extend(current_log.drop("index").collect())
        except FileNotFoundError:
            export = export_log.collect()
        export.sort("time").with_row_index().write_parquet(parquet_path)

        # Latest Run
        parquet_path = os.path.join(dir_logs, "latest_run.parquet")
        current_log = pl.scan_parquet(parquet_path).filter(pl.col("key") != self.key)
        export_log.sort("time").collect().with_row_index().write_parquet(parquet_path)

        # Checks & Balances
        export_cab = self.checks_and_balances.lazy().join(
            pl.LazyFrame(data=[[self.key, datetime.now()]], orient="row", schema={"key": pl.Utf8, "log_time": pl.Datetime}), how="cross"
        )
        parquet_path = os.path.join(dir_logs, "checks_and_balances.parquet")
        current_cab = pl.scan_parquet(parquet_path).filter(pl.col("key") != self.key)
        try:
            export = export_cab.collect().extend(current_cab.drop("index").collect())
        except FileNotFoundError:
            export = export_cab.collect()
        export.sort("log_time").with_row_index().write_parquet(parquet_path)

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
                            file_path=self.file_path,
                            exclude_last_n_pages=self.config.exclude_last_n_pages,
                        ),
                        in_place=True,
                    )
            if results.height > 0:
                results = results.pivot(values="value", index="section", on="field")
        elif section == "pages" and self.config_pages:
            for config in self.config_pages:
                if self.pdf:
                    results.vstack(
                        get_results(
                            self.pdf,
                            section,
                            config,
                            scope="success",
                            logs=self.logs,
                            file_path=self.file_path,
                            exclude_last_n_pages=self.config.exclude_last_n_pages,
                        ),
                        in_place=True,
                    )
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
                            file_path=self.file_path,
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
                self.logs,
                self.file_path,
            )
        self.logs.rechunk()
        return results.rechunk().lazy()

    def get_config(self) -> Account | None:
        if self.pdf is None:
            return None
        start = time.time()
        if self.account_key:
            config = get_config_from_account(self.account_key, self.logs, self.file_path)
        elif self.company_key:
            config = get_config_from_company(self.company_key, self.pdf, self.logs, self.file_path)
        else:
            config = get_config_from_statement(self.pdf, self.file_path, self.logs)
        log = pl.DataFrame(
            [[self.file_path, "statement_classes", "get_config", time.time() - start, 1, datetime.now(), ""]],
            schema=self.logs.schema,
            orient="row",
        )
        self.logs.vstack(log, in_place=True)
        return config if config else None

    def close_pdf(self):
        if self.pdf is not None:
            pdf_close(self.pdf, logs=self.logs, file_path=self.file_path)
            self.pdf = None


folder = "/home/boscorat/Downloads/2025/quarantine/compare"
dir_list = os.listdir(folder)

for id, file in enumerate(dir_list):
    if file in ["quarantine", "success"] or id < 0:
        continue
    file_path = os.path.join(folder, file)
    print(f"\n\n{file_path.center(80, '=')}")
    stmt = Statement(file_path)
    print(f"\n\n{(stmt.company + '---' + stmt.account).center(80, '=')}")
    # print(f"\n KEY: {stmt.key}\n")
    print(f"\n SUCCESS: {stmt.success}\n")
    if not stmt.success:
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(stmt.checks_and_balances)
    print()

with pl.Config(tbl_cols=-1, tbl_rows=-1, set_fmt_str_lengths=100):
    path = os.path.join(dir_parquet, "DIM_Statement.parquet")
    print(pl.read_parquet(path))
    print(pl.read_parquet(path).select("STD_FILEPATH", "STD_STATEMENT_DATE", "STD_ACCOUNT").sort("STD_ACCOUNT", "STD_STATEMENT_DATE"))
    # path = os.path.join(dir_logs, "checks_and_balances.parquet")
    # print(pl.read_parquet(path))
    # path = os.path.join(dir_parquet, "FACT_Transaction.parquet")
    # print(pl.read_parquet(path))
