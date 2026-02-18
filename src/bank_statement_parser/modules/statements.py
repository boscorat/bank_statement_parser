"""
Bank statement parsing and processing module.

This module provides classes for parsing and processing bank statement PDFs,
extracting transaction data, and validating financial information through
checks and balances calculations.

Classes:
    Statement: Represents a single bank statement PDF with extraction and validation.
    StatementBatch: Handles batch processing of multiple bank statements.
"""

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

import bank_statement_parser.modules.parquet as pq
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
    """
    Represents a single bank statement PDF with data extraction and validation.

    This class handles opening a PDF bank statement, extracting header and line
    data using configured patterns, performing checks and balances validation,
    and generating unique identifiers for the statement.

    Attributes:
        company_key: Optional company identifier for config lookup.
        file: Path to the PDF file.
        file_renamed: New filename after smart rename (if enabled).
        account_key: Optional account identifier for config lookup.
        ID_BATCH: Batch identifier this statement belongs to.
        ID_ACCOUNT: Unique account identifier based on company, account type, and account number.
        checks_and_balances: DataFrame containing validation results for extracted data.
        pdf: Open PDF document handle.
        ID_STATEMENT: Unique SHA512-based identifier for the statement.
        config: Parsed configuration for statement extraction.
        company: Company name from config.
        account: Account number/identifier from statement.
        statement_type: Type of statement (e.g., checking, savings).
        config_header: Header extraction configurations.
        config_lines: Line/transaction extraction configurations.
        header_results: Extracted header data as LazyFrame.
        lines_results: Extracted transaction lines as LazyFrame.
        success: Whether statement processing passed all validation checks.
        config_path: Optional custom path to configuration files.

    Class Attributes:
        logs: DataFrame storing execution logs for debugging and performance tracking.
    """

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
        "config_path",
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
        config_path: Path | None = None,
    ):
        """
        Initialize a Statement object by parsing the PDF and extracting data.

        Args:
            file: Path to the PDF bank statement file.
            company_key: Optional company identifier for configuration lookup.
            account_key: Optional account identifier for configuration lookup.
            ID_BATCH: Optional batch identifier to associate this statement with.
            smart_rename: If True, rename file based on extracted account and date.
            config_path: Optional custom path to configuration files directory.

        The constructor opens the PDF, loads the appropriate extraction config,
        extracts header and line data, performs validation checks, and determines
        if the statement was successfully processed.
        """
        self.file = file
        self.file_renamed = None
        self.company_key = company_key
        self.account_key = account_key
        self.ID_BATCH = ID_BATCH
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()
        self.pdf = pdf_open(str(file.absolute()), logs=self.logs)
        self.ID_STATEMENT = self.build_id()
        self.ID_ACCOUNT: str | None = None
        self.config_path = config_path
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

            # Perform validation checks on extracted financial data
            # Compares calculated totals against stated totals from the statement
            self.checks_and_balances = self.checks_and_balances.with_columns(
                # Verify payments in (deposits) match between extracted and stated values
                BAL_PAYMENTS_IN=pl.when(pl.col("STD_PAYMENTS_IN").sub(pl.col("STD_PAYMENT_IN")) == 0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                # Verify payments out (withdrawals) match between extracted and stated values
                BAL_PAYMENTS_OUT=pl.when(pl.col("STD_PAYMENTS_OUT").sub(pl.col("STD_PAYMENT_OUT")) == 0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                # Verify net movement equals balance change and matches payments difference
                BAL_MOVEMENT=pl.when(
                    (pl.col("STD_STATEMENT_MOVEMENT").sub(pl.col("STD_MOVEMENT")) == 0)
                    & (pl.col("STD_MOVEMENT").sub(pl.col("STD_BALANCE_OF_PAYMENTS")) == 0)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False)),
                # Verify closing balance matches running balance or payments calculation
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
                # Check if statement has zero transactions (header-only statement)
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
                # Build unique account ID from company key, account type, and account number
                acct_number = str(self.header_results.select("STD_ACCOUNT_NUMBER").collect().head(1).item()).replace(" ", "")
                self.ID_ACCOUNT = f"{self.config.company_key}_{self.config.account_type_key}_{acct_number}"
                if smart_rename:
                    # Generate descriptive filename: {account_id}_{date}.pdf
                    stmt_date = str(self.header_results.select("STD_STATEMENT_DATE").collect().head(1).item()).replace("-", "")
                    self.file_renamed = f"{self.ID_ACCOUNT}_{str(stmt_date)}.pdf"

    def build_id(self):
        """
        Generate a unique SHA512-based identifier for the statement.

        Creates a hash based on the text content from the first page of the PDF,
        providing a unique fingerprint for deduplication and identification.

        Returns:
            str: A hex string of the SHA512 hash, or "0" if PDF failed to open.
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
        """
        Determine if the statement was successfully processed and validated.

        A statement is considered successful if:
        - It has valid header and line results (unless it's a zero-transaction statement)
        - All checks and balances validations pass

        Returns:
            bool: True if processing passed all validation checks, False otherwise.
        """
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
        """
        Extract data from the PDF for a given section (header or lines).

        Applies configured extraction patterns to the PDF and transforms
        the results into a standardized format with pivot tables.

        Args:
            section: The section to extract - either "header" or "lines".

        Returns:
            pl.LazyFrame: Extracted data as a Polars LazyFrame with standardized fields.
        """
        results: pl.DataFrame = pl.DataFrame()
        if not self.config:
            return results.lazy()
        if section == "header" and self.config_header:
            # Iterate through header configurations and extract matching data
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
                # Pivot results to have fields as columns for easier access
                results = results.pivot(values="value", index="section", on="field")
        elif section == "lines" and self.config_lines:
            # Iterate through line configurations and extract transaction data
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
            # Apply standard field transformations based on statement type
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
        """
        Load the appropriate configuration for statement extraction.

        Determines which config to use based on provided keys or attempts
        to auto-detect from the statement content.

        Returns:
            Account: Deep copy of the configuration object, or None if not found.
        """
        if self.pdf is None:
            return None
        if self.account_key:
            # Use explicit account key if provided
            config = get_config_from_account(self.account_key, self.logs, str(self.file.absolute()), self.config_path)
        elif self.company_key:
            # Use explicit company key if provided
            config = get_config_from_company(self.company_key, self.pdf, self.logs, str(self.file.absolute()), self.config_path)
        else:
            # Attempt auto-detection from statement content
            config = get_config_from_statement(self.pdf, str(self.file.absolute()), self.logs, self.config_path)
        return deepcopy(config) if config else None  # we return a deepcopy in case we need to make statement-specific modifications

    def cleanup(self):
        """
        Release resources and clear references to aid garbage collection.

        Closes the PDF document and clears large data structures to free memory.
        """
        if self.pdf is not None:
            pdf_close(self.pdf, logs=self.logs, file_path=str(self.file.absolute()))
        self.config = None
        self.config_header = None
        self.config_lines = None
        self.lines_results = pl.LazyFrame()
        self.header_results = pl.LazyFrame()
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()


class StatementBatch:
    """
    Handles batch processing of multiple bank statement PDFs.

    This class manages the processing of multiple bank statements, either
    sequentially or in parallel using multiprocessing. It handles parquet
    file updates, error tracking, and optional file renaming.

    Attributes:
        process_time: Timestamp when batch processing started.
        path: String representation of parent directories of PDFs.
        ID_BATCH: Unique identifier for this batch.
        company_key: Optional company identifier for all statements.
        account_key: Optional account identifier for all statements.
        print_log: Whether to print progress messages.
        pdfs: List of PDF file paths to process.
        pdf_count: Number of PDFs in the batch.
        log: List of log messages.
        errors: Count of failed statement processings.
        duration_secs: Total processing time in seconds.
        process_secs: Time spent processing PDFs.
        parquet_secs: Time spent updating parquet files.
        db_secs: Time spent on database operations.
        batch_lines: List of batch line data for parquet.
        statements: List of processed Statement objects.
        turbo: Whether to use parallel processing.
        smart_rename: Whether to rename files based on extracted data.
        config_path: Optional custom path to configuration files.
        processed_pdfs: List of processed PDF results (tuples or exceptions).
    """

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
        "process_secs",
        "parquet_secs",
        "db_secs",
        "batch_lines",
        "timer_start",
        "statements",
        "turbo",
        "smart_rename",
        "config_path",
        "processed_pdfs",
    )

    def __init__(
        self,
        pdfs: list[Path],
        company_key: str | None = None,
        account_key: str | None = None,
        print_log: bool = True,
        turbo: bool = False,
        smart_rename: bool = True,
        config_path: Path | None = None,
    ):
        """
        Initialize and process a batch of bank statements.

        Args:
            pdfs: List of PDF file paths to process.
            company_key: Optional company identifier for config lookup.
            account_key: Optional account identifier for config lookup.
            print_log: Whether to print progress messages to console.
            turbo: If True, use parallel processing with multiprocessing.
            smart_rename: If True, rename processed files based on extracted data.
            config_path: Optional custom path to configuration files directory.

        The constructor automatically begins processing upon initialization.
        Processing time is tracked in process_secs, and parquet update time
        is tracked separately (call update_parquet() to complete the process).
        """
        print("processing...")
        self.process_time: datetime = datetime.now()
        self.timer_start = time()
        self.ID_BATCH: str = str(uuid4())
        self.company_key = company_key
        self.account_key = account_key
        self.print_log = print_log
        self.turbo = turbo
        self.smart_rename = smart_rename
        self.config_path = config_path
        self.pdfs = pdfs
        # Build path string from unique parent directories of all PDFs
        self.path: str = ", ".join(map(str, set([p.parent for p in self.pdfs])))
        self.pdf_count: int = len(self.pdfs)
        self.log: list = []
        self.errors: int = 0
        self.duration_secs: float = 0.00
        self.process_secs: float = 0.00
        self.parquet_secs: float = 0.00
        self.db_secs: float = 0.00
        self.batch_lines = []
        self.statements = []
        self.processed_pdfs: list[BaseException | tuple] = []
        if self.turbo:
            # Use async parallel processing for better performance
            asyncio.run(self.process_turbo(), debug=False)
            self.process_secs = time() - self.timer_start
            self.duration_secs += self.process_secs
        else:
            # Use sequential processing
            self.process()
            self.process_secs = time() - self.timer_start
            self.duration_secs += self.process_secs

    def process(self):
        """
        Process the batch sequentially.

        This is the main entry point for sequential processing,
        calling the internal batch processor. Results are stored
        in self.processed_pdfs for later parquet file updates via
        update_parquet().
        """
        self.__process_batch()

    def __process_batch(self):
        """
        Internal method to process all PDFs sequentially.

        Iterates through each PDF and processes it individually.
        Results are stored in self.processed_pdfs for later parquet updates.
        """
        for id, pdf in enumerate(self.pdfs):
            self.processed_pdfs.append(self.process_single_pdf(id, pdf))

    async def process_turbo(self):
        """
        Process the batch in parallel using async/await with ProcessPoolExecutor.

        Launches parallel processing and then updates the database with results.

        Returns:
            bool: True when complete.
        """
        self.processed_pdfs = await self.__process_batch_turbo()
        return True

    async def __process_batch_turbo(self):
        """
        Internal async method for parallel PDF processing.

        Uses ProcessPoolExecutor to distribute PDF processing across
        multiple CPU cores for improved performance.

        Returns:
            list: List of processed PDF results from all workers.
        """
        self.turbo = True
        loop = asyncio.get_running_loop()

        with ProcessPoolExecutor(max_workers=CPU_WORKERS) as executor:
            # Submit all PDF processing tasks to the executor
            tasks = [loop.run_in_executor(executor, self.process_single_pdf, id, pdf) for id, pdf in enumerate(self.pdfs)]

            # Wait for all tasks to complete and collect results
            processed_pdfs = await asyncio.gather(*tasks, return_exceptions=True)

        return processed_pdfs

    def process_single_pdf(self, id, pdf):
        """
        Process a single PDF file and save results to parquet files.

        This method handles the complete processing workflow for one PDF:
        1. Creates a Statement object to parse the PDF
        2. Extracts header and line data
        3. Saves results to parquet files
        4. Optionally renames the file based on extracted data
        5. Cleans up resources

        Args:
            id: Index position of this PDF in the batch.
            pdf: Path to the PDF file to process.

        Returns:
            tuple: A tuple containing file paths for batch lines, statement heads,
                   statement lines, and checks & balances parquet files.
        """
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
            # Parse and extract data from the PDF statement
            stmt = Statement(
                file=pdf,
                company_key=self.company_key,
                account_key=self.account_key,
                ID_BATCH=self.ID_BATCH,
                smart_rename=self.smart_rename,
                config_path=self.config_path,
            )
            batch_line["ID_STATEMENT"] = stmt.ID_STATEMENT
            batch_line["STD_ACCOUNT"] = stmt.account
            batch_line["STD_SUCCESS"] = stmt.success
            if not stmt.success:
                # Mark as error if validation checks failed
                self.errors += 1
                batch_line["ERROR_CAB"] = True
                batch_line["STD_ERROR_MESSAGE"] += "** Checks & Balances Failure **"
            else:
                # Save extracted header data
                pq_statement_heads = pq.StatementHeads(statement=stmt, id=id)
                pq_statement_heads.create()
                statement_heads_file = pq_statement_heads.file
                pq_statement_heads.cleanup()
                pq_statement_heads = None

                # Save extracted transaction line data
                pq_statement_lines = pq.StatementLines(statement=stmt, id=id)
                pq_statement_lines.create()
                statement_lines_file = pq_statement_lines.file
                pq_statement_lines.cleanup()
                pq_statement_lines = None

            # Save validation/check results regardless of success
            pq_cab = pq.ChecksAndBalances(statement=stmt, id=id)
            pq_cab.create()
            cab_file = pq_cab.file
            pq_cab.cleanup()
            pq_cab = None

            # Handle file renaming if enabled
            file_old = deepcopy(stmt.file)
            file_new = stmt.file_renamed if stmt.file_renamed else str(file_old)
            stmt.cleanup()
            stmt = None
            if self.smart_rename:
                file_old.rename(file_old.with_name(file_new))
        except BaseException as e:
            # Handle configuration or parsing errors
            print(e)
            self.errors += 1
            batch_line["ERROR_CONFIG"] = True
            batch_line["STD_ERROR_MESSAGE"] += "** Configuration Failure **"

        # Record processing time and timestamp
        line_end = time()
        batch_line["STD_DURATION_SECS"] = line_end - line_start
        batch_line["STD_UPDATETIME"] = datetime.now()

        # Save batch line data (different handling for turbo vs sequential)
        pq_batch_lines = pq.BatchLines(batch_lines=[batch_line], id=id)
        pq_batch_lines.create()
        batch_lines_file = pq_batch_lines.file
        pq_batch_lines.cleanup()
        pq_batch_lines = None

        return (batch_lines_file, statement_heads_file, statement_lines_file, cab_file)

    def update_parquet(self, folder: Path | None = None):
        """
        Update parquet files with processed results from all PDFs.

        Iterates through processed PDFs, handles any exceptions, and updates
        the main parquet files from temporary files created during processing.
        Records timing in parquet_secs and updates total duration.

        This method should be called after processing to finalize the batch
        and write the batch header information.

        Args:
            folder: Optional custom folder path for parquet output.
                    If not provided, uses the default exports/parquet folder.
        """
        custom_batch_lines = None
        custom_statement_heads = None
        custom_statement_lines = None
        custom_cab = None
        custom_batch_heads = None

        if folder:
            folder.mkdir(parents=True, exist_ok=True)
            custom_batch_lines = folder.joinpath("batch_lines.parquet")
            custom_statement_heads = folder.joinpath("statement_heads.parquet")
            custom_statement_lines = folder.joinpath("statement_lines.parquet")
            custom_cab = folder.joinpath("checks_and_balances.parquet")
            custom_batch_heads = folder.joinpath("batch_heads.parquet")

        update_start = time()
        for pdf in self.processed_pdfs:
            # Skip any exceptions that occurred during processing
            if type(pdf) is BaseException:
                return None
            elif type(pdf) is tuple:
                batch, head, lines, cab = pdf
                if batch:
                    bl = pq.BatchLines(source_file=batch, destination_file=custom_batch_lines)
                    bl.update()
                    bl.delete_source_file()
                    bl.cleanup()
                    bl = None
                if head:
                    sh = pq.StatementHeads(source_file=head, destination_file=custom_statement_heads)
                    sh.update()
                    sh.delete_source_file()
                    sh.cleanup()
                    sh = None
                if lines:
                    sl = pq.StatementLines(source_file=lines, destination_file=custom_statement_lines)
                    sl.update()
                    sl.delete_source_file()
                    sl.cleanup()
                    sl = None
                if cab:
                    cb = pq.ChecksAndBalances(source_file=cab, destination_file=custom_cab)
                    cb.update()
                    cb.delete_source_file()
                    cb.cleanup()
                    cb = None

        # Record parquet update time and update total duration
        self.parquet_secs = time() - update_start
        self.duration_secs += self.parquet_secs

        # Write batch header metadata to parquet
        pq_heads = pq.BatchHeads(self, destination_file=custom_batch_heads)
        pq_heads.create()
        pq_heads.cleanup()
        pq_heads = None
