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
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from time import time
from uuid import uuid4

import polars as pl

import bank_statement_parser.modules.config as _config_module
import bank_statement_parser.modules.parquet as pq
from bank_statement_parser.modules.config import (
    get_config_from_account,
    get_config_from_company,
    get_config_from_statement,
)
from bank_statement_parser.modules.data import Account, PdfResult, StandardFields
from bank_statement_parser.modules.database import update_db
from bank_statement_parser.modules.paths import get_paths, validate_or_initialise_project
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
        file_renamed: Canonical filename ``{id_account}_{YYYYMMDD}.pdf`` computed on
            successful processing; ``None`` if processing failed or no config matched.
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
        project_path: Optional custom project root directory.
        logs: DataFrame storing execution logs for debugging and performance tracking.
    """

    __slots__ = (
        "company_key",
        "file",
        "file_absolute",
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
        "error_message",
        "project_path",
        "skip_project_validation",
        "logs",
    )

    def __init__(
        self,
        file: Path,
        company_key: str | None = None,
        account_key: str | None = None,
        ID_BATCH: str | None = None,
        project_path: Path | None = None,
        skip_project_validation: bool = False,
    ):
        """
        Initialize a Statement object by parsing the PDF and extracting data.

        Args:
            file: Path to the PDF bank statement file.
            company_key: Optional company identifier for configuration lookup.
            account_key: Optional account identifier for configuration lookup.
            ID_BATCH: Optional batch identifier to associate this statement with.
            project_path: Optional custom project root directory.  When ``None``
                (the default), the bundled ``project/`` directory inside the
                package is used and will be initialised automatically if its
                database is absent.
            skip_project_validation: If True, skip the project validation / initialisation
                step.  Pass ``True`` when constructing a ``Statement`` from inside a
                ``StatementBatch``, which already ran validation once during its own
                ``__init__``.

        The constructor opens the PDF, loads the appropriate extraction config,
        extracts header and line data, performs validation checks, and determines
        if the statement was successfully processed.  When successful, ``file_renamed``
        is always populated with the canonical ``{id_account}_{YYYYMMDD}.pdf`` name
        regardless of whether the caller intends to copy the file anywhere.

        This constructor never raises.  If any step fails, ``success`` is set to
        ``False``, ``error_message`` is populated with a description of the failure,
        and the traceback is written to stderr.  ``ID_STATEMENT`` is always set — to
        the SHA512 hash of the first page when the PDF opens successfully, or to the
        sentinel ``"<PDF ERROR>"`` when it does not.
        """
        if not skip_project_validation:
            validate_or_initialise_project(get_paths(project_path).root)
        self.logs: pl.DataFrame = pl.DataFrame(
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
        self.file = file
        self.file_absolute: str = str(file.absolute())
        self.file_renamed = None
        self.company_key = company_key
        self.account_key = account_key
        self.ID_BATCH = ID_BATCH
        self.project_path = project_path
        self.skip_project_validation = skip_project_validation

        # Safe defaults — ensure every slot is initialised before the processing
        # try block so that is_successfull() and cleanup() never hit an
        # uninitialised-slot AttributeError regardless of where a failure occurs.
        self.error_message: str = ""
        self.ID_STATEMENT: str = "<PDF ERROR>"
        self.ID_ACCOUNT: str | None = None
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()
        self.pdf = None
        self.config = None
        self.company: str = ""
        self.account: str = ""
        self.statement_type = None
        self.config_header = None
        self.config_lines = None
        self.header_results: pl.LazyFrame = pl.LazyFrame()
        self.lines_results: pl.LazyFrame = pl.LazyFrame()
        self.success: bool = False

        try:
            self.pdf = pdf_open(self.file_absolute, logs=self.logs)
            self.ID_STATEMENT = self.build_id()
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
                        pl.col("STD_PAYMENTS_IN")
                        .add(pl.col("STD_PAYMENTS_OUT"))
                        .add(pl.col("STD_PAYMENT_IN").add(pl.col("STD_PAYMENT_OUT")))
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
                    # Always populate the canonical rename target for use by copy_statements_to_project
                    stmt_date = str(self.header_results.select("STD_STATEMENT_DATE").collect().head(1).item()).replace("-", "")
                    self.file_renamed = f"{self.ID_ACCOUNT}_{str(stmt_date)}.pdf"
        except Exception as e:
            self.error_message = f"** Configuration Failure **: {e}"
            self.success = False
            traceback.print_exc(file=sys.stderr)

    def build_id(self):
        """
        Generate a unique SHA512-based identifier for the statement.

        Creates a hash based on the text content from the first page of the PDF,
        providing a unique fingerprint for deduplication and identification.

        Returns:
            str: A hex string of the SHA512 hash, or ``"<PDF ERROR>"`` if PDF failed to open.
        """
        if not self.pdf:
            return "<PDF ERROR>"

        text_p1 = "".join([chr["text"] for chr in self.pdf.pages[0].chars])
        bytes_p1 = text_p1.encode("UTF-8")
        stmt_hash = hashlib.sha512(bytes_p1, usedforsecurity=False).hexdigest()
        text_p1 = None
        bytes_p1 = None
        return stmt_hash

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
                            file_path=self.file_absolute,
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
                            file_path=self.file_absolute,
                            exclude_last_n_pages=self.config.exclude_last_n_pages,
                        ),
                        in_place=True,
                    )

        if self.statement_type:
            # Resolve config_standard_fields at call time via the module reference so that:
            #   a) the lazy __getattr__ singleton is not frozen at import time, and
            #   b) a custom project_path on this Statement is respected.
            if self.project_path:
                from bank_statement_parser.modules.config import ConfigManager

                std_fields: dict[str, StandardFields] = ConfigManager(self.project_path).standard_fields
            else:
                std_fields = _config_module.config_standard_fields  # type: ignore[assignment]
            # Apply standard field transformations based on statement type
            results = results.pipe(
                get_standard_fields,
                section,
                std_fields,
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
            config = get_config_from_account(self.account_key, self.logs, self.file_absolute, self.project_path)
        elif self.company_key:
            # Use explicit company key if provided
            config = get_config_from_company(self.company_key, self.pdf, self.logs, self.file_absolute, self.project_path)
        else:
            # Attempt auto-detection from statement content
            config = get_config_from_statement(self.pdf, self.file_absolute, self.logs, self.project_path)
        return deepcopy(config) if config else None  # we return a deepcopy in case we need to make statement-specific modifications

    def cleanup(self):
        """
        Release resources and clear references to aid garbage collection.

        Closes the PDF document and clears large data structures to free memory.
        """
        if self.pdf is not None:
            pdf_close(self.pdf, logs=self.logs, file_path=self.file_absolute)
        self.config = None
        self.config_header = None
        self.config_lines = None
        self.lines_results = pl.LazyFrame()
        self.header_results = pl.LazyFrame()
        self.checks_and_balances: pl.DataFrame = pl.DataFrame()


def _cab_detail(cab: pl.DataFrame) -> str:
    """
    Build a human-readable indented detail block for a checks & balances failure.

    Inspects the four boolean check columns in *cab* and, for each that is
    ``False``, appends an indented line showing the relevant stated vs.
    extracted figures and their delta.

    Args:
        cab: The ``checks_and_balances`` DataFrame from a :class:`Statement`.

    Returns:
        A string that starts with ``"\\n"`` followed by one indented line per
        failing check, or ``""`` if *cab* is empty or all checks pass.
    """
    if cab.is_empty():
        return ""
    row = cab.row(0, named=True)
    lines: list[str] = []
    if not row["BAL_PAYMENTS_IN"]:
        stated = row["STD_PAYMENTS_IN"]
        extracted = row["STD_PAYMENT_IN"]
        lines.append(f"  BAL_PAYMENTS_IN   stated={stated}  extracted={extracted}  delta={round(stated - extracted, 2)}")
    if not row["BAL_PAYMENTS_OUT"]:
        stated = row["STD_PAYMENTS_OUT"]
        extracted = row["STD_PAYMENT_OUT"]
        lines.append(f"  BAL_PAYMENTS_OUT  stated={stated}  extracted={extracted}  delta={round(stated - extracted, 2)}")
    if not row["BAL_MOVEMENT"]:
        lines.append(
            f"  BAL_MOVEMENT      statement_movement={row['STD_STATEMENT_MOVEMENT']}"
            f"  extracted_movement={row['STD_MOVEMENT']}"
            f"  balance_of_payments={row['STD_BALANCE_OF_PAYMENTS']}"
        )
    if not row["BAL_CLOSING"]:
        stated = row["STD_CLOSING_BALANCE"]
        calculated = row["STD_RUNNING_BALANCE"]
        lines.append(f"  BAL_CLOSING       stated={stated}  calculated={calculated}  delta={round(stated - calculated, 2)}")
    return ("\n" + "\n".join(lines)) if lines else ""


def process_pdf_statement(
    idx: int,
    pdf: Path,
    batch_id: str,
    company_key: str | None,
    account_key: str | None,
    project_path: Path | None,
    skip_project_validation: bool = False,
) -> PdfResult:
    """
    Process a single bank statement PDF and save results to parquet files.

    This standalone function handles the complete processing workflow for one PDF:
    1. Creates a Statement object to parse the PDF
    2. Extracts header and line data
    3. Saves results to parquet files (temp files keyed by *idx*)
    4. Cleans up resources

    File copying to the project ``statements/`` directory is handled separately
    via :func:`copy_statements_to_project`, which operates on the full list of
    processed results after all PDFs have been hashed and processed.

    Args:
        idx: Index position of this PDF in the batch.
        pdf: Path to the PDF file to process.
        batch_id: Unique identifier for the batch this PDF belongs to.
        company_key: Optional company identifier for config lookup.
        account_key: Optional account identifier for config lookup.
        project_path: Optional project root directory.
        skip_project_validation: If True, skip the project validation / initialisation
            step inside ``Statement.__init__``.  Pass ``True`` when this function is
            called from ``StatementBatch``, which already validated the project once.

    Returns:
        PdfResult: Named tuple with fields ``batch_lines_stem``, ``statement_heads_stem``,
            ``statement_lines_stem``, ``cab_stem``, ``file_src``, ``file_dst``,
            ``error_cab``, and ``error_config``.  See :data:`PdfResult` for full field
            descriptions.
    """
    paths = get_paths(project_path)
    batch_lines_stem: str | None = None
    statement_heads_stem: str | None = None
    statement_lines_stem: str | None = None
    cab_stem: str | None = None
    file_src: str | None = None
    file_dst: str | None = None
    error_cab: bool = False
    error_config: bool = False
    line_start = time()
    batch_line: dict = {}
    batch_line["ID_BATCH"] = batch_id
    batch_line["ID_BATCHLINE"] = batch_id + "_" + str(idx + 1)
    batch_line["ID_STATEMENT"] = ""
    batch_line["STD_BATCH_LINE"] = idx + 1
    batch_line["STD_FILENAME"] = pdf.name
    batch_line["STD_ACCOUNT"] = ""
    batch_line["STD_DURATION_SECS"] = 0.00
    batch_line["STD_UPDATETIME"] = datetime.now()
    batch_line["STD_SUCCESS"] = False
    batch_line["STD_ERROR_MESSAGE"] = ""
    batch_line["ERROR_CAB"] = False
    batch_line["ERROR_CONFIG"] = False
    try:
        # Parse and extract data from the PDF statement.
        # Statement.__init__ never raises — on any internal failure it sets
        # stmt.error_message, stmt.success = False, and returns a usable object.
        stmt = Statement(
            file=pdf,
            company_key=company_key,
            account_key=account_key,
            ID_BATCH=batch_id,
            project_path=project_path,
            skip_project_validation=skip_project_validation,
        )
        batch_line["ID_STATEMENT"] = stmt.ID_STATEMENT
        batch_line["STD_ACCOUNT"] = stmt.account
        batch_line["STD_SUCCESS"] = stmt.success

        if stmt.error_message:
            # Statement construction failed (config / PDF error)
            error_config = True
            batch_line["ERROR_CONFIG"] = True
            batch_line["STD_ERROR_MESSAGE"] += stmt.error_message
            print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {stmt.error_message}")
        elif not stmt.success:
            # Statement parsed but failed checks & balances validation
            error_cab = True
            batch_line["ERROR_CAB"] = True
            detail = _cab_detail(stmt.checks_and_balances)
            cab_message = f"** Checks & Balances Failure **{detail}"
            batch_line["STD_ERROR_MESSAGE"] += cab_message
            print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {cab_message}")
        else:
            # Statement parsed and validated successfully — persist to parquet.
            # A write failure is appended to the error message and marks the
            # batch line as unsuccessful; it does not prevent the CAB write below.
            try:
                # Save extracted header data
                pq_statement_heads = pq.StatementHeads(
                    id_statement=stmt.ID_STATEMENT,
                    id_batchline=batch_line["ID_BATCHLINE"],
                    id_account=stmt.ID_ACCOUNT,
                    company=stmt.company,
                    statement_type=stmt.statement_type,
                    account=stmt.account,
                    header_results=stmt.header_results,
                    id=idx,
                    project_path=project_path,
                )
                pq_statement_heads.create()
                statement_heads_stem = paths.statement_heads_temp_stem(idx)
                pq_statement_heads.cleanup()
                pq_statement_heads = None

                # Save extracted transaction line data
                pq_statement_lines = pq.StatementLines(
                    id_statement=stmt.ID_STATEMENT,
                    lines_results=stmt.lines_results,
                    id=idx,
                    project_path=project_path,
                )
                pq_statement_lines.create()
                statement_lines_stem = paths.statement_lines_temp_stem(idx)
                pq_statement_lines.cleanup()
                pq_statement_lines = None
            except Exception as e:
                parquet_error = f"** Parquet Write Failure **: {e}"
                batch_line["STD_ERROR_MESSAGE"] += parquet_error
                batch_line["STD_SUCCESS"] = False
                error_config = True
                batch_line["ERROR_CONFIG"] = True
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {parquet_error}")
                traceback.print_exc(file=sys.stderr)

        # Save checks & balances only when the DataFrame is populated — an empty
        # frame means Statement construction failed before the validation step ran,
        # and attempting to write it would produce a misleading secondary error.
        if not stmt.checks_and_balances.is_empty():
            try:
                pq_cab = pq.ChecksAndBalances(
                    id_statement=stmt.ID_STATEMENT,
                    id_batch=stmt.ID_BATCH,
                    checks_and_balances=stmt.checks_and_balances,
                    id=idx,
                    project_path=project_path,
                )
                pq_cab.create()
                cab_stem = paths.cab_temp_stem(idx)
                pq_cab.cleanup()
                pq_cab = None
            except Exception as e:
                cab_error = f"** CAB Parquet Write Failure **: {e}"
                batch_line["STD_ERROR_MESSAGE"] += cab_error
                batch_line["STD_SUCCESS"] = False
                error_config = True
                batch_line["ERROR_CONFIG"] = True
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {cab_error}")
                traceback.print_exc(file=sys.stderr)

        # Capture source path and rename target before destroying the statement object
        file_src = str(stmt.file.absolute())
        file_dst = stmt.file_renamed  # bare filename string, or None
        stmt.cleanup()
        stmt = None
    except Exception as e:
        # Last-resort guard — should not be reached under normal operation now that
        # Statement.__init__ is non-raising, but kept to protect against unexpected
        # failures outside the statement constructor (e.g. path resolution errors).
        error_config = True
        batch_line["ERROR_CONFIG"] = True
        error_message = f"** Unexpected Failure **: {e}"
        batch_line["STD_ERROR_MESSAGE"] += error_message
        print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {error_message}")
        traceback.print_exc(file=sys.stderr)

    # Record processing time and timestamp
    line_end = time()
    batch_line["STD_DURATION_SECS"] = line_end - line_start
    batch_line["STD_UPDATETIME"] = datetime.now()

    # Save batch line data
    pq_batch_lines = pq.BatchLines(batch_lines=[batch_line], id=idx, project_path=project_path)
    pq_batch_lines.create()
    batch_lines_stem = paths.batch_lines_temp_stem(idx)
    pq_batch_lines.cleanup()
    pq_batch_lines = None

    return PdfResult(
        batch_lines_stem=batch_lines_stem,
        statement_heads_stem=statement_heads_stem,
        statement_lines_stem=statement_lines_stem,
        cab_stem=cab_stem,
        file_src=file_src,
        file_dst=file_dst,
        error_cab=error_cab,
        error_config=error_config,
    )


def delete_temp_files(
    processed_pdfs: list[BaseException | PdfResult],
    project_path: Path | None = None,
) -> None:
    """
    Delete temporary parquet files created during batch processing.

    Cleans up the temporary parquet files that were created when processing
    each PDF. Should be called after calling both update_parquet() and
    update_db() to ensure data has been persisted before deletion.

    Args:
        processed_pdfs: List of :class:`PdfResult` entries as returned by
            :func:`process_pdf_statement`, or :class:`BaseException` for any
            entry that raised an unhandled worker error.
        project_path: Optional project root directory used to resolve stems
            to full paths.
    """
    paths = get_paths(project_path)
    for pdf in processed_pdfs:
        if isinstance(pdf, BaseException):
            return None
        elif isinstance(pdf, PdfResult):
            for stem in (pdf.batch_lines_stem, pdf.statement_heads_stem, pdf.statement_lines_stem, pdf.cab_stem):
                if stem is not None:
                    full_path = paths.parquet / f"{stem}.parquet"
                    if full_path.exists():
                        full_path.unlink()


def update_parquet(
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
    Update parquet files with processed results from all PDFs in a batch.

    Iterates through processed PDFs, handles any exceptions, and updates
    the main parquet files from temporary files created during processing.
    Also writes batch header metadata. Should be called after all PDFs have
    been processed to finalise the batch.

    Args:
        processed_pdfs: List of :class:`PdfResult` entries as returned by
            :func:`process_pdf_statement`, or :class:`BaseException` for any
            entry that raised an unhandled worker error.
        batch_id: Unique identifier for this batch.
        path: String representation of parent directories of the processed PDFs.
        company_key: Optional company identifier used for this batch.
        account_key: Optional account identifier used for this batch.
        pdf_count: Total number of PDFs in the batch.
        errors: Count of failed statement processings.
        duration_secs: Total processing time accumulated so far (seconds).
        process_time: Timestamp when batch processing started.
        project_path: Optional project root directory for parquet output.
            If not provided, uses the default project folder.

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
                bl = pq.BatchLines(source_filename=pdf.batch_lines_stem, project_path=project_path)
                bl.update()
                bl.cleanup()
                bl = None
            if pdf.statement_heads_stem:
                sh = pq.StatementHeads(source_filename=pdf.statement_heads_stem, project_path=project_path)
                sh.update()
                sh.cleanup()
                sh = None
            if pdf.statement_lines_stem:
                sl = pq.StatementLines(source_filename=pdf.statement_lines_stem, project_path=project_path)
                sl.update()
                sl.cleanup()
                sl = None
            if pdf.cab_stem:
                cb = pq.ChecksAndBalances(source_filename=pdf.cab_stem, project_path=project_path)
                cb.update()
                cb.cleanup()
                cb = None

    parquet_secs = time() - update_start

    # Write batch header metadata to parquet
    pq_heads = pq.BatchHeads(
        batch_id=batch_id,
        path=path,
        company_key=company_key,
        account_key=account_key,
        pdf_count=pdf_count,
        errors=errors,
        duration_secs=duration_secs + parquet_secs,
        process_time=process_time,
        project_path=project_path,
    )
    pq_heads.create()
    pq_heads.cleanup()
    pq_heads = None

    return parquet_secs


def copy_statements_to_project(
    processed_pdfs: list[BaseException | PdfResult],
    project_path: Path | None = None,
) -> list[Path]:
    """
    Copy processed statement PDFs into the project ``statements/`` directory.

    Each PDF is copied (not moved) to::

        <project>/statements/<year>/<id_account>/<filename>

    where *year* is derived from the last eight characters of the target filename
    stem (``YYYYMMDD``) and *id_account* is everything before the trailing
    ``_YYYYMMDD`` suffix.

    If two statements in the same batch resolve to the same destination path the
    later copy overwrites the earlier one, which is consistent with the
    ``INSERT OR REPLACE`` semantics used by :func:`update_db` and the merge
    behaviour of :func:`update_parquet`.

    Entries in *processed_pdfs* that are a :class:`BaseException` (a fatal
    worker error) or that carry no rename target (``file_dst`` is ``None``) are
    silently skipped.

    Args:
        processed_pdfs: List of :class:`PdfResult` entries as returned by
            :func:`process_pdf_statement`, or :class:`BaseException` for any
            entry that raised an unhandled worker error.
        project_path: Optional project root directory.  When ``None`` the
            default bundled ``project/`` directory is used.

    Returns:
        List of :class:`~pathlib.Path` objects for every file that was copied.
    """
    paths = get_paths(project_path)
    copied: list[Path] = []
    for entry in processed_pdfs:
        if not isinstance(entry, PdfResult):
            continue
        if entry.file_src is None or entry.file_dst is None:
            continue
        # Derive year from the last 8 characters of the stem (YYYYMMDD)
        stem = Path(entry.file_dst).stem  # e.g. "HSBC_UK_SAV_41462695_20210328"
        year = stem[-8:-4]  # characters 0-3 of the date portion → "2021"
        # id_account is everything before the trailing "_YYYYMMDD"
        id_account = stem[: -(len("_YYYYMMDD"))]  # strip "_" + 8 date chars
        dest_dir = paths.statements_dir(year, id_account)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / entry.file_dst
        Path(entry.file_src).copy(dest_path)
        copied.append(dest_path)
    return copied


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
        project_path: Optional custom project root directory.
        processed_pdfs: List of processed PDF results (PdfResult entries or exceptions).
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
        "project_path",
        "skip_project_validation",
        "processed_pdfs",
    )

    def __init__(
        self,
        pdfs: list[Path],
        company_key: str | None = None,
        account_key: str | None = None,
        print_log: bool = True,
        turbo: bool = False,
        project_path: Path | None = None,
        skip_project_validation: bool = False,
    ):
        """
        Initialize and process a batch of bank statements.

        Args:
            pdfs: List of PDF file paths to process.
            company_key: Optional company identifier for config lookup.
            account_key: Optional account identifier for config lookup.
            print_log: Whether to print progress messages to console.
            turbo: If True, use parallel processing with multiprocessing.
            project_path: Optional custom project root directory.  When ``None``
                (the default), the bundled ``project/`` directory inside the
                package is used and will be initialised automatically if its
                database is absent.
            skip_project_validation: If True, skip the project validation /
                initialisation step.  Rarely needed externally; exists for
                symmetry with :class:`Statement`.

        The constructor automatically begins processing upon initialization.
        Processing time is tracked in process_secs, and parquet update time
        is tracked separately (call update_parquet() to complete the process).
        To copy processed PDFs into the project statements directory call
        copy_statements_to_project() after processing.
        """
        if not skip_project_validation:
            validate_or_initialise_project(get_paths(project_path).root)
        print("processing...")
        self.process_time: datetime = datetime.now()
        self.timer_start = time()
        self.ID_BATCH: str = str(uuid4())
        self.company_key = company_key
        self.account_key = account_key
        self.print_log = print_log
        self.turbo = turbo
        self.project_path = project_path
        self.skip_project_validation = skip_project_validation
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
        self.processed_pdfs: list[BaseException | PdfResult] = []
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
        Error counting is done here in the parent process for consistency with
        the turbo path.
        """
        for idx, pdf in enumerate(self.pdfs):
            result = self.process_single_pdf(idx, pdf)
            self.processed_pdfs.append(result)
            if isinstance(result, PdfResult) and (result.error_cab or result.error_config):
                self.errors += 1

    async def process_turbo(self):
        """
        Process the batch in parallel using async/await with ProcessPoolExecutor.

        Launches parallel processing and then updates the database with results.
        Error counting is deferred to here (the parent process) because worker
        mutations to ``self.errors`` across process boundaries are lost.

        Returns:
            bool: True when complete.
        """
        self.processed_pdfs = await self.__process_batch_turbo()
        for result in self.processed_pdfs:
            if isinstance(result, PdfResult) and (result.error_cab or result.error_config):
                self.errors += 1
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
            tasks = [loop.run_in_executor(executor, self.process_single_pdf, idx, pdf) for idx, pdf in enumerate(self.pdfs)]

            # Wait for all tasks to complete and collect results
            processed_pdfs = await asyncio.gather(*tasks, return_exceptions=True)

        return processed_pdfs

    def process_single_pdf(self, idx: int, pdf: Path) -> PdfResult:
        """
        Process a single PDF file and save results to parquet files.

        Delegates to the module-level `process_pdf_statement` function, passing
        only the data required (no reference to this StatementBatch instance).
        Returns a :class:`PdfResult` so the parent process can count errors
        after all workers complete (necessary for turbo mode where worker-side
        mutations to ``self.errors`` are lost across process boundaries).

        Args:
            idx: Index position of this PDF in the batch.
            pdf: Path to the PDF file to process.

        Returns:
            PdfResult: Named tuple with all stems, source/dest paths, and error flags.
                See :data:`~bank_statement_parser.modules.data.PdfResult` for field descriptions.
        """
        return process_pdf_statement(
            idx=idx,
            pdf=pdf,
            batch_id=self.ID_BATCH,
            company_key=self.company_key,
            account_key=self.account_key,
            project_path=self.project_path,
            skip_project_validation=True,
        )

    def update_parquet(self, project_path: Path | None = None) -> None:
        """
        Update parquet files with processed results from all PDFs.

        Delegates to the module-level `update_parquet` function, passing only
        the data required (no reference to this StatementBatch instance).
        Records timing in parquet_secs and updates total duration.

        This method should be called after processing to finalize the batch
        and write the batch header information.

        Args:
            project_path: Optional project root directory for parquet output.
                If not provided, uses the project_path set on this batch, or
                the default project folder.
        """
        self.parquet_secs = update_parquet(
            processed_pdfs=self.processed_pdfs,
            batch_id=self.ID_BATCH,
            path=self.path,
            company_key=self.company_key,
            account_key=self.account_key,
            pdf_count=self.pdf_count,
            errors=self.errors,
            duration_secs=self.duration_secs,
            process_time=self.process_time,
            project_path=project_path if project_path is not None else self.project_path,
        )
        self.duration_secs += self.parquet_secs

    def copy_statements_to_project(self, project_path: Path | None = None) -> list[Path]:
        """
        Copy processed statement PDFs into the project ``statements/`` directory.

        Delegates to the module-level :func:`copy_statements_to_project` function.
        Each PDF is copied (not moved) to::

            <project>/statements/<year>/<id_account>/<filename>

        This method must be called after the batch has finished processing.
        It is safe to call even when two statements in the batch resolve to the
        same destination — the later copy overwrites the earlier one, matching
        the ``INSERT OR REPLACE`` semantics used by :meth:`update_db` and the
        merge behaviour of :meth:`update_parquet`.

        Args:
            project_path: Optional project root directory.  If not provided,
                uses the project_path set on this batch, or the default
                project folder.

        Returns:
            List of :class:`~pathlib.Path` objects for every file that was copied.
        """
        return copy_statements_to_project(
            processed_pdfs=self.processed_pdfs,
            project_path=project_path if project_path is not None else self.project_path,
        )

    def delete_temp_files(self) -> None:
        """
        Delete temporary parquet files created during processing.

        Delegates to the module-level `delete_temp_files` function, passing
        only the processed PDF results list (no reference to this instance).

        This method cleans up the temporary parquet files that were created
        when processing each PDF. Call this method after calling both
        update_parquet() and update_db() to persist the data in multiple
        formats.
        """
        delete_temp_files(self.processed_pdfs, self.project_path)

    def update_db(self, project_path: Path | None = None):
        """
        Update database tables with processed results from all PDFs.

        Delegates to the module-level `update_db` function in database.py,
        passing only the data required (no reference to this StatementBatch
        instance). Records timing in db_secs and updates total duration.

        This method should be called after processing to finalize the batch
        and write the batch header information to the database.

        Args:
            project_path: Optional project root directory.
                If not provided, uses the project_path set on this batch, or
                the default project directory.
        """
        self.db_secs = update_db(
            processed_pdfs=self.processed_pdfs,
            batch_id=self.ID_BATCH,
            path=self.path,
            company_key=self.company_key,
            account_key=self.account_key,
            pdf_count=self.pdf_count,
            errors=self.errors,
            duration_secs=self.duration_secs,
            process_time=self.process_time,
            project_path=project_path if project_path is not None else self.project_path,
        )
        self.duration_secs += self.db_secs

    def __del__(self):
        """Destructor to ensure temporary files are cleaned up."""
        self.delete_temp_files()
