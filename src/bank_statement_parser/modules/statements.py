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
import getpass
import hashlib
import multiprocessing
import os
import sys
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from time import time
from typing import Literal
from uuid import uuid4

import polars as pl

import bank_statement_parser.modules.config as _config_module
import bank_statement_parser.modules.parquet as pq
from bank_statement_parser.modules.config import (
    get_config_from_account,
    get_config_from_company,
    get_config_from_statement,
)
from bank_statement_parser.modules.data import (
    Account,
    Failure,
    ParquetFiles,
    PdfResult,
    Review,
    StandardFields,
    StatementInfo,
    Success,
)
from bank_statement_parser.modules.database import update_db
from bank_statement_parser.modules.parquet import update_parquet
from bank_statement_parser.modules.paths import ProjectPaths, validate_or_initialise_project
from bank_statement_parser.modules.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.statement_functions import get_results, get_standard_fields

CPU_WORKERS = os.cpu_count()

_MAX_STRING_LEN = 500  # truncate long strings captured from locals


def _build_error_detail(exc: BaseException) -> dict:
    """
    Build a structured error detail dict from a live exception.

    Walks every frame in the traceback chain and extracts:
    - A human-readable traceback as a list of frame dicts.
    - All string-typed local variables from every frame (truncated to
      ``_MAX_STRING_LEN`` characters).  Large objects (DataFrames, PDFs, etc.)
      are skipped because they are not ``str`` instances.

    Args:
        exc: The caught exception, with its ``__traceback__`` still attached.

    Returns:
        A dict with keys ``exception_type``, ``message``, ``traceback``, and
        ``string_locals_by_frame``.
    """
    tb = exc.__traceback__
    tb_frames = []
    string_locals_by_frame = []

    raw_tb = traceback.extract_tb(tb)
    for summary in raw_tb:
        tb_frames.append(
            {
                "file": summary.filename,
                "line": summary.lineno,
                "function": summary.name,
                "text": summary.line,
            }
        )

    # Walk the live traceback chain to collect frame locals.
    current_tb = tb
    while current_tb is not None:
        frame = current_tb.tb_frame
        str_vars = {
            k: (v if len(v) <= _MAX_STRING_LEN else f"{v[:_MAX_STRING_LEN]}…")
            for k, v in frame.f_locals.items()
            if isinstance(v, str) and k != "__doc__"
        }
        if str_vars:
            string_locals_by_frame.append(
                {
                    "function": frame.f_code.co_name,
                    "file": frame.f_code.co_filename,
                    "line": current_tb.tb_lineno,
                    "string_locals": str_vars,
                }
            )
        current_tb = current_tb.tb_next

    return {
        "exception_type": type(exc).__name__,
        "message": str(exc),
        "traceback": tb_frames,
        "string_locals_by_frame": string_locals_by_frame,
    }


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
        "error_detail",
        # Lightweight summary scalars — populated on success; None otherwise.
        # Avoids a second .collect() call in process_pdf_statement().
        "std_statement_date",
        "std_payments_in",
        "std_payments_out",
        "std_opening_balance",
        "std_closing_balance",
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
            validate_or_initialise_project(ProjectPaths.resolve(project_path).root)
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
        self.error_detail: dict | None = None
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
        # Scalar summary fields — None until populated on a successful parse.
        self.std_statement_date = None
        self.std_payments_in = None
        self.std_payments_out = None
        self.std_opening_balance = None
        self.std_closing_balance = None

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
            if self.config:
                # Collect header once; use it for ID, rename target, and summary scalars.
                # Populated for both SUCCESS and REVIEW (CAB failure) paths — scalar fields
                # must be available whenever extraction succeeded, regardless of CAB outcome.
                _hdr = self.header_results.select(
                    "STD_ACCOUNT_NUMBER",
                    "STD_STATEMENT_DATE",
                    "STD_OPENING_BALANCE",
                    "STD_CLOSING_BALANCE",
                ).collect()
                if _hdr.height > 0:
                    acct_number = str(_hdr["STD_ACCOUNT_NUMBER"][0]).replace(" ", "")
                    self.ID_ACCOUNT = f"{self.config.company_key}_{self.config.account_type_key}_{acct_number}"
                    # Always populate the canonical rename target for use by copy_statements_to_project
                    stmt_date_raw = _hdr["STD_STATEMENT_DATE"][0]
                    self.file_renamed = f"{self.ID_ACCOUNT}_{str(stmt_date_raw).replace('-', '')}.pdf"
                    # Populate scalar summary fields for PdfResult
                    self.std_statement_date = stmt_date_raw
                    self.std_opening_balance = _hdr["STD_OPENING_BALANCE"][0]
                    self.std_closing_balance = _hdr["STD_CLOSING_BALANCE"][0]
                    # Payments in/out come from checks_and_balances (already a DataFrame)
                    if not self.checks_and_balances.is_empty():
                        self.std_payments_in = self.checks_and_balances["STD_PAYMENTS_IN"][0]
                        self.std_payments_out = self.checks_and_balances["STD_PAYMENTS_OUT"][0]
        except Exception as e:
            self.error_message = f"** Configuration Failure **: {e}"
            self.error_detail = _build_error_detail(e)
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
                            account_currency=self.config.currency,
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
                            account_currency=self.config.currency,
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
    return (" | " + " | ".join(lines)) if lines else ""


def process_pdf_statement(
    pdf: Path,
    batch_id: str,
    session_id: str,
    user_id: str,
    company_key: str | None,
    account_key: str | None,
    project_path: Path | None,
    idx: int = 0,
    skip_project_validation: bool = False,
) -> PdfResult:
    """
    Process a single bank statement PDF and save results to parquet files.

    This standalone function handles the complete processing workflow for one PDF:

    1. Creates a :class:`Statement` object to parse the PDF.
    2. Extracts header and line data.
    3. Saves results to temporary Parquet files (keyed by *idx*).
    4. Cleans up resources.
    5. Returns a :class:`~bank_statement_parser.modules.data.PdfResult` that
       discriminates success from failure and carries all typed metadata.

    File copying to the project ``statements/`` directory is handled separately
    via :func:`copy_statements_to_project`, which operates on the full list of
    processed results after all PDFs have been hashed and processed.

    Args:
        idx: Index position of this PDF in the batch.
        pdf: Path to the PDF file to process.
        batch_id: Unique identifier for the batch this PDF belongs to.
        session_id: UUID4 session identifier generated by the parent
            :class:`StatementBatch` at construction time.
        user_id: OS username of the user who initiated the batch, obtained
            via :func:`getpass.getuser` in :class:`StatementBatch`.
        company_key: Optional company identifier for config lookup.
        account_key: Optional account identifier for config lookup.
        project_path: Optional project root directory.
        skip_project_validation: If True, skip the project validation /
            initialisation step inside ``Statement.__init__``.  Pass ``True``
            when this function is called from ``StatementBatch``, which already
            validated the project once.

    Returns:
        :class:`~bank_statement_parser.modules.data.PdfResult` with
        ``result == "SUCCESS"`` when extraction and validation both pass, or
        ``result == "FAILURE"`` otherwise.  On success ``detail.payload`` is a
        :class:`~bank_statement_parser.modules.data.Success` instance carrying a
        :class:`~bank_statement_parser.modules.data.StatementInfo` and a
        :class:`~bank_statement_parser.modules.data.ParquetFiles`.  On failure
        ``detail.payload`` is a
        :class:`~bank_statement_parser.modules.data.Failure` instance with
        ``error_type`` set to ``"config"``, ``"cab"``, ``"data"``, or
        ``"other"``.  ``detail.payload.parquet_files`` on a ``Failure`` holds
        any temporary files that *were* written before the failure, including
        the ``batch_lines`` file which is always written.
    """
    paths = ProjectPaths.resolve(project_path)
    batch_lines_path: Path | None = None
    statement_heads_path: Path | None = None
    statement_lines_path: Path | None = None
    cab_path: Path | None = None
    error_cab: bool = False
    error_config: bool = False
    error_data: bool = False
    error_other: bool = False
    error_message: str = ""
    error_detail_str: str = ""
    # StatementInfo is only populated on a clean success path
    statement_info: StatementInfo | None = None
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
    batch_line["ERROR_DATA"] = False
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
            error_message = stmt.error_message
            print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {stmt.error_message}")
        else:
            # Statement parsed — persist all data regardless of CAB outcome.
            # A CAB failure returns REVIEW (not FAILURE), so statement_heads,
            # statement_lines, and checks_and_balances are all written here.
            # If CAB failed, note the error but continue to write everything.
            if not stmt.success:
                error_cab = True
                batch_line["ERROR_CAB"] = True
                detail_str = _cab_detail(stmt.checks_and_balances)
                cab_message = f"** Checks & Balances Failure **{detail_str}"
                batch_line["STD_ERROR_MESSAGE"] += cab_message
                error_message = cab_message
                error_detail_str = detail_str
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {cab_message}")

            # Derive currency directly from the account configuration
            _currency: str | None = stmt.config.currency if stmt.config else None
            try:
                # Save extracted header data
                pq_statement_heads = pq.StatementHeads(
                    file=paths.statement_heads_temp(idx, batch_id),
                    id_statement=stmt.ID_STATEMENT,
                    id_batchline=batch_line["ID_BATCHLINE"],
                    id_account=stmt.ID_ACCOUNT,
                    company=stmt.company,
                    statement_type=stmt.statement_type,
                    account=stmt.account,
                    header_results=stmt.header_results,
                    currency=_currency,
                )
                pq_statement_heads.create()
                statement_heads_path = paths.statement_heads_temp(idx, batch_id)
                pq_statement_heads.cleanup()
                pq_statement_heads = None
            except Exception as e:
                parquet_error = f"** Data Failure (StatementHeads) **: {e}"
                batch_line["STD_ERROR_MESSAGE"] += parquet_error
                batch_line["STD_SUCCESS"] = False
                error_data = True
                batch_line["ERROR_DATA"] = True
                error_message += parquet_error
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {parquet_error}")
                traceback.print_exc(file=sys.stderr)

            try:
                # Save extracted transaction line data
                pq_statement_lines = pq.StatementLines(
                    file=paths.statement_lines_temp(idx, batch_id),
                    id_statement=stmt.ID_STATEMENT,
                    lines_results=stmt.lines_results,
                )
                pq_statement_lines.create()
                statement_lines_path = paths.statement_lines_temp(idx, batch_id)
                pq_statement_lines.cleanup()
                pq_statement_lines = None
            except Exception as e:
                parquet_error = f"** Data Failure (StatementLines) **: {e}"
                batch_line["STD_ERROR_MESSAGE"] += parquet_error
                batch_line["STD_SUCCESS"] = False
                error_data = True
                batch_line["ERROR_DATA"] = True
                error_message += parquet_error
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {parquet_error}")
                traceback.print_exc(file=sys.stderr)

            # Build StatementInfo from scalar summary slots populated by Statement.__init__.
            # Built for both SUCCESS and REVIEW paths; omitted only if a data write failed
            # or header extraction returned no rows (scalars remain None).
            if (
                not error_data
                and stmt.config
                and stmt.std_statement_date is not None
                and stmt.std_payments_in is not None
                and stmt.std_payments_out is not None
                and stmt.std_opening_balance is not None
                and stmt.std_closing_balance is not None
            ):
                statement_info = StatementInfo(
                    id_statement=stmt.ID_STATEMENT,
                    id_account=stmt.ID_ACCOUNT or "",
                    account=stmt.account or "",
                    statement_date=stmt.std_statement_date,
                    payments_in=stmt.std_payments_in,
                    payments_out=stmt.std_payments_out,
                    opening_balance=stmt.std_opening_balance,
                    closing_balance=stmt.std_closing_balance,
                    filename_new=stmt.file_renamed or "",
                )

        # Save checks & balances only when the DataFrame is populated — an empty
        # frame means Statement construction failed before the validation step ran,
        # and attempting to write it would produce a misleading secondary error.
        if not stmt.checks_and_balances.is_empty():
            try:
                pq_cab = pq.ChecksAndBalances(
                    file=paths.cab_temp(idx, batch_id),
                    id_batchline=batch_line["ID_BATCHLINE"],
                    id_batch=stmt.ID_BATCH,
                    checks_and_balances=stmt.checks_and_balances,
                )
                pq_cab.create()
                cab_path = paths.cab_temp(idx, batch_id)
                pq_cab.cleanup()
                pq_cab = None
            except Exception as e:
                cab_error = f"** Data Failure (ChecksAndBalances) **: {e}"
                batch_line["STD_ERROR_MESSAGE"] += cab_error
                batch_line["STD_SUCCESS"] = False
                error_data = True
                batch_line["ERROR_DATA"] = True
                error_message += cab_error
                print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {cab_error}")
                traceback.print_exc(file=sys.stderr)

        stmt.cleanup()
        stmt = None
    except Exception as e:
        # Last-resort guard — should not be reached under normal operation now that
        # Statement.__init__ is non-raising, but kept to protect against unexpected
        # failures outside the statement constructor (e.g. path resolution errors).
        error_other = True
        batch_line["ERROR_CONFIG"] = True
        error_message = f"** Unexpected Failure **: {e}"
        batch_line["STD_ERROR_MESSAGE"] += error_message
        print(f"[line {batch_line['STD_BATCH_LINE']}] {pdf.name}: {error_message}")
        traceback.print_exc(file=sys.stderr)

    # Record processing time and timestamp
    line_end = time()
    batch_line["STD_DURATION_SECS"] = line_end - line_start
    batch_line["STD_UPDATETIME"] = datetime.now()

    # Save batch line data — always written regardless of success/failure
    pq_batch_lines = pq.BatchLines(file=paths.batch_lines_temp(idx, batch_id), batch_lines=[batch_line])
    pq_batch_lines.create()
    batch_lines_path = paths.batch_lines_temp(idx, batch_id)
    pq_batch_lines.cleanup()
    pq_batch_lines = None

    # Build the ParquetFiles record — statement-level files only (SUCCESS / REVIEW paths)
    parquet_files = ParquetFiles(
        statement_heads=statement_heads_path,
        statement_lines=statement_lines_path,
    )

    # SUCCESS: extraction + CAB both passed, no write errors
    if not error_config and not error_cab and not error_data and not error_other and statement_info is not None:
        return PdfResult(
            result="SUCCESS",
            outcome="SUCCESS",
            batch_lines=batch_lines_path,
            checks_and_balances=cab_path,
            payload=Success(
                statement_info=statement_info,
                parquet_files=parquet_files,
            ),
        )

    # REVIEW: CAB failed but extraction + writes succeeded — data needs human sign-off
    if error_cab and not error_data and statement_info is not None:
        return PdfResult(
            result="REVIEW",
            outcome="REVIEW CAB",
            batch_lines=batch_lines_path,
            checks_and_balances=cab_path,
            payload=Review(
                statement_info=statement_info,
                parquet_files=parquet_files,
                message=error_message or batch_line["STD_ERROR_MESSAGE"] or "Unknown CAB failure",
                message_detail=error_detail_str,
            ),
        )

    # FAILURE: determine the most specific failure type
    if error_config:
        outcome: Literal["FAILURE CONFIG", "FAILURE DATA", "FAILURE OTHER"] = "FAILURE CONFIG"
        error_type: Literal["config", "data", "other"] = "config"
    elif error_data:
        outcome = "FAILURE DATA"
        error_type = "data"
    else:
        outcome = "FAILURE OTHER"
        error_type = "other"

    return PdfResult(
        result="FAILURE",
        outcome=outcome,
        batch_lines=batch_lines_path,
        checks_and_balances=cab_path,
        payload=Failure(
            message=error_message or batch_line["STD_ERROR_MESSAGE"] or "Unknown failure",
            error_type=error_type,
            message_detail=error_detail_str,
        ),
    )


def delete_temp_files(
    processed_pdfs: list[BaseException | PdfResult],
    project_path: Path | None = None,
) -> None:
    """
    Delete temporary parquet files created during batch processing.

    Cleans up the temporary Parquet files that were created when processing
    each PDF.  Should be called after calling both :func:`update_parquet` and
    :func:`~bank_statement_parser.modules.database.update_db` to ensure data
    has been persisted before deletion.

    ``batch_lines`` (always written) and ``checks_and_balances`` (written for
    SUCCESS and REVIEW) are read directly from :class:`~bank_statement_parser.modules.data.PdfResult`.
    ``statement_heads`` and ``statement_lines`` are read from
    :attr:`~bank_statement_parser.modules.data.ParquetFiles` inside
    :class:`~bank_statement_parser.modules.data.Success` or
    :class:`~bank_statement_parser.modules.data.Review` payloads.

    Args:
        processed_pdfs: List of :class:`~bank_statement_parser.modules.data.PdfResult`
            entries as returned by :func:`process_pdf_statement`, or
            :class:`BaseException` for any entry that raised an unhandled worker
            error.
        project_path: Unused — retained for API compatibility.  Paths are now
            stored as full :class:`~pathlib.Path` objects inside each result.
    """
    for entry in processed_pdfs:
        if isinstance(entry, BaseException):
            continue
        if not isinstance(entry, PdfResult):
            continue
        # batch_lines is always present; checks_and_balances may be None
        top_level_paths = (entry.batch_lines, entry.checks_and_balances)
        # statement_heads / statement_lines live inside Success or Review payloads
        pq_files = entry.payload.parquet_files if isinstance(entry.payload, (Success, Review)) else None
        payload_paths = (pq_files.statement_heads, pq_files.statement_lines) if pq_files is not None else ()
        for full_path in (*top_level_paths, *payload_paths):
            if full_path is not None and full_path.exists():
                full_path.unlink()


def copy_statements_to_project(
    processed_pdfs: list[BaseException | PdfResult],
    pdfs: list[Path],
    project_path: Path | None = None,
) -> list[Path]:
    """
    Copy processed statement PDFs into the project ``statements/`` directory.

    Each successfully processed PDF is copied (not moved) to::

        <project>/statements/<filename>

    All statements are written directly at the top level of the
    ``statements/`` directory, with no year or account sub-folders.

    Pairing between source PDFs and results is done by index: ``pdfs[i]``
    is the source file for ``processed_pdfs[i]``.  This avoids the need to
    store the source path inside the result object.

    If two statements in the same batch resolve to the same destination path
    the later copy overwrites the earlier one, consistent with the
    ``INSERT OR REPLACE`` semantics used by
    :func:`~bank_statement_parser.modules.database.update_db` and the merge
    behaviour of :func:`~bank_statement_parser.modules.parquet.update_parquet`.

    Entries in *processed_pdfs* that are a :class:`BaseException` (a fatal
    worker error) or that have ``result == "FAILURE"`` (no valid rename target)
    are silently skipped.

    Args:
        processed_pdfs: List of :class:`~bank_statement_parser.modules.data.PdfResult`
            entries as returned by :func:`process_pdf_statement`, or
            :class:`BaseException` for any entry that raised an unhandled worker
            error.
        pdfs: The original list of PDF :class:`~pathlib.Path` objects passed to
            the batch, in the same order.  ``pdfs[i]`` is the source for
            ``processed_pdfs[i]``.
        project_path: Optional project root directory.  When ``None`` the
            default bundled ``project/`` directory is used.

    Returns:
        List of :class:`~pathlib.Path` objects for every file that was copied.
    """
    paths = ProjectPaths.resolve(project_path)
    copied: list[Path] = []
    for entry, pdf_path in zip(processed_pdfs, pdfs):
        if isinstance(entry, BaseException):
            continue
        if not isinstance(entry, PdfResult):
            continue
        if entry.result != "SUCCESS":
            continue
        info = entry.payload.statement_info  # type: ignore[union-attr]
        if not info.filename_new:
            continue
        dest_dir = paths.statements
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / info.filename_new
        Path(pdf_path).copy(dest_path)
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
        ID_SESSION: UUID4 session identifier generated at construction time.
        ID_USER: OS username of the user who initiated the batch.
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
        project_path: Optional custom project root directory.
        processed_pdfs: List of processed PDF results (PdfResult entries or exceptions).
    """

    __slots__ = (
        "process_time",
        "path",
        "ID_BATCH",
        "ID_SESSION",
        "ID_USER",
        "__type",
        "company_key",
        "account_key",
        "print_log",
        "pdfs",
        "pdf_count",
        "log",
        "errors",
        "reviews",
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
            validate_or_initialise_project(ProjectPaths.resolve(project_path).root)
        print("processing...")
        self.process_time: datetime = datetime.now()
        self.timer_start = time()
        self.ID_BATCH: str = str(uuid4())
        self.ID_SESSION: str = str(uuid4())
        self.ID_USER: str = getpass.getuser()
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
        self.reviews: int = 0
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
            if isinstance(result, PdfResult) and result.result in "FAILURE":
                self.errors += 1
            if isinstance(result, PdfResult) and result.result == "REVIEW":
                self.reviews += 1

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
            if isinstance(result, PdfResult) and result.result == "FAILURE":
                self.errors += 1
            if isinstance(result, PdfResult) and result.result == "REVIEW":
                self.reviews += 1
        return True

    async def __process_batch_turbo(self):
        """
        Internal async method for parallel PDF processing.

        Uses ProcessPoolExecutor to distribute PDF processing across
        multiple CPU cores for improved performance.

        Workers are submitted as calls to the module-level
        :func:`process_pdf_statement` function rather than bound methods,
        so no ``StatementBatch`` instance is pickled and sent to the child
        process.

        An explicit ``forkserver`` multiprocessing context is used on all
        platforms that support it (Linux / macOS).  This prevents the
        ``fork``-safety deadlock that occurs when worker processes are forked
        from a parent that already has asyncio threads running (the default
        ``fork`` start-method on Python ≤ 3.13 Linux).  Python 3.14 changed
        the default to ``forkserver`` on Linux (gh-84559), but being explicit
        here ensures consistent behaviour across Python 3.11–3.14+.

        Returns:
            list: List of processed PDF results from all workers.
        """
        self.turbo = True
        loop = asyncio.get_running_loop()
        mp_context = multiprocessing.get_context("forkserver")

        with ProcessPoolExecutor(max_workers=CPU_WORKERS, mp_context=mp_context) as executor:
            # Pass the module-level function and individual scalar args directly —
            # avoids pickling the entire StatementBatch instance (self).
            tasks = [
                loop.run_in_executor(
                    executor,
                    process_pdf_statement,
                    pdf,
                    self.ID_BATCH,
                    self.ID_SESSION,
                    self.ID_USER,
                    self.company_key,
                    self.account_key,
                    self.project_path,
                    idx,
                    True,  # skip_project_validation
                )
                for idx, pdf in enumerate(self.pdfs)
            ]

            # return_exceptions=True so a worker crash is captured as a
            # BaseException entry rather than aborting the whole gather.
            processed_pdfs = await asyncio.gather(*tasks, return_exceptions=True)

        return processed_pdfs

    def process_single_pdf(self, idx: int, pdf: Path) -> PdfResult:
        """
        Process a single PDF file and save results to parquet files.

        Delegates to the module-level :func:`process_pdf_statement` function,
        passing only the data required (no reference to this
        :class:`StatementBatch` instance).

        .. note::
            This method is used by the sequential (:meth:`process`) path.
            The turbo path (:meth:`__process_batch_turbo`) calls
            :func:`process_pdf_statement` directly to avoid pickling ``self``
            across process boundaries.

        Args:
        idx: Index position of this PDF in the batch.  Defaults to ``0``
            when called directly (i.e. outside a batch).  The batch sets
            this to the enumeration index so that temporary file names are
            unique across concurrent workers.
            pdf: Path to the PDF file to process.

        Returns:
            PdfResult: Named tuple with all stems, source/dest paths, and error flags.
                See :data:`~bank_statement_parser.modules.data.PdfResult` for field descriptions.
        """
        return process_pdf_statement(
            idx=idx,
            pdf=pdf,
            batch_id=self.ID_BATCH,
            session_id=self.ID_SESSION,
            user_id=self.ID_USER,
            company_key=self.company_key,
            account_key=self.account_key,
            project_path=self.project_path,
            skip_project_validation=True,
        )

    def update_data(
        self,
        datadestination: Literal["parquet", "database", "both"] = "both",
        project_path: Path | None = None,
    ) -> None:
        """
        Persist processed batch results to Parquet files, the SQLite database, or both.

        Delegates to the module-level :func:`update_parquet` and/or
        :func:`update_db` functions, passing only the data required (no
        reference to this :class:`StatementBatch` instance).  Records timing
        in :attr:`parquet_secs` / :attr:`db_secs` and updates
        :attr:`duration_secs` accordingly.

        Args:
            datadestination: Persistence target.  ``"parquet"`` writes only to
                the permanent Parquet files; ``"database"`` writes only to the
                SQLite star-schema; ``"both"`` (default) writes to both in
                sequence — Parquet first, then the database.
            project_path: Optional project root directory.  If not provided,
                uses the project_path set on this batch, or the default project
                folder.
        """
        resolved = project_path if project_path is not None else self.project_path
        if datadestination in ("parquet", "both"):
            self.parquet_secs = update_parquet(
                processed_pdfs=self.processed_pdfs,
                batch_id=self.ID_BATCH,
                session_id=self.ID_SESSION,
                user_id=self.ID_USER,
                path=self.path,
                company_key=self.company_key,
                account_key=self.account_key,
                pdf_count=self.pdf_count,
                errors=self.errors,
                reviews=self.reviews,
                duration_secs=self.duration_secs,
                process_time=self.process_time,
                paths=ProjectPaths.resolve(resolved),
            )
            self.duration_secs += self.parquet_secs
        if datadestination in ("database", "both"):
            self.db_secs = update_db(
                processed_pdfs=self.processed_pdfs,
                batch_id=self.ID_BATCH,
                session_id=self.ID_SESSION,
                user_id=self.ID_USER,
                path=self.path,
                company_key=self.company_key,
                account_key=self.account_key,
                pdf_count=self.pdf_count,
                errors=self.errors,
                reviews=self.reviews,
                duration_secs=self.duration_secs,
                process_time=self.process_time,
                project_path=resolved,
            )
            self.duration_secs += self.db_secs

    def copy_statements_to_project(self, project_path: Path | None = None) -> list[Path]:
        """
        Copy processed statement PDFs into the project ``statements/`` directory.

        Delegates to the module-level :func:`copy_statements_to_project` function.
        Each PDF is copied (not moved) to::

            <project>/statements/<filename>

        All statements are written directly at the top level of the
        ``statements/`` directory, with no year or account sub-folders.

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
            pdfs=self.pdfs,
            project_path=project_path if project_path is not None else self.project_path,
        )

    def delete_temp_files(self) -> None:
        """
        Delete temporary parquet files created during processing.

        Delegates to the module-level `delete_temp_files` function, passing
        only the processed PDF results list (no reference to this instance).

        This method cleans up the temporary parquet files that were created
        when processing each PDF. Call this method after calling
        update_data() to persist the data.
        """
        delete_temp_files(self.processed_pdfs, self.project_path)

    def export(
        self,
        filetype: Literal["excel", "csv", "json", "all", "both", "reporting"] = "excel",
        folder: Path | None = None,
        type: str = "simple",
        project_path: Path | None = None,
    ) -> None:
        """
        Export processed batch data to a file or set of files.

        Delegates to the DB-backed module-level export function based on
        *filetype*.  When neither *folder* nor *project_path* is supplied the
        function writes to the project's ``export/csv/``, ``export/excel/``, or
        ``export/json/`` sub-directory, creating it if absent.

        Args:
            filetype: Output format.  ``"excel"`` writes a single ``.xlsx``
                workbook; ``"csv"`` writes one CSV file per report table;
                ``"json"`` writes one JSON file per report table; ``"all"``
                writes Excel, CSV, and JSON in sequence; ``"reporting"`` writes
                CSV feeds to ``reporting/data/simple/`` and
                ``reporting/data/full/`` inside the project directory.
                Defaults to ``"excel"``.

                .. deprecated::
                    ``"both"`` is a deprecated alias for ``"all"`` and will be
                    removed in a future release.  Use ``"all"`` instead.
            folder: Output path passed through to the underlying export
                function.  For CSV and JSON this is the directory to write
                files into; for Excel this is the full workbook path.  When
                ``None`` the default export sub-directory for the resolved
                project is used.
            type: Export preset passed through to the underlying function.
                ``"simple"`` exports the flat transactions table only;
                ``"full"`` exports separate star-schema tables for loading
                into a database.  Defaults to ``"simple"``.
            project_path: Optional project root directory.  If not provided,
                uses the project_path set on this batch, or the default project
                folder.
        """
        if filetype == "both":
            warnings.warn(
                "filetype='both' is deprecated and will be removed in a future release. Use filetype='all' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            filetype = "all"
        if filetype == "all":
            self.export(filetype="excel", folder=folder, type=type, project_path=project_path)
            self.export(filetype="csv", folder=folder, type=type, project_path=project_path)
            self.export(filetype="json", folder=folder, type=type, project_path=project_path)
            return
        resolved_project_path = project_path if project_path is not None else self.project_path

        import bank_statement_parser.modules.reports_db as _rd

        if filetype == "excel":
            _rd.export_excel(path=folder, type=type, project_path=resolved_project_path)
        elif filetype == "csv":
            _rd.export_csv(folder=folder, type=type, project_path=resolved_project_path)
        elif filetype == "json":
            _rd.export_json(folder=folder, type=type, project_path=resolved_project_path)
        elif filetype == "reporting":
            _rd.export_reporting_data(project_path=resolved_project_path)

    def debug(self, project_path: Path | None = None) -> int:
        """
        Re-process failing statements to collect diagnostic information.

        For each statement that failed in the batch (either ``ERROR_CONFIG`` or
        ``ERROR_CAB``), re-opens and re-processes the PDF to capture:

        - Raw page text for every page.
        - Full extraction results including failing rows (``scope="all"``).
        - The checks & balances DataFrame with per-check failure detail.
        - The error message and error type.

        Writes a single ``debug.json`` file per failing statement to::

            <project>/log/debug/<parent_dir>_<filename>/debug.json

        Any prior file at that path is overwritten.  No parquet or database
        writes are performed.  The debug run is always sequential regardless
        of whether the original batch used turbo mode.

        Args:
            project_path: Optional project root directory.  If not provided,
                uses the project_path set on this batch, or the default project
                folder.

        Returns:
            int: Number of debug JSON files written.
        """
        # Local import to break the circular dependency:
        # statements.py → debug.py → statements.py
        from bank_statement_parser.modules.debug import debug_statements

        return debug_statements(
            processed_pdfs=self.processed_pdfs,
            pdfs=self.pdfs,
            batch_id=self.ID_BATCH,
            company_key=self.company_key,
            account_key=self.account_key,
            project_path=project_path if project_path is not None else self.project_path,
        )

    def __del__(self):
        """Destructor to ensure temporary files are cleaned up."""
        # self.delete_temp_files()
        pass
