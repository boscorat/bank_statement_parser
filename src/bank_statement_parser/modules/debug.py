"""
Diagnostic debug module for bank statement processing failures.

This module provides functions to re-process failing bank statement PDFs and
capture diagnostic information — raw page text, full extraction results
(including failures), checks & balances data, and error details — into a
structured JSON file.  No parquet or database writes are performed.

Functions:
    debug_pdf_statement: Re-process a single failing PDF and write a debug.json.
    debug_statements: Re-process all failing entries from a completed batch.
"""

import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.parquet import (
    _build_checks_and_balances_data,
    _build_statement_heads_data,
    _build_statement_lines_data,
)
from bank_statement_parser.modules.paths import ProjectPaths
from bank_statement_parser.modules.statement_functions import get_results


@contextmanager
def _suppress_stdout():
    """Context manager that redirects stdout to /dev/null for its duration."""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def _cab_detail_rows(cab: pl.DataFrame) -> list[dict]:
    """
    Build a list of dicts describing each failing checks & balances row.

    For each of the four balance checks that is False, appends a dict with the
    check name, stated value, extracted value, and delta (where applicable).

    Args:
        cab: The ``checks_and_balances`` DataFrame from a :class:`Statement`.

    Returns:
        A list of dicts, one per failing check, or an empty list if cab is
        empty or all checks pass.
    """
    if cab.is_empty():
        return []
    row = cab.row(0, named=True)
    failing: list[dict] = []
    if not row.get("BAL_PAYMENTS_IN", True):
        stated = row["STD_PAYMENTS_IN"]
        extracted = row["STD_PAYMENT_IN"]
        failing.append(
            {
                "check": "BAL_PAYMENTS_IN",
                "stated": stated,
                "extracted": extracted,
                "delta": round(float(stated) - float(extracted), 2),
            }
        )
    if not row.get("BAL_PAYMENTS_OUT", True):
        stated = row["STD_PAYMENTS_OUT"]
        extracted = row["STD_PAYMENT_OUT"]
        failing.append(
            {
                "check": "BAL_PAYMENTS_OUT",
                "stated": stated,
                "extracted": extracted,
                "delta": round(float(stated) - float(extracted), 2),
            }
        )
    if not row.get("BAL_MOVEMENT", True):
        failing.append(
            {
                "check": "BAL_MOVEMENT",
                "statement_movement": row["STD_STATEMENT_MOVEMENT"],
                "extracted_movement": row["STD_MOVEMENT"],
                "balance_of_payments": row["STD_BALANCE_OF_PAYMENTS"],
            }
        )
    if not row.get("BAL_CLOSING", True):
        stated = row["STD_CLOSING_BALANCE"]
        calculated = row["STD_RUNNING_BALANCE"]
        failing.append(
            {
                "check": "BAL_CLOSING",
                "stated": stated,
                "calculated": calculated,
                "delta": round(float(stated) - float(calculated), 2),
            }
        )
    return failing


def _diagnose_parquet_schemas(
    stmt: object,
) -> list[dict]:
    """
    Attempt each parquet data-build step and capture diagnostics on failure.

    This is the *expensive* diagnostic step that only runs inside the debug
    path.  For each of the three per-statement parquet classes
    (ChecksAndBalances, StatementHeads, StatementLines) the corresponding
    ``_build_*_data()`` helper is called to produce the intermediate data
    DataFrame, then ``.extend()`` is attempted against the expected schema.
    If the call raises a :class:`polars.exceptions.SchemaError` (or any other
    exception), a diagnostic dict is captured containing:

    - ``parquet_class``: Which parquet class failed.
    - ``error``: The exception message.
    - ``expected_schema``: Column-name -> dtype mapping from the empty schema.
    - ``actual_dtypes``: Column-name -> dtype mapping of the data that was
      produced *before* the ``.extend()`` call.  ``None`` if the data could
      not be built at all.
    - ``mismatched_columns``: List of ``{"column", "expected", "actual"}``
      dicts for every column whose dtype differs.
    - ``data_sample``: First 5 rows of the actual data as a list of dicts,
      or ``None``.

    Only classes whose data-build or ``.extend()`` call fails are included.
    An empty list means all three classes would succeed (i.e. the original
    failure was transient or environment-specific).

    Args:
        stmt: A :class:`~bank_statement_parser.modules.statements.Statement`
            instance that has already been fully constructed (header_results,
            lines_results, and checks_and_balances populated).  Typed as
            ``object`` to avoid a circular import.

    Returns:
        A list of diagnostic dicts, one per failing parquet class.
    """
    # Schemas are defined inline to match the class constructors exactly,
    # avoiding Parquet I/O or project-path resolution.
    diagnostics: list[dict] = []

    # --- ChecksAndBalances ---
    cab_schema = pl.DataFrame(
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
    _run_build_diagnostic(
        diagnostics,
        "ChecksAndBalances",
        cab_schema,
        _build_checks_and_balances_data,
        stmt.ID_STATEMENT,  # type: ignore[attr-defined]
        stmt.ID_BATCH or "",  # type: ignore[attr-defined]
        stmt.checks_and_balances,  # type: ignore[attr-defined]
    )

    # --- StatementHeads ---
    heads_schema = pl.DataFrame(
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
    _run_build_diagnostic(
        diagnostics,
        "StatementHeads",
        heads_schema,
        _build_statement_heads_data,
        stmt.ID_STATEMENT,  # type: ignore[attr-defined]
        "DEBUG",  # id_batchline placeholder — not available on Statement
        stmt.ID_ACCOUNT,  # type: ignore[attr-defined]
        stmt.company,  # type: ignore[attr-defined]
        stmt.statement_type,  # type: ignore[attr-defined]
        stmt.account,  # type: ignore[attr-defined]
        stmt.header_results,  # type: ignore[attr-defined]
    )

    # --- StatementLines ---
    lines_schema = pl.DataFrame(
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
    _run_build_diagnostic(
        diagnostics,
        "StatementLines",
        lines_schema,
        _build_statement_lines_data,
        stmt.ID_STATEMENT,  # type: ignore[attr-defined]
        stmt.lines_results,  # type: ignore[attr-defined]
    )

    return diagnostics


def _run_build_diagnostic(
    diagnostics: list[dict],
    class_name: str,
    schema: pl.DataFrame,
    data_fn: object,
    *args: object,
) -> None:
    """
    Build intermediate data via *data_fn*, attempt ``.extend()``, and capture diagnostics on failure.

    Two-phase approach:

    1. Call *data_fn* with *args* to produce the raw data DataFrame (this is
       the step *before* ``.extend()``).  If this step itself fails, we still
       capture the exception but ``actual_dtypes`` and ``data_sample`` will
       be ``None``.
    2. Attempt ``schema.clone().extend(data)`` to reproduce the exact failure
       that occurs in the batch path.  If this succeeds, the class is healthy
       and nothing is appended.

    Args:
        diagnostics: The accumulator list to append to on failure.
        class_name: Human-readable class name for the diagnostic entry.
        schema: The expected schema DataFrame (used to report expected dtypes).
        data_fn: One of the ``_build_*_data()`` functions from
            :mod:`~bank_statement_parser.modules.parquet`.
        *args: Positional arguments forwarded to *data_fn*.
    """
    expected = {col: str(dtype) for col, dtype in schema.schema.items()}
    actual_dtypes: dict[str, str] | None = None
    data_sample: list[dict] | None = None
    data: pl.DataFrame | None = None

    # Phase 1: build the intermediate data DataFrame
    try:
        data = data_fn(*args)  # type: ignore[operator]
        actual_dtypes = {col: str(dtype) for col, dtype in data.schema.items()}
        data_sample = data.head(5).to_dicts()
    except Exception as exc:
        # Data construction itself failed — record and return
        diagnostics.append(
            {
                "parquet_class": class_name,
                "error": f"data construction failed: {exc}",
                "error_type": type(exc).__qualname__,
                "expected_schema": expected,
                "actual_dtypes": None,
                "mismatched_columns": [],
                "data_sample": None,
            }
        )
        return

    # Phase 2: attempt .extend() to reproduce the schema mismatch
    try:
        schema.clone().extend(data)
    except Exception as exc:
        mismatched: list[dict] = []
        for col, exp_dtype in expected.items():
            act_dtype = actual_dtypes.get(col)
            if act_dtype is not None and act_dtype != exp_dtype:
                mismatched.append({"column": col, "expected": exp_dtype, "actual": act_dtype})

        diagnostics.append(
            {
                "parquet_class": class_name,
                "error": str(exc),
                "error_type": type(exc).__qualname__,
                "expected_schema": expected,
                "actual_dtypes": actual_dtypes,
                "mismatched_columns": mismatched,
                "data_sample": data_sample,
            }
        )


def debug_pdf_statement(
    pdf: Path,
    batch_id: str,
    company_key: str | None,
    account_key: str | None,
    project_path: Path | None = None,
) -> Path | None:
    """
    Re-process a single failing PDF and write a debug.json diagnostic file.

    Re-opens and re-processes the PDF using the same Statement construction
    path as the original batch run, then captures:

    - Raw page text for every page (from pdfplumber ``chars``).
    - Full extraction results for header and lines sections with ``scope="all"``,
      so both successful and failing field extractions are visible.
    - The checks & balances DataFrame (may be empty if processing failed before
      the validation step ran).
    - The error message and error type.
    - Parquet schema diagnostics — each ``build_*_records()`` call is replayed
      and any schema mismatches are captured with expected vs actual dtypes.

    The output is written to::

        <project>/log/debug/<pdf.parent.name>_<pdf.name>/debug.json

    Any existing file at that path is overwritten.  No parquet or database
    writes are performed.

    Args:
        pdf: Path to the PDF file to re-process.
        batch_id: The batch identifier from the original run (recorded in meta).
        company_key: Optional company identifier, passed through to Statement.
        account_key: Optional account identifier, passed through to Statement.
        project_path: Optional project root directory.

    Returns:
        Path to the debug.json file that was written, or None if an unexpected
        error prevented writing (the error is printed to stdout).
    """
    # Local import to avoid a circular dependency:
    # debug.py → statements.py → debug.py
    from bank_statement_parser.modules.statements import Statement

    try:
        with _suppress_stdout():
            stmt = Statement(
                file=pdf,
                company_key=company_key,
                account_key=account_key,
                ID_BATCH=batch_id,
                project_path=project_path,
                skip_project_validation=True,
            )

            # ----------------------------------------------------------------
            # 1. Raw page text — captured while stmt.pdf is still open
            # ----------------------------------------------------------------
            pages: dict[str, str] = {}
            if stmt.pdf is not None:
                for i, page in enumerate(stmt.pdf.pages):
                    pages[f"page_{i + 1}"] = "".join(c["text"] for c in page.chars)

            # ----------------------------------------------------------------
            # 2. Full extraction results (scope="all" — includes failures)
            #    debug_collector accumulates get_region / get_table_from_region
            #    call data for each location processed.
            # ----------------------------------------------------------------
            header_rows: list[dict] = []
            header_debug: list[dict] = []
            if stmt.config_header and stmt.pdf is not None and stmt.config is not None:
                for config in stmt.config_header:
                    collector: list[dict] = []
                    raw: pl.DataFrame = get_results(
                        stmt.pdf,
                        "header",
                        config,
                        stmt.logs,
                        stmt.file_absolute,
                        scope="all",
                        exclude_last_n_pages=stmt.config.exclude_last_n_pages,
                        debug_collector=collector,
                    )
                    header_rows.extend(raw.to_dicts())
                    header_debug.extend(collector)

            lines_rows: list[dict] = []
            lines_debug: list[dict] = []
            if stmt.config_lines and stmt.pdf is not None and stmt.config is not None:
                for config in stmt.config_lines:
                    collector = []
                    raw = get_results(
                        stmt.pdf,
                        "lines",
                        config,
                        stmt.logs,
                        stmt.file_absolute,
                        scope="all",
                        exclude_last_n_pages=stmt.config.exclude_last_n_pages,
                        debug_collector=collector,
                    )
                    lines_rows.extend(raw.to_dicts())
                    lines_debug.extend(collector)

            # ----------------------------------------------------------------
            # 3. Checks & balances — capture before cleanup
            # ----------------------------------------------------------------
            cab_rows: list[dict] = stmt.checks_and_balances.to_dicts()
            cab_failing: list[dict] = _cab_detail_rows(stmt.checks_and_balances)

            # ----------------------------------------------------------------
            # 4. Error classification
            # ----------------------------------------------------------------
            if stmt.error_message:
                error_type = "ERROR_CONFIG"
                error_message = stmt.error_message
            elif not stmt.success:
                error_type = "ERROR_CAB"
                error_message = "** Checks & Balances Failure **"
            else:
                # Statement re-processed successfully — the original failure
                # must have occurred during the parquet write step (schema /
                # data-type mismatch).
                error_type = "ERROR_DATA"
                error_message = "** Data Failure ** (statement re-processed successfully; original failure was during parquet write)"
            error_detail: dict | None = stmt.error_detail

            # ----------------------------------------------------------------
            # 5. Parquet schema diagnostics — expensive, debug-path only
            # ----------------------------------------------------------------
            parquet_diagnostics: list[dict] = _diagnose_parquet_schemas(stmt)

            id_statement = stmt.ID_STATEMENT
            stmt.cleanup()
            stmt = None  # type: ignore[assignment]

        # ----------------------------------------------------------------
        # 6. Write debug.json
        # ----------------------------------------------------------------
        paths = ProjectPaths.resolve(project_path)
        folder_name = f"{pdf.parent.name}_{pdf.name}"
        debug_dir = paths.log_debug_dir(folder_name)
        debug_dir.mkdir(parents=True, exist_ok=True)
        out_file = debug_dir / "debug.json"

        payload: dict = {
            "meta": {
                "STD_FILENAME": pdf.name,
                "STD_FILENAME_FOLDER": str(pdf.parent),
                "ID_STATEMENT": id_statement,
                "ID_BATCH": batch_id,
                "error_type": error_type,
                "error_message": error_message,
                "debug_timestamp": datetime.now().isoformat(timespec="seconds"),
            },
            "error_detail": error_detail,
            "parquet_diagnostics": parquet_diagnostics,
            "checks_and_balances": cab_rows,
            "checks_and_balances_failing": cab_failing,
            "header_extraction": header_rows,
            "header_debug": header_debug,
            "lines_extraction": lines_rows,
            "lines_debug": lines_debug,
            "pages": pages,
        }

        out_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return out_file

    except Exception as e:
        print(f"[debug] unexpected error processing {pdf.name}: {e}")
        return None


def debug_statements(
    processed_pdfs: list[BaseException | PdfResult],
    pdfs: list[Path],
    batch_id: str,
    company_key: str | None,
    account_key: str | None,
    project_path: Path | None = None,
) -> int:
    """
    Re-process all failing statements from a completed batch and write debug files.

    Iterates through the batch results and, for each entry that carries an
    ``error_cab`` or ``error_config`` flag, calls :func:`debug_pdf_statement`
    to re-process the PDF and write a diagnostic JSON file.  The run is always
    sequential regardless of whether the original batch used turbo mode, since
    failing sets are typically small.

    No parquet or database writes are performed.

    Args:
        processed_pdfs: List of :class:`~bank_statement_parser.modules.data.PdfResult`
            entries (or :class:`BaseException` for fatal worker errors) as returned
            by the batch processing step.
        pdfs: The original list of PDF :class:`~pathlib.Path` objects passed to the
            batch.  Used to resolve ``PdfResult.file_src`` back to a ``Path``.
        batch_id: The batch identifier from the original run.
        company_key: Optional company identifier used for the original batch.
        account_key: Optional account identifier used for the original batch.
        project_path: Optional project root directory.

    Returns:
        int: Number of debug JSON files successfully written.
    """
    # Build a lookup from absolute path string → Path object so we can resolve
    # PdfResult.file_src (an absolute path string) back to the original Path.
    pdf_lookup: dict[str, Path] = {str(p.absolute()): p for p in pdfs}

    count = 0
    for entry in processed_pdfs:
        if not isinstance(entry, PdfResult):
            continue
        if not (entry.error_cab or entry.error_config or entry.error_data):
            continue
        if entry.file_src is None:
            continue
        pdf_path = pdf_lookup.get(entry.file_src)
        if pdf_path is None:
            continue
        result = debug_pdf_statement(
            pdf=pdf_path,
            batch_id=batch_id,
            company_key=company_key,
            account_key=account_key,
            project_path=project_path,
        )
        if result is not None:
            count += 1
            print(f"[debug] written → {result}")
    return count
