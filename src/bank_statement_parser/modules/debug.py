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
from datetime import datetime
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.paths import get_paths
from bank_statement_parser.modules.statement_functions import get_results


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
        # ----------------------------------------------------------------
        header_rows: list[dict] = []
        if stmt.config_header and stmt.pdf is not None and stmt.config is not None:
            for config in stmt.config_header:
                raw: pl.DataFrame = get_results(
                    stmt.pdf,
                    "header",
                    config,
                    stmt.logs,
                    stmt.file_absolute,
                    scope="all",
                    exclude_last_n_pages=stmt.config.exclude_last_n_pages,
                )
                header_rows.extend(raw.to_dicts())

        lines_rows: list[dict] = []
        if stmt.config_lines and stmt.pdf is not None and stmt.config is not None:
            for config in stmt.config_lines:
                raw = get_results(
                    stmt.pdf,
                    "lines",
                    config,
                    stmt.logs,
                    stmt.file_absolute,
                    scope="all",
                    exclude_last_n_pages=stmt.config.exclude_last_n_pages,
                )
                lines_rows.extend(raw.to_dicts())

        # ----------------------------------------------------------------
        # 3. Checks & balances — capture before cleanup
        # ----------------------------------------------------------------
        cab_rows: list[dict] = stmt.checks_and_balances.to_dicts()
        cab_failing: list[dict] = _cab_detail_rows(stmt.checks_and_balances)

        # ----------------------------------------------------------------
        # 4. Error classification
        # ----------------------------------------------------------------
        error_type = "ERROR_CONFIG" if stmt.error_message else "ERROR_CAB"
        error_message = stmt.error_message if stmt.error_message else "** Checks & Balances Failure **"

        id_statement = stmt.ID_STATEMENT
        stmt.cleanup()
        stmt = None  # type: ignore[assignment]

        # ----------------------------------------------------------------
        # 5. Write debug.json
        # ----------------------------------------------------------------
        paths = get_paths(project_path)
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
            "checks_and_balances": cab_rows,
            "checks_and_balances_failing": cab_failing,
            "header_extraction": header_rows,
            "lines_extraction": lines_rows,
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
        if not (entry.error_cab or entry.error_config):
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
