"""
Diagnostic debug module for bank statement processing failures.

This module provides functions to re-process failing bank statement PDFs and
capture diagnostic information — raw page text, full extraction results
(including failures), and error details — into a structured JSON file.
No parquet or database writes are performed.

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
            # 3. Error classification
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

            id_statement = stmt.ID_STATEMENT
            stmt.cleanup()
            stmt = None  # type: ignore[assignment]

        # ----------------------------------------------------------------
        # 4. Write debug.json
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

    Iterates through the batch results and, for each entry whose
    ``result == "FAILURE"``, calls :func:`debug_pdf_statement` to re-process
    the PDF and write a diagnostic JSON file.  The run is always sequential
    regardless of whether the original batch used turbo mode, since failing
    sets are typically small.

    Pairing between source PDFs and results is done by index: ``pdfs[i]``
    is the source file for ``processed_pdfs[i]``.

    No parquet or database writes are performed.

    Args:
        processed_pdfs: List of :class:`~bank_statement_parser.modules.data.PdfResult`
            entries (or :class:`BaseException` for fatal worker errors) as returned
            by the batch processing step.
        pdfs: The original list of PDF :class:`~pathlib.Path` objects passed to the
            batch, in the same order.  ``pdfs[i]`` is the source for
            ``processed_pdfs[i]``.
        batch_id: The batch identifier from the original run.
        company_key: Optional company identifier used for the original batch.
        account_key: Optional account identifier used for the original batch.
        project_path: Optional project root directory.

    Returns:
        int: Number of debug JSON files successfully written.
    """
    count = 0
    for entry, pdf_path in zip(processed_pdfs, pdfs):
        if not isinstance(entry, PdfResult):
            continue
        if entry.result != "FAILURE":
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
