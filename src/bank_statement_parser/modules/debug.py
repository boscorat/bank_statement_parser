# This file is part of bank_statement_parser.
#
# Copyright (c) 2026 Jason Farrar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Diagnostic debug module for bank statement processing failures.

Thin backwards-compatible wrapper around the debug infrastructure built
into :mod:`bank_statement_parser.modules.statements`.  All heavy lifting
(event collection, serialisation) is handled by :class:`Statement` with
``debug=True`` and :func:`_write_debug_json`.

Functions:
    debug_pdf_statement: Re-process a single failing PDF and write a debug.json.
    debug_statements: Re-process all failing entries from a completed batch.
"""

from pathlib import Path

from bank_statement_parser.modules.data import PdfResult


def debug_pdf_statement(
    pdf: Path,
    batch_id: str,
    company_key: str | None,
    account_key: str | None,
    project_path: Path | None = None,
) -> Path | None:
    """Re-process a single failing PDF and write a debug.json diagnostic file.

    Creates a :class:`~bank_statement_parser.modules.statements.Statement`
    with ``debug=True`` so that the in-statement debug infrastructure
    (page text, extraction events, checks-and-balances) is captured
    automatically.  :func:`_write_debug_json` is then called explicitly
    to guarantee the file is written regardless of whether the re-processed
    statement succeeds or fails.

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
        Path to the debug.json file that was written, or ``None`` if an
        unexpected error prevented writing (the error is printed to stdout).
    """
    # Local import to avoid a circular dependency:
    # debug.py → statements.py → debug.py
    from bank_statement_parser.modules.statements import Statement, _write_debug_json

    try:
        stmt = Statement(
            file=pdf,
            company_key=company_key,
            account_key=account_key,
            ID_BATCH=batch_id,
            project_path=project_path,
            skip_project_validation=True,
            debug=True,
        )
        _write_debug_json(stmt)
        stmt.cleanup()

        from bank_statement_parser.modules.paths import ProjectPaths

        paths = ProjectPaths.resolve(project_path)
        folder_name = f"{pdf.parent.name}_{pdf.name}"
        return paths.log_debug_dir(folder_name) / "debug.json"

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
    """Re-process all failing statements from a completed batch and write debug files.

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
        if isinstance(entry, BaseException):
            continue
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
