"""
conftest.py — session-scoped fixtures for integration tests.

Two project lifecycles are provided:

``good_project``
    Processes all PDFs in the ``test_data/pdfs/good/`` directory (bundled or
    cloned from the private ``boscorat/bank-statement-data`` repo) into a fresh
    project at ``tests/test_project/``.  The project is fully populated
    (parquet + SQLite) before any test runs, and torn down afterwards.
    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    PDFs without corresponding .json metadata sidecars are skipped with a console warning.

``bad_project``
    Processes all PDFs in the ``test_data/pdfs/bad/`` directory (bundled or
    cloned from the private ``boscorat/bank-statement-data`` repo) into a fresh
    project at ``tests/test_project_bad/``.  Used to verify that bad PDFs
    are flagged as errors rather than processed successfully.
    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    PDFs without corresponding .json metadata sidecars are skipped with a console warning.
"""

import json
import re
import shutil
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest

from bank_statement_parser.modules.paths import validate_or_initialise_project
from bank_statement_parser.modules.statements import StatementBatch
from bank_statement_parser.testing import _pdf_dir

_TESTS_DIR = Path(__file__).parent
_GOOD_PDFS_DIR = _pdf_dir("good")
_BAD_PDFS_DIR = _pdf_dir("bad")
_GOOD_PROJECT_DIR = _TESTS_DIR / "test_project"
_BAD_PROJECT_DIR = _TESTS_DIR / "test_project_bad"

# Track whether PDF fixtures are available for session-end summary
_PDF_FIXTURES_AVAILABLE = _GOOD_PDFS_DIR is not None and _BAD_PDFS_DIR is not None

# Populated by fixtures; read by pytest_sessionfinish for the summary line
_good_pdf_count: int | None = None
_bad_pdf_count: int | None = None
_good_bank_counts: dict[str, int] | None = None


def load_pdf_metadata(pdf_path: Path) -> dict | None:
    """Load test expectations from a JSON sidecar file.

    Args:
        pdf_path: Path to a .pdf file.

    Returns:
        dict of metadata if .json sidecar exists, None if missing.

    Raises:
        json.JSONDecodeError if metadata file is malformed.
    """
    metadata_path = pdf_path.with_suffix(".json")
    if not metadata_path.exists():
        return None
    with open(metadata_path) as f:
        return json.load(f)


@dataclass
class ProjectContext:
    """Holds all state produced by a project lifecycle fixture."""

    project_path: Path
    batch: StatementBatch
    pdfs: list[Path]


@pytest.fixture(scope="session", autouse=False)
def good_project() -> Generator[ProjectContext, None, None]:  # type: ignore[misc]
    """
    Session fixture: scaffold a fresh project, process all good PDFs with metadata,
    populate parquet and SQLite, then yield.  Tears down on session end.

    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    PDFs without .json metadata sidecars are logged as skipped.
    """
    if _GOOD_PDFS_DIR is None:
        pytest.skip("Good PDF fixtures unavailable (requires boscorat/bank-statement-data access)")

    project_path = _GOOD_PROJECT_DIR
    project_path.mkdir(parents=True, exist_ok=True)

    try:
        validate_or_initialise_project(project_path)

        all_pdfs = sorted(_GOOD_PDFS_DIR.glob("*.pdf"))

        # Filter to PDFs with metadata
        pdfs_with_metadata = []
        for pdf in all_pdfs:
            metadata = load_pdf_metadata(pdf)
            if metadata is None:
                print(f"⚠️  METADATA MISSING: {pdf.name} - will not be tested")
            else:
                pdfs_with_metadata.append(pdf)

        if not pdfs_with_metadata:
            pytest.skip("No good PDFs with metadata sidecars found")

        # Count banks for session-end summary
        global _good_bank_counts  # noqa: PLW0603
        _good_bank_counts = {}
        for pdf in pdfs_with_metadata:
            match = re.match(r"anonymised_([A-Za-z]+)", pdf.name)
            bank = match.group(1).upper() if match else "UNKNOWN"
            _good_bank_counts[bank] = _good_bank_counts.get(bank, 0) + 1

        batch = StatementBatch(
            pdfs=pdfs_with_metadata,
            turbo=True,
            project_path=project_path,
        )
        batch.update_data()
        batch.delete_temp_files()

        global _good_pdf_count  # noqa: PLW0603
        _good_pdf_count = len(pdfs_with_metadata)

        yield ProjectContext(project_path=project_path, batch=batch, pdfs=pdfs_with_metadata)

    finally:
        if project_path.exists():
            shutil.rmtree(project_path)


@pytest.fixture(scope="session", autouse=False)
def bad_project() -> Generator[ProjectContext, None, None]:  # type: ignore[misc]
    """
    Session fixture: scaffold a fresh project, process all bad PDFs with metadata,
    then yield.  Tears down on session end.

    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    PDFs without .json metadata sidecars are logged as skipped.
    """
    if _BAD_PDFS_DIR is None:
        pytest.skip("Bad PDF fixtures unavailable (requires boscorat/bank-statement-data access)")

    project_path = _BAD_PROJECT_DIR
    project_path.mkdir(parents=True, exist_ok=True)

    try:
        validate_or_initialise_project(project_path)

        all_pdfs = sorted(_BAD_PDFS_DIR.glob("*.pdf"))

        # Filter to PDFs with metadata
        pdfs_with_metadata = []
        for pdf in all_pdfs:
            metadata = load_pdf_metadata(pdf)
            if metadata is None:
                print(f"⚠️  METADATA MISSING: {pdf.name} - will not be tested")
            else:
                pdfs_with_metadata.append(pdf)

        if not pdfs_with_metadata:
            pytest.skip("No bad PDFs with metadata sidecars found")

        batch = StatementBatch(
            pdfs=pdfs_with_metadata,
            turbo=True,
            project_path=project_path,
        )

        global _bad_pdf_count  # noqa: PLW0603
        _bad_pdf_count = len(pdfs_with_metadata)

        yield ProjectContext(project_path=project_path, batch=batch, pdfs=pdfs_with_metadata)

    finally:
        if project_path.exists():
            shutil.rmtree(project_path)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    """Display session summary, including skipped PDF-dependent tests if applicable."""
    if _PDF_FIXTURES_AVAILABLE:
        parts: list[str] = []
        if _good_pdf_count is not None and _good_bank_counts:
            bank_str = ", ".join(f"{n} {b}" for b, n in sorted(_good_bank_counts.items()))
            parts.append(f"Good PDFs processed: {_good_pdf_count} ({bank_str})")
        if _bad_pdf_count is not None:
            parts.append(f"Bad PDFs processed:  {_bad_pdf_count}")
        if parts:
            print("\n" + "=" * 50)
            print("TEST DATA SUMMARY")
            print("=" * 50)
            for line in parts:
                print(f"  {line}")
            print("=" * 50 + "\n")
    else:
        print("\n" + "=" * 70)
        print("PDF-DEPENDENT TESTS SKIPPED")
        print("=" * 70)
        print(
            "\nThese tests require anonymised bank statement PDFs from the private repo:\n"
            "  boscorat/bank-statement-data\n"
            "\nTo run these tests locally:\n"
            "  1. Ensure you have SSH access to boscorat/bank-statement-data\n"
            "  2. Set up your SSH key (~/.ssh/id_rsa or similar)\n"
            "  3. Run pytest again\n"
            "\nIn CI, these tests will run automatically with SSH secret access.\n"
        )
        print("=" * 70 + "\n")
