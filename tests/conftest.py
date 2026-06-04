"""
conftest.py — session-scoped fixtures for integration tests.

Two project lifecycles are provided:

``good_project``
    Processes all PDFs in the ``test_data/pdfs/good/`` directory (bundled or
    cloned from the private ``boscorat/bank-statement-data`` repo) into a fresh
    project at ``tests/test_project/``.  The project is fully populated
    (parquet + SQLite) before any test runs, and torn down afterwards.
    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).

``bad_project``
    Processes all PDFs in the ``test_data/pdfs/bad/`` directory (bundled or
    cloned from the private ``boscorat/bank-statement-data`` repo) into a fresh
    project at ``tests/test_project_bad/``.  Used to verify that bad PDFs
    are flagged as errors rather than processed successfully.
    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
"""

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


@dataclass
class ProjectContext:
    """Holds all state produced by a project lifecycle fixture."""

    project_path: Path
    batch: StatementBatch
    pdfs: list[Path]


@pytest.fixture(scope="session", autouse=False)
def good_project() -> Generator[ProjectContext, None, None]:  # type: ignore[misc]
    """
    Session fixture: scaffold a fresh project, process all good PDFs,
    populate parquet and SQLite, then yield.  Tears down on session end.

    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    """
    if _GOOD_PDFS_DIR is None:
        pytest.skip("Good PDF fixtures unavailable (requires boscorat/bank-statement-data access)")

    project_path = _GOOD_PROJECT_DIR
    project_path.mkdir(parents=True, exist_ok=True)

    try:
        validate_or_initialise_project(project_path)

        pdfs = sorted(_GOOD_PDFS_DIR.glob("*.pdf"))

        batch = StatementBatch(
            pdfs=pdfs,
            turbo=True,
            project_path=project_path,
        )
        batch.update_data()
        batch.delete_temp_files()

        yield ProjectContext(project_path=project_path, batch=batch, pdfs=pdfs)

    finally:
        if project_path.exists():
            shutil.rmtree(project_path)


@pytest.fixture(scope="session", autouse=False)
def bad_project() -> Generator[ProjectContext, None, None]:  # type: ignore[misc]
    """
    Session fixture: scaffold a fresh project, process all bad PDFs,
    then yield.  Tears down on session end.

    Skipped if PDF fixtures are unavailable (e.g., no SSH access to private repo).
    """
    if _BAD_PDFS_DIR is None:
        pytest.skip("Bad PDF fixtures unavailable (requires boscorat/bank-statement-data access)")

    project_path = _BAD_PROJECT_DIR
    project_path.mkdir(parents=True, exist_ok=True)

    try:
        validate_or_initialise_project(project_path)

        pdfs = sorted(_BAD_PDFS_DIR.glob("*.pdf"))

        batch = StatementBatch(
            pdfs=pdfs,
            turbo=True,
            project_path=project_path,
        )

        yield ProjectContext(project_path=project_path, batch=batch, pdfs=pdfs)

    finally:
        if project_path.exists():
            shutil.rmtree(project_path)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    """Display session summary, including skipped PDF-dependent tests if applicable."""
    if not _PDF_FIXTURES_AVAILABLE:
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
