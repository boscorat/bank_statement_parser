"""
conftest.py — session-scoped fixtures for integration tests.

Two project lifecycles are provided:

``good_project``
    Processes all PDFs in ``tests/pdfs/good/`` into a fresh project at
    ``tests/test_project/``.  The project is fully populated (parquet +
    SQLite) before any test runs, and torn down afterwards.

``bad_project``
    Processes all PDFs in ``tests/pdfs/bad/`` into a fresh project at
    ``tests/test_project_bad/``.  Used to verify that bad PDFs are flagged
    as errors rather than processed successfully.
"""

import shutil
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest

from bank_statement_parser.modules.paths import validate_or_initialise_project
from bank_statement_parser.modules.statements import StatementBatch

_TESTS_DIR = Path(__file__).parent
_GOOD_PDFS_DIR = _TESTS_DIR / "pdfs" / "good"
_BAD_PDFS_DIR = _TESTS_DIR / "pdfs" / "bad"
_GOOD_PROJECT_DIR = _TESTS_DIR / "test_project"
_BAD_PROJECT_DIR = _TESTS_DIR / "test_project_bad"


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
    """
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
        batch.update_parquet()
        batch.update_db()
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
    """
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
