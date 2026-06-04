"""
conftest.py — session-scoped fixtures for integration tests.

Two project lifecycles are provided:

``good_project``
    Processes all PDFs in the bundled ``test_data/pdfs/good/`` directory into a
    fresh project at ``tests/test_project/``.  The project is fully populated
    (parquet + SQLite) before any test runs, and torn down afterwards.

    Uses anonymised PDFs if symlinks are available (symlink setup), otherwise
    falls back to synthetic PDFs or skips if neither available.

``bad_project``
    Processes all PDFs in the bundled ``test_data/pdfs/bad/`` directory into a
    fresh project at ``tests/test_project_bad/``.  Used to verify that bad PDFs
    are flagged as errors rather than processed successfully.

    Uses anonymised PDFs if symlinks are available, otherwise falls back to
    synthetic PDFs or skips if neither available.

New fixtures for central PDF management:

``anonymised_pdf_dir``
    Path to anonymised PDFs from central bank-statement-data repo.
    Falls back to bundled PDFs if central repo is not available.

``synthetic_pdf_dir``
    Path to synthetic PDFs (committed to this repo for fast testing).

``sample_good_pdf``
    Random good PDF from the available collection.

``sample_bad_pdf``
    Random bad PDF from the available collection.
"""

import random
import shutil
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest

from bank_statement_parser.modules.paths import validate_or_initialise_project
from bank_statement_parser.modules.statements import StatementBatch
from bank_statement_parser.testing import _pdf_dir

_TESTS_DIR = Path(__file__).parent
_TEST_DATA_DIR = Path(__file__).parent.parent / "src/bank_statement_parser/test_data/pdfs"
_GOOD_PROJECT_DIR = _TESTS_DIR / "test_project"
_BAD_PROJECT_DIR = _TESTS_DIR / "test_project_bad"

# ============================================================================
# PDF Mode Detection (Anonymised vs. Synthetic)
# ============================================================================

def _detect_pdf_mode() -> tuple[str, Path, Path]:
    """
    Detect which PDF set is available and return mode + paths.
    
    Returns:
        Tuple of (mode, good_pdfs_dir, bad_pdfs_dir) where:
        - mode: "anonymised" if symlinks exist, "synthetic" if stubs available, "none" if neither
        - good_pdfs_dir: Path to good PDFs directory
        - bad_pdfs_dir: Path to bad PDFs directory
    """
    # Check for anonymised symlinks (manual setup by developer)
    anonymised_good = _TEST_DATA_DIR / "anonymised_good"
    anonymised_bad = _TEST_DATA_DIR / "anonymised_bad"
    
    if anonymised_good.is_symlink() and anonymised_bad.is_symlink():
        return ("anonymised", anonymised_good, anonymised_bad)
    
    # Check for synthetic PDFs (fallback)
    synthetic_good = _TEST_DATA_DIR / "good"
    synthetic_bad = _TEST_DATA_DIR / "bad"
    
    if list(synthetic_good.glob("synthetic_*.pdf")) or list(synthetic_bad.glob("synthetic_*.pdf")):
        return ("synthetic", synthetic_good, synthetic_bad)
    
    # Check for any PDFs at all (fallback to _pdf_dir from installed package)
    fallback_good = _pdf_dir("good")
    fallback_bad = _pdf_dir("bad")
    
    if list(fallback_good.glob("*.pdf")) or list(fallback_bad.glob("*.pdf")):
        return ("bundled", fallback_good, fallback_bad)
    
    return ("none", synthetic_good, synthetic_bad)


# Initialize PDF mode at module load time
PDF_MODE, _GOOD_PDFS_DIR, _BAD_PDFS_DIR = _detect_pdf_mode()

# Output PDF mode to test session
print(f"\n[PDF_FIXTURES] Mode: {PDF_MODE.upper()}")
if PDF_MODE == "anonymised":
    print(f"[PDF_FIXTURES] Using ANONYMISED PDFs (symlinks detected)")
    print(f"[PDF_FIXTURES] Location: {_GOOD_PDFS_DIR}")
elif PDF_MODE == "synthetic":
    print(f"[PDF_FIXTURES] Using SYNTHETIC PDFs (symlinks not available)")
    print(f"[PDF_FIXTURES] Location: {_GOOD_PDFS_DIR}")
elif PDF_MODE == "bundled":
    print(f"[PDF_FIXTURES] Using BUNDLED PDFs from installed package")
else:
    print(f"[PDF_FIXTURES] No PDFs available (tests requiring PDFs will skip)")


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
    
    Automatically detects and uses:
    1. Anonymised PDFs if symlinks are set up
    2. Synthetic PDFs if available
    3. Bundled PDFs from installed package
    4. Skips if no PDFs available
    """
    if PDF_MODE == "none":
        pytest.skip(
            "No PDF fixtures available. Set up symlinks to anonymised PDFs "
            "for comprehensive testing. See: bank-statement-data/SYMLINK_SETUP.md"
        )
    
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
    
    Automatically detects and uses:
    1. Anonymised PDFs if symlinks are set up
    2. Synthetic PDFs if available
    3. Bundled PDFs from installed package
    4. Skips if no PDFs available
    """
    if PDF_MODE == "none":
        pytest.skip(
            "No PDF fixtures available. Set up symlinks to anonymised PDFs "
            "for comprehensive testing. See: bank-statement-data/SYMLINK_SETUP.md"
        )
    
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


# ---------------------------------------------------------------------------
# Central PDF Management Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def anonymised_pdf_dir() -> Path:
    """
    Path to anonymised PDFs from central bank-statement-data repo.

    For development/CI: Uses bundled test PDFs (fallback).
    For production/release CI: Would fetch from private repo via SSH.

    Returns:
        Path to directory containing anonymised PDFs (good/ and bad/ subdirs).
    """
    # In Phase 2, this will attempt to fetch from bank-statement-data repo
    # For now, return the bundled PDFs directory
    return _GOOD_PDFS_DIR.parent


@pytest.fixture(scope="session")
def synthetic_pdf_dir() -> Path:
    """
    Path to synthetic PDFs committed to this repo.

    These are completely synthetic (no real data) and safe for public repos.
    Generated by bank-statement-data repo's generator and committed here.

    Returns:
        Path to synthetic_pdfs directory (good/ and bad/ subdirs).

    Note:
        Falls back to bundled PDFs if synthetic PDFs not available.
    """
    synthetic_path = Path(__file__).parent / "test_data" / "synthetic_pdfs"
    if synthetic_path.exists():
        return synthetic_path
    # Fallback to bundled PDFs if synthetic not available
    return _GOOD_PDFS_DIR.parent


@pytest.fixture
def sample_good_pdf(anonymised_pdf_dir) -> Path:
    """
    Random good PDF from anonymised collection.

    Useful for parameterized tests that need a single PDF.

    Args:
        anonymised_pdf_dir: Path to anonymised PDFs directory.

    Returns:
        Path to a randomly selected valid PDF.
    """
    good_pdfs = list((anonymised_pdf_dir / "good").glob("*.pdf"))
    if not good_pdfs:
        raise FileNotFoundError(f"No good PDFs found in {anonymised_pdf_dir / 'good'}")
    return random.choice(good_pdfs)


@pytest.fixture
def sample_bad_pdf(anonymised_pdf_dir) -> Path:
    """
    Random bad PDF from anonymised collection.

    Useful for parameterized tests that expect parsing errors.

    Args:
        anonymised_pdf_dir: Path to anonymised PDFs directory.

    Returns:
        Path to a randomly selected malformed PDF.
    """
    bad_pdfs = list((anonymised_pdf_dir / "bad").glob("*.pdf"))
    if not bad_pdfs:
        raise FileNotFoundError(f"No bad PDFs found in {anonymised_pdf_dir / 'bad'}")
    return random.choice(bad_pdfs)
