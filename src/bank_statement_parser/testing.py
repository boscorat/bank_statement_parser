"""
testing.py — programmatic test harness for dependent projects.

Exposes :class:`TestHarness`, which allows an external project (e.g. *openstan*)
to:

1. Build a fully-populated bsp project from anonymised PDFs (bundled or from
   the private ``boscorat/bank-statement-data`` repo via SSH).
2. Run bsp's own pytest suite as a quality gate.
3. Obtain the path to the resulting SQLite database for direct comparison queries.
4. Tear the temporary project down when finished.

Typical usage::

    from bank_statement_parser.testing import TestHarness
    from bank_statement_parser.errors import TestGateFailure

    # Explicit lifecycle
    harness = TestHarness()
    harness.setup()                     # raises TestGateFailure if bsp tests fail
    db_path = harness.db_path           # Path to live project.db — query freely
    harness.teardown()

    # Context manager (auto-teardown)
    with TestHarness() as h:
        compare_databases(h.db_path, openstan_db_path)

Functions:
    _pdf_dir: Returns the path to test PDFs (bundled or cloned from private repo).
    _clone_test_data: Clones test PDFs from private repo if not bundled.
    _tests_dir: Returns the path to the bsp pytest suite (dev/editable installs only).
"""

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from bank_statement_parser.modules.errors import TestGateFailure
from bank_statement_parser.modules.paths import ProjectPaths, validate_or_initialise_project
from bank_statement_parser.modules.statements import StatementBatch

# ---------------------------------------------------------------------------
# Module-level path helpers (also imported by tests/conftest.py)
# ---------------------------------------------------------------------------

_TEST_DATA_DIR: Path = Path(__file__).parent / "test_data"
_PDFS_DIR: Path = _TEST_DATA_DIR / "pdfs"

# Repo root is three levels up: src/bank_statement_parser/testing.py → repo root
_REPO_ROOT: Path = Path(__file__).parent.parent.parent

# Cache directory for cloned test data (cross-platform)
_CACHE_DIR: Path = Path.home() / ".cache" / "bank_statement_data"
_PRIVATE_REPO_URL: str = "git@github.com:boscorat/bank-statement-data.git"


def _clone_test_data() -> Path | None:
    """Clone or update test data from private repo into cache directory.

    Clones ``boscorat/bank-statement-data`` repo via SSH into ``~/.cache/bank_statement_data/``.
    Uses SSH for authentication (requires SSH key access to the private repo).

    On subsequent calls, pulls latest changes from the remote repo so that
    newly added PDFs are picked up automatically.

    Returns:
        Absolute :class:`~pathlib.Path` to the ``pdfs`` directory if successful.
        ``None`` if the clone/pull fails (missing SSH key, network error, etc.).

    Note:
        The clone is cached locally to avoid repeated full downloads. The git
        repo is retained so that ``git pull`` can fetch incremental updates.
    """
    pdfs_cache = _CACHE_DIR / "pdfs"
    repo_dir = _CACHE_DIR / "repo"

    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if (repo_dir / ".git").is_dir():
            # Repo already cloned — pull latest changes
            result = subprocess.run(
                ["git", "-C", str(repo_dir), "pull", "--ff-only"],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                # Pull failed — return existing cache if available
                return pdfs_cache if pdfs_cache.is_dir() else None
        else:
            # Fresh clone
            result = subprocess.run(
                ["git", "clone", _PRIVATE_REPO_URL, str(repo_dir)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                return None

        # Copy pdfs from repo to cache
        repo_pdfs = repo_dir / "pdfs"
        if not repo_pdfs.is_dir():
            return pdfs_cache if pdfs_cache.is_dir() else None

        # Replace cache with fresh copy from repo
        if pdfs_cache.is_dir():
            shutil.rmtree(pdfs_cache)
        shutil.copytree(repo_pdfs, pdfs_cache)

        return pdfs_cache if pdfs_cache.is_dir() else None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # FileNotFoundError: git not found
        # TimeoutExpired: clone/pull took too long
        return pdfs_cache if pdfs_cache.is_dir() else None


def _pdf_dir(category: str) -> Path | None:
    """Return the test-PDF directory for *category* (``"good"`` or ``"bad"``).

    First attempts to use bundled PDFs in the installed package at
    ``bank_statement_parser/test_data/pdfs/<category>/``.  If not found,
    attempts to clone from the private repo ``boscorat/bank-statement-data``
    via SSH, caching the result locally.

    Args:
        category: Either ``"good"`` (real, anonymised PDFs that parse cleanly)
            or ``"bad"`` (PDFs that are expected to produce errors).

    Returns:
        Absolute :class:`~pathlib.Path` to the requested PDF directory if found.
        ``None`` if neither bundled PDFs nor remote clone are available.

    Note:
        Returns ``None`` rather than raising an exception to allow graceful
        test skipping when PDFs are unavailable (e.g., users without SSH access
        to the private repo).
    """
    # Try bundled path first
    bundled_path = _PDFS_DIR / category
    if bundled_path.is_dir():
        return bundled_path

    # Try to clone from private repo
    pdf_cache = _clone_test_data()
    if pdf_cache is not None:
        cached_path = pdf_cache / category
        if cached_path.is_dir():
            return cached_path

    return None


def _tests_dir() -> Path:
    """Return the path to the bsp pytest suite.

    Resolves to ``<repo_root>/tests/``, which is present when bsp is installed
    as an editable dependency (``uv add --editable``).

    Returns:
        Absolute :class:`~pathlib.Path` to the ``tests/`` directory.

    Raises:
        FileNotFoundError: If ``tests/`` is not found (e.g. wheel install).
    """
    path = _REPO_ROOT / "tests"
    if not path.is_dir():
        raise FileNotFoundError(
            f"bsp tests/ directory not found at {path}. Ensure bank_statement_parser is installed as an editable dependency."
        )
    return path


# ---------------------------------------------------------------------------
# TestHarness
# ---------------------------------------------------------------------------


class TestHarness:
    """Programmatic test environment for integration testing by dependent projects.

    Builds a real-PDF bsp project (using the bundled anonymised PDFs), optionally
    runs bsp's own pytest suite as a quality gate, and exposes the resulting SQLite
    database path for external comparison queries.

    The project directory is either provided by the caller or created in a
    temporary directory that is owned (and cleaned up) by this harness.

    Args:
        skip_bsp_tests: When ``True``, the bsp pytest suite is not executed
            during :meth:`setup`.  Use this when bsp is installed as a wheel
            (non-editable) and the ``tests/`` directory is not present — for
            example in CI environments.  Defaults to ``False``.

    Example::

        harness = TestHarness()
        harness.setup()
        db_path = harness.db_path   # open with sqlite3 and run your own queries
        harness.teardown()

        # Skip internal bsp tests (e.g. in CI where bsp is a wheel install)
        harness = TestHarness(skip_bsp_tests=True)
        harness.setup()

    The harness also supports the context manager protocol::

        with TestHarness() as h:
            compare_databases(h.db_path, my_db_path)
    """

    __slots__ = ("_project_path", "_owned", "_test_results", "_batch", "_ready", "_skip_bsp_tests")

    def __init__(self, skip_bsp_tests: bool = False) -> None:
        self._project_path: Path | None = None
        self._owned: bool = False  # True when we created the temp dir
        self._test_results: dict | None = None
        self._batch: StatementBatch | None = None
        self._ready: bool = False
        self._skip_bsp_tests: bool = skip_bsp_tests

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    def setup(self, project_path: Path | None = None) -> "TestHarness":
        """Build the real-PDF bsp project and optionally run bsp's pytest suite.

        Steps performed:

        1. Create (or use) the project directory.
        2. Discover the bundled anonymised good PDFs.
        3. Initialise the project scaffold and process all PDFs via
           :class:`~bank_statement_parser.modules.statements.StatementBatch`.
        4. Unless ``skip_bsp_tests=True`` was passed to :meth:`__init__`, run
           bsp's own pytest suite; raise
           :exc:`~bank_statement_parser.errors.TestGateFailure` if any test
           fails or errors.  Pass ``skip_bsp_tests=True`` when bsp is installed
           from a wheel (non-editable) and the bsp ``tests/`` directory is not
           available on the filesystem — for example in CI environments where
           bsp is installed as a regular package dependency.

        After a successful ``setup()`` call :attr:`db_path` is available.
        :attr:`test_results` is also available unless ``skip_bsp_tests=True``.

        Args:
            project_path: Optional directory in which to build the project.
                If ``None``, a temporary directory is created and owned by this
                harness (deleted on :meth:`teardown`).

        Returns:
            ``self``, allowing method chaining: ``harness.setup().db_path``.

        Raises:
            TestGateFailure: If bsp's pytest suite reports failures or errors
                (only when ``skip_bsp_tests=False``).
        """
        if project_path is None:
            self._project_path = Path(tempfile.mkdtemp(prefix="bsp_test_harness_"))
            self._owned = True
        else:
            project_path.mkdir(parents=True, exist_ok=True)
            self._project_path = project_path
            self._owned = False

        validate_or_initialise_project(self._project_path)

        pdfs = sorted(_pdf_dir("good").glob("*.pdf"))

        self._batch = StatementBatch(
            pdfs=pdfs,
            turbo=True,
            project_path=self._project_path,
        )
        self._batch.update_data()
        self._batch.delete_temp_files()

        if not self._skip_bsp_tests:
            self._run_bsp_tests()
        self._ready = True
        return self

    def teardown(self) -> None:
        """Destroy the test project and reset harness state.

        Deletes the project directory (only if created by this harness, i.e.
        ``project_path`` was not supplied to :meth:`setup`).  Resets all
        internal state so the harness can be reused with a fresh :meth:`setup`
        call if desired.
        """
        if self._owned and self._project_path is not None and self._project_path.exists():
            shutil.rmtree(self._project_path, ignore_errors=True)

        self._project_path = None
        self._owned = False
        self._test_results = None
        self._batch = None
        self._ready = False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TestHarness":
        """Set up the harness and return it for use as a context manager.

        Returns:
            This :class:`TestHarness` instance after calling :meth:`setup`.
        """
        return self.setup()

    def __exit__(self, *args: object) -> None:
        """Tear down the harness on context exit, regardless of exceptions.

        Args:
            *args: Standard ``__exit__`` arguments (exc_type, exc_val, exc_tb);
                all are ignored.
        """
        self.teardown()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def db_path(self) -> Path:
        """Path to the project SQLite database.

        Returns:
            Absolute :class:`~pathlib.Path` to ``project.db`` inside the test
            project directory.

        Raises:
            RuntimeError: If :meth:`setup` has not been called yet.
        """
        if not self._ready or self._project_path is None:
            raise RuntimeError("TestHarness.setup() must be called before accessing db_path.")
        return ProjectPaths.resolve(self._project_path).project_db

    @property
    def project_path(self) -> Path:
        """Root directory of the test project.

        Returns:
            Absolute :class:`~pathlib.Path` to the project root.

        Raises:
            RuntimeError: If :meth:`setup` has not been called yet.
        """
        if not self._ready or self._project_path is None:
            raise RuntimeError("TestHarness.setup() must be called before accessing project_path.")
        return self._project_path

    @property
    def test_results(self) -> dict:
        """Summary of the bsp pytest run.

        Returns:
            A dict with the following keys:

            - ``passed`` (:class:`int`): number of passing tests.
            - ``failed`` (:class:`int`): number of failing tests.
            - ``errors`` (:class:`int`): number of test errors.
            - ``returncode`` (:class:`int`): pytest process return code.
            - ``output`` (:class:`str`): combined stdout/stderr from pytest.

        Raises:
            RuntimeError: If :meth:`setup` has not been called yet, or if
                ``skip_bsp_tests=True`` was passed to :meth:`__init__`.
        """
        if not self._ready or self._test_results is None:
            raise RuntimeError("TestHarness.setup() must be called before accessing test_results, and skip_bsp_tests must be False.")
        return self._test_results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_bsp_tests(self) -> None:
        """Run bsp's pytest suite and populate :attr:`_test_results`.

        Executes pytest in a subprocess, parses the summary line, and raises
        :exc:`~bank_statement_parser.errors.TestGateFailure` if any tests fail
        or error.

        Raises:
            TestGateFailure: If pytest returns a non-zero exit code or reports
                failures/errors in its summary output.
        """
        tests_path = _tests_dir()

        result = subprocess.run(
            ["python", "-m", "pytest", str(tests_path), "-q", "--tb=short"],
            capture_output=True,
            text=True,
        )

        output = result.stdout + result.stderr

        passed = _parse_summary(output, "passed")
        failed = _parse_summary(output, "failed")
        errors = _parse_summary(output, "error")

        self._test_results = {
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "returncode": result.returncode,
            "output": output,
        }

        if result.returncode != 0 or failed > 0 or errors > 0:
            raise TestGateFailure(failed=failed, errors=errors, output=output)


# ---------------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------------


def _parse_summary(output: str, keyword: str) -> int:
    """Extract an integer count for *keyword* from a pytest summary line.

    Parses output such as ``"3 failed, 47 passed in 12.3s"`` and returns the
    integer preceding *keyword*.  Returns ``0`` if no match is found.

    Args:
        output: The combined stdout/stderr text from a pytest subprocess run.
        keyword: One of ``"passed"``, ``"failed"``, or ``"error"``.

    Returns:
        Integer count, or ``0`` if the keyword was not found in the output.
    """
    match = re.search(rf"(\d+)\s+{re.escape(keyword)}", output)
    return int(match.group(1)) if match else 0
