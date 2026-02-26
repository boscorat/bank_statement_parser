from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from bank_statement_parser.modules.errors import (
    ProjectConfigMissing,
    ProjectDatabaseMissing,
    ProjectFolderNotFound,
    ProjectSubFolderNotFound,
)

# ---------------------------------------------------------------------------
# Package-internal constants (never change — tied to the installed package)
# ---------------------------------------------------------------------------

# The shipped default TOML config files (inside the package, not the project).
_BSP = Path(__file__).parent.parent

# The default project root bundled with the package.
_DEFAULT_PROJECT_ROOT: Path = _BSP.joinpath("project")

# Base config lives inside the default project's config sub-directory.
BASE_CONFIG = _DEFAULT_PROJECT_ROOT / "config"

# The modules directory (used by a few internal references).
MODULES = _BSP.joinpath("modules")
DATA = _BSP.joinpath("data")


# ---------------------------------------------------------------------------
# ProjectPaths — all runtime paths derived from a single project root
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """
    All file-system paths for a bank_statement_parser project, derived from
    a single root directory.

    Sub-directory layout (mirrors the default project folder)::

        <root>/
          config/
          parquet/
          database/
            project.db
          export/
            csv/  excel/  json/
          log/
            debug/

    Use :func:`get_paths` to obtain an instance rather than instantiating
    this class directly.
    """

    root: Path

    # ------------------------------------------------------------------
    # Sub-directories
    # ------------------------------------------------------------------

    @property
    def config(self) -> Path:
        """Directory containing TOML config files."""
        return self.root / "config"

    @property
    def parquet(self) -> Path:
        """Directory containing permanent and temporary Parquet files."""
        return self.root / "parquet"

    @property
    def database(self) -> Path:
        """Directory containing the SQLite database."""
        return self.root / "database"

    @property
    def exports(self) -> Path:
        """Root exports directory."""
        return self.root / "export"

    @property
    def csv(self) -> Path:
        return self.exports / "csv"

    @property
    def excel(self) -> Path:
        return self.exports / "excel"

    @property
    def json(self) -> Path:
        return self.exports / "json"

    @property
    def statements(self) -> Path:
        """Root directory for statement copies organised by year and account."""
        return self.root / "statements"

    def statements_dir(self, year: str, id_account: str) -> Path:
        """Directory for statement copies for a given year and account."""
        return self.statements / year / id_account

    @property
    def logs(self) -> Path:
        return self.root / "log"

    @property
    def log_debug(self) -> Path:
        return self.logs / "debug"

    def log_debug_dir(self, filename: str) -> Path:
        """Returns the per-statement debug output directory inside log/debug/."""
        return self.log_debug / filename

    # ------------------------------------------------------------------
    # Database file
    # ------------------------------------------------------------------

    @property
    def project_db(self) -> Path:
        """Path to the SQLite database file."""
        return self.database / "project.db"

    # ------------------------------------------------------------------
    # Permanent Parquet files
    # ------------------------------------------------------------------

    @property
    def cab(self) -> Path:
        return self.parquet / "checks_and_balances.parquet"

    @property
    def batch_heads(self) -> Path:
        return self.parquet / "batch_heads.parquet"

    @property
    def batch_lines(self) -> Path:
        return self.parquet / "batch_lines.parquet"

    @property
    def statement_heads(self) -> Path:
        return self.parquet / "statement_heads.parquet"

    @property
    def statement_lines(self) -> Path:
        return self.parquet / "statement_lines.parquet"

    # ------------------------------------------------------------------
    # Log files
    # ------------------------------------------------------------------

    @property
    def log_error(self) -> Path:
        return self.logs / "error.parquet"

    @property
    def log_perf(self) -> Path:
        return self.logs / "perf.parquet"

    # ------------------------------------------------------------------
    # Temporary Parquet files (written per-PDF during batch processing)
    # ------------------------------------------------------------------

    def cab_temp(self, id: int) -> Path:
        """Temporary checks-and-balances file for PDF at index *id*."""
        return self.parquet / f"checks_and_balances_temp_{id}.parquet"

    def batch_lines_temp(self, id: int) -> Path:
        """Temporary batch-lines file for PDF at index *id*."""
        return self.parquet / f"batch_lines_temp_{id}.parquet"

    def statement_heads_temp(self, id: int) -> Path:
        """Temporary statement-heads file for PDF at index *id*."""
        return self.parquet / f"statement_heads_temp_{id}.parquet"

    def statement_lines_temp(self, id: int) -> Path:
        """Temporary statement-lines file for PDF at index *id*."""
        return self.parquet / f"statement_lines_temp_{id}.parquet"

    # ------------------------------------------------------------------
    # Filename stems (no directory, no extension)
    # ------------------------------------------------------------------

    def cab_temp_stem(self, id: int) -> str:
        return f"checks_and_balances_temp_{id}"

    def batch_lines_temp_stem(self, id: int) -> str:
        return f"batch_lines_temp_{id}"

    def statement_heads_temp_stem(self, id: int) -> str:
        return f"statement_heads_temp_{id}"

    def statement_lines_temp_stem(self, id: int) -> str:
        return f"statement_lines_temp_{id}"

    # ------------------------------------------------------------------
    # Sub-directory validation helpers
    # ------------------------------------------------------------------

    def require_subdir_for_read(self, subdir: Path) -> None:
        """Raise ProjectSubFolderNotFound if *subdir* does not exist (read guard)."""
        if not subdir.is_dir():
            raise ProjectSubFolderNotFound(subdir)

    def ensure_subdir_for_write(self, subdir: Path) -> None:
        """Create *subdir* (and any parents) if it does not already exist (write guard)."""
        subdir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Directory creation
    # ------------------------------------------------------------------

    def ensure_dirs(self) -> None:
        """Create all project sub-directories if they do not already exist."""
        for directory in (
            self.config,
            self.parquet,
            self.database,
            self.csv,
            self.excel,
            self.json,
            self.log_debug,
        ):
            directory.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def validate_project_path(project_path: Path) -> None:
    """
    Raise :exc:`ProjectFolderNotFound` if *project_path* is not an existing directory.

    Args:
        project_path: The project root directory to validate.

    Raises:
        ProjectFolderNotFound: If the path does not exist or is not a directory.
    """
    if not project_path.is_dir():
        raise ProjectFolderNotFound(project_path)


def get_paths(project_path: Path | None = None) -> ProjectPaths:
    """
    Return a :class:`ProjectPaths` for the given project root.

    When *project_path* is ``None``, the default project folder bundled with
    the package is used (``src/bank_statement_parser/project/``).

    This is a pure path-computation factory. It does **not** validate that the
    project directory exists or is correctly structured — use
    :func:`validate_or_initialise_project` for that (called automatically by
    :class:`~bank_statement_parser.modules.statements.Statement` and
    :class:`~bank_statement_parser.modules.statements.StatementBatch`).

    Args:
        project_path: Root of the project directory tree.  Must follow the
            standard sub-directory layout (``config/``, ``parquet/``,
            ``database/``, etc.).

    Returns:
        A :class:`ProjectPaths` instance with all derived path attributes.
    """
    return ProjectPaths(root=project_path if project_path is not None else _DEFAULT_PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Ensure the default project structure exists on first import
# ---------------------------------------------------------------------------

_default_paths = get_paths()
_default_paths.ensure_dirs()


# ---------------------------------------------------------------------------
# copy_project_folders — public helper
# ---------------------------------------------------------------------------


def copy_project_folders(destination: Path) -> list[Path]:
    """
    Copy the project folder structure (directories only) to a destination.

    Recursively copies every sub-directory of the default ``project`` folder
    into *destination*, creating the destination and any parents as needed.
    No files are included in the copy — only the folder hierarchy is reproduced.

    Args:
        destination: Root directory to create the project folder structure in.
                     The directory (and any missing parents) will be created if
                     it does not already exist.

    Returns:
        List of ``Path`` objects for every directory that was created.

    Raises:
        NotADirectoryError: If *destination* exists but is a file, not a directory.

    Example::

        import bank_statement_parser as bsp
        from pathlib import Path

        bsp.copy_project_folders(Path("~/my_project").expanduser())
    """
    if destination.exists() and not destination.is_dir():
        raise NotADirectoryError(f"Destination exists and is not a directory: {destination}")

    destination.mkdir(parents=True, exist_ok=True)

    created: list[Path] = []
    for src_dir in sorted(_DEFAULT_PROJECT_ROOT.rglob("*")):
        if src_dir.is_dir():
            relative = src_dir.relative_to(_DEFAULT_PROJECT_ROOT)
            dest_dir = destination / relative
            dest_dir.mkdir(parents=True, exist_ok=True)
            created.append(dest_dir)

    return created


# ---------------------------------------------------------------------------
# validate_or_initialise_project — called by Statement and StatementBatch only
# ---------------------------------------------------------------------------


def validate_or_initialise_project(project_path: Path) -> None:
    """
    Validate an existing project or initialise a new one at *project_path*.

    This is the single gatekeeper for project-path correctness and must be
    called once — at the top of
    :class:`~bank_statement_parser.modules.statements.Statement` and
    :class:`~bank_statement_parser.modules.statements.StatementBatch` — before
    any downstream code touches files within the project.  All other functions
    and classes that accept *project_path* rely on this guarantee and will
    raise specific errors if required files are absent rather than trying to
    create them.

    Decision table (evaluated top-to-bottom):

    +------------------------------------------+-----------------------------------+
    | Condition                                | Action                            |
    +==========================================+===================================+
    | *project_path* does not exist            | raise :exc:`ProjectFolderNotFound`|
    +------------------------------------------+-----------------------------------+
    | Root exists; no config ``.toml`` files   | scaffold new project (create all  |
    | **and** no ``database/project.db``       | dirs, copy default TOMLs, create  |
    |                                          | empty ``project.db``)             |
    +------------------------------------------+-----------------------------------+
    | Root exists; config present; DB absent;  | create ``project.db`` only        |
    | root is the default bundled project      | (config is committed to source    |
    |                                          | control; DB is excluded)          |
    +------------------------------------------+-----------------------------------+
    | Root exists; config present; DB absent   | raise :exc:`ProjectDatabaseMissing`|
    | (custom project path)                    |                                   |
    +------------------------------------------+-----------------------------------+
    | Root exists; DB present; config absent   | raise :exc:`ProjectConfigMissing` |
    +------------------------------------------+-----------------------------------+
    | Root exists; both config and DB present  | no-op (valid project)             |
    +------------------------------------------+-----------------------------------+

    Args:
        project_path: The project root directory to validate or initialise.

    Raises:
        ProjectFolderNotFound: If *project_path* does not exist.
        ProjectDatabaseMissing: If the project looks like an existing project
            (config present) but ``database/project.db`` is absent and the
            project is not the default bundled project.
        ProjectConfigMissing: If the project looks like an existing project
            (database present) but ``config/`` contains no ``.toml`` files.
    """
    # Rule 1: root must exist.
    if not project_path.is_dir():
        raise ProjectFolderNotFound(project_path)

    paths = ProjectPaths(root=project_path)

    has_toml = paths.config.is_dir() and bool(list(paths.config.glob("*.toml")))
    has_db = paths.project_db.exists()

    # Rule 2: neither present → new project, scaffold in full.
    if not has_toml and not has_db:
        _scaffold_new_project(paths)
        return

    # Rule 3: config present but DB missing.
    if has_toml and not has_db:
        # The default bundled project ships with config committed to source
        # control but excludes project.db from version control.  Create it
        # automatically rather than raising an error.
        if project_path.resolve() == _DEFAULT_PROJECT_ROOT.resolve():
            _create_project_db(paths)
            return
        raise ProjectDatabaseMissing(paths.project_db)

    # Rule 4: DB present but config missing → corrupted/partial project.
    if has_db and not has_toml:
        raise ProjectConfigMissing(paths.config)

    # Rule 5: both present → valid, nothing to do.


def _create_project_db(paths: ProjectPaths) -> None:
    """
    Create a new empty SQLite database for an existing project.

    Used when the project directory and config are already in place (e.g. the
    default bundled project on a fresh clone where ``project.db`` is excluded
    from source control) but the database file itself is absent.

    This is an internal helper called only by
    :func:`validate_or_initialise_project`.

    Args:
        paths: A :class:`ProjectPaths` instance for the target project.
    """
    paths.ensure_subdir_for_write(paths.database)
    from bank_statement_parser.data.create_project_db import main as create_db  # noqa: PLC0415

    create_db(db_path=paths.project_db, with_fk=True)


def _scaffold_new_project(paths: ProjectPaths) -> None:
    """
    Create all project sub-directories, copy default TOML configs, and
    initialise a new empty SQLite database.

    This is an internal helper called only by
    :func:`validate_or_initialise_project`.

    Args:
        paths: A :class:`ProjectPaths` instance whose ``root`` is the
               destination for the new project.
    """
    # 1. Create the full directory tree.
    paths.ensure_dirs()

    # 2. Copy default TOML config files (skip any that already exist).
    for src in BASE_CONFIG.glob("*.toml"):
        dst = paths.config / src.name
        if not dst.exists():
            shutil.copy2(src, dst)

    # 3. Create the SQLite database with the full schema.
    #    Import here to avoid a circular dependency at module level
    #    (database.py → paths.py; paths.py must not import database.py at top).
    from bank_statement_parser.data.create_project_db import main as create_db  # noqa: PLC0415

    create_db(db_path=paths.project_db, with_fk=True)
