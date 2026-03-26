"""
paths — file-system layout constants and ProjectPaths factory.

Module-level constants and the :class:`ProjectPaths` dataclass that derives
every runtime path from a single project root directory.

Constants:
    BASE_CONFIG: Root of the default project's ``config/`` directory.
    BASE_CONFIG_IMPORT: Default ``config/import/`` directory (bank-parsing TOMLs).
    BASE_CONFIG_EXPORT: Default ``config/export/`` directory (export spec files).
    BASE_CONFIG_REPORT: Default ``config/report/`` directory (report config).
    BASE_CONFIG_USER: Default ``config/user/`` directory (user-specific config).
    MODULES: The ``modules/`` directory inside the installed package.
    DATA: The ``data/`` directory inside the installed package.
"""

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

# Root of the default project's config/ directory.
BASE_CONFIG: Path = _DEFAULT_PROJECT_ROOT / "config"

# Sub-directory constants beneath BASE_CONFIG.
BASE_CONFIG_IMPORT: Path = BASE_CONFIG / "import"
BASE_CONFIG_EXPORT: Path = BASE_CONFIG / "export"
BASE_CONFIG_REPORT: Path = BASE_CONFIG / "report"
BASE_CONFIG_USER: Path = BASE_CONFIG / "user"

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
            import/    ← TOML configs for parsing bank statements
            export/    ← export spec TOML files
            report/    ← report config (reserved)
            user/      ← user-specific config (e.g. anonymise.toml)
          parquet/
          database/
            project.db
          export/
            csv/  excel/  json/
          reporting/
            data/
              simple/  full/
          log/
            debug/

    Use :meth:`resolve` to obtain an instance rather than instantiating
    this class directly.
    """

    root: Path

    # ------------------------------------------------------------------
    # Sub-directories
    # ------------------------------------------------------------------

    @property
    def config_root(self) -> Path:
        """Root config directory (``config/``)."""
        return self.root / "config"

    @property
    def config_import(self) -> Path:
        """Directory containing import TOML config files (``config/import/``)."""
        return self.config_root / "import"

    @property
    def config_export(self) -> Path:
        """Directory containing export spec files (``config/export/``)."""
        return self.config_root / "export"

    @property
    def config_report(self) -> Path:
        """Directory containing report config files (``config/report/``)."""
        return self.config_root / "report"

    @property
    def config_user(self) -> Path:
        """Directory containing user-specific config files (``config/user/``)."""
        return self.config_root / "user"

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
    def export_config(self) -> Path:
        """Directory containing export spec files (``config/export/``)."""
        return self.config_export

    def export_output(self, spec_stem: str) -> Path:
        """Output directory for a named export spec (``export/<spec_stem>/``).

        Args:
            spec_stem: The stem of the spec TOML file (filename without extension),
                e.g. ``"quickbooks_3column"``.

        Returns:
            A :class:`Path` pointing to ``export/<spec_stem>/`` inside the project.
        """
        return self.exports / spec_stem

    @property
    def reporting(self) -> Path:
        """Root reporting directory."""
        return self.root / "reporting"

    @property
    def reporting_data(self) -> Path:
        """Directory containing reporting data feeds (CSV files)."""
        return self.reporting / "data"

    @property
    def reporting_data_simple(self) -> Path:
        """Reporting data directory for the simple (flat transactions) feed."""
        return self.reporting_data / "simple"

    @property
    def reporting_data_full(self) -> Path:
        """Reporting data directory for the full (star-schema) feed."""
        return self.reporting_data / "full"

    @property
    def statements(self) -> Path:
        """Root directory for copied statement PDFs."""
        return self.root / "statements"

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

    @property
    def forex_config(self) -> Path:
        """Path to the optional forex API config file."""
        return self.config_import / "forex_api_config.toml"

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

    def cab_temp(self, id: int, batch_id: str) -> Path:
        """Temporary checks-and-balances file for PDF at index *id* in batch *batch_id*."""
        return self.parquet / f"checks_and_balances_temp_{batch_id}_{id}.parquet"

    def batch_lines_temp(self, id: int, batch_id: str) -> Path:
        """Temporary batch-lines file for PDF at index *id* in batch *batch_id*."""
        return self.parquet / f"batch_lines_temp_{batch_id}_{id}.parquet"

    def statement_heads_temp(self, id: int, batch_id: str) -> Path:
        """Temporary statement-heads file for PDF at index *id* in batch *batch_id*."""
        return self.parquet / f"statement_heads_temp_{batch_id}_{id}.parquet"

    def statement_lines_temp(self, id: int, batch_id: str) -> Path:
        """Temporary statement-lines file for PDF at index *id* in batch *batch_id*."""
        return self.parquet / f"statement_lines_temp_{batch_id}_{id}.parquet"

    # ------------------------------------------------------------------
    # Filename stems (no directory, no extension)
    # ------------------------------------------------------------------

    def cab_temp_stem(self, id: int, batch_id: str) -> str:
        return f"checks_and_balances_temp_{batch_id}_{id}"

    def batch_lines_temp_stem(self, id: int, batch_id: str) -> str:
        return f"batch_lines_temp_{batch_id}_{id}"

    def statement_heads_temp_stem(self, id: int, batch_id: str) -> str:
        return f"statement_heads_temp_{batch_id}_{id}"

    def statement_lines_temp_stem(self, id: int, batch_id: str) -> str:
        return f"statement_lines_temp_{batch_id}_{id}"

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def resolve(cls, project_path: Path | None = None) -> "ProjectPaths":
        """Return a :class:`ProjectPaths` for *project_path*.

        When *project_path* is ``None``, the default project folder bundled
        with the package is used (``src/bank_statement_parser/project/``).

        This is a pure path-computation factory.  It does **not** validate
        that the project directory exists or is correctly structured — call
        :func:`validate_or_initialise_project` for that (done automatically
        by :class:`~bank_statement_parser.modules.statements.Statement` and
        :class:`~bank_statement_parser.modules.statements.StatementBatch`).

        Args:
            project_path: Root of the project directory tree.  Must follow
                the standard sub-directory layout (``config/``, ``parquet/``,
                ``database/``, etc.).  Pass ``None`` to use the default
                bundled project.

        Returns:
            A :class:`ProjectPaths` instance with all derived path attributes.
        """
        return cls(root=project_path if project_path is not None else _DEFAULT_PROJECT_ROOT)

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
            self.config_import,
            self.config_export,
            self.config_report,
            self.config_user,
            self.parquet,
            self.database,
            self.csv,
            self.excel,
            self.json,
            self.export_config,
            self.reporting_data_simple,
            self.reporting_data_full,
            self.log_debug,
        ):
            directory.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Ensure the default project structure exists on first import
# ---------------------------------------------------------------------------

ProjectPaths.resolve().ensure_dirs()


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

    paths = ProjectPaths.resolve(project_path)

    has_toml = paths.config_import.is_dir() and bool(list(paths.config_import.rglob("*.toml")))
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
        raise ProjectConfigMissing(paths.config_import)

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

    # 2. Copy default TOML config files (including any company subfolders).
    for src in BASE_CONFIG_IMPORT.rglob("*.toml"):
        relative = src.relative_to(BASE_CONFIG_IMPORT)
        dst = paths.config_import / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copy2(src, dst)

    # 3. Create the SQLite database with the full schema.
    #    Import here to avoid a circular dependency at module level
    #    (database.py → paths.py; paths.py must not import database.py at top).
    from bank_statement_parser.data.create_project_db import main as create_db  # noqa: PLC0415

    create_db(db_path=paths.project_db, with_fk=True)
