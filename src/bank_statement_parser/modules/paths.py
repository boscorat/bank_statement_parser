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

# Anchor: the bank_statement_parser package root (src/bank_statement_parser/).
_BSP: Path = Path(__file__).parent.parent

# The default project root bundled with the package.
_DEFAULT_PROJECT_ROOT: Path = _BSP.joinpath("project")

# Root of the default project's config/ directory.
BASE_CONFIG: Path = _DEFAULT_PROJECT_ROOT.joinpath("config")

# Sub-directory constants beneath BASE_CONFIG.
BASE_CONFIG_IMPORT: Path = BASE_CONFIG.joinpath("import")
BASE_CONFIG_EXPORT: Path = BASE_CONFIG.joinpath("export")
BASE_CONFIG_REPORT: Path = BASE_CONFIG.joinpath("report")
BASE_CONFIG_USER: Path = BASE_CONFIG.joinpath("user")

# The modules and data directories (used by a few internal references).
MODULES: Path = _BSP.joinpath("modules")
DATA: Path = _BSP.joinpath("data")


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
            report/    ← report config
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
    # Config sub-directories
    # ------------------------------------------------------------------

    @property
    def config_root(self) -> Path:
        """Root config directory (``config/``)."""
        return self.root.joinpath("config")

    @property
    def config_import(self) -> Path:
        """Directory containing import TOML config files (``config/import/``)."""
        return self.config_root.joinpath("import")

    @property
    def config_export(self) -> Path:
        """Directory containing export spec files (``config/export/``)."""
        return self.config_root.joinpath("export")

    @property
    def config_report(self) -> Path:
        """Directory containing report config files (``config/report/``)."""
        return self.config_root.joinpath("report")

    @property
    def config_user(self) -> Path:
        """Directory containing user-specific config files (``config/user/``)."""
        return self.config_root.joinpath("user")

    # ------------------------------------------------------------------
    # Data directories
    # ------------------------------------------------------------------

    @property
    def parquet(self) -> Path:
        """Directory containing permanent and temporary Parquet files."""
        return self.root.joinpath("parquet")

    @property
    def database(self) -> Path:
        """Directory containing the SQLite database."""
        return self.root.joinpath("database")

    # ------------------------------------------------------------------
    # Export output directories
    # ------------------------------------------------------------------

    @property
    def exports(self) -> Path:
        """Root exports directory (``export/``)."""
        return self.root.joinpath("export")

    @property
    def csv(self) -> Path:
        """Directory for CSV export output (``export/csv/``)."""
        return self.exports.joinpath("csv")

    @property
    def excel(self) -> Path:
        """Directory for Excel export output (``export/excel/``)."""
        return self.exports.joinpath("excel")

    @property
    def json(self) -> Path:
        """Directory for JSON export output (``export/json/``)."""
        return self.exports.joinpath("json")

    @property
    def export_specs(self) -> Path:
        """Directory containing export spec TOML files (``config/export/``)."""
        return self.config_export

    def export_specs_output(self, spec_stem: str) -> Path:
        """Output directory for a named export spec (``export/<spec_stem>/``).

        Args:
            spec_stem: The stem of the spec TOML file (filename without
                extension), e.g. ``"quickbooks_3column"``.

        Returns:
            A :class:`Path` pointing to ``export/<spec_stem>/`` inside the
            project.
        """
        return self.exports.joinpath(spec_stem)

    # ------------------------------------------------------------------
    # Reporting directories
    # ------------------------------------------------------------------

    @property
    def reporting(self) -> Path:
        """Root reporting directory (``reporting/``)."""
        return self.root.joinpath("reporting")

    @property
    def reporting_data(self) -> Path:
        """Directory containing reporting data feeds (``reporting/data/``)."""
        return self.reporting.joinpath("data")

    @property
    def reporting_data_single(self) -> Path:
        """Reporting data directory for the single (flat transactions) feed."""
        return self.reporting_data.joinpath("single")

    @property
    def reporting_data_multi(self) -> Path:
        """Reporting data directory for the multi (star-schema) feed."""
        return self.reporting_data.joinpath("multi")

    # ------------------------------------------------------------------
    # Statements directory
    # ------------------------------------------------------------------

    @property
    def statements(self) -> Path:
        """Root directory for copied statement PDFs (``statements/``)."""
        return self.root.joinpath("statements")

    # ------------------------------------------------------------------
    # Log directories
    # ------------------------------------------------------------------

    @property
    def logs(self) -> Path:
        """Root log directory (``log/``)."""
        return self.root.joinpath("log")

    @property
    def log_debug(self) -> Path:
        """Directory for per-statement debug output (``log/debug/``)."""
        return self.logs.joinpath("debug")

    def log_debug_dir(self, filename: str) -> Path:
        """Returns the per-statement debug output directory inside ``log/debug/``.

        Args:
            filename: The statement filename used to name the debug sub-directory.

        Returns:
            A :class:`Path` pointing to ``log/debug/<filename>/``.
        """
        return self.log_debug.joinpath(filename)

    # ------------------------------------------------------------------
    # Database file
    # ------------------------------------------------------------------

    @property
    def project_db(self) -> Path:
        """Path to the SQLite database file (``database/project.db``)."""
        return self.database.joinpath("project.db")

    @property
    def forex_config(self) -> Path:
        """Path to the optional forex API config file (``config/import/forex_api_config.toml``)."""
        return self.config_import.joinpath("forex_api_config.toml")

    # ------------------------------------------------------------------
    # Permanent Parquet files
    # ------------------------------------------------------------------

    @property
    def cab(self) -> Path:
        """Permanent checks-and-balances Parquet file."""
        return self.parquet.joinpath("checks_and_balances.parquet")

    @property
    def batch_heads(self) -> Path:
        """Permanent batch-heads Parquet file."""
        return self.parquet.joinpath("batch_heads.parquet")

    @property
    def batch_lines(self) -> Path:
        """Permanent batch-lines Parquet file."""
        return self.parquet.joinpath("batch_lines.parquet")

    @property
    def statement_heads(self) -> Path:
        """Permanent statement-heads Parquet file."""
        return self.parquet.joinpath("statement_heads.parquet")

    @property
    def statement_lines(self) -> Path:
        """Permanent statement-lines Parquet file."""
        return self.parquet.joinpath("statement_lines.parquet")

    # ------------------------------------------------------------------
    # Log files
    # ------------------------------------------------------------------

    @property
    def log_error(self) -> Path:
        """Error log Parquet file (``log/error.parquet``)."""
        return self.logs.joinpath("error.parquet")

    @property
    def log_perf(self) -> Path:
        """Performance log Parquet file (``log/perf.parquet``)."""
        return self.logs.joinpath("perf.parquet")

    # ------------------------------------------------------------------
    # Temporary Parquet files (written per-PDF during batch processing)
    # ------------------------------------------------------------------

    def cab_temp(self, idx: int, batch_id: str) -> Path:
        """Temporary checks-and-balances file for PDF at index *idx* in batch *batch_id*."""
        return self.parquet.joinpath(f"checks_and_balances_temp_{batch_id}_{idx}.parquet")

    def batch_lines_temp(self, idx: int, batch_id: str) -> Path:
        """Temporary batch-lines file for PDF at index *idx* in batch *batch_id*."""
        return self.parquet.joinpath(f"batch_lines_temp_{batch_id}_{idx}.parquet")

    def statement_heads_temp(self, idx: int, batch_id: str) -> Path:
        """Temporary statement-heads file for PDF at index *idx* in batch *batch_id*."""
        return self.parquet.joinpath(f"statement_heads_temp_{batch_id}_{idx}.parquet")

    def statement_lines_temp(self, idx: int, batch_id: str) -> Path:
        """Temporary statement-lines file for PDF at index *idx* in batch *batch_id*."""
        return self.parquet.joinpath(f"statement_lines_temp_{batch_id}_{idx}.parquet")

    # ------------------------------------------------------------------
    # Filename stems (no directory, no extension)
    # ------------------------------------------------------------------

    def cab_temp_stem(self, idx: int, batch_id: str) -> str:
        """Stem of the temporary checks-and-balances file for PDF *idx* in batch *batch_id*."""
        return f"checks_and_balances_temp_{batch_id}_{idx}"

    def batch_lines_temp_stem(self, idx: int, batch_id: str) -> str:
        """Stem of the temporary batch-lines file for PDF *idx* in batch *batch_id*."""
        return f"batch_lines_temp_{batch_id}_{idx}"

    def statement_heads_temp_stem(self, idx: int, batch_id: str) -> str:
        """Stem of the temporary statement-heads file for PDF *idx* in batch *batch_id*."""
        return f"statement_heads_temp_{batch_id}_{idx}"

    def statement_lines_temp_stem(self, idx: int, batch_id: str) -> str:
        """Stem of the temporary statement-lines file for PDF *idx* in batch *batch_id*."""
        return f"statement_lines_temp_{batch_id}_{idx}"

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
        """Raise ProjectSubFolderNotFound if *subdir* does not exist (read guard).

        Args:
            subdir: The sub-directory path to check.

        Raises:
            ProjectSubFolderNotFound: If *subdir* is not a directory.
        """
        if not subdir.is_dir():
            raise ProjectSubFolderNotFound(subdir)

    def ensure_subdir_for_write(self, subdir: Path) -> None:
        """Create *subdir* (and any parents) if it does not already exist (write guard).

        Args:
            subdir: The sub-directory path to create.
        """
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
            self.reporting_data_single,
            self.reporting_data_multi,
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
            dest_dir = destination.joinpath(relative)
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
    Create all project sub-directories, copy default TOML configs, copy default
    export specs, copy default report configs, and initialise a new empty SQLite
    database.

    This is an internal helper called only by
    :func:`validate_or_initialise_project`.

    Args:
        paths: A :class:`ProjectPaths` instance whose ``root`` is the
               destination for the new project.
    """
    # 1. Create the full directory tree.
    paths.ensure_dirs()

    # 2. Copy default import TOML config files (including company subfolders).
    for src in BASE_CONFIG_IMPORT.rglob("*.toml"):
        relative = src.relative_to(BASE_CONFIG_IMPORT)
        dst = paths.config_import.joinpath(relative)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copy2(src, dst)

    # 3. Copy export spec files (.toml and .sql) from the default config/export/.
    for src in BASE_CONFIG_EXPORT.rglob("*"):
        if src.is_file():
            relative = src.relative_to(BASE_CONFIG_EXPORT)
            dst = paths.config_export.joinpath(relative)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                shutil.copy2(src, dst)

    # 4. Copy report config files from the default config/report/.
    for src in BASE_CONFIG_REPORT.rglob("*"):
        if src.is_file():
            relative = src.relative_to(BASE_CONFIG_REPORT)
            dst = paths.config_report.joinpath(relative)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                shutil.copy2(src, dst)

    # 5. Create the SQLite database with the full schema.
    #    Import here to avoid a circular dependency at module level
    #    (database.py → paths.py; paths.py must not import database.py at top).
    from bank_statement_parser.data.create_project_db import main as create_db  # noqa: PLC0415

    create_db(db_path=paths.project_db, with_fk=True)
