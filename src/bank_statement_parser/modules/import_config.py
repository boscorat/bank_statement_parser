"""
Import configuration management for bank statement parsing.

This module provides :class:`ImportConfigManager` for loading and accessing
TOML-based configuration files that drive the bank-statement import pipeline
(companies, accounts, statement types, statement tables, standard fields).

Configuration files live under ``<project>/config/import/``.  The shipped
defaults are in ``src/bank_statement_parser/project/config/import/``.
"""

import shutil
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import polars as pl
from dacite import from_dict
from pdfplumber.pdf import PDF
from tomllib import load

from bank_statement_parser.modules.data import (
    Account,
    AccountType,
    Company,
    StandardFields,
    StatementTable,
    StatementType,
)
from bank_statement_parser.modules.errors import ConfigError, ProjectConfigMissing, StatementError
from bank_statement_parser.modules.paths import BASE_CONFIG_IMPORT, ProjectPaths
from bank_statement_parser.modules.statement_functions import get_results


class _ConfigEntry(TypedDict):
    """Internal structure for each section of the config loading dict."""

    dataclass: type
    config: dict[str, Any]


REQUIRED_CONFIG_FILES: list[str] = [
    "companies.toml",
    "account_types.toml",
    "accounts.toml",
    "statement_types.toml",
    "statement_tables.toml",
    "standard_fields.toml",
]


def copy_default_import_config(destination: Path, overwrite: bool = False) -> list[Path]:
    """
    Copy all default import TOML configuration files to a destination directory.

    Copies every ``*.toml`` file (including company sub-directories) from the
    shipped ``base_config/import`` folder into *destination*, preserving the
    relative directory structure.  The destination directory (and any parents)
    are created if they do not already exist.

    The copied files are the starting-point for a user config override: place
    the returned files in ``<project_path>/config/import/`` and pass that
    project root as ``project_path`` to :class:`ImportConfigManager` (or
    ``Statement``/``StatementBatch``) to override the built-in defaults.

    Args:
        destination: Directory to copy the TOML files into.
        overwrite: If ``True``, existing files in *destination* are replaced.
                   If ``False`` (the default), existing files are left untouched
                   and skipped silently.

    Returns:
        List of ``Path`` objects for every file that was actually copied.

    Raises:
        NotADirectoryError: If *destination* exists but is a file, not a directory.

    Example::

        import bank_statement_parser as bsp
        from pathlib import Path

        copied = bsp.copy_default_import_config(Path("my_project/config/import"))
        # Edit the TOML files, then:
        batch = bsp.StatementBatch(pdfs=[...], project_path=Path("my_project"))
    """
    if destination.exists() and not destination.is_dir():
        raise NotADirectoryError(f"Destination exists and is not a directory: {destination}")

    destination.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    for src in BASE_CONFIG_IMPORT.rglob("*.toml"):
        relative = src.relative_to(BASE_CONFIG_IMPORT)
        dst = destination.joinpath(relative)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not overwrite:
            continue
        shutil.copy2(src, dst)
        copied.append(dst)

    return copied


class ImportConfigManager:
    """
    Manages import configuration loading and access for bank statement parsing.

    Loads TOML configuration files from ``<project>/config/import/``, converts
    them to dataclasses, links cross-section references, and provides lookup
    methods for accounts, companies, and statement types.

    Attributes:
        _project_path: Optional project root directory path.
        _config_dict: Internal storage for loaded configuration.
        _accounts_df: Lazy-loaded DataFrame of accounts.
        _statement_types_df: Lazy-loaded DataFrame of statement types.
        _companies_df: Lazy-loaded DataFrame of companies.

    Example:
        >>> config = ImportConfigManager()
        >>> account = config.get_account("my_account")
        >>> accounts = config.get_accounts_for_company("my_company")
    """

    __slots__ = ("_project_path", "_config_dict", "_accounts_df", "_statement_types_df", "_companies_df")

    def __init__(self, project_path: Path | None = None) -> None:
        """
        Initialise ImportConfigManager with an optional project path.

        Args:
            project_path: Optional Path to the project root directory.
                          Config files are read from
                          ``project_path / "config" / "import"``.
                          If ``None``, falls back to the shipped
                          ``BASE_CONFIG_IMPORT`` directory bundled with the
                          package.
        """
        self._project_path: Path | None = project_path
        self._config_dict: dict[str, _ConfigEntry] | None = None
        self._accounts_df: pl.DataFrame | None = None
        self._statement_types_df: pl.DataFrame | None = None
        self._companies_df: pl.DataFrame | None = None

    @property
    def config_dir(self) -> Path:
        """Return the effective import configuration directory path."""
        if self._project_path is not None:
            return ProjectPaths.resolve(self._project_path).config_import
        return BASE_CONFIG_IMPORT

    @property
    def config_dict(self) -> dict[str, _ConfigEntry]:
        """Return the full configuration dictionary, loading if necessary."""
        if self._config_dict is None:
            self._load_config()
        return self._config_dict  # type: ignore[return-value]

    @property
    def accounts(self) -> dict[str, Account]:
        """Return dictionary of all configured accounts keyed by account name."""
        return self.config_dict["accounts"]["config"]

    @property
    def statement_types(self) -> dict[str, StatementType]:
        """Return dictionary of all statement types keyed by type name."""
        return self.config_dict["statement_types"]["config"]

    @property
    def companies(self) -> dict[str, Company]:
        """Return dictionary of all configured companies keyed by company name."""
        return self.config_dict["companies"]["config"]

    @property
    def standard_fields(self) -> dict[str, StandardFields]:
        """Return dictionary of all standard fields keyed by field name."""
        return self.config_dict["standard_fields"]["config"]

    @property
    def accounts_df(self) -> pl.DataFrame:
        """Return accounts as a Polars DataFrame with account names as headers."""
        if self._accounts_df is None:
            self._accounts_df = pl.DataFrame(self.accounts).transpose(include_header=True, header_name="account", column_names=["config"])
        return self._accounts_df

    @property
    def statement_types_df(self) -> pl.DataFrame:
        """Return statement types as a Polars DataFrame with type names as headers."""
        if self._statement_types_df is None:
            self._statement_types_df = pl.DataFrame(self.statement_types).transpose(
                include_header=True, header_name="statement_type", column_names=["config"]
            )
        return self._statement_types_df

    @property
    def companies_df(self) -> pl.DataFrame:
        """Return companies as a Polars DataFrame with company names as headers."""
        if self._companies_df is None:
            self._companies_df = pl.DataFrame(self.companies).transpose(include_header=True, header_name="company", column_names=["config"])
        return self._companies_df

    def _require_config_dir(self) -> None:
        """
        Validate that the project import config directory exists and contains .toml files.

        Only called when ``_project_path`` is not ``None``.  The project is
        expected to have been initialised already (by
        :func:`~bank_statement_parser.modules.paths.validate_or_initialise_project`
        called from ``Statement`` or ``StatementBatch``).

        Scans both the root config/import directory and any immediate company
        subdirectories for ``.toml`` files.

        Raises:
            ProjectConfigMissing: If ``config/import/`` is absent or contains
                no ``.toml`` files at any level.
        """
        config_dir = self.config_dir
        if not config_dir.is_dir() or not list(config_dir.rglob("*.toml")):
            raise ProjectConfigMissing(config_dir)

    def _load_config(self) -> None:
        """
        Load and parse all TOML configuration files.

        For each config section, loads the root-level file (e.g.
        ``config/import/account_types.toml``) if present, then discovers and
        merges all matching files from immediate company subdirectories (e.g.
        ``config/import/HSBC_UK/accounts.toml``).  All top-level TOML keys
        must be unique across files; no key may appear in more than one file
        for the same section.

        Converts raw TOML data to dataclasses and links cross-section
        references between statement tables and account objects.
        """
        if self._project_path is not None:
            self._require_config_dir()

        config_dict: dict[str, _ConfigEntry] = {
            "companies": {"dataclass": Company, "config": dict()},
            "account_types": {"dataclass": AccountType, "config": dict()},
            "accounts": {"dataclass": Account, "config": dict()},
            "statement_types": {"dataclass": StatementType, "config": dict()},
            "statement_tables": {"dataclass": StatementTable, "config": dict()},
            "standard_fields": {"dataclass": StandardFields, "config": dict()},
        }

        for key in config_dict:
            file_name = f"{key}.toml"
            # Load root-level file first (e.g. account_types.toml, standard_fields.toml)
            self._merge_toml_file(config_dict, key, self.config_dir.joinpath(file_name))
            # Load matching files from each immediate company subdirectory
            for subdir in sorted(self.config_dir.iterdir()):
                if subdir.is_dir():
                    self._merge_toml_file(config_dict, key, subdir.joinpath(file_name))

        for v in config_dict.values():
            for k in v["config"]:
                v["config"][k] = from_dict(data_class=v["dataclass"], data=v["config"][k])

        self._link_statement_tables(config_dict)
        self._link_account_references(config_dict)

        self._config_dict = config_dict

    def _merge_toml_file(self, config_dict: dict[str, _ConfigEntry], key: str, file_path: Path) -> None:
        """
        Load a single TOML file and merge its entries into the config dictionary.

        Each top-level key in the TOML file is added to the section's config
        dict.  If the file does not exist it is silently skipped.  Duplicate
        top-level keys across files for the same section will overwrite earlier
        values; config authors should ensure all keys are unique.

        Args:
            config_dict: The configuration dictionary to populate.
            key: The config section key (e.g., 'companies', 'accounts').
            file_path: Path to the TOML file to load.
        """
        try:
            with open(file_path, "rb") as toml:
                config_dict[key]["config"].update(deepcopy(load(toml)))
        except FileNotFoundError:
            pass

    def _link_statement_tables(self, config_dict: dict[str, _ConfigEntry]) -> None:
        """
        Link statement table configurations to their parent statement types.

        For each statement type header and lines config that has a
        ``statement_table_key``, resolves the actual :class:`StatementTable`
        object from the config dictionary.

        Args:
            config_dict: The fully loaded (but not yet linked) config dictionary.
        """
        for key, statement_type in config_dict["statement_types"]["config"].items():
            for config_group in [statement_type.header.configs, statement_type.lines.configs]:
                if config_group:
                    for idx, cfg in enumerate(config_group):
                        if cfg.statement_table_key:
                            config_group[idx].statement_table = config_dict["statement_tables"]["config"][cfg.statement_table_key]

    def _link_account_references(self, config_dict: dict[str, _ConfigEntry]) -> None:
        """
        Link account objects to their corresponding account type, statement type, and company.

        Populates the ``account_type``, ``statement_type``, and ``company``
        attributes on each :class:`Account` object by looking up the referenced
        keys.  Also validates that ``account.currency`` is a recognised ISO 4217
        code present in ``currency_spec``.

        Args:
            config_dict: The fully loaded (but not yet linked) config dictionary.

        Raises:
            ConfigError: If ``account.currency`` is not a key in ``currency_spec``.
        """
        from bank_statement_parser.modules.currency import currency_spec

        for key, account in config_dict["accounts"]["config"].items():
            account.account_type = config_dict["account_types"]["config"][account.account_type_key]
            account.statement_type = config_dict["statement_types"]["config"][account.statement_type_key]
            account.company = config_dict["companies"]["config"][account.company_key]
            if account.currency not in currency_spec:
                valid = ", ".join(sorted(currency_spec.keys()))
                raise ConfigError(f"Account '{key}': currency '{account.currency}' is not a recognised ISO 4217 code. Valid codes: {valid}")

    def get_account(self, account_key: str) -> Account | None:
        """
        Retrieve an account by its key name.

        Args:
            account_key: The unique identifier for the account.

        Returns:
            The Account object if found, ``None`` otherwise.
        """
        return self.accounts.get(account_key)

    def get_accounts_for_company(self, company_key: str) -> list[Account]:
        """
        Get all accounts belonging to a specific company.

        Args:
            company_key: The company identifier to filter by.

        Returns:
            List of Account objects for the specified company.
        """
        return [acct for acct in self.accounts.values() if acct.company_key == company_key]

    def get_company(self, company_key: str) -> Company | None:
        """
        Retrieve a company by its key name.

        Args:
            company_key: The unique identifier for the company.

        Returns:
            The Company object if found, ``None`` otherwise.
        """
        return self.companies.get(company_key)

    def identify_from_pdf(self, pdf: PDF, file_path: str, logs: pl.DataFrame) -> tuple[Account, str]:
        """
        Identify the company and account from a PDF bank statement.

        Attempts to match the PDF against known company configurations by
        looking for identifying text patterns.  Returns the matched account
        and company key.

        Args:
            pdf: The opened PDF object.
            file_path: Path to the PDF file being processed.
            logs: Polars DataFrame for logging operations.

        Returns:
            Tuple of (Account object, company_key string).

        Raises:
            StatementError: If the company cannot be identified.
        """
        start = time.time()

        for key, company in self.companies.items():
            config = company.config
            if not config:
                continue
            result = get_results(pdf, "pick", config, scope="success", logs=logs, file_path=file_path)
            if len(result) > 0:
                logs.vstack(
                    pl.DataFrame(
                        [[file_path, "config", "identify_from_pdf", time.time() - start, 1, datetime.now(), ""]],
                        schema=logs.schema,
                        orient="row",
                    ),
                    in_place=True,
                )
                account = self.get_accounts_for_company(key)[0]
                return account, key

        raise StatementError("Unable to identify the company from the statement provided")

    def get_config_from_account(self, account_key: str, logs: pl.DataFrame, file_path: str) -> Account:
        """
        Retrieve an account configuration with performance logging.

        Args:
            account_key: The account identifier to look up.
            logs: Polars DataFrame for logging operations.
            file_path: Path to the PDF file being processed.

        Returns:
            The Account object.

        Raises:
            StatementError: If the account cannot be found.
        """
        start = time.time()
        account = self.get_account(account_key)
        if not account:
            raise StatementError(f"Unable to identify the account from the statement provided: {file_path}")
        logs.vstack(
            pl.DataFrame(
                [[file_path, "config", "get_config_from_account", time.time() - start, 1, datetime.now(), ""]],
                schema=logs.schema,
                orient="row",
            ),
            in_place=True,
        )
        return account

    def get_config_from_company(self, company_key: str, pdf: PDF, logs: pl.DataFrame, file_path: str) -> Account:
        """
        Identify the correct account for a company by testing against the PDF.

        Iterates through all accounts for the given company, attempting to
        match each against the PDF until a successful match is found.

        Args:
            company_key: The company identifier.
            pdf: The opened PDF object.
            logs: Polars DataFrame for logging operations.
            file_path: Path to the PDF file being processed.

        Returns:
            The matched Account object.

        Raises:
            StatementError: If no account can be matched or company is invalid.
        """
        start = time.time()
        company_accounts = self.get_accounts_for_company(company_key)
        if not company_accounts:
            raise StatementError(f"{company_key} is not a valid company key")

        for account in company_accounts:
            config = account.config
            if not config:
                continue
            result = get_results(pdf, "pick", config, scope="success", logs=logs, file_path=file_path)
            if len(result) > 0:
                logs.vstack(
                    pl.DataFrame(
                        [[file_path, "config", "get_config_from_company", time.time() - start, 1, datetime.now(), ""]],
                        schema=logs.schema,
                        orient="row",
                    ),
                    in_place=True,
                )
                return account

        raise StatementError(f"Unable to identify the account from the statement provided: {file_path}")

    def get_config_from_statement(self, pdf: PDF, file_path: str, logs: pl.DataFrame) -> Account:
        """
        Fully identify both company and account from a PDF statement.

        This is the main entry point for automatic detection.  It attempts to
        identify the company first, then the specific account within that company.

        Args:
            pdf: The opened PDF object.
            file_path: Path to the PDF file being processed.
            logs: Polars DataFrame for logging operations.

        Returns:
            The identified Account object.

        Raises:
            StatementError: If neither company nor account can be identified.
        """
        start = time.time()

        for key, company in self.companies.items():
            config = company.config
            if not config:
                continue
            result = get_results(pdf, "pick", config, scope="success", logs=logs, file_path=file_path)
            if len(result) > 0:
                account = self.get_config_from_company(key, pdf, logs, file_path)
                logs.vstack(
                    pl.DataFrame(
                        [[file_path, "config", "get_config_from_statement", time.time() - start, 1, datetime.now(), ""]],
                        schema=logs.schema,
                        orient="row",
                    ),
                    in_place=True,
                )
                return account

        raise StatementError(f"Unable to identify the company from the statement provided: {file_path}")
