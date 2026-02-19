"""
Configuration management for bank statement parsing.

This module provides ConfigManager class for loading and accessing TOML-based
configuration files, as well as backward-compatible module-level functions.
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
from bank_statement_parser.modules.errors import StatementError
from bank_statement_parser.modules.paths import BASE_CONFIG, USER_CONFIG
from bank_statement_parser.modules.statement_functions import get_results


class _ConfigEntry(TypedDict):
    """Internal structure for each section of the config loading dict."""

    dataclass: type
    config: dict[str, Any]


REQUIRED_CONFIG_FILES = [
    "companies.toml",
    "account_types.toml",
    "accounts.toml",
    "statement_types.toml",
    "statement_tables.toml",
    "standard_fields.toml",
]


def copy_default_config(destination: Path, overwrite: bool = False) -> list[Path]:
    """
    Copy all default TOML configuration files to a destination directory.

    Copies every ``*.toml`` file from the shipped ``base_config`` folder into
    *destination*, creating the directory (and any parents) if it does not
    already exist.  The copied files are the starting-point for a user config
    override: place the returned files in a directory and pass that directory
    as ``config_path`` to ``ConfigManager`` (or ``Statement``/``StatementBatch``)
    to override the built-in defaults.

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

        copied = bsp.copy_default_config(Path("my_config"))
        # Edit the TOML files, then:
        batch = bsp.StatementBatch(pdfs=[...], config_path=Path("my_config"))
    """
    if destination.exists() and not destination.is_dir():
        raise NotADirectoryError(f"Destination exists and is not a directory: {destination}")

    destination.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    for src in BASE_CONFIG.glob("*.toml"):
        dst = destination / src.name
        if dst.exists() and not overwrite:
            continue
        shutil.copy2(src, dst)
        copied.append(dst)

    return copied


class ConfigManager:
    """
    Manages configuration loading and access for bank statement parsing.

    This class handles loading TOML configuration files, converting them to
    dataclasses, linking references between config objects, and providing
    access to account, company, and statement type configurations.

    Attributes:
        _config_path: Optional custom config directory path.
        _config_dict: Internal storage for loaded configuration.
        _accounts_df: Lazy-loaded DataFrame of accounts.
        _statement_types_df: Lazy-loaded DataFrame of statement types.
        _companies_df: Lazy-loaded DataFrame of companies.

    Example:
        >>> config = ConfigManager()
        >>> account = config.get_account("my_account")
        >>> accounts = config.get_accounts_for_company("my_company")
    """

    def __init__(self, config_path: Path | None = None):
        """
        Initialize ConfigManager with optional custom config path.

        Args:
            config_path: Optional Path to a custom configuration directory.
                        If None, uses USER_CONFIG or BASE_CONFIG.
        """
        self._config_path = config_path
        self._config_dict: dict | None = None
        self._accounts_df: pl.DataFrame | None = None
        self._statement_types_df: pl.DataFrame | None = None
        self._companies_df: pl.DataFrame | None = None

    @property
    def config_path(self) -> Path:
        """Return the effective configuration directory path."""
        if self._config_path is not None:
            return self._config_path
        return USER_CONFIG if USER_CONFIG.exists() else BASE_CONFIG

    @property
    def config_dict(self) -> dict:
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

    def _load_config(self) -> None:
        """
        Load and parse all TOML configuration files.

        Attempts to load from USER_CONFIG first, falling back to BASE_CONFIG.
        Converts raw TOML data to dataclasses and links references between
        statement tables and account objects.
        """
        config_dict: dict[str, _ConfigEntry] = {
            "companies": {"dataclass": Company, "config": dict()},
            "account_types": {"dataclass": AccountType, "config": dict()},
            "accounts": {"dataclass": Account, "config": dict()},
            "statement_types": {"dataclass": StatementType, "config": dict()},
            "statement_tables": {"dataclass": StatementTable, "config": dict()},
            "standard_fields": {"dataclass": StandardFields, "config": dict()},
        }

        for key in config_dict:
            file_path = f"{key}.toml"
            self._load_toml_file(config_dict, key, USER_CONFIG / file_path)
            if not config_dict[key]["config"]:
                self._load_toml_file(config_dict, key, BASE_CONFIG / file_path)

        for v in config_dict.values():
            for k in v["config"]:
                v["config"][k] = from_dict(data_class=v["dataclass"], data=v["config"][k])

        self._link_statement_tables(config_dict)
        self._link_account_references(config_dict)

        self._config_dict = config_dict

    def _load_toml_file(self, config_dict: dict[str, _ConfigEntry], key: str, file_path: Path) -> None:
        """
        Load a single TOML file into the config dictionary.

        Args:
            config_dict: The configuration dictionary to populate.
            key: The config section key (e.g., 'companies', 'accounts').
            file_path: Path to the TOML file to load.
        """
        try:
            with open(file_path, "rb") as toml:
                config_dict[key]["config"] = deepcopy(load(toml))
        except FileNotFoundError:
            pass

    def _link_statement_tables(self, config_dict: dict[str, _ConfigEntry]) -> None:
        """
        Link statement table configurations to their parent statement types.

        For each statement type header and lines config that has a statement_table_key,
        resolves the actual StatementTable object from the config dictionary.
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

        Populates the account_type, statement_type, and company attributes on each
        Account object by looking up the referenced keys.
        """
        for key, account in config_dict["accounts"]["config"].items():
            account.account_type = config_dict["account_types"]["config"][account.account_type_key]
            account.statement_type = config_dict["statement_types"]["config"][account.statement_type_key]
            account.company = config_dict["companies"]["config"][account.company_key]

    def get_account(self, account_key: str) -> Account | None:
        """
        Retrieve an account by its key name.

        Args:
            account_key: The unique identifier for the account.

        Returns:
            The Account object if found, None otherwise.
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
            The Company object if found, None otherwise.
        """
        return self.companies.get(company_key)

    def identify_from_pdf(self, pdf: PDF, file_path: str, logs: pl.DataFrame) -> tuple[Account, str]:
        """
        Identify the company and account from a PDF bank statement.

        Attempts to match the PDF against known company configurations by
        looking for identifying text patterns. Returns the matched account
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

        This is the main entry point for automatic detection. It attempts to
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

        raise StatementError(f"Unable to identify the account from the statement provided: {file_path}")


# Module-level singleton for default configuration
_default_config: ConfigManager | None = None


def _get_default_config() -> ConfigManager:
    """
    Get or create the default ConfigManager singleton.

    Returns:
        The default ConfigManager instance.
    """
    global _default_config
    if _default_config is None:
        _default_config = ConfigManager()
    return _default_config


def _get_accounts() -> dict[str, Account]:
    """Get accounts dict from default config."""
    return _get_default_config().accounts


def _get_statement_types() -> dict[str, StatementType]:
    """Get statement types dict from default config."""
    return _get_default_config().statement_types


def _get_companies() -> dict[str, Company]:
    """Get companies dict from default config."""
    return _get_default_config().companies


def _get_standard_fields() -> dict[str, StandardFields]:
    """Get standard fields dict from default config."""
    return _get_default_config().standard_fields


# Backward-compatible module-level exports
# Singletons are initialised lazily on first access via __getattr__ so that
# importing this module does NOT trigger TOML file I/O immediately.
_LAZY_SINGLETONS: dict[str, object] = {}

_LAZY_FACTORIES: dict[str, object] = {
    "config_accounts": _get_accounts,
    "config_statement_types": _get_statement_types,
    "config_companies": _get_companies,
    "config_standard_fields": _get_standard_fields,
}


def __getattr__(name: str) -> object:
    """Lazily initialise module-level config singletons on first access."""
    if name in _LAZY_FACTORIES:
        if name not in _LAZY_SINGLETONS:
            _LAZY_SINGLETONS[name] = _LAZY_FACTORIES[name]()  # type: ignore[operator]
        return _LAZY_SINGLETONS[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_config_from_account(account_key: str, logs: pl.DataFrame, file_path: str, config_path: Path | None = None) -> Account:
    """
    Retrieve account configuration by key (backward-compatible function).

    Args:
        account_key: The account identifier.
        logs: Polars DataFrame for logging operations.
        file_path: Path to the PDF file being processed.
        config_path: Optional custom config directory path.

    Returns:
        The Account object.

    Raises:
        StatementError: If the account cannot be found.
    """
    config = ConfigManager(config_path) if config_path else _get_default_config()
    return config.get_config_from_account(account_key, logs, file_path)


def get_config_from_company(company_key: str, pdf: PDF, logs: pl.DataFrame, file_path: str, config_path: Path | None = None) -> Account:
    """
    Identify account from company by testing against PDF (backward-compatible function).

    Args:
        company_key: The company identifier.
        pdf: The opened PDF object.
        logs: Polars DataFrame for logging operations.
        file_path: Path to the PDF file being processed.
        config_path: Optional custom config directory path.

    Returns:
        The matched Account object.

    Raises:
        StatementError: If no account can be matched.
    """
    config = ConfigManager(config_path) if config_path else _get_default_config()
    return config.get_config_from_company(company_key, pdf, logs, file_path)


def get_config_from_statement(pdf: PDF, file_path: str, logs: pl.DataFrame, config_path: Path | None = None) -> Account:
    """
    Identify company and account from PDF (backward-compatible function).

    Args:
        pdf: The opened PDF object.
        file_path: Path to the PDF file being processed.
        logs: Polars DataFrame for logging operations.
        config_path: Optional custom config directory path.

    Returns:
        The identified Account object.

    Raises:
        StatementError: If neither company nor account can be identified.
    """
    config = ConfigManager(config_path) if config_path else _get_default_config()
    return config.get_config_from_statement(pdf, file_path, logs)


def config_company_accounts(company_key: str, config_path: Path | None = None) -> list[Account]:
    """
    Get all accounts for a company (backward-compatible function).

    Args:
        company_key: The company identifier to filter by.
        config_path: Optional custom config directory path.

    Returns:
        List of Account objects for the specified company.
    """
    config = ConfigManager(config_path) if config_path else _get_default_config()
    return config.get_accounts_for_company(company_key)
