"""
bank_statement_parser — public API
===================================

Parse HSBC UK bank statement PDFs, validate financial data, persist to Parquet
files and a SQLite star-schema data mart, and export reports.

Quick start
-----------

    import bank_statement_parser as bsp
    from pathlib import Path

    # Process a batch of PDFs
    batch = bsp.StatementBatch(pdfs=[Path("statement.pdf")])
    batch.update_parquet()
    batch.update_db()
    batch.delete_temp_files()

    # Read reports from the DB backend
    flat = bsp.db.FlatTransaction().all.collect()

    # Read reports from the Parquet backend
    flat = bsp.parquet.FlatTransaction().all.collect()

    # Rebuild the star-schema mart after loading
    bsp.build_datamart(db_path=Path("project.db"))

    # Copy project folder structure to a new location (no files, dirs only)
    bsp.copy_project_folders(Path("~/my_project").expanduser())

Namespaced report backends
--------------------------
Both ``bsp.parquet`` and ``bsp.db`` expose the same class names
(FlatTransaction, FactBalance, DimTime, DimStatement, DimAccount,
FactTransaction, GapReport) plus ``export_csv`` / ``export_excel`` helpers.

    bsp.parquet.FlatTransaction(...)
    bsp.db.FlatTransaction(...)

Database utilities
------------------
    bsp.build_datamart(db_path)                 -- rebuild star-schema mart tables
    bsp.create_db(db_path)                      -- create (or recreate) the raw database
    bsp.Housekeeping(db_path)                   -- orphan-detection and cascaded-delete
    bsp.copy_default_config(dest)               -- copy shipped TOML configs to a directory
    bsp.copy_project_folders(dest)              -- copy project folder structure (dirs only)
    bsp.validate_or_initialise_project(path)    -- validate or scaffold a project directory

Errors
------
    bsp.StatementError          -- base exception
    bsp.ProjectDatabaseMissing  -- project.db absent in an otherwise-valid project
    bsp.ProjectConfigMissing    -- config/ absent or empty in an otherwise-valid project
"""

__app_name__ = "bank-statement-parser"
__version__ = "0.1.0"

# ---------------------------------------------------------------------------
# Namespaced report backends — import the sub-modules so callers can do
#   bsp.parquet.FlatTransaction(...)  /  bsp.db.FlatTransaction(...)
# ---------------------------------------------------------------------------
import bank_statement_parser.modules.reports_db as db
import bank_statement_parser.modules.reports_parquet as parquet

# ---------------------------------------------------------------------------
# Statement processing
# ---------------------------------------------------------------------------
from bank_statement_parser.modules.statements import (
    Statement,
    StatementBatch,
    delete_temp_files,
    process_pdf_statement,
    update_parquet,
)

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
from bank_statement_parser.modules.errors import (
    ProjectConfigMissing,
    ProjectDatabaseMissing,
    StatementError,
)

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
from bank_statement_parser.modules.config import copy_default_config
from bank_statement_parser.modules.paths import copy_project_folders, validate_or_initialise_project

# ---------------------------------------------------------------------------
# PDF anonymisation utility
# ---------------------------------------------------------------------------
from bank_statement_parser.modules.anonymise import anonymise_folder, anonymise_pdf

# ---------------------------------------------------------------------------
# Low-level PDF helpers
# ---------------------------------------------------------------------------
from bank_statement_parser.modules.pdf_functions import (
    get_table_from_region,
    page_crop,
    page_text,
    pdf_open,
    region_search,
)

# ---------------------------------------------------------------------------
# Database / data-mart utilities
# ---------------------------------------------------------------------------
from bank_statement_parser.data import Housekeeping, build_datamart, create_db

__all__ = [
    # Meta
    "__app_name__",
    "__version__",
    # Namespaced report backends
    "parquet",
    "db",
    # Statement processing
    "Statement",
    "StatementBatch",
    "process_pdf_statement",
    "delete_temp_files",
    "update_parquet",
    # Errors
    "StatementError",
    "ProjectDatabaseMissing",
    "ProjectConfigMissing",
    # Config helpers
    "copy_default_config",
    "copy_project_folders",
    "validate_or_initialise_project",
    # Low-level PDF helpers
    "pdf_open",
    "page_crop",
    "page_text",
    "region_search",
    "get_table_from_region",
    # Data-mart / database
    "build_datamart",
    "create_db",
    "Housekeeping",
    # PDF anonymisation
    "anonymise_pdf",
    "anonymise_folder",
]
