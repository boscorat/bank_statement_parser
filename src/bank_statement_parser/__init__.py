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

Namespaced report backends
--------------------------
Both ``bsp.parquet`` and ``bsp.db`` expose the same class names
(FlatTransaction, FactBalance, DimTime, DimStatement, DimAccount,
FactTransaction, GapReport) plus ``export_csv`` / ``export_excel`` helpers.

    bsp.parquet.FlatTransaction(...)
    bsp.db.FlatTransaction(...)

Database utilities
------------------
    bsp.build_datamart(db_path)   -- rebuild star-schema mart tables
    bsp.create_db(db_path)        -- create (or recreate) the raw database
    bsp.Housekeeping(db_path)     -- orphan-detection and cascaded-delete
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
from bank_statement_parser.modules.errors import StatementError

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
]
