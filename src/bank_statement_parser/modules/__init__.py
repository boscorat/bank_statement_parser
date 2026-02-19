"""
Public interface for the bank_statement_parser.modules sub-package.

Flat exports (imported directly):
    Statement, StatementBatch           -- PDF parsing / batch processing
    process_pdf_statement               -- process a single PDF (module-level)
    delete_temp_files                   -- clean up temp parquet files
    update_parquet                      -- merge temp parquets into permanent files
    StatementError                      -- root of the exception hierarchy
    pdf_open, page_crop, page_text,
    region_search, get_table_from_region -- low-level pdfplumber helpers

Namespaced report sub-modules (use to avoid name collisions):
    parquet   -- bank_statement_parser.modules.reports_parquet
    db        -- bank_statement_parser.modules.reports_db

Both sub-modules expose the same class names (FlatTransaction, FactBalance,
DimTime, DimStatement, DimAccount, FactTransaction, GapReport) plus
export_csv / export_excel functions.  Access them via the namespace:

    import bank_statement_parser as bsp
    bsp.parquet.FlatTransaction(...)
    bsp.db.FlatTransaction(...)
"""

import bank_statement_parser.modules.reports_db as db
import bank_statement_parser.modules.reports_parquet as parquet
from bank_statement_parser.modules.config import copy_default_config
from bank_statement_parser.modules.errors import StatementError
from bank_statement_parser.modules.pdf_functions import get_table_from_region, page_crop, page_text, pdf_open, region_search
from bank_statement_parser.modules.statements import Statement, StatementBatch, delete_temp_files, process_pdf_statement, update_parquet

__all__ = [
    # Statement processing
    "Statement",
    "StatementBatch",
    "process_pdf_statement",
    "delete_temp_files",
    "update_parquet",
    # Config helpers
    "copy_default_config",
    # Errors
    "StatementError",
    # Low-level PDF helpers
    "pdf_open",
    "page_crop",
    "page_text",
    "region_search",
    "get_table_from_region",
    # Namespaced report backends
    "parquet",
    "db",
]
