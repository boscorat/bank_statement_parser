"""
Public interface for the bank_statement_parser.modules sub-package.

Flat exports (imported directly):
    Statement, StatementBatch           -- PDF parsing / batch processing
    process_pdf_statement               -- process a single PDF (module-level)
    PdfResult                           -- dataclass returned by process_pdf_statement
    Success, Review, Failure            -- result payload dataclasses
    StatementInfo, ParquetFiles         -- typed sub-fields of Success / Review
    delete_temp_files                   -- clean up temp parquet files
    StatementError                      -- root of the exception hierarchy
    pdf_open, page_crop, page_text,
    region_search, get_table_from_region -- low-level pdfplumber helpers

Namespaced report sub-module (use to avoid name collisions):
    db        -- bank_statement_parser.modules.reports_db

The sub-module exposes FlatTransaction, FactBalance, DimTime, DimStatement,
DimAccount, FactTransaction, GapReport plus export_csv / export_excel
functions.  Access them via the namespace:

    import bank_statement_parser as bsp
    bsp.db.FlatTransaction(...)
"""

import bank_statement_parser.modules.reports_db as db
from bank_statement_parser.modules.config import copy_default_config
from bank_statement_parser.modules.paths import copy_project_folders
from bank_statement_parser.modules.data import Failure, ParquetFiles, PdfResult, Review, StatementInfo, Success
from bank_statement_parser.modules.errors import StatementError
from bank_statement_parser.modules.pdf_functions import get_table_from_region, page_crop, page_text, pdf_open, region_search
from bank_statement_parser.modules.statements import (
    Statement,
    StatementBatch,
    delete_temp_files,
    process_pdf_statement,
)

__all__ = [
    # Statement processing
    "Statement",
    "StatementBatch",
    "process_pdf_statement",
    "PdfResult",
    "Success",
    "Review",
    "Failure",
    "StatementInfo",
    "ParquetFiles",
    "delete_temp_files",
    # Config helpers
    "copy_default_config",
    "copy_project_folders",
    # Errors
    "StatementError",
    # Low-level PDF helpers
    "pdf_open",
    "page_crop",
    "page_text",
    "region_search",
    "get_table_from_region",
    # Namespaced report backend
    "db",
]
