# AGENTS.md — Bank Statement Parser

## Project Overview

Python library for parsing bank statement PDFs, extracting structured transaction data using configurable patterns, validating financial information through checks and balances, persisting results to Parquet files and a SQLite star-schema data mart.

## Technology Stack

- **Python**: 3.14+ · **Package Manager**: uv · **Build Backend**: uv_build
- **Data**: Polars ≥ 1.36.1 (LazyFrames preferred) · xlsxwriter
- **PDF**: pdfplumber ≥ 0.11.9 · pikepdf ≥ 10.3.0
- **Config**: dacite (TOML → dataclasses)
- **Lint / Format**: ruff (line-length 140, `unfixable = ["F401"]`)
- **Testing**: pytest ≥ 9.0.2

## Commands

```bash
uv sync                              # install dependencies

bsp                                  # CLI entry point (bank_statement_parser.cli:main)
bsp process --pdfs ~/stmts           # parse PDFs, persist, export
bsp anonymise statement.pdf          # redact PII from a PDF

pytest                               # run all tests
pytest -v                            # verbose
pytest -k "test_name"                # substring match
pytest tests/test_datamart.py::TestDimTime                  # one class
pytest tests/test_datamart.py::TestDimTime::test_row_count  # one test

ruff check .                         # lint
ruff check --fix .                   # auto-fix (F401 unused-imports are NOT auto-fixed)
ruff format .                        # format
```

## Test Fixtures

- `tests/test_datamart.py` — session-scoped `_db_lifecycle` fixture creates `tests/test_project.db`, populates it via `generate_mock_data()`, builds the mart, runs tests, then deletes the DB. Self-contained.
- `tests/conftest.py` — session-scoped `good_project` and `bad_project` fixtures process real PDFs from `tests/pdfs/good/` and `tests/pdfs/bad/` into temporary project directories. Torn down via `shutil.rmtree` after the session.

## Architecture

```
PDF files → StatementBatch (statements.py) → Statement (statements.py)
  → get_results()          (statement_functions.py)  [spawn_locations → extract_fields]
  → get_standard_fields()                            [maps raw → STD_* cols]
  → Parquet classes        (parquet.py)              [temp per-PDF → merged files]
  → update_db()            (database.py)             [INSERT OR REPLACE into SQLite]
  → build_datamart()       (build_datamart.py)       [drops & rebuilds star-schema]
  → DimTime / DimAccount / DimStatement / FactTransaction / FactBalance
```

`StatementBatch` uses `asyncio` + `ProcessPoolExecutor` for parallel mode (`turbo=True`). The worker is a module-level function to avoid pickling issues.

## Code Style

### Imports — absolute only, grouped: stdlib → third-party → local, separated by blank lines.

```python
import sqlite3
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.config import ConfigManager
```

### Type Hints — required on all parameters and return values. Use `str | None` (not `Optional[str]`). Use `pl.LazyFrame` / `pl.DataFrame` for Polars types.

### Naming

| Kind | Style | Example |
|---|---|---|
| Classes | PascalCase | `StatementBatch` |
| Functions / methods | snake_case | `process_statement` |
| Constants / DDL | SCREAMING_SNAKE_CASE | `_DDL_FACT_TRANSACTION` |
| Private | leading underscore | `_internal_method` |
| Files / modules | snake_case | `statement_functions.py` |

### Classes — use `__slots__` on every class. Regular classes declare a tuple; dataclasses use `slots=True`. Subclasses declare only their own additional slots.

```python
@dataclass(frozen=True, slots=True)   # immutable value objects
class AccountType:
    account_type: str

@dataclass(frozen=False, slots=True)  # mutable state holders
class Account:
    account: str
    company_key: str
```

### Docstrings — Google-style (Args / Returns / Raises) on all public functions and methods. Every module starts with a module-level docstring. Dataclass fields use inline comments.

### `__all__` — every `__init__.py` defines `__all__` as a list of strings, grouped with comment headers.

### Polars — prefer `LazyFrame`; `.collect()` only at the boundary. Use `.pipe()` for pipelines. `vstack(in_place=True)` for row accumulation; `hstack(in_place=True)` for column mutation. No Python-level row iteration.

### Strings — f-strings exclusively. No `.format()` or `%`.

### Paths — `Path(__file__).parent` chains for internal paths. Always `mkdir(parents=True, exist_ok=True)`. Pass `str(path)` to libraries that reject `Path`.

### Logging — no `logging` module. User output via `print()`. `pl.Config` display blocks only in `if __name__ == "__main__"` guards.

### Error Handling — raise from the custom hierarchy in `errors.py`:

- `StatementError(BaseException)` — root
  - `ConfigError` → `ConfigFileError`, `NotAValidConfigFolder`
  - `ProjectError` → `ProjectFolderNotFound`, `ProjectSubFolderNotFound`, `ProjectDatabaseMissing`, `ProjectConfigMissing`

Let exceptions propagate unless the caller can meaningfully recover. `assert` is for tests only.

### SQL — DDL stored as `_DDL_*` module-level constants. Queries use triple-quoted f-strings. `INSERT OR REPLACE … executemany`. Dynamic identifiers guarded by `frozenset` whitelist + `_validate_identifier()`.

### Configuration — TOML files in `project/config/` (user overrides) or shipped `base_config/` defaults. Loaded via `ConfigManager` + `dacite`.

## File Organisation

```
src/bank_statement_parser/
├── __init__.py                 # Public API (__all__, version)
├── __main__.py                 # Entry point
├── cli.py                      # CLI subcommands (bsp process, bsp anonymise)
├── dev.py                      # Developer scratch — do NOT use as reference
├── data/
│   ├── build_datamart.py       # build_datamart() — star-schema rebuild
│   ├── build_datamart.sql      # SQL equivalent (MUST stay in sync with .py)
│   ├── create_project_db.py    # create_db() — raw SQLite schema
│   ├── create_project_db_views.py  # GapReport + FlatTransaction views
│   ├── housekeeping.py         # Orphan detection + cascaded delete
│   └── mock_project_data.py    # generate_mock_data() — tests only
├── modules/
│   ├── anonymise.py            # anonymise_pdf / anonymise_folder
│   ├── config.py               # ConfigManager + copy_default_config
│   ├── currency.py             # CurrencySpec definitions
│   ├── data.py                 # All dataclasses
│   ├── database.py             # update_db() → SQLite persistence
│   ├── debug.py                # debug_pdf_statement / debug_statements
│   ├── errors.py               # Exception hierarchy
│   ├── parquet.py              # Parquet read/write classes
│   ├── paths.py                # ProjectPaths + project scaffold helpers
│   ├── pdf_functions.py        # pdfplumber wrappers
│   ├── reports_db.py           # Report classes backed by SQLite
│   ├── reports_parquet.py      # Report classes backed by Parquet files
│   ├── statement_functions.py  # Field extraction pipeline
│   └── statements.py           # Statement + StatementBatch
└── project/config/             # User TOML config overrides
tests/
├── conftest.py                 # Session-scoped good_project / bad_project fixtures
├── test_datamart.py            # Star-schema mart tests (mock data, self-contained DB)
└── test_statements.py          # Integration tests (real PDFs, reports, exports)
```

## Development Notes

- `dev.py` has hardcoded local paths — never reference it in production code.
- `cli.py` is the correct place to add new CLI subcommands.
- **`build_datamart.py` and `build_datamart.sql` must stay in sync.** The `.py` file is authoritative at runtime; the `.sql` file is for direct `sqlite3` CLI use.
- `build_datamart()` runs a full star-schema rebuild on every `update_db()` call.
- `mock_project_data.py` is for test seeding only — never import it in production paths.
