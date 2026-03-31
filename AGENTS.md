# AGENTS.md — Bank Statement Parser

## Project Overview

Python library for parsing bank statement PDFs, extracting structured transaction data using configurable patterns, validating financial information through checks and balances, persisting results to Parquet files and a SQLite star-schema data mart.

## Technology Stack

- **Python**: 3.14+ (`target-version = "py314"`) · **Package Manager**: uv · **Build Backend**: uv_build
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

- `tests/test_datamart.py` — session-scoped `_db_lifecycle` fixture (`autouse=True`) creates `tests/test_project.db`, populates it via `generate_mock_data()`, builds the mart, runs tests, then deletes the DB. A separate `scope="module"` fixture `conn` provides a shared `sqlite3.Connection`. Self-contained.
- `tests/conftest.py` — session-scoped `good_project` and `bad_project` fixtures (`autouse=False`) process real PDFs from the bundled `test_data/pdfs/good/` and `test_data/pdfs/bad/` directories into temporary project directories. Each fixture yields a `ProjectContext` dataclass (`project_path`, `batch`, `pdfs`). Torn down via `shutil.rmtree` in a `try/finally` after the session.

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

`StatementBatch` uses `asyncio` + `ProcessPoolExecutor` for parallel mode (`turbo=True`). The worker is a module-level function (`process_pdf_statement`) — not a bound method — so it can be pickled for child processes. `ProcessPoolExecutor` is created with the `forkserver` multiprocessing context. Invoked via `asyncio.run(self.process_turbo(), debug=False)`.

## Code Style

### Imports — absolute only, grouped: stdlib → third-party → local, separated by blank lines.

```python
import sqlite3
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.config import ConfigManager
```

Deferred imports (inside function bodies, to break circular dependencies) are annotated with `# noqa: PLC0415`. This is the only accepted reason for a non-top-level import.

`from __future__ import annotations` is used in exactly five files (`paths.py`, `anonymise.py`, `parquet.py`, `cli.py`, `_anonymise_shared.py`) where forward references are needed. It is **not** a project-wide convention. Elsewhere, forward references use quoted strings (e.g. `"ProjectPaths"`, `"Success | Review | Failure"`).

### Type Hints — required on all parameters and return values. Use `str | None` (not `Optional[str]`). Use `pl.LazyFrame` / `pl.DataFrame` for Polars types.

**Exception**: `data.py` configuration dataclasses (loaded by `dacite` from TOML) use `Optional[T]` throughout because `dacite` requires it for optional TOML keys. Do not change these to `T | None`; the `Optional` import in `data.py` is intentional.

`Literal[...]` is used for fields and parameters whose values are drawn from a fixed set of strings (e.g. `Literal["SUCCESS", "REVIEW", "FAILURE"]` on `PdfResult.result`; `Literal["config", "data", "other"]` on `Failure.error_type`). Use it whenever a string parameter or field has a closed set of valid values.

There are no `TypeAlias`, `Protocol`, `ABC`, or `@abstractmethod` uses in this codebase. Do not introduce them.

### `type: ignore` — always supply the specific mypy error code: `# type: ignore[union-attr]`, `# type: ignore[arg-type]`, etc. A bare `# type: ignore` (without a code) is not acceptable.

### `# noqa` — always supply the specific ruff rule code. Active codes in use: `PLC0415` (deferred import), `S608` (SQL string), `ARG001` (unused argument), `F401` (re-export). A bare `# noqa` is not acceptable.

### Naming

| Kind | Style | Example |
|---|---|---|
| Classes | PascalCase | `StatementBatch` |
| Functions / methods | snake_case | `process_statement` |
| Module-level constants / DDL | SCREAMING_SNAKE_CASE | `_DDL_FACT_TRANSACTION` |
| Private | leading underscore | `_internal_method` |
| Files / modules | snake_case | `statement_functions.py` |

Private constants use a leading underscore: `_ALLOWED_TABLES`. Public shared constants do not: `FLOAT_TOL`.

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

**Class-level constants** (lookup tables, whitelists) that are shared across all instances are declared as plain class-body attributes — they are **not** listed in `__slots__` and are not instance variables. Example from `housekeeping.py`:

```python
class Housekeeping:
    FK_RELATIONSHIPS = [...]          # class variable, not in __slots__
    _ALLOWED_TABLES: frozenset[str] = frozenset([...])
```

Use `frozenset[str]` for any identifier or name whitelist used in membership tests (SQL injection guards, column name validation, etc.).

### Decorators — `@property`, `@classmethod`, `@staticmethod`

- `@property`: used for computed attributes that must be validated before access (e.g. `ProjectPaths` exposes 26 `@property` accessors; `TestHarness` guards `db_path` with a readiness check).
- `@classmethod`: used for alternative constructors (e.g. `ProjectPaths.resolve`).
- `@staticmethod`: used for pure helper functions attached to a class by namespace (e.g. `Housekeeping._validate_identifier`).

### Context Managers

- Use `@contextmanager` (from `contextlib`) with `try/finally` for function-based context managers.
- Classes that own a resource lifecycle implement `__enter__` / `__exit__`. `__enter__` returns `self` (or the result of a setup call); `__exit__` accepts `*args: object` and always cleans up regardless of exceptions.

```python
def __enter__(self) -> "TestHarness":
    return self.setup()

def __exit__(self, *args: object) -> None:
    self.teardown()
```

- The codebase has **no generator functions** (bare `yield` outside a `@contextmanager`).

### Docstrings — Google-style (Args / Returns / Raises) on all public functions and methods. Every module starts with a module-level docstring. Dataclass fields use inline comments prefixed with `# [ACTIVE]` or `# [STUB]` where applicable.

### `__all__` — every `__init__.py` defines `__all__` as a list of strings, grouped with comment headers. Sub-packages are re-exported with a namespace alias where appropriate (e.g. `import bank_statement_parser.modules.reports_db as db`).

### Polars — prefer `LazyFrame`; `.collect()` only at the boundary. Use `.pipe()` for pipelines. `vstack(in_place=True)` for row accumulation; `hstack(in_place=True)` for column mutation. No Python-level row iteration.

### Strings — f-strings exclusively. No `.format()` or `%`.

### Paths — `Path(__file__).parent` chains for internal paths. Always `mkdir(parents=True, exist_ok=True)`. Pass `str(path)` to libraries that reject `Path`.

### Logging — no `logging` module. User output via `print()`. `pl.Config` display blocks only in `if __name__ == "__main__"` guards.

### Warnings — use `warnings.warn` for non-fatal issues:
- `DeprecationWarning` for deprecated API aliases (e.g. deprecated `filetype='both'` parameter).
- `UserWarning` for non-fatal operational problems (e.g. FX rate lookup failures in `forex.py`).

### Error Handling — raise from the custom hierarchy in `errors.py`:

- `StatementError(BaseException)` — root
  - `ConfigError` → `ConfigFileError`, `NotAValidConfigFolder`
  - `ProjectError` → `ProjectFolderNotFound`, `ProjectSubFolderNotFound`, `ProjectDatabaseMissing`, `ProjectConfigMissing`

Let exceptions propagate unless the caller can meaningfully recover. `assert` is for tests only.

### SQL — DDL stored as `_DDL_*` module-level constants. Queries use triple-quoted f-strings. `INSERT OR REPLACE … executemany`. Dynamic identifiers guarded by `frozenset` whitelist + `_validate_identifier()`.

`sqlite3` date/datetime adapters are registered at module level in `database.py` to emit ISO-format strings and avoid Python 3.12+ deprecation warnings:

```python
sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
```

### Configuration — TOML files in `project/config/` (user overrides) or shipped `base_config/` defaults. Loaded via `ConfigManager` + `dacite`.

## Testing Conventions

- All fixtures are `scope="session"` unless noted. `autouse=True` only for lifecycle fixtures that every test in the file depends on (e.g. `_db_lifecycle`).
- Fixtures yield, with teardown in a `try/finally` block.
- The `conftest.py` fixtures yield a `ProjectContext` dataclass — never a raw path.
- All test methods live inside named classes. No standalone test functions at module level.
- Each test method has a single-sentence docstring.
- `@pytest.mark.parametrize` is used only in `test_cli.py` and `test_docs.py`. Integration and datamart tests do not use it.
- `FLOAT_TOL = 0.005` is the module-level constant for monetary float comparisons: `abs(a - b) < FLOAT_TOL`.
- Use `_scalar(conn, sql, params=())` (defined in `test_datamart.py`) to reduce SQL boilerplate in datamart tests.
- Use `dataclasses.replace()` to produce mutated copies of frozen dataclasses in tests — do not bypass `frozen=True` with `object.__setattr__`.
- Use `warnings.catch_warnings(record=True)` to assert that deprecation or user warnings are emitted.

## File Organisation

```
src/bank_statement_parser/
├── __init__.py                 # Public API (__all__, version)
├── __main__.py                 # Entry point
├── cli.py                      # CLI subcommands (bsp process, bsp anonymise)
├── dev.py                      # Developer scratch — do NOT use as reference
├── testing.py                  # TestHarness — for dependent projects only
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
├── test_statements.py          # Integration tests (real PDFs, reports, exports)
├── test_cli.py                 # CLI subcommand tests (uses parametrize)
└── test_docs.py                # Documentation/API surface tests (uses parametrize)
```

## Development Notes

- `dev.py` has hardcoded local paths — never reference it in production code.
- `cli.py` is the correct place to add new CLI subcommands.
- **`build_datamart.py` and `build_datamart.sql` must stay in sync.** The `.py` file is authoritative at runtime; the `.sql` file is for direct `sqlite3` CLI use.
- `build_datamart()` runs a full star-schema rebuild on every `update_db()` call.
- `mock_project_data.py` is for test seeding only — never import it in production paths.
- `testing.py` exposes `TestHarness` for use by **dependent external projects** (e.g. integration test suites that need a live bsp database). It is not used by bsp's own test suite directly.
- There is no `py.typed` marker file. PEP 561 typed-package compliance is not established; do not add `py.typed` without also auditing all `type: ignore` suppressions and ensuring mypy passes cleanly.
