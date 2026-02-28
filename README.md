# bank-statement-parser

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.14+](https://img.shields.io/badge/Python-3.14%2B-blue.svg)](https://www.python.org/downloads/)

Parse bank statement PDFs, extract structured transaction data, validate
financial information through checks and balances, and persist results to
Parquet files and a SQLite star-schema data mart. Export reports as Excel
workbooks or CSV files.

## Features

- **PDF extraction** — configurable pattern-based parsing of bank statement
  PDFs using pdfplumber.
- **Checks and balances** — automatic validation of opening/closing balances,
  payment totals, and running balances against statement header values.
- **Dual persistence** — write results to Parquet files, a SQLite database, or
  both.
- **Star-schema data mart** — automatically builds dimension and fact tables
  (`DimTime`, `DimAccount`, `DimStatement`, `FactTransaction`, `FactBalance`)
  plus a `GapReport` for detecting missing statements.
- **Dual report backends** — read the same report classes from either Parquet
  or SQLite, with identical schemas.
- **Export** — single flat transactions table (default) or separate star-schema
  tables, as Excel and/or CSV.
- **PDF anonymisation** — redact personally identifiable information from
  statement PDFs using a user-supplied mapping file. Transaction descriptions
  are scrambled so merchant names cannot be recovered.
- **Parallel processing** — async + multiprocess batch mode for large PDF sets.
- **Cross-platform** — pure Python with no OS-specific dependencies.

## Installation

Requires **Python 3.14** or later.

### From PyPI

The recommended way to install for most users. Both `pipx` and `uv tool`
create an isolated virtualenv and put `bsp` on your `$PATH`.

```bash
# Using pipx
pipx install bank-statement-parser

# Using uv (faster)
uv tool install bank-statement-parser
```

To upgrade later:

```bash
pipx upgrade bank-statement-parser   # or
uv tool upgrade bank-statement-parser
```

### Debian / Ubuntu (.deb)

Download the `.deb` from the
[latest GitHub Release](https://github.com/boscorat/bank_statement_parser/releases/latest),
then install:

```bash
sudo dpkg -i bank-statement-parser_0.1.0_all.deb
```

This installs a self-contained virtualenv to `/opt/bank-statement-parser/`
and a `bsp` wrapper to `/usr/bin/bsp`. Uninstall with
`sudo dpkg -r bank-statement-parser`.

### Fedora / RHEL (.rpm)

Download the `.rpm` from the
[latest GitHub Release](https://github.com/boscorat/bank_statement_parser/releases/latest),
then install:

```bash
sudo rpm -i bank-statement-parser-0.1.0-1.noarch.rpm
```

Uninstall with `sudo rpm -e bank-statement-parser`.

### From source

```bash
git clone https://github.com/boscorat/bank_statement_parser.git
cd bank_statement_parser
uv sync
```

## Quick Start

### Command line

Process all PDFs in a folder and export an Excel workbook and CSV file:

```bash
bsp process --pdfs ~/statements/
```

This creates a `bsp_project/` directory in your current working directory
containing the SQLite database, Parquet files, and exported reports.

### Python API

```python
import bank_statement_parser as bsp
from pathlib import Path

# Process a batch of PDFs
batch = bsp.StatementBatch(pdfs=sorted(Path("~/statements").expanduser().glob("*.pdf")))

# Persist to Parquet + SQLite
batch.update_data()

# Export a flat transactions table as Excel and CSV
batch.export(filetype="both")

# Copy source PDFs into the project tree
batch.copy_statements_to_project()

# Clean up temporary files
batch.delete_temp_files()
```

Read reports directly:

```python
import bank_statement_parser as bsp

# From the SQLite backend
flat = bsp.db.FlatTransaction().all.collect()

# From the Parquet backend
flat = bsp.parquet.FlatTransaction().all.collect()
```

Both backends return Polars LazyFrames with identical schemas.

## CLI Reference

### `bsp process`

Parse bank statement PDFs, persist data, and export reports.

```
bsp process [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--project PATH` | `./bsp_project/` | Project folder path. Created if absent. |
| `--pdfs PATH` | Current directory | Folder to scan for PDF files. |
| `--pattern GLOB` | `**/*.pdf` | Glob pattern for PDF discovery. |
| `--no-turbo` | Off | Disable parallel processing. |
| `--company KEY` | Auto-detect | Company key for config lookup. |
| `--account KEY` | Auto-detect | Account key for config lookup. |
| `--data {parquet,database,both}` | `both` | Where to persist extracted data. |
| `--export-data {parquet,database}` | `database` | Which backend to read when exporting. |
| `--export-format {excel,csv,both}` | `both` | Output file format. |
| `--export-type {full,simple}` | `simple` | Export preset (see [Export Options](#export-options)). |
| `--no-export` | Off | Skip the export step entirely. |
| `--no-copy` | Off | Skip copying source PDFs into the project. |

**Examples:**

```bash
# Process PDFs in ~/statements, write to a specific project folder
bsp process --pdfs ~/statements --project ~/my_project

# Process only top-level PDFs (no subdirectories), export CSV only
bsp process --pdfs ~/statements --pattern "*.pdf" --export-format csv

# Process without exporting (data only)
bsp process --pdfs ~/statements --no-export

# Full star-schema export for loading into an external database
bsp process --pdfs ~/statements --export-type full
```

### `bsp anonymise`

Replace personally identifiable information in bank statement PDFs with dummy
values. The anonymiser physically rewrites the PDF content stream — sensitive
text is removed from the file, not merely covered with a rectangle. Transaction
descriptions are also scrambled (each letter replaced with a random different
letter) so that merchant names and references cannot be recovered.

#### Setting up your anonymise config

Anonymisation is driven by a TOML config file (`anonymise.toml`) that maps
your real personal details to dummy replacements. **This file is never included
in the default project** because it contains PII and is excluded from source
control via `.gitignore`.

When you create a project (via `bsp process` or `validate_or_initialise_project()`),
an example template is copied into the project config directory:

```
bsp_project/config/anonymise_example.toml
```

To set up anonymisation:

1. **Copy** `anonymise_example.toml` to `anonymise.toml` in the same directory
   (or any location you prefer).
2. **Edit** `anonymise.toml` — replace the left-hand (search) values with the
   real text as it appears in your PDFs, and the right-hand (replacement) values
   with the dummy text you want rendered instead.
3. **Pass the path** to your `anonymise.toml` via the `--config` flag, since the
   default project directory will never contain one.

The config has two sections:

- **`[global_replacements]`** — applied on every page across the full page area.
  Use for names, account numbers, sort codes, IBANs, and card numbers.
- **`[address_replacements]`** — applied on **page 1 only**, within the personal
  address block at the top-left corner. Use for address lines, city names, and
  postcodes that might also appear as merchant/location names in transaction
  descriptions (where you would *not* want them replaced).

**Ordering matters:** within each section, entries are applied top-to-bottom.
Always place longer, more specific strings *before* shorter fragments. For
example, list `"John William Surname"` before `"Surname"` — otherwise the
fragment match fires first and corrupts the full-name replacement.

#### Checking your output

Anonymised PDFs should always be **reviewed carefully before sharing**. The
anonymiser cannot guarantee perfect results in every case — font encoding
differences, unusual character spacing, or layout variations may cause some
replacements to render incorrectly or miss certain occurrences. Open each
output file and verify that:

- All personal details (names, addresses, account numbers) have been replaced.
- Replacement text renders correctly and is the expected length.
- No sensitive information remains in headers, footers, or transaction
  descriptions.

You may need to make manual edits to the PDF or adjust your `anonymise.toml`
mappings and re-run.

#### Command reference

```
bsp anonymise PATH [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `PATH` | *(required)* | PDF file or folder (with `--folder`). |
| `--folder` | Off | Treat PATH as a directory. |
| `--pattern GLOB` | `*.pdf` | Glob for PDF discovery in folder mode. |
| `--output OUT_FILE` | `<stem>_anonymised.pdf` | Output path (single-file mode). |
| `--output-dir OUT_DIR` | Alongside source | Output directory (folder mode). |
| `--config CONFIG_TOML` | Project config | Path to a custom anonymise.toml. |

**Examples:**

```bash
# Anonymise a single PDF using a config in your home directory
bsp anonymise statement.pdf --config ~/anonymise.toml

# Anonymise all PDFs in a folder
bsp anonymise ~/statements --folder --config ~/anonymise.toml

# Anonymise to a specific output directory
bsp anonymise ~/statements --folder --output-dir ~/anonymised --config ~/anonymise.toml
```

## Python API Reference

### Statement Processing

#### `StatementBatch`

The main entry point for processing PDFs. Extraction starts on construction.

```python
batch = bsp.StatementBatch(
    pdfs=[Path("a.pdf"), Path("b.pdf")],  # list of PDF paths
    company_key="hsbc_uk",                 # optional — auto-detected if omitted
    account_key="current_account",         # optional — auto-detected if omitted
    turbo=True,                            # parallel processing (default: True)
    project_path=Path("my_project"),       # optional — uses default project if omitted
)
```

| Method | Description |
|---|---|
| `update_data(datadestination="both")` | Persist results. `"parquet"`, `"database"`, or `"both"`. |
| `export(...)` | Export reports. See [Export Options](#export-options). |
| `copy_statements_to_project()` | Copy source PDFs into `project/statements/{year}/{account}/`. |
| `delete_temp_files()` | Remove temporary per-PDF Parquet files. |
| `debug(project_path=None)` | Re-process failing PDFs and write diagnostic JSON. |

| Property | Description |
|---|---|
| `pdf_count` | Number of PDFs in the batch. |
| `errors` | Number of PDFs that failed processing. |
| `duration_secs` | Wall-clock processing time in seconds. |
| `processed_pdfs` | List of processed `Statement` objects. |
| `ID_BATCH` | Unique batch identifier (UUID). |

### Report Backends

Both `bsp.db` (SQLite) and `bsp.parquet` (Parquet files) expose identical
report classes. Each class has an `.all` attribute that returns a Polars
`LazyFrame`.

```python
# All classes accept an optional project_path keyword argument.
# When omitted, the default project directory is used.
flat  = bsp.db.FlatTransaction(project_path=Path("my_project"))
df    = flat.all.collect()
```

| Class | Description |
|---|---|
| `FlatTransaction` | Denormalised transactions with account and statement details. |
| `FactTransaction` | Transaction fact table (one row per transaction line). |
| `FactBalance` | Daily balance series per account (fills gaps between statements). |
| `DimTime` | Date dimension with calendar attributes (year, quarter, month, weekday, etc.). |
| `DimAccount` | Account dimension (company, account type, number, sort code, holder). |
| `DimStatement` | Statement dimension (statement date, filename, batch timestamp). |
| `GapReport` | Statement continuity check — flags gaps where closing/opening balances disagree. |

### Export Helpers

Module-level functions on both backends:

```python
# Export from the SQLite backend
bsp.db.export_csv(folder=None, type="simple", project_path=None)
bsp.db.export_excel(path=None, type="simple", project_path=None)

# Export from the Parquet backend
bsp.parquet.export_csv(folder=None, type="simple", project_path=None)
bsp.parquet.export_excel(path=None, type="simple", project_path=None)
```

When `folder` / `path` is omitted, files are written to the project's
`export/csv/` or `export/excel/` sub-directory automatically.

### Database Utilities

| Function / Class | Description |
|---|---|
| `build_datamart(db_path)` | Drop and rebuild all star-schema mart tables. |
| `create_db(db_path)` | Create (or recreate) the raw SQLite schema. |
| `Housekeeping(db_path)` | Orphan detection and cascaded delete utilities. |

### Project Scaffolding

| Function | Description |
|---|---|
| `validate_or_initialise_project(path)` | Validate an existing project or scaffold a new one. |
| `copy_project_folders(dest)` | Copy the project directory structure (directories only). |
| `copy_default_config(dest)` | Copy shipped TOML config files to a directory. |

### PDF Anonymisation

```python
bsp.anonymise_pdf(input_path, output_path=None, config_path=None, scramble_descriptions=True)
bsp.anonymise_folder(folder_path, pattern="*.pdf", output_dir=None, config_path=None, scramble_descriptions=True)
```

Both functions require a path to your `anonymise.toml` via `config_path`.
There is no default `anonymise.toml` in the project — you must create one from
the `anonymise_example.toml` template (see
[Setting up your anonymise config](#setting-up-your-anonymise-config) above).
If `config_path` is omitted, the function looks in the default project config
directory and raises `FileNotFoundError` with instructions if the file is
missing.

Set `scramble_descriptions=False` to disable the random letter substitution
of transaction descriptions (enabled by default).

Always review the output files before sharing — see
[Checking your output](#checking-your-output) above.

## Project Structure

Running `bsp process` creates the following project layout:

```
bsp_project/
├── config/              # TOML configuration files
│   └── anonymise_example.toml  # Template — copy to anonymise.toml and edit
├── database/
│   └── project.db       # SQLite database (raw tables + star-schema mart)
├── export/
│   ├── csv/             # Exported CSV files
│   └── excel/           # Exported Excel workbooks
├── log/                 # Processing logs and debug output
├── parquet/             # Parquet data files
│   ├── batch_lines.parquet
│   ├── statement_heads.parquet
│   └── statement_lines.parquet
└── statements/          # Archived source PDFs (organised by year/account)
```

The SQLite database contains both raw extraction tables (`statement_heads`,
`statement_lines`) and a full star-schema data mart that is rebuilt
automatically on each `update_data()` call.

## Export Options

The `type` parameter controls what gets exported:

### `simple` (default)

Exports a single **flat transactions table** — one row per transaction with
account and statement details denormalised into each row. This is the most
useful format for analysis in Excel, Google Sheets, or Pandas/Polars.

- **CSV:** `transactions_table.csv`
- **Excel:** single `transactions_table` sheet in `transactions.xlsx`

### `full`

Exports **separate star-schema tables** intended for loading into an external
database or BI tool. Since the SQLite database is already available in the
project folder, this is mainly useful when you need the data in a different
database system.

- **CSV:** `statement.csv`, `account.csv`, `calendar.csv`, `transactions.csv`,
  `balances.csv`, `gaps.csv`
- **Excel:** one sheet per table in `transactions.xlsx`

## Contributing

Developer guidelines, architecture notes, code style rules, and test commands
are documented in
[AGENTS.md](https://github.com/boscorat/bank_statement_parser/blob/main/AGENTS.md).

```bash
# Run the test suite
pytest -v

# Lint and format
ruff check .
ruff format .
```

### Releasing a new version

1. Bump the version in `pyproject.toml` (the single source of truth).
2. Commit and tag:
   ```bash
   git add pyproject.toml uv.lock
   git commit -m "release: v0.2.0"
   git tag -a v0.2.0 -m "v0.2.0"
   git push origin main --tags
   ```
3. The `release.yml` workflow runs automatically — builds and publishes to
   PyPI, builds `.deb` and `.rpm` packages, and creates a GitHub Release with
   all assets attached.

## License

[MIT](LICENSE)
