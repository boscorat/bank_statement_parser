# uk-bank-statement-parser

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

### Using uv (recommended)

[uv](https://docs.astral.sh/uv/) manages its own Python installations, so no
system Python 3.14 is required. If uv is not already installed, follow the
[uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

```bash
uv tool install uk-bank-statement-parser
```

This creates an isolated environment and puts `bsp` on your `$PATH`.

To upgrade later:

```bash
uv tool upgrade uk-bank-statement-parser
```

### Debian / Ubuntu (.deb)

Download the `.deb` from the
[latest GitHub Release](https://github.com/boscorat/bank_statement_parser/releases/latest),
then install:

```bash
sudo dpkg -i uk-bank-statement-parser_*_all.deb
```

This installs a self-contained virtualenv to `/opt/uk-bank-statement-parser/`
and a `bsp` wrapper to `/usr/bin/bsp`. No system Python is required. Uninstall
with `sudo dpkg -r uk-bank-statement-parser`.

### Fedora / RHEL (.rpm)

Download the `.rpm` from the
[latest GitHub Release](https://github.com/boscorat/bank_statement_parser/releases/latest),
then install:

```bash
sudo rpm -i uk-bank-statement-parser-*-1.noarch.rpm
```

No system Python is required. Uninstall with
`sudo rpm -e uk-bank-statement-parser`.

### From source

```bash
git clone https://github.com/boscorat/bank_statement_parser.git
cd bank_statement_parser
uv sync
```

> **Prefer not to use uv?** See
> [Alternative installation (pipx / venv)](#alternative-installation-pipx--venv)
> for instructions using pipx or a manually created virtual environment.

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

## Documentation

Full documentation is available at
[boscorat.github.io/bank_statement_parser](https://boscorat.github.io/bank_statement_parser/).

### Guides

- [Adding a New Bank](https://boscorat.github.io/bank_statement_parser/guides/new-bank-config/) —
  TOML configuration for parsing statements from a new bank.
- [Anonymisation](https://boscorat.github.io/bank_statement_parser/guides/anonymisation/) —
  redacting PII from statement PDFs, config setup, and output review.
- [Project Structure](https://boscorat.github.io/bank_statement_parser/guides/project-structure/) —
  directory layout, SQLite schema, and Parquet file organisation.
- [Export Options](https://boscorat.github.io/bank_statement_parser/guides/exports/) —
  simple vs. full export presets, CSV and Excel output.

### Reference

- [CLI Reference](https://boscorat.github.io/bank_statement_parser/reference/cli/) —
  all `bsp process` and `bsp anonymise` options with examples.
- [Python API Reference](https://boscorat.github.io/bank_statement_parser/reference/python-api/) —
  `StatementBatch`, report backends, export helpers, and database utilities.

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
   # Include uv.lock only if dependencies changed since the last commit
   git add pyproject.toml
   git add uv.lock  # omit if no dependency changes
   git commit -m "release: v0.2.0"
   git tag v0.2.0
   git push origin master --tags
   ```
3. The `release.yml` workflow runs automatically — builds and publishes to
   PyPI, builds `.deb` and `.rpm` packages, and creates a GitHub Release with
   all assets attached.

## Alternative installation (pipx / venv)

This package requires **Python 3.14 or later**. Python 3.14 is not yet
bundled by most system package managers, so you will need to install it
separately before using pipx or a plain virtual environment.

### Installing Python 3.14

The easiest cross-platform option is
[python-build-standalone](https://github.com/indygreg/python-build-standalone)
via [pyenv](https://github.com/pyenv/pyenv), or by downloading directly from
[python.org](https://www.python.org/downloads/).

On Ubuntu/Debian you can use the
[deadsnakes PPA](https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa):

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.14 python3.14-venv
```

On Fedora/RHEL, check whether your version ships 3.14 via `dnf`, otherwise
build from source or use pyenv.

### Using pipx

Once Python 3.14 is available on your system:

```bash
pipx install uk-bank-statement-parser --python python3.14
```

To upgrade later:

```bash
pipx upgrade uk-bank-statement-parser
```

### Using a virtual environment manually

```bash
python3.14 -m venv ~/.venvs/bsp
~/.venvs/bsp/bin/pip install uk-bank-statement-parser
```

Then either activate the environment or invoke `bsp` directly:

```bash
# Activate (adds bsp to PATH for the session)
source ~/.venvs/bsp/bin/activate
bsp --help

# Or run without activating
~/.venvs/bsp/bin/bsp --help
```

To upgrade:

```bash
~/.venvs/bsp/bin/pip install --upgrade uk-bank-statement-parser
```

## License

[MIT](LICENSE)
