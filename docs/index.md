# Bank Statement Parser

Welcome to the documentation for **bank_statement_parser** — a Python library for parsing
bank statement PDFs, extracting structured transaction data, and persisting results to
Parquet files and a SQLite star-schema data mart.

## Guides

- **[Adding a New Bank](guides/new-bank-config.md)** — step-by-step guide to creating
  configuration files for parsing statements from a new bank, including the TOML file
  structure, field extraction rules, and standard field mappings.
- **[Anonymisation](guides/anonymisation.md)** — redacting personally identifiable
  information from statement PDFs, config setup, and output review.
- **[Project Structure](guides/project-structure.md)** — directory layout, SQLite
  schema, and Parquet file organisation.
- **[Export Options](guides/exports.md)** — simple vs. full export presets, CSV and
  Excel output.

## Reference

- **[CLI Reference](reference/cli.md)** — all `bsp process` and `bsp anonymise`
  options with examples.
- **[Python API Reference](reference/python-api.md)** — `StatementBatch`, report
  backends, export helpers, and database utilities.

## Quick Links

- [GitHub Repository](https://github.com/boscorat/bank_statement_parser)
- [PyPI Package](https://pypi.org/project/uk-bank-statement-parser/)
- [Issue Tracker](https://github.com/boscorat/bank_statement_parser/issues)

## About This Documentation

This site is built with [Zensical](https://zensical.org/), a modern static site
generator compatible with the Material for MkDocs theme. Reference pages and guide
sections are **auto-generated** from docstrings and comments in the source code using
`scripts/generate_docs.py`, ensuring the documentation stays in sync with the codebase.
