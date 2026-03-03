# Bank Statement Parser

Welcome to the documentation for **bank_statement_parser** — a Python library for parsing
bank statement PDFs, extracting structured transaction data, and persisting results to
Parquet files and a SQLite star-schema data mart.

## Guides

- **[Adding a New Bank](guides/new-bank-config.md)** — step-by-step guide to creating
  configuration files for parsing statements from a new bank, including the TOML file
  structure, field extraction rules, and standard field mappings.

## Quick Links

- [GitHub Repository](https://github.com/boscorat/bank_statement_parser)
- [PyPI Package](https://pypi.org/project/uk-bank-statement-parser/)
- [Issue Tracker](https://github.com/boscorat/bank_statement_parser/issues)

## About This Documentation

This site is built with [Zensical](https://zensical.org/), a modern static site
generator compatible with the Material for MkDocs theme. Reference sections within the
guides are **auto-generated** from docstrings and comments in the source code
(specifically `data.py`) using `scripts/generate_docs.py`, ensuring the documentation
stays in sync with the codebase.
