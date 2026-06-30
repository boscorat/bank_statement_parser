## [0.3.1] - 2026-06-29

# Changelog

All notable changes to the `uk-bank-statement-parser` package will be documented in this file.

This project follows [Semantic Versioning](https://semver.org/) and uses [towncrier](https://towncrier.readthedocs.io/) for changelog management. Fragments live in the `changes/` directory.

## [0.3.0] - 2026-06-28

### Features

- First official stable release under Semantic Versioning.
- Async parallel PDF processing via `StatementBatch` with `turbo=True`.
- Star-schema data mart with DimTime, DimAccount, DimStatement, FactTransaction, and FactBalance.
- Configurable bank import patterns via TOML files in `project/config/import/`.
- PDF anonymisation support (optional dependency: `uk-bank-statement-anonymiser`).
- Forex rate lookup with configurable API sources.
- Export to Excel, CSV, JSON, and reporting data feeds.
- System packages (`.deb` and `.rpm`) via fpm in CI.
- Versioned documentation via mkdocs-material + mike.

### Breaking Changes

- The `filetype='both'` parameter (deprecated since 0.2.0) has been removed. Use `filetype='all'` instead.
