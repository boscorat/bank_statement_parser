# Changelog

All notable changes to the `uk-bank-statement-parser` package will be documented in this file.

This project follows [Semantic Versioning](https://semver.org/) and uses [towncrier](https://towncrier.readthedocs.io/) for changelog management. Fragments live in the `changes/` directory.

<!-- towncrier release notes start -->

## [0.3.3] - 2026-07-01


<details>

<summary>Internal Changes</summary>
- update _VIEW_MIGRATIONS to use DimDate and *_int surrogate keys
</details>

## [0.3.2.post1] - 2026-06-30


#### Bug Fixes
- Fix release workflow to push changelog updates to master when releasing from any branch
<details>

<summary>Internal Changes</summary>
- Enforce towncrier changelog fragments in CI and pre-commit hooks for consistent release notes
- Fixed towncrier package name config and improved release workflow robustness with date consistency and fragment cleanup validation
- Release workflow now accepts version-branch tags (e.g., v0.3.1) for distributed release strategy
- Release workflow now accepts tags from any branch, enabling flexible release strategies (major/minor from master, hotfixes from version branches, etc.)
- Replace complex worktree cherry-pick approach with simpler verification and automatic changelog-update branch creation.
- Fix towncrier template to iterate sections dynamically and filter empty entries, and add start_string marker for correct changelog insertion ordering
- Add GitHub Actions ecosystem to Dependabot for automated action version updates
- render internal changes in collapsed details block
</details>## [0.3.0] - 2026-06-28

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
