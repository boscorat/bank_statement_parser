<!-- markdownlint-disable MD003 MD007 -->

# Contributing to Bank Statement Parser

Thank you for your interest in contributing to Bank Statement Parser! We welcome contributions of all kinds — new bank configurations, bug fixes, documentation improvements, and more.

## Table of Contents

- [Before You Start](#before-you-start)
- [Response SLAs & Expectations](#response-slas--expectations)
- [Types of Contributions](#types-of-contributions)
- [Submission Workflow](#submission-workflow)
- [Test Data Requirements](#test-data-requirements)
- [Getting Help](#getting-help)

---

## Before You Start

Please review our [AGENTS.md](./AGENTS.md) for architectural context and development guidelines. All contributions should follow the project's code style and testing requirements.

### Quick Links

- **Development Setup:** [AGENTS.md](./AGENTS.md)
- **Code Style:** ruff (linting & formatting)
- **Tests:** pytest (see [AGENTS.md](./AGENTS.md) for test commands)
- **Architecture:** [AGENTS.md](./AGENTS.md)
- **Release Notes:** GitHub Releases (auto-generated from PR titles)

---

## Response SLAs & Expectations

Bank Statement Parser is actively maintained as a solo project. Here's what you can expect:

### Standard Response Times

| Request Type | Response SLA |
|---|---|
| **Bug reports** | 10 days (triage) |
| **Security vulnerabilities** | 48 hours (see [SECURITY.md](./SECURITY.md)) |
| **PRs from community** | 14 days (review) |
| **Feature requests / ideas** | 10 days (acknowledge) |
| **Bank configuration requests** | 10 days (acknowledge); implementation timeline varies |
| **General questions** | 10 days (answer or redirect) |

### Important Notes

- **Out of scope:** Feature requests aligned with [VISION.md](./VISION.md) scope boundaries may be deprioritised or politely declined. Check [VISION.md](./VISION.md) before investing time in a feature request.
- **Off-season:** I take ~2 weeks off per year. During those periods, responses may be slower.
- **Accelerated security responses:** Security vulnerabilities receive priority; see [SECURITY.md](./SECURITY.md) for details.

### What "Acknowledge" Means

When we "acknowledge" a feature request or bank config request within 10 days, we will:
- Confirm receipt of your request
- Indicate whether it aligns with [VISION.md](./VISION.md) scope
- If out of scope, explain why and suggest alternatives (e.g., opening a separate issue for discussion)
- If in scope, provide next steps

Implementation may take longer depending on complexity and priority.

---

## Types of Contributions

### 1. Adding a New Bank Configuration

**What:** Configure Bank Statement Parser to extract transactions from a bank that isn't currently supported.

**Effort:** 2–4 hours (medium–high difficulty)

**Requirements:**
- Anonymised test PDFs (3+ different statements from the bank/account type)
- Corresponding metadata JSON files describing expected parsing outcomes
- New `.toml` configuration files

**Start Here:** [Contributing: Adding a New Bank](./docs/guides/contributing-new-bank.md)

**Then:** [Test Data Submission Workflow](./docs/guides/test-data-submission.md)

---

### 2. Adding a New Account Type to an Existing Bank

**What:** HSBC current accounts are supported; you want to add HSBC savings accounts.

**Effort:** 1–2 hours (medium difficulty)

**Requirements:**
- Anonymised test PDFs (3+ statements for the new account type)
- Corresponding metadata JSON files
- Modified `.toml` configuration files (changes to `accounts.toml`, possibly others)

**Start Here:** [Contributing: Adding a New Bank](./docs/guides/contributing-new-bank.md) — Choose Scenario B

**Then:** [Test Data Submission Workflow](./docs/guides/test-data-submission.md)

---

### 3. Improving an Existing Bank Configuration

**What:** A bank's configuration has a bug, or you want to enhance it (better transaction extraction, support for new transaction types, etc.).

**Effort:** 30 mins–1 hour (low–medium difficulty)

**Requirements:**
- If config changes: Anonymised test PDFs (3+ statements) + metadata JSON
- PR description explaining the fix/improvement

**Start Here:** [Contributing: Adding a New Bank](./docs/guides/contributing-new-bank.md) — Choose Scenario C

**Then (if needed):** [Test Data Submission Workflow](./docs/guides/test-data-submission.md)

---

### 4. Bug Reports & Issues

**What:** You found a bug or have a feature request.

**How:**
1. Check [existing issues](https://github.com/boscorat/bank_statement_parser/issues) to avoid duplicates
2. Open a [new issue](https://github.com/boscorat/bank_statement_parser/issues/new) with:
   - Clear description of the problem
   - Steps to reproduce (if applicable)
   - Expected vs. actual behavior
   - Your environment (OS, Python version)

**Note:** If you're reporting a parsing issue with a bank, please include an anonymised PDF for context.

---

### 5. Documentation & Code Improvements

**What:** Improve existing docs, fix typos, refactor code, add tests, etc.

**How:**
1. Open a PR with your changes
2. Reference any related issues
3. Ensure all tests pass: `pytest tests/`
4. Ensure linting passes: `ruff check .`

---

## Submission Workflow

### For Bank Configurations (New Bank / New Account Type / Major Fixes)

**Step 1: Prepare Locally** (Your Work)
- Create TOML configuration files
- Test with real anonymised PDFs from the bank
- Generate metadata JSON describing expected outcomes
- See: [Local Testing Guide](./docs/guides/local-testing.md)

**Step 2: Submit Public PR** (Your Work)
- Create PR to `bank_statement_parser` repo with:
  - New/modified `.toml` config files
  - **No anonymised PDFs** (don't commit them!)
  - PR description explains: bank name, account types, any known limitations
  - Note mentioning: "Test data available; ready to send upon request"

**Step 3: Maintainer Review** (Our Work)
- We review your configuration files
- We contact you privately (email/DM) to request anonymised PDFs + metadata JSON
- We validate locally against your test data

**Step 4: Merge & Release** (Our Work)
- If approved, we merge your public PR
- We release a new version (e.g., v0.5.0) with your configuration
- Users can download and use it immediately

**Step 5: Private Test Data Integration** (Our Work)
- We create a PR on the private `bank-statement-data` repo
- We add your anonymised PDFs + metadata JSON
- We tag the test data with `min_bsp_version: 0.5.0` (or current release)
- We merge the private PR
- Your test PDFs now become permanent regression tests (run on all future PRs)

**Step 6: Your Tests Protect Future Changes** (Automatic)
- Every future PR to this repo will run tests against your PDFs
- If a future change breaks your config, tests fail immediately
- Your contribution stays protected against regression ✅

---

### For Other Changes (Bug Fixes, Docs, Code Improvements)

1. Create a PR with your changes
2. Reference any related issues
3. Ensure tests pass: `pytest tests/`
4. Ensure linting passes: `ruff check .` and `ruff format .`
5. We review and merge

---

## Test Data Requirements

### Minimum Requirements Per Submission

For **new bank configurations** or **new account types**, you must provide:
- ✅ **3+ anonymised PDFs** (different statements from the bank/account type)
- ✅ **Corresponding `.json` metadata files** (one per PDF)
- ✅ **Safe filenames** (no account numbers, sort codes, names)

**Why 3+?** One PDF might hit a parsing edge case. Three PDFs prove the config works across different dates, balances, and transaction patterns.

### Anonymisation Requirements

Your PDFs must have **no personally identifiable information (PII)**:
- ❌ Account numbers
- ❌ Sort codes
- ❌ Names (account holder, merchants)
- ❌ Email addresses
- ❌ Phone numbers
- ✅ Dates (statement dates, transaction dates)
- ✅ Amounts (proves config extracts correctly)
- ✅ Transaction types (DD, FT, CHQ, etc.)
- ✅ Running balances (proves checks-and-balances validation)

**Tool:** Use `bsp anonymise` command:
```bash
bsp anonymise statement_original.pdf --output anonymised_hsbc_creditcard_20220111.pdf
```

### Filename Conventions

⚠️ **CRITICAL:** Do NOT put PII in filenames.

**Pattern:** `anonymised_{BANK}_{ACCOUNT_TYPE}_{STATEMENT_DATE}.pdf`

**Good Examples:**
```
anonymised_hsbc_creditcard_20220111.pdf
anonymised_tsb_current_20220111.pdf
anonymised_natwest_savings_20220111.pdf
```

**Bad Examples (Don't Do This):**
```
john_smith_account_40_37_28_20220111.pdf      ❌ (account number)
hsbc_j.farrar_20220111.pdf                    ❌ (name)
20220111_chase_5432_savings.pdf               ❌ (last 4 digits)
```

---

## Metadata JSON

For each anonymised PDF, create a `.json` sidecar file describing expected parsing outcomes.

**Example structure:**
```json
{
  "pdf_name": "anonymised_hsbc_creditcard_20220111.pdf",
  "comment": "HSBC Rewards Credit Card, January 2022",
  "expected_result": "SUCCESS",
  "expected_outcome": "statement",
  "expected_statement_info": {
    "account": "Rewards Credit Card",
    "statement_date": "2022-01-11",
    "opening_balance": 1234.56,
    "closing_balance": 2345.67,
    "payments_in": 5000.00,
    "payments_out": 3888.89
  },
  "expected_transaction_count": 47,
  "expected_checks_and_balances_pass": true,
  "notes": "Tests multi-page statement with foreign transactions"
}
```

**How to generate:** Run the parser locally and copy the extracted values.

**Full documentation:** [Test Data Submission Workflow](./docs/guides/test-data-submission.md)

---

## Getting Help

### Documentation

- **Quick Start:** [Getting Started](./docs/guides/quick-start.md)
- **Adding a New Bank:** [Contributing: Adding a New Bank](./docs/guides/contributing-new-bank.md)
- **Local Testing:** [Testing Your Config Locally](./docs/guides/local-testing.md)
- **Test Data Submission:** [Test Data Submission Workflow](./docs/guides/test-data-submission.md)
- **Developer Architecture:** [AGENTS.md](./AGENTS.md)

### Questions?

- Check existing [GitHub Issues](https://github.com/boscorat/bank_statement_parser/issues)
- Open a [new issue](https://github.com/boscorat/bank_statement_parser/issues/new) with your question (we'll tag it as `question`)
- Discussions (if enabled on the repo)

### Feedback

We welcome feedback on the contribution process itself! If something is unclear or could be improved, please let us know:
- Open an issue with tag `documentation`
- Or mention it in your PR

---

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please treat all contributors with respect. Harassment or discrimination of any kind will not be tolerated.

---

## License

By contributing to this project, you agree that your contributions will be licensed under the [LGPL-3.0-or-later](LICENSE) license (see LICENSE file).

---

## Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin (DCO)](DCO) to certify that contributors have the right to submit code under the project's license.

### How to sign off your commits

Add a `Signed-off-by` line to every commit message using the `-s` flag:

```bash
git commit -s -m "Your commit message"
```

This appends the following to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

Ensure `git config user.name` and `git config user.email` are set correctly.

### Fixing unsigned commits

If you forgot to sign off, you can amend the last commit:

```bash
git commit --amend -s
```

To sign off all commits in a branch during rebase:

```bash
git rebase --signoff upstream/main
```

### Installing the auto-signoff hook (optional)

This repository includes a `prepare-commit-msg` hook that automatically adds the `Signed-off-by` line to your commit messages. To install it:

```bash
git config core.hooksPath hooks
```

This is optional — you can always use `git commit -s` manually instead.

---

Thank you for contributing! 🙏
