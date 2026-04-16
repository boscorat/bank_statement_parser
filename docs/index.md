# Bank Statement Parser

Stop copying transactions from PDF statements into spreadsheets by hand. **Bank Statement Parser** reads your bank statement PDFs, pulls out every transaction automatically, and gives you clean, ready-to-use files — CSV, Excel, or direct import formats for accounting software.

Everything runs on your own computer. No data is sent anywhere.

---

## What it does for you

- **Reads your PDF statements** — drop a folder of PDFs in, get structured data out
- **No manual re-entry** — dates, amounts, descriptions, and running balances are all extracted automatically
- **Export to the format you need** — CSV, Excel, or QuickBooks-compatible import files
- **Handles multiple accounts at once** — current accounts, savings accounts, and credit cards in one pass
- **Avoids duplicates** — re-running on the same statements won't create duplicate records
- **Works fully offline** — your statements never leave your machine
- **Free and open source** — no subscription, no sign-up

---

## Supported banks and accounts

| Bank | Supported accounts |
|---|---|
| **HSBC UK** | Bank Account (Current), HSBC Advance, Flexible Saver, Online Bonus Saver, Rewards Credit Card |
| **TSB UK** | Spend & Save (Current Account) |
| **NatWest UK** | *(coming soon)* |

> Support for more banks can be added by creating configuration files. See the [Adding a New Bank](guides/new-bank-config.md) guide.

---

## Getting started

New to Bank Statement Parser? The [Quick Start guide](guides/quick-start.md) walks you through installation and your first run step by step — no technical experience needed.

---

## Going further

| | |
|---|---|
| [Quick Start](guides/quick-start.md) | Install and run for the first time |
| [Export Options](guides/exports.md) | CSV, Excel, and accounting software formats |
| [Anonymisation](guides/anonymisation.md) | Redact personal details from PDFs before sharing |
| [Project Structure](guides/project-structure.md) | How output files and folders are organised |
| [Adding a New Bank](guides/new-bank-config.md) | Configure support for a bank not listed above |
| [CLI Reference](reference/cli.md) | All command-line options |
| [Python API Reference](reference/python-api.md) | Automate from your own scripts |

---

## Links

- [GitHub Repository](https://github.com/boscorat/bank_statement_parser)
- [PyPI Package](https://pypi.org/project/uk-bank-statement-parser/)
- [Issue Tracker](https://github.com/boscorat/bank_statement_parser/issues)
