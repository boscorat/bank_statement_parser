# Vision: Bank Statement Parser

## Our Mission

Bank Statement Parser solves one problem brilliantly: **extract structured transaction data from UK bank statement PDFs without manual data entry**. We are a data extraction tool, not a financial platform or accounting package.

---

## What We Do

✅ **PDF extraction** — Parse bank statement PDFs using configurable pattern-based extraction (pdfplumber)

✅ **UK bank coverage** — Support common UK banks (HSBC, Barclays, TSB, NatWest, Lloyds, etc.) and account types (current, savings, credit cards)

✅ **Validation** — Automatic checks and balances: opening/closing balance verification, payment totals, running balance validation

✅ **Persistence** — Export extracted data to Parquet files, SQLite database, or both

✅ **Reporting** — Star-schema data mart with dimension and fact tables (DimTime, DimAccount, DimStatement, FactTransaction, FactBalance)

✅ **Export flexibility** — Output as CSV, Excel workbooks, or direct database queries

✅ **PDF anonymisation** — Redact personally identifiable information from statements for safe data sharing

✅ **Batch processing** — Async + multiprocess support for large PDF sets

✅ **Cross-platform** — Pure Python with no OS-specific dependencies

---

## What We Don't Do

❌ **International banks** — Scope is UK only. We don't support US, EU, or other international banking formats.

❌ **Cryptocurrency or crypto exchanges** — No support for blockchain-based financial statements.

❌ **Real-time transaction feeds** — We process static PDF statements, not live API feeds or pushes.

❌ **Accounting software integration** — We don't sync to QuickBooks, Xero, FreeAgent, etc. (That's consultancy.)

❌ **Reconciliation tools** — We extract transactions; reconciliation logic is outside our scope.

❌ **Multi-currency transactions** — We support basic FX rate lookups for informational purposes only. Precise forex accounting is out of scope.

❌ **Bank feed connectivity** — No direct connections to banking APIs or SWIFT feeds.

❌ **Real-time balance monitoring** — No websockets, push notifications, or live alerts.

❌ **Tax calculations** — We don't compute tax liabilities, allowances, or deductions.

❌ **Investment account tracking** — Scope is limited to transactional accounts (current, savings, credit cards).

---

## Philosophy

**One tool, one job, done well.**

- We prioritise **accuracy** over feature breadth. A transaction extracted incorrectly is worse than a missing feature.
- We embrace **simplicity** over configuration complexity. TOML-based configs should be understandable by non-developers.
- We respect **privacy** by processing everything offline. No data leaves your machine except by explicit export.
- We value **extensibility** without touching Python. Community contributors should be able to add bank configs without modifying core code.

---

## Roadmap Philosophy

### In Scope for Future Development
- Additional UK banks (community contributions welcome)
- New account types for existing banks (savings, joint accounts, etc.)
- Enhanced validation rules
- Better error handling and diagnostics
- Performance improvements (larger batches, faster extraction)
- Documentation and examples

### Out of Scope (Even if Requested)
- International bank support
- Real-time feeds or API integrations
- Direct accounting software connectors
- GUI application (desktop UI is a separate project: [openstan](https://github.com/boscorat/openstan))
- Mobile apps
- Hosted SaaS platform

---

## When to File a Feature Request

✅ **Do** file a feature request if:
- It helps extract transactions from UK bank statements more accurately
- It improves validation or balance checking
- It makes the tool easier to use for UK banks
- It's a new UK bank or account type

❌ **Don't** file a feature request for:
- International banks or crypto exchanges
- Real-time feeds or API connectivity
- Accounting software integration
- Tax calculations or reconciliation
- Investment/trading accounts

When in doubt, check [CONTRIBUTING.md](./CONTRIBUTING.md) → "Adding a Bank" or open a [GitHub Discussion](https://github.com/boscorat/bank_statement_parser/discussions) to ask before investing time.

---

## Contact & Questions

- **Feature requests?** Open a [GitHub Discussion](https://github.com/boscorat/bank_statement_parser/discussions/new?category=ideas) (not an issue) and reference this vision doc.
- **Adding a bank?** See [CONTRIBUTING.md](./CONTRIBUTING.md) → "Adding a New Bank Configuration"
- **Security vulnerability?** See [SECURITY.md](./SECURITY.md)
- **General help?** Open a [GitHub Discussion](https://github.com/boscorat/bank_statement_parser/discussions/new?category=questions)
