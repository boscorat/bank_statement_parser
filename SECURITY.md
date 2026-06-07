# Security Policy

Bank Statement Parser processes sensitive financial data. We take security seriously and welcome responsible vulnerability disclosures.

---

## Reporting a Vulnerability

**DO NOT** open a public GitHub issue for security vulnerabilities.

Instead, please email your report directly to:

**[farrar.jason1@gmail.com](mailto:farrar.jason1@gmail.com)**

Include the following in your report:

- **Description** — What is the vulnerability?
- **Affected versions** — Which versions of BSP are affected?
- **Steps to reproduce** — How can the vulnerability be demonstrated?
- **Potential impact** — What could an attacker do with this vulnerability?
- **Suggested fix** (optional) — Do you have a proposed patch?

---

## Response SLAs

| Response Type | Timeline |
|---|---|
| **Initial acknowledgment** | Within 48 hours |
| **Initial assessment** | Within 7 days |
| **Security patch (critical)** | Within 14 days |
| **Security patch (non-critical)** | Within 30 days |

Once a fix is released, we will:
1. Publish a security advisory on GitHub
2. Tag the release with `[SECURITY]` in the release notes
3. Notify users who have starred the repository (if GitHub's security advisory system is available)

---

## Security Model

### Data Processing

Bank Statement Parser operates **entirely offline**:
- PDFs are processed on your local machine
- No data is sent to external servers during normal operation
- Extracted transactions are stored locally in SQLite or Parquet files
- No telemetry, analytics, or crash reporting is collected

### FX Rate Lookups

The optional FX rate lookup feature uses public APIs to fetch exchange rates:
- Requests contain **only** the date and currency pair (e.g., "GBP/USD on 2025-06-01")
- Requests do **NOT** contain transaction data, amounts, descriptions, or account information
- Failed lookups emit a `UserWarning` but do not halt processing
- Users can disable FX lookups entirely via configuration

### PDF Input

- PDFs are parsed using [pdfplumber](https://github.com/jaidedai/pdfplumber) (open-source)
- Bank statement PDFs are assumed to be trusted input (you are extracting your own statements)
- No code execution or JavaScript is extracted from PDFs

### Anonymisation Feature

The optional `uk-bank-statement-anonymiser` utility:
- Redacts PII (names, addresses, account numbers) using pikepdf
- Scrambles transaction descriptions so merchant names cannot be recovered
- Processes anonymised PDFs **locally**; no data is sent to third parties

---

## Supported Versions

Only the latest minor version of Bank Statement Parser receives security patches.

| Version | Status | Support Ends |
|---|---|---|
| 0.2.x | **Active** | Current — all critical/non-critical patches |
| 0.1.x | **EOL** | 2025-12-31 — no backports |
| <0.1 | **Unsupported** | N/A |

---

## Known Limitations

### PDF Extraction

- **Format stability** — Bank statement PDF layouts change over time. If your bank updates their statement format, parsing may break until we update the configuration. We will attempt to maintain configurations, but new formats may require community contributions.
- **OCR requirement** — Scanned (image-based) PDFs are not supported. Your bank's statements must be text-searchable.

### FX Rate Lookups

- **Estimates only** — FX rates from public APIs are estimates, not precise interbank rates.
- **Not suitable for forex accounting** — Do not use FX lookups for precise accounting; they are informational only.
- **Rate timing** — Rates are fetched as-of the transaction date, but exact timing may differ from your bank's settlement rates.

### Data Validation

- **Balance tolerance** — Opening/closing balance checks use a configurable tolerance (default 0.005 to account for rounding). Very small discrepancies may go undetected.
- **Incomplete statements** — If your bank omits transactions or statement sections, BSP cannot detect this (we validate against bank-reported balances, not external sources).

---

## Dependency Security

Bank Statement Parser's production dependencies are:

- **dacite** — TOML configuration deserialization
- **pdfplumber** — PDF text extraction
- **polars** — Data manipulation and export
- **pikepdf** — PDF anonymisation (optional dependency)
- **xlsxwriter** — Excel export

All dependencies are open-source and regularly scanned for vulnerabilities via GitHub's Dependabot. We monitor security advisories and update promptly when issues are discovered.

---

## Best Practices for Users

If you're using Bank Statement Parser to process financial data:

1. **Review extracted transactions** — Always audit extracted transactions against your original bank statements before relying on them.
2. **Protect the SQLite database** — The database contains extracted transactions; store it securely and don't share it.
3. **Anonymise before sharing** — Use the anonymisation feature before sharing extracted data with third parties.
4. **Keep software up to date** — Upgrade BSP regularly to get security fixes and improvements.
5. **Report suspicious behavior** — If you notice unexpected parsing results or suspect a security issue, report it (see [Reporting a Vulnerability](#reporting-a-vulnerability)).

---

## Disclosure Timeline

We follow a **coordinated disclosure** model:

1. You report the vulnerability privately
2. We acknowledge receipt within 48 hours
3. We develop and test a fix (typically 7–14 days for critical issues)
4. We release a patched version
5. We publish a security advisory **after** users have had time to upgrade (typically 3–7 days after release)

This gives users a window to upgrade before the vulnerability is publicly disclosed.

---

## Questions?

If you have security questions or concerns about using Bank Statement Parser, please email [farrar.jason1@gmail.com](mailto:farrar.jason1@gmail.com).

For non-security issues, please use [GitHub Issues](https://github.com/boscorat/bank_statement_parser/issues) or [GitHub Discussions](https://github.com/boscorat/bank_statement_parser/discussions).
