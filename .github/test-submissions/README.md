# Test Submissions

This directory holds metadata JSON files for test data submissions. These files accompany pull requests that add new banks, new account types, or improve existing configurations.

## Directory Purpose

Contributors submitting test data include **metadata JSON files only** in their public PR. The actual PDF files are transmitted privately to maintainers via email, keeping sensitive financial documents out of the public repository.

Metadata files enable:
- Automated validation of extracted transaction counts and balances
- Regression testing once maintainers integrate the PDFs into the private `bank-statement-data` repo
- Clear documentation of what each test PDF validates

## Filename Convention

**Do NOT include PII (personally identifiable information) in filenames.**

Valid filename format:
```
anonymised_{BANK}_{ACCOUNT_TYPE}_{STATEMENT_DATE}.json
```

Examples:
- ✅ `anonymised_hsbc_credit_card_2024-01-31.json`
- ✅ `anonymised_barclays_current_2024-02-28.json`
- ✅ `anonymised_natwest_business_2024-03-15.json`
- ❌ `J_Smith_account_2024-01-31.json` (contains name)
- ❌ `12-34-56_statement_2024-01-31.json` (contains sort code)
- ❌ `5678901234_statement_2024-01-31.json` (contains account number)

## JSON Schema

Each metadata file documents one anonymised test PDF:

```json
{
  "expected_result": "SUCCESS",
  "expected_outcome": "EXTRACTION_SUCCESS",
  "expected_statement_info": {
    "account": "Anonymised Account",
    "company": "HSBC",
    "statement_type": "Credit Card",
    "currency": "GBP",
    "period_start": "2024-01-01",
    "period_end": "2024-01-31"
  },
  "expected_transaction_count": 42,
  "expected_checks_and_balances_pass": true,
  "notes": "Test validates credit card multi-line transaction splitting and fee detection",
  "min_bsp_version": null
}
```

### Field Descriptions

| Field | Type | Description |
|---|---|---|
| `expected_result` | `"SUCCESS"` \| `"REVIEW"` \| `"FAILURE"` | Parsing result expectation |
| `expected_outcome` | `"EXTRACTION_SUCCESS"` \| `"EXTRACTION_FAILURE"` \| `"VALIDATION_FAILURE"` | What stage the result reached |
| `expected_statement_info.account` | string | Anonymised account name (e.g., "Anonymised Account", "Test Current A/C") |
| `expected_statement_info.company` | string | Bank/institution name |
| `expected_statement_info.statement_type` | string | Account type (e.g., "Current", "Credit Card", "Business") |
| `expected_statement_info.currency` | string | 3-letter ISO code (e.g., "GBP", "EUR", "USD") |
| `expected_statement_info.period_start` | string | Statement start date (ISO format: YYYY-MM-DD) |
| `expected_statement_info.period_end` | string | Statement end date (ISO format: YYYY-MM-DD) |
| `expected_transaction_count` | integer | Number of transactions extracted from PDF |
| `expected_checks_and_balances_pass` | boolean | Whether financial validation checks pass |
| `notes` | string | What this test validates (e.g., edge cases, special formatting) |
| `min_bsp_version` | `null` \| string | Minimum bsp version required; set by maintainers during private PR merge |

## How to Create Metadata Files

1. **Prepare test PDF**: Anonymise using `bsp anonymise` command
   ```bash
   bsp anonymise path/to/original_statement.pdf
   ```

2. **Extract test data**: Run local parsing to gather expected values
   ```bash
   bsp process --pdfs path/to/anonymised_statement.pdf
   ```

3. **Create metadata**: Use the extracted data to populate a JSON file
   - Transaction count: Query the test database or count rows in extracted Parquet
   - Account name: Use generic placeholder (e.g., "Anonymised Account")
   - Notes: Describe what edge case or formatting this test validates

4. **Include in PR**: Place the `.json` file in this directory; include anonymised PDF link in PR description

## Submission Workflow

1. **Contributor submits PR** with:
   - Metadata JSON file(s) in `.github/test-submissions/`
   - Anonymised PDFs (via external link or email to maintainers)
   - PR description explaining test rationale

2. **Maintainer reviews** and requests changes if needed

3. **Contributor sends PDFs** to maintainers via email (private channel)

4. **Maintainer creates private PR** in `bank-statement-data` repo with:
   - PDF files
   - Metadata JSON with `min_bsp_version` set (if needed for compatibility)
   - Link to public PR

5. **Both PRs merge**: Public PR first → release tag → private PR follows

6. **Test data becomes permanent**: PDFs integrated into regression test suite

## Troubleshooting

**Q: My PDF contains sensitive information. Can I submit it to the public repo?**
A: No. Always anonymise using `bsp anonymise`. Only metadata JSON files belong in the public PR. PDFs are transmitted privately to maintainers.

**Q: How do I know if my PDF is sufficiently anonymised?**
A: Check `anonymise` documentation in `docs/guides/test-data-submission.md` for anonymisation standards and verification steps.

**Q: What if the transaction count or checks don't match?**
A: This is expected during development. Metadata reflects *expected* values once the config is working correctly. If values are uncertain, use `"TBD"` as a placeholder and maintainers will populate during private PR merge.

**Q: Can I use multiple PDFs for one bank/account type?**
A: Yes, create separate metadata files for each PDF, one per statement date.

## For More Information

- Full test data submission workflow: `docs/guides/test-data-submission.md`
- Anonymisation guide: `docs/guides/test-data-submission.md#anonymising-pdfs`
- Contributing a new bank: `docs/guides/contributing-new-bank.md`
- Local testing & verification: `docs/guides/local-testing.md`
