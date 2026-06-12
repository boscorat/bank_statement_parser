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
  "expected_outcome": "SUCCESS",
  "expected_filename": "HALIFAX_UK_CUR_12121212_20220131.pdf",
  "expected_statement_date": "2022-01-31",
  "expected_account": "Current Account",
  "expected_id_account": "HALIFAX_UK_CUR_12121212",
  "expected_opening_balance": "2289.7200",
  "expected_closing_balance": "2857.6800",
  "expected_payments_in": "2874.7500",
  "expected_payments_out": "2306.7900",
  "expected_transaction_count": "10",
  "description": "Halifax UK Current Account - January 2022"
}
```

### Field Descriptions

| Field | Type | Description |
|---|---|---|
| `expected_result` | `"SUCCESS"` \| `"REVIEW"` \| `"FAILURE"` | Parsing result expectation |
| `expected_outcome` | `"SUCCESS"` \| `"REVIEW CAB"` \| `"FAILURE CONFIG"` \| `"FAILURE DATA"` \| `"FAILURE OTHER"` | What stage the result reached |
| `expected_filename` | string | New filename assigned after processing (e.g., `"HSBC_UK_CUR_11111111_20190610.pdf"`) |
| `expected_statement_date` | string | Statement date (ISO format: YYYY-MM-DD) |
| `expected_account` | string | Account product name (e.g., "Current Account", "Credit Card") |
| `expected_id_account` | string | Account ID (e.g., `"HALIFAX_UK_CUR_12121212"`) |
| `expected_opening_balance` | string | Opening balance as decimal string (e.g., `"2289.7200"`) |
| `expected_closing_balance` | string | Closing balance as decimal string |
| `expected_payments_in` | string | Total credits as decimal string |
| `expected_payments_out` | string | Total debits as decimal string |
| `expected_transaction_count` | string | Number of transactions extracted from PDF (as string) |
| `description` | string | Human-readable note about what this test covers |

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
