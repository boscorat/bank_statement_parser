<!-- markdownlint-disable MD003 MD007 MD013 -->

# Test Data Submission Workflow

When you contribute a new bank configuration, you'll provide anonymised PDFs that become permanent regression tests. This guide explains how to prepare and submit test data.

## The Big Picture

Here's how your test data flows from submission into permanent testing:

```
YOU (Contributor)
  │
  ├─ Prepare 3+ anonymised PDFs
  │  + JSON metadata for each
  │
  └─ Submit public PR with config only
     (no PDFs attached!)
     └─ Note in PR: "Test PDFs available; contact me for download"

MAINTAINERS
  │
  ├─ Review config PR
  │
  ├─ Create release (v0.5.0)
  │
  ├─ Contact you privately
  │  (email/DM for anonymised PDFs + JSON metadata)
  │
  ├─ Create private PR
  │  (bank-statement-data repo with your PDFs + metadata)
  │
  └─ Merge private PR
     └─ Your tests now active on all future public PRs ✅
```

---

## What You Do: Prepare Test Data

### Step 1: Anonymise Your PDFs

Your bank statements contain sensitive information. You must anonymise them before sending.

#### Using the Built-in Command (Recommended)

```bash
bsp anonymise statement_original.pdf \
  --output anonymised_hsbc_creditcard_20220111.pdf
```

The `anonymise` command redacts:
- Account numbers
- Sort codes
- Names
- Email addresses
- Phone numbers

And preserves:
- Dates (transaction dates, statement dates)
- Amounts (proves extraction works)
- Transaction types (DD, FT, CHQ, etc.)
- Running balances (proves checks-and-balances)

#### Manual Anonymisation (Alternative)

If you prefer, manually redact sensitive data using a PDF editor and save as a new file.

**What to remove:**
- Account numbers
- Sort codes  
- Account holder names
- Any personal identifiers
- Merchant details that might reveal you (e.g., "Dr. Smith's Medical Practice")

**What to keep:**
- Dates (all of them)
- Amounts
- Transaction descriptions (generic ones)
- Running balances

---

### Step 2: Name Files Safely (NO PII!)

⚠️ **CRITICAL:** Do NOT put personally identifiable information in filenames.

#### Safe Pattern

```
anonymised_{BANK}_{ACCOUNT_TYPE}_{STATEMENT_DATE}.pdf
```

#### Good Examples ✅

```
anonymised_hsbc_creditcard_20220111.pdf
anonymised_tsb_current_20220111.pdf
anonymised_natwest_savings_20220111.pdf
anonymised_lloyds_studentaccount_20220111.pdf
```

#### Bad Examples ❌ (Don't Do These)

```
john_smith_account_40_37_28_20220111.pdf      ❌ (account number visible)
hsbc_j.farrar_20220111.pdf                    ❌ (name visible)
20220111_chase_5432_savings.pdf               ❌ (card last-4 visible)
statement_001234567_20220111.pdf              ❌ (account number visible)
```

---

### Step 3: Create Metadata JSON Files

For each anonymised PDF, create a `.json` sidecar file describing expected parsing outcomes.

#### Complete Real Example

```json
{
  "expected_result": "SUCCESS",
  "expected_outcome": "SUCCESS",
  "expected_filename": "HSBC_UK_CUR_11111111_20220111.pdf",
  "expected_statement_date": "2022-01-11",
  "expected_account": "Bank Account",
  "expected_id_account": "HSBC_UK_CUR_11111111",
  "expected_opening_balance": "612.1900",
  "expected_closing_balance": "1830.1800",
  "expected_payments_in": "612.1900",
  "expected_payments_out": "1830.1800",
  "expected_transaction_count": "39",
  "description": "HSBC Rewards Credit Card, January 2022 statement"
}
```

#### Field Reference

| Field | Type | Purpose | Example |
|-------|------|---------|---------|
| `expected_result` | string | Expected result: `"SUCCESS"` / `"REVIEW"` / `"FAILURE"` | `"SUCCESS"` |
| `expected_outcome` | string | Specific outcome: `"SUCCESS"` / `"REVIEW CAB"` / `"FAILURE CONFIG"` / `"FAILURE DATA"` / `"FAILURE OTHER"` | `"SUCCESS"` |
| `expected_filename` | string | New filename assigned after processing | `"HSBC_UK_CUR_11111111_20220111.pdf"` |
| `expected_statement_date` | string | Statement date (ISO format: YYYY-MM-DD) | `"2022-01-11"` |
| `expected_account` | string | Account product name | `"Bank Account"` |
| `expected_id_account` | string | Account ID | `"HSBC_UK_CUR_11111111"` |
| `expected_opening_balance` | string | Opening balance as decimal string (4 decimal places) | `"612.1900"` |
| `expected_closing_balance` | string | Closing balance as decimal string | `"1830.1800"` |
| `expected_payments_in` | string | Total credits as decimal string | `"612.1900"` |
| `expected_payments_out` | string | Total debits as decimal string | `"1830.1800"` |
| `expected_transaction_count` | string | Number of transactions extracted (as string) | `"39"` |
| `description` | string | Human-readable note about what this test covers | `"HSBC Rewards, Jan 2022"` |

#### How to Generate Values

Run the parser locally with your anonymised PDF:

```bash
bsp process --pdfs /path/to/anonymised/
```

Then query the SQLite database:

```bash
sqlite3 bsp_project/database/project.db

# Run this query:
SELECT account, statement_date, opening_balance, closing_balance, 
       payments_in, payments_out FROM DimStatement LIMIT 1;
```

Copy these values into your JSON file, formatting balances as strings with 4 decimal places (e.g., `"612.1900"`).

**For the filename:**

```bash
# The expected_filename is the new name assigned after processing:
SELECT filename_new FROM DimStatement LIMIT 1;
```

**For transaction count:**

```bash
sqlite3 bsp_project/database/project.db

# Run this query:
SELECT COUNT(*) FROM statement_lines WHERE ID_STATEMENT = 'STATEMENT_ID';
```

---

## What You Do: Submit

### Option 1: Public PR (Recommended)

1. **Create PR to `bank_statement_parser`** with:
   - New/modified `.toml` config files
   - PR description explaining the bank, account types, any limitations
   - **Do NOT commit PDFs or JSON to this repo**
   - Add note: `"Test data available: 3 anonymised PDFs + metadata JSON. Ready to provide via email."`

2. **Wait for maintainer contact**
   - We'll review your config
   - We'll email you asking for anonymised PDFs + JSON metadata files
   - Reply with the files (or send via file sharing service)

### Option 2: Email (Alternative)

Contact a maintainer directly with:
- Anonymised PDFs (3+)
- Corresponding JSON metadata files
- Your config PR link (if applicable)
- Brief explanation of the bank/account type

---

## What Maintainers Do

### Review Workflow

1. **Review config PR** (public repo)
   - Check TOML syntax and structure
   - Verify it follows configuration conventions
   - Contact you privately for anonymised PDFs + metadata JSON

2. **Receive PDFs from you**
   - You send anonymised PDFs + JSON files
   - Maintainer validates metadata against actual parsing output

3. **Test locally**
   - Run parser against your PDFs
   - Verify extracted values match metadata JSON
   - Verify checks-and-balances pass (or match expected failure)

4. **Merge config PR** → **Release new version** (v0.5.0)
   - Your config is now live
   - Users can download and use immediately

5. **Create private PR** (`bank-statement-data` repo)
   - Add your anonymised PDFs + metadata JSON
   - Tag with version: `"min_bsp_version": "0.5.0"`
   - This ensures tests only run on versions that understand your config

6. **Merge private PR**
   - Your test data is now active
   - Every future PR runs tests against your PDFs
   - If a future change breaks your config, tests fail immediately

---

## Version Pinning: Why It Matters

When you submit test data for v0.5.0, it works perfectly with that version. But what if v1.0.0 (released a year later) changes the extraction engine?

**Without version pinning:**
- Tests run your v0.5.0 config on v1.0.0 parser
- Tests fail (false positive — not actually broken)
- Confusing error messages

**With version pinning:**
- You set `"min_bsp_version": "0.5.0"`
- Tests skip your data on v1.0.0+
- No false failures
- Clear signal: "This config works on 0.5.0 through 0.9.9"

**Timeline example:**
```
Jan 2022: Config PR merged → v0.5.0 released
Feb 2022: Private PR merged → min_bsp_version: 0.5.0 set
         (tests active from now on)

Jan 2023: v1.0.0 released (breaking changes to parser)
         Maintainers can optionally re-test your config
         If still valid, update min_bsp_version to 1.0.0
         If invalid, note it in docs
```

---

## Troubleshooting

### My Metadata JSON Doesn't Match Parser Output

1. **Re-run parser locally:**
   ```bash
   bsp process --pdfs /path/to/anonymised/pdf
   ```

2. **Check the export CSV or database:**
   ```bash
   sqlite3 bsp_project/database/project.db
   SELECT account, statement_date, opening_balance, closing_balance FROM DimStatement;
   ```

3. **Common issues:**
   - **Rounding:** Parser uses floating-point. Copy exact values, don't round.
   - **Anonymisation corruption:** Did anonymisation tool change the numbers? Re-check original PDF.
   - **Date format:** JSON expects ISO (YYYY-MM-DD), not regional (DD/MM/YYYY).

### Parser Fails on My PDF Locally

- Your config isn't ready yet. Debug it locally (see [Local Testing Guide](./local-testing.md)) before submitting.
- Don't submit metadata JSON for a failing PDF.

### Can I Include PDFs with REVIEW Result?

- Yes! REVIEW means extraction succeeded but checks-and-balances failed.
- Set `"expected_result": "REVIEW"` and `"expected_outcome": "REVIEW CAB"`.
- Maintainers will review and may ask why (e.g., bank statement rounding issue) or accept it.

### How Long Until Tests Are Active?

- After config PR merges and releases: Usually 1–2 weeks for private PR to be created and merged
- Then your tests run on all future PRs automatically

---

## FAQ

**Q: What if I don't have 3 PDFs?**  
A: Please try to get 3 different statements from your bank (different months/years if possible). They don't need to be huge; one transaction is enough if all 3 together show the config is robust.

**Q: Can I include screenshots instead of PDFs?**  
A: No, the parser needs actual PDF files. Screenshots won't work.

**Q: Do I have to anonymise using `bsp anonymise`?**  
A: No, manual anonymisation or other tools are fine. Just ensure all PII is removed.

**Q: What if my PDF doesn't parse correctly even with my config?**  
A: Debug it locally first. See [Local Testing Guide](./local-testing.md) for strategies. If still stuck, open an issue with the anonymised PDF.

**Q: Will you reject my PR if test data isn't perfect?**  
A: We'll work with you to fix it! If metadata doesn't match actual parsing, we'll help debug. If checks-and-balances fail unexpectedly, we'll investigate together.

**Q: Can other people access my anonymised PDFs?**  
A: No. The `bank-statement-data` repo is private. Only maintainers and authorized contributors can see them.

---

## Next Steps

1. **Test your config locally** (See [Local Testing Guide](./local-testing.md))
2. **Prepare anonymised PDFs** (3+) + metadata JSON (one per PDF)
3. **Submit config PR** to `bank_statement_parser`
4. **Wait for maintainer contact** for PDFs
5. **Send anonymised PDFs + JSON** privately
6. **Your tests activate** and protect your work! ✅

---

## Questions?

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for links to all guides and how to ask for help.
