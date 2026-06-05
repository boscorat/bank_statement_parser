<!-- markdownlint-disable MD003 MD007 MD013 -->

# Testing Your Config Locally

Before submitting a new bank configuration, test it with real (anonymised) PDFs from your bank to ensure extraction works correctly. This guide shows how to run the parser locally and interpret the results.

## Quick Test

Run the parser against a folder of anonymised PDFs:

```bash
bsp process --pdfs /path/to/your/anonymised/pdfs/
```

**Output:** A new `bsp_project/` folder with:
- `database/project.db` — SQLite database with extracted data
- `export/` — CSV, JSON, Excel exports
- `parquet/` — Raw Parquet files
- `log/debug/` — Detailed debug logs for each PDF

---

## Interpreting Results

### SUCCESS ✅

**What it means:**
- Parser successfully extracted all required fields
- Checks-and-balances validation passed (opening + paid_in - paid_out = closing)
- Data is ready to use

**What to check:**
- Look at `export/transactions.csv` — transactions should look correct
- Query the database:
  ```bash
  sqlite3 bsp_project/database/project.db
  SELECT * FROM DimStatement LIMIT 1;
  ```
- Verify balances match your PDF

**Next steps:** Your config is ready! Prepare metadata JSON files (see [Test Data Submission Guide](./test-data-submission.md)).

---

### REVIEW ⚠️

**What it means:**
- Parser successfully extracted all fields
- BUT checks-and-balances validation **failed**
- Example: opening + paid_in - paid_out ≠ closing (by £0.47)

**Possible causes:**
- Bank statement rounding error (bank rounds, you don't — or vice versa)
- Your config incorrectly identifies debit vs. credit
- Missing transaction (bank didn't list it)
- Parser bug (unlikely, but possible)
- PDF corruption or anonymisation tool artifact

**What to check:**
1. **Query the database:**
   ```bash
   sqlite3 bsp_project/database/project.db
   SELECT account, statement_date, opening_balance, payments_in, 
          payments_out, closing_balance FROM DimStatement LIMIT 1;
   ```

2. **Compare to your PDF:**
   - Do the balances match what's printed?
   - Are all transactions listed in `export/transactions.csv`?
   - Are debits/credits assigned correctly (positive vs. negative)?

3. **Check the CAB report:**
   ```bash
   sqlite3 bsp_project/database/project.db
   SELECT * FROM GapReport;
   ```

**Is it a real problem?**
- Small differences (< £1) are often rounding errors — acceptably include in test data
- Large differences (> £1) — debug your config
- Completely missing transactions — config needs fixing

**Next steps:** 
- If acceptable: Include in test data with `"expected_result": "REVIEW"`
- If problematic: Debug your config (see next section)

---

### FAILURE ❌

**What it means:**
- Parser couldn't extract critical data (dates, balances, account number)
- No usable output
- Configuration needs debugging

**What to check:**
1. **Look at the terminal error message** — it should say what's missing
2. **Check debug logs:**
   ```bash
   cat bsp_project/log/debug/bad_YOUR_PDF_NAME/debug.json
   ```
3. **Query the database for partial results:**
   ```bash
   sqlite3 bsp_project/database/project.db
   SELECT * FROM batch_lines WHERE STD_SUCCESS = 0;
   ```

**Next steps:** Debug your config (see below).

---

## Debugging Extraction

If extraction fails, here's how to diagnose and fix it.

### Step 1: Verify Table Detection

Did the parser find the transaction table on the page?

**Visual inspection:**
1. Open your PDF in Preview (macOS) or Adobe Reader
2. Look at the transaction table
3. Count how many columns it has
4. Note the approximate position (top-left, bottom-right of table)

**Check your config:**
- `statement_tables.toml`: Does `table_columns` match? (e.g., `table_columns = 6` for 6-column table)
- `locations.vertical_lines`: Are there enough values? (Should be `2 * table_columns` entries)

**Debug tip:**
If no tables are detected, the `locations` bounding box is probably wrong. Try adjusting `{page_number = 1, top_left = [50, 100], bottom_right = [500, 600]}` to encompass the entire transaction table area.

---

### Step 2: Verify Column Dividers (Vertical Lines)

Are column boundaries correct?

**Measure in PDF viewer:**
1. Open your PDF
2. Use Measure tool (Preview, Adobe Reader, etc.)
3. Find where each column starts and ends (x-coordinates in points)
4. Note the pixel positions

**Example:** 6-column table might have dividers at:
```toml
vertical_lines = [50, 100, 100, 150, 150, 250, 250, 350, 350, 450, 450, 555]
```

Meaning:
- Column 1: 50–100
- Column 2: 100–150
- Column 3: 150–250
- Column 4: 250–350
- Column 5: 350–450
- Column 6: 450–555

**Fix if needed:**
- Adjust coordinates in `statement_tables.toml`
- Re-run parser
- Verify extraction improves

---

### Step 3: Verify Field Patterns (Regex)

Are field patterns matching the actual text?

**Check the field definition:**
```toml
{field = 'date', column = 0, vital=false, type = "string", string_pattern ='^[0-3][0-9]\s?[A-Z][a-z]{2}\s?[0-3][0-9]$'}
```

This pattern matches dates like: `11 Jan 11`, `1Jan11`, `01 Jan 01`, etc.

**Verify the pattern:**
1. Look at your PDF
2. Find a date in that column
3. Does it match the pattern? Try on [regex101.com](https://regex101.com)
4. If not, adjust the pattern

**Common pattern issues:**
- **Single-digit days:** Pattern `[0-3][0-9]` requires 2 characters. Use `[0-3]?[0-9]` for optional leading zero.
- **Month abbreviations:** Pattern `[A-Z][a-z]{2}` assumes 3-letter month (`Jan`, `Feb`). Check your PDF.
- **Spaces:** Pattern assumes spaces. Check if your PDF uses tabs or no space: adjust `\s` or `\s?`

**Fix example:**
```toml
# OLD: requires 2-digit day
string_pattern ='^[0-3][0-9]\s[A-Z][a-z]{2}\s[0-3][0-9]$'

# NEW: allows single-digit day
string_pattern ='^[0-3]?[0-9]\s[A-Z][a-z]{2}\s[0-3]?[0-9]$'
```

---

### Step 4: Verify Balance/Amount Extraction

Are opening/closing balances and amounts extracted correctly?

**Check in database:**
```bash
sqlite3 bsp_project/database/project.db
SELECT opening_balance, payments_in, payments_out, closing_balance FROM DimStatement LIMIT 1;
```

**Compare to PDF:**
- Do values match what's printed on the statement?
- Are they the right sign? (positive vs. negative)
- Are decimals correct?

**Common issues:**
- **Wrong column:** Amount is in column 3, but config says column 2
- **Wrong type:** Config says `"currency"`, but field is text (need to parse it)
- **Sign flip:** Debits should be negative, credits positive (or vice versa depending on config)

**Fix:**
- Check `statement_tables.toml` — column assignment
- Check `statement_types.toml` or `statement_tables.toml` — is debit marked as negative?
- Verify regex pattern for currency values (if applicable)

---

### Step 5: Check Entire Config Structure

Review all TOML files:

1. **companies.toml** — Company identification correct?
2. **statement_types.toml** — Statement type matching correctly?
3. **statement_tables.toml** — Column count and field extraction correct?
4. **accounts.toml** — Account type matching correctly?
5. **bank_config.toml** — Overall structure correct?
6. **standard_fields.toml** — All 13 standard fields mapped?

See [Contributing: Adding a New Bank](./contributing-new-bank.md) for full TOML reference.

---

## Common Issues & Fixes

| Issue | Likely Cause | Fix |
|-------|--------------|-----|
| "Unable to identify company" | Companies.toml not matching PDF | Adjust location/pattern in companies.toml |
| No tables detected | Table location bounding box wrong | Update `locations.top_left` / `bottom_right` in statement_tables.toml |
| Wrong transaction dates | Date column not extracted or pattern wrong | Fix column assignment or date regex pattern |
| Wrong balances | Balances in wrong columns or wrong sign | Check balance column assignments and sign logic |
| Checks-and-balances fail | Missing transaction or sign error | Verify all transactions extracted; check debit/credit logic |
| Only some transactions extracted | Table boundary cuts off rows | Adjust `top_left` / `bottom_right` to include full table |

---

## Verifying Before Submission

Use this checklist before you're ready to submit:

- [ ] `bsp process` runs without errors
- [ ] Output shows SUCCESS (or expected REVIEW with explanation)
- [ ] Checks-and-balances pass (if expecting SUCCESS)
- [ ] All transactions appear in `export/transactions.csv`
- [ ] Balances match the PDF (or documented difference for REVIEW)
- [ ] 3+ different PDFs tested (ideally different dates/amounts)
- [ ] Each PDF has a matching `.json` metadata file
- [ ] PDFs are anonymised (no account numbers, names, sort codes in content or filenames)
- [ ] Filenames follow pattern: `anonymised_{BANK}_{TYPE}_{DATE}.pdf`

---

## SQL Queries for Verification

**Get statement summary:**
```sql
SELECT account, statement_date, opening_balance, closing_balance, 
       payments_in, payments_out FROM DimStatement;
```

**Get transaction count:**
```sql
SELECT COUNT(*) AS transaction_count FROM statement_lines;
```

**Get all transactions for a statement:**
```sql
SELECT transaction_date, transaction_desc, payments_in, payments_out 
FROM statement_lines 
WHERE ID_STATEMENT = 'YOUR_STATEMENT_ID'
ORDER BY transaction_number;
```

**Check checks-and-balances:**
```sql
SELECT * FROM checks_and_balances;
```

**Find failed statements:**
```sql
SELECT * FROM batch_lines WHERE STD_SUCCESS = 0;
```

---

## Next Steps

1. **Debug using the steps above** until your config extracts data correctly
2. **Prepare 3+ anonymised PDFs** with this working config
3. **Create metadata JSON files** for each PDF (see [Test Data Submission Guide](./test-data-submission.md))
4. **Submit PR** with your config files and note about available test data

---

## Questions?

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for help resources and how to ask for assistance.
