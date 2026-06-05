<!-- markdownlint-disable MD003 MD007 MD013 -->

# Contributing: Adding a New Bank

This guide walks you through configuring Bank Statement Parser to parse statements from a new bank. **No Python code required** — it's all TOML configuration.

## Is This For You?

This guide covers three distinct scenarios:

- **Scenario A:** Adding support for an entirely new bank (never seen before)
- **Scenario B:** Adding a new account type to an existing bank (HSBC current accounts exist, now add HSBC savings)
- **Scenario C:** Improving or fixing an existing bank's configuration

**Choose your scenario below,** or continue reading for the complete walkthrough.

---

## Scenario A: Adding an Entirely New Bank

**Effort:** 2–4 hours (medium–high difficulty)

**Requirements:**
- Understanding of your bank's PDF layout (columns, table positions)
- 3+ anonymised test PDFs (different statement dates)
- TOML configuration (4 new files, ~100 lines each)

**What you'll create:**
- `project/config/import/NEWBANK_UK/companies.toml` — Bank identification
- `project/config/import/NEWBANK_UK/accounts.toml` — Account definitions
- `project/config/import/NEWBANK_UK/statement_types.toml` — Statement layout
- `project/config/import/NEWBANK_UK/statement_tables.toml` — Transaction table extraction

**Next steps:**
1. Read the [Complete Reference](#complete-reference) section (auto-generated technical guide)
2. Test locally with your PDFs (see [Local Testing Guide](./local-testing.md))
3. Prepare 3+ anonymised test PDFs + metadata JSON (see [Test Data Submission Guide](./test-data-submission.md))
4. Submit PR with config files and note about test data

---

## Scenario B: Adding a New Account Type to Existing Bank

**Effort:** 1–2 hours (medium difficulty)

**Requirements:**
- Bank already supported (e.g., HSBC current accounts work)
- New account type (e.g., HSBC savings accounts)
- 3+ anonymised test PDFs for the **new account type**
- Modify existing TOML files (don't create new bank folder)

**What you'll modify:**
- `project/config/import/HSBC_UK/accounts.toml` — Add new account definition
- `project/config/import/HSBC_UK/statement_tables.toml` — Add new table extraction rules (if layout differs)
- `project/config/import/HSBC_UK/statement_types.toml` — Add new statement type (if needed)

**Next steps:**
1. Read the [Complete Reference](#complete-reference) section
2. Compare your new account type's PDF to the existing one (are columns in same place?)
3. Modify only the files that differ
4. Test locally (see [Local Testing Guide](./local-testing.md))
5. Prepare test data and submit PR

---

## Scenario C: Improving or Fixing Existing Config

**Effort:** 30 mins–1 hour (low–medium difficulty)

**Requirements:**
- Bank already configured
- Fix or enhancement (better extraction, missing fields, etc.)
- If config changes: 3+ new anonymised PDFs + metadata JSON
- If small fix (typo, comment): test locally to verify

**What you'll do:**
1. Identify the problem (via issue or your own discovery)
2. Fix the TOML in one or more files
3. Test locally to verify fix works (see [Local Testing Guide](./local-testing.md))
4. If config changed meaningfully: prepare test data
5. Submit PR with explanation

---

## Worked Example: Adding HSBC Credit Card Config

Let's walk through a real (anonymised) example of what TOML files look like.

### File 1: companies.toml (Bank Identification)

This tells the parser how to identify that a PDF is from HSBC.

```toml
[HSBC_UK]
company = 'HSBC Bank UK'

[HSBC_UK.config]
config = 'Company Info'
locations = [
    {page_number = 1, top_left = [475, 110], bottom_right = [575, 130]},
]
fields = [
    {
        field = 'website',
        vital = true,
        type = "string",
        string_pattern = '^www\.hsbc\.co\.uk$'
    },
]
```

**What this does:**
- Looks for "www.hsbc.co.uk" on page 1 between pixels (475,110) and (575,130)
- If found, this is an HSBC PDF

### File 2: accounts.toml (Account Definitions)

This tells the parser what account types HSBC offers, and how to identify them.

```toml
[HSBC_UK_CRD]
account_type = "CRD"  # Credit Card
company = 'HSBC_UK'

[HSBC_UK_CRD.config]
config = 'Account Identification'
account = 'Rewards Credit Card'
locations = [
    {page_number = 1, top_left = [50, 150], bottom_right = [300, 180]},
]
fields = [
    {field = 'account', vital = true, type = "string", string_pattern = 'Rewards.*Card'},
]

[HSBC_UK_SAV]
account_type = "SAV"  # Savings Account
company = 'HSBC_UK'

[HSBC_UK_SAV.config]
config = 'Account Identification'
account = 'Easy Saver'
locations = [
    {page_number = 1, top_left = [50, 150], bottom_right = [300, 180]},
]
fields = [
    {field = 'account', vital = true, type = "string", string_pattern = 'Easy.*Saver'},
]
```

**What this does:**
- First account type: If PDF says "Rewards ... Card", it's a Credit Card
- Second account type: If PDF says "Easy ... Saver", it's a Savings Account

### File 3: statement_types.toml (Statement Layout)

This tells the parser where to find statement-level data (opening balance, dates, etc.) and per-page transaction lines.

```toml
[HSBC_UK_CRD_STATEMENT]
account_ref = 'HSBC_UK_CRD'
statement_type = 'Standard'

[HSBC_UK_CRD_STATEMENT.header]
config = 'Header'
table_type = "statement_header"
locations = [
    {page_number = 1, top_left = [50, 100], bottom_right = [500, 300]},
]
fields = [
    {field = 'statement_date', column = 2, vital = true, type = "string", string_pattern = '^[0-3][0-9]\s[A-Z][a-z]{2}\s[0-9]{4}$'},
    {field = 'opening_balance', column = 3, vital = true, type = "currency"},
    {field = 'payments_in', column = 4, vital = true, type = "currency"},
]

[HSBC_UK_CRD_STATEMENT.lines]
config = 'Transaction Lines'
statement_table = 'HSBC_UK_CRD_TRANSACTIONS'
page_detection = {column = 0, value = "From beginning of statement"}
```

**What this does:**
- Header section: Extracts statement date and opening balance from page 1
- Lines section: Extracts transaction rows (defined separately in statement_tables.toml)

### File 4: statement_tables.toml (Transaction Table Extraction)

This tells the parser where the transaction table is and how to extract each column.

```toml
[HSBC_UK_CRD_TRANSACTIONS]
type = "transaction"
statement_table = 'Transactions'
table_columns = 6

[HSBC_UK_CRD_TRANSACTIONS.locations]
vertical_lines = [50, 100, 100, 150, 150, 250, 250, 350, 350, 450, 450, 555]

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = 'date'
column = 0
vital = false
type = "string"
string_pattern = '^[0-3][0-9]\s[A-Z][a-z]{2}$'

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = 'details'
column = 2
vital = true
type = "string"

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = '£_paid_in'
column = 4
vital = false
type = "currency"

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = '£_paid_out'
column = 5
vital = false
type = "currency"
```

**What this does:**
- Defines a 6-column transaction table
- Vertical dividers at pixels: 50–100 (col 1), 100–150 (col 2), etc.
- Column 0: Transaction date (regex pattern matches "11 Jan" format)
- Column 2: Transaction details (merchant name, etc.)
- Column 4: Credits (paid in)
- Column 5: Debits (paid out)

---

## Finding Column Coordinates

The trickiest part of TOML config is measuring column coordinates. Here's how:

**1. Get a PDF measurement tool:**
- macOS: Preview (built-in) has a Measure tool
- Windows: Adobe Reader (free) or PDF tool
- Online: Try [pdfmeasure.com](https://pdfmeasure.com) (browser-based)

**2. Open your bank's PDF**

**3. Find the transaction table and measure:**
- Where does the first column start? (left edge of table)
- Where does each column divider appear? (vertical lines between columns)
- Where does the last column end? (right edge of table)

**Example:**
```
| Column 1      | Column 2    | Column 3      |
|---------|---------|---------|
50px     100px    150px     250px
```

Becomes in TOML:
```toml
vertical_lines = [50, 100, 100, 150, 150, 250]
```

**💡 Tip:** Values are in points (PDF units), roughly equivalent to 1/72 inch. An 8.5"×11" page is about 612×792 points.

---

## Complete Reference

Below is the complete technical reference (auto-generated from code). It covers all TOML fields, dataclasses, and configuration options.

---

<!-- START OF AUTO-GENERATED REFERENCE -->
<!-- This content is generated by scripts/generate_docs.py -->
<!-- To regenerate, run: python scripts/generate_docs.py -->

# Adding a New Bank — Technical Reference

This section contains the complete TOML configuration reference for adding bank support. Most users should read the [Worked Example](#worked-example-adding-hsbc-credit-card-config) section first.

## Overview

Adding support for a new bank involves creating and editing several TOML files that describe how to identify the bank's PDFs, locate tables on each page, extract field values, and map them to standard output columns.

The configuration lives in two places:

| Location | Purpose |
| --- | --- |
| `project/config/import/<BANK_COUNTRY>/` | Bank-specific config folder (4 TOML files) |
| `project/config/import/account_types.toml` | Shared account type registry |
| `project/config/import/standard_fields.toml` | Shared standard field mappings |

### Bank config folder structure

Each bank has its own subfolder named in `SCREAMING_SNAKE_CASE` (e.g. `HSBC_UK`, `TSB_UK`). A complete folder contains exactly four files:

| File | Purpose | Key Dataclass |
| --- | --- | --- |
| `companies.toml` | Bank identification (name + PDF detection rule) | [`Company`](#company) |
| `accounts.toml` | Account definitions (one per product/card type) | [`Account`](#account) |
| `statement_types.toml` | Statement layout definitions (header + lines extraction) | [`StatementType`](#statementtype) |
| `statement_tables.toml` | Physical table extraction rules (locations, fields, bookends) | [`StatementTable`](#statementtable) |

### Processing pipeline

Understanding the processing order helps when writing config:

1. **Company identification** — the `Company.config` extraction is run against page 1 to determine which bank issued the PDF.
2. **Account identification** — each `Account.config` is tried until one matches, identifying the specific account product.
3. **Header extraction** — the `StatementType.header` configs run to extract statement-level metadata (dates, balances, account details).
4. **Lines extraction** — the `StatementType.lines` configs run per-page to extract transaction rows.
5. **Standard field mapping** — raw extracted fields are mapped to `STD_*` output columns via `standard_fields.toml`.
6. **Checks & balances** — opening balance + payments in - payments out = closing balance is validated.

---

## Step 1: Register the Account Type

If your bank uses an account type not already in `account_types.toml`, add a new entry. Most banks will use the existing types (`CRD`, `CUR`, `SAV`, `ISA`).

**File:** `project/config/import/account_types.toml`

```toml
[CRD]
account_type = "Credit Card"

[CUR]
account_type = "Current Account"

[SAV]
account_type = "Savings Account"

[ISA]
account_type = "ISA"
```

### `AccountType`

Simple lookup label for an account type category.

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `account_type` | `str` | ACTIVE | Account type label (e.g. "CRD" for credit card, "CUR" for current account).  Populated at load time but not subsequently consumed by the pipeline; present for potential reporting or routing use. |

---

## Step 2: Create the Bank Config Folder

Create a new subfolder under `project/config/import/` using the naming convention `<BANK>_<COUNTRY>` in SCREAMING_SNAKE_CASE:

```
project/config/import/
  HSBC_UK/          # existing
  TSB_UK/           # existing
  NEWBANK_UK/       # <- your new folder
    companies.toml
    accounts.toml
    statement_types.toml
    statement_tables.toml
```

---

## Step 3: companies.toml — Bank Identification

Tells the parser how to identify a PDF as belonging to this bank.

**File:** `project/config/import/<BANK_COUNTRY>/companies.toml`

```toml
[HSBC_UK]
company = 'HSBC Bank UK'

[HSBC_UK.config]
config = 'Company Info'
locations = [
    {page_number = 1, top_left = [475, 110], bottom_right = [575, 130]},
]
fields = [
    {field = 'website', vital = true, type = "string", string_pattern = '^www\.hsbc\.co\.uk$'},
]
```

### `Company`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `company` | `str` | ACTIVE | Display name of the bank (e.g., 'HSBC Bank UK') |
| `config` | `Config` | ACTIVE | Extraction rule to identify the bank from page 1 |

### `Config` (for Company Identification)

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `config` | `str` | ACTIVE | Always set to `"Company Info"` for identification rules |
| `locations` | `list[PDFLocation]` | ACTIVE | List of page regions to search for the bank name |
| `fields` | `list[ConfigField]` | ACTIVE | Extraction rules (usually one: the company website or name) |

### `PDFLocation`

Specifies a rectangular region on a PDF page (in points).

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `page_number` | `int` | ACTIVE | Page number (1-indexed; 1 = first page) |
| `top_left` | `list[int]` | ACTIVE | [x, y] coordinates of top-left corner in PDF points |
| `bottom_right` | `list[int]` | ACTIVE | [x, y] coordinates of bottom-right corner in PDF points |

### `ConfigField`

Defines a field to extract from a region.

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `field` | `str` | ACTIVE | Field name (e.g., `"website"`, `"account_number"`) |
| `vital` | `bool` | ACTIVE | If `true`, extraction must succeed or the entire identification fails |
| `type` | `str` | ACTIVE | Field type: `"string"`, `"currency"`, `"number"`, `"date"` |
| `string_pattern` | `str` | CONDITIONAL | Regex pattern (required if `type = "string"`). Example: `^www\.hsbc\.co\.uk$` |
| `string_max_length` | `int` | OPTIONAL | Max length of extracted string |

---

## Step 4: accounts.toml — Account Definitions

Defines account types supported by this bank and how to identify them.

**File:** `project/config/import/<BANK_COUNTRY>/accounts.toml`

```toml
[HSBC_UK_CRD]
account_type = "CRD"
company = 'HSBC_UK'

[HSBC_UK_CRD.config]
config = 'Account Identification'
account = 'Rewards Credit Card'
locations = [
    {page_number = 1, top_left = [50, 150], bottom_right = [300, 180]},
]
fields = [
    {field = 'account', vital = true, type = "string", string_pattern = 'Rewards.*Card'},
]
```

### `Account`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `account_type` | `str` | ACTIVE | Reference to account type (e.g., `"CRD"`, `"CUR"`, `"SAV"`, `"ISA"`) |
| `company` | `str` | ACTIVE | Reference to company key (e.g., `"HSBC_UK"`) |
| `config` | `Config` | ACTIVE | Extraction rule to identify this specific account type |

---

## Step 5: statement_types.toml — Statement Layout

Defines how to extract statement-level metadata (header) and per-page transaction rows (lines).

**File:** `project/config/import/<BANK_COUNTRY>/statement_types.toml`

```toml
[HSBC_UK_CRD_STATEMENT]
account_ref = 'HSBC_UK_CRD'
statement_type = 'Standard'

[HSBC_UK_CRD_STATEMENT.header]
config = 'Header'
table_type = "statement_header"
locations = [
    {page_number = 1, top_left = [50, 100], bottom_right = [500, 300]},
]
fields = [
    {field = 'statement_date', column = 2, vital = true, type = "string", string_pattern = '^[0-3][0-9]\s[A-Z][a-z]{2}\s[0-9]{4}$'},
    {field = 'opening_balance', column = 3, vital = true, type = "currency"},
]

[HSBC_UK_CRD_STATEMENT.lines]
config = 'Transaction Lines'
statement_table = 'HSBC_UK_CRD_TRANSACTIONS'
page_detection = {column = 0, value = "From beginning of statement"}
```

### `StatementType`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `account_ref` | `str` | ACTIVE | Reference to account key (e.g., `"HSBC_UK_CRD"`) |
| `statement_type` | `str` | ACTIVE | Type label (usually `"Standard"`) |
| `header` | `TableExtraction` | ACTIVE | Rules for extracting header/metadata |
| `lines` | `TableExtraction` | ACTIVE | Rules for extracting transaction rows |

### `TableExtraction`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `config` | `str` | ACTIVE | Description (`"Header"` or `"Transaction Lines"`) |
| `statement_table` | `str` | CONDITIONAL | Reference to StatementTable (required for `lines`) |
| `table_type` | `str` | OPTIONAL | Table classification (`"statement_header"`, etc.) |
| `locations` | `list[PDFLocation]` | ACTIVE | Page regions to search |
| `fields` | `list[ConfigField]` | ACTIVE | Fields to extract |
| `page_detection` | `dict` | OPTIONAL | How to identify pages containing this table |

---

## Step 6: statement_tables.toml — Transaction Table Extraction

Defines the physical layout of the transaction table (columns, vertical dividers, field mappings).

**File:** `project/config/import/<BANK_COUNTRY>/statement_tables.toml`

```toml
[HSBC_UK_CRD_TRANSACTIONS]
type = "transaction"
statement_table = 'Transactions'
table_columns = 6

[HSBC_UK_CRD_TRANSACTIONS.locations]
vertical_lines = [50, 100, 100, 150, 150, 250, 250, 350, 350, 450, 450, 555]

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = 'date'
column = 0
vital = false
type = "string"
string_pattern = '^[0-3][0-9]\s[A-Z][a-z]{2}$'

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = 'details'
column = 2
vital = true
type = "string"

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = '£_paid_in'
column = 4
vital = false
type = "currency"

[[HSBC_UK_CRD_TRANSACTIONS.fields]]
field = '£_paid_out'
column = 5
vital = false
type = "currency"
```

### `StatementTable`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `type` | `str` | ACTIVE | Table type (`"transaction"` or `"header"`) |
| `statement_table` | `str` | ACTIVE | Display name of the table (e.g., `"Transactions"`, `"Header"`) |
| `table_columns` | `int` | ACTIVE | Number of columns in the table |
| `locations` | `object` | ACTIVE | Bounding box and divider lines |
| `fields` | `list[TableField]` | ACTIVE | Field extraction rules per column |

### `TableField`

| Field | Type | Status | Description |
| --- | --- | --- | --- |
| `field` | `str` | ACTIVE | Field name |
| `column` | `int` | ACTIVE | Column index (0-based) |
| `vital` | `bool` | ACTIVE | If `true`, field must extract successfully |
| `type` | `str` | ACTIVE | Field type (`"string"`, `"currency"`, `"number"`, `"date"`) |
| `string_pattern` | `str` | CONDITIONAL | Regex pattern for string fields |

---

## Step 7: Register Standard Field Mappings

The last step is to map your extracted fields to the standard output columns (`STD_*`).

**File:** `project/config/import/standard_fields.toml`

This file defines how raw extracted fields map to standardized output columns:

```toml
[HSBC_UK_CRD]
# [ACTIVE] Raw extracted field → Standard output column mappings
STD_STATEMENT_DATE = "statement_date"
STD_OPENING_BALANCE = "opening_balance"
STD_CLOSING_BALANCE = "closing_balance"
STD_PAYMENTS_IN = "payments_in"
STD_PAYMENTS_OUT = "payments_out"
STD_TRANSACTION_DATE = "date"
STD_TRANSACTION_DESC = "details"
STD_PAYMENTS_IN_TRANS = "£_paid_in"
STD_PAYMENTS_OUT_TRANS = "£_paid_out"
STD_ACCOUNT = "account"
STD_CURRENCY = "GBP"  # or extract from PDF
STD_COMPANY = "HSBC_UK"
STD_STATEMENT_TYPE = "Standard"
```

---

## Before You Submit

✅ **Checklist:**
- [ ] All 4 TOML files created/modified in bank folder
- [ ] Bank name and account types verified
- [ ] Company identification rule tested
- [ ] Account identification rule tested
- [ ] Header fields extract correctly (dates, balances)
- [ ] Transaction table columns measured and dividers set correctly
- [ ] All transaction fields extract correctly
- [ ] Checks-and-balances pass
- [ ] Standard field mappings registered
- [ ] 3+ anonymised PDFs tested locally
- [ ] Metadata JSON created for each PDF
- [ ] PR description explains bank name and account types

<!-- END OF AUTO-GENERATED REFERENCE -->

---

## Next Steps

1. **Test your config locally** (see [Local Testing Guide](./local-testing.md))
2. **Prepare anonymised PDFs** (3+ different statements)
3. **Create metadata JSON files** (one per PDF) (see [Test Data Submission Guide](./test-data-submission.md))
4. **Submit PR** with config files
5. **Maintainers will request test PDFs** and create private PR to activate tests

---

## Questions?

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for help resources.
