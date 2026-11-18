<!-- markdownlint-disable MD013 -->

# Field Transformations with Expressions

The expression evaluator lets you transform extracted PDF fields using a
declarative mini-language inside `standard_fields.toml`. Instead of writing
Python code, you write an `expression` string directly in the `std_refs` entry.

Use expressions when a plain `field` reference is not enough — for example,
when you need to combine fields, normalise casing, extract a substring, or
apply conditional logic based on another field's value. For simple pass-through
mappings, stick with `field`.

The [Santander STD_TRANSACTION_DATE example](#complex-example-santander-uk-current-account)
at the end of this guide shows a real production expression that combines
`CONCAT`, `SPLIT`, `MONTH_NAME`, `MONTH`, `YEAR`, `CASE`, and `CAST` to infer
a transaction year from statement metadata.

---

## Expression Basics

An expression is a single TOML string assigned to the `expression` key of a
`std_refs` entry:

```toml
{statement_type="My Bank", expression="UPPER(TRIM(description))"}
```

### Literals and column references

- **String literals** — single-quoted: `'hello'`, `'Jan'`
- **Numeric literals** — bare integers or floats: `1`, `3`, `1.5`
- **Column references** — bare identifiers matching column names in the
  extraction context: `date`, `amount`, `STD_STATEMENT_DATE`

### Operators

| Category | Symbols / Keywords |
|---|---|
| Arithmetic | `+` `-` `*` `/` |
| Comparison | `==` `!=` `<` `>` `<=` `>=` |
| Logical | `AND` `OR` `NOT` |
| Membership | `IN` (used in `CASE/WHEN` conditions) |
| Array index | `[0]` `[-1]` (applied to `SPLIT` results) |

### CASE / WHEN / THEN / ELSE / END

```
CASE
  WHEN condition THEN result
  WHEN condition THEN result
  ELSE default_result
END
```

The first matching `WHEN` branch is returned. `ELSE` is optional (defaults to
NULL if omitted and no branch matches).

### Data types

Expressions work with four types:

| Type | Description |
|---|---|
| **String** | Text; operations include `CONCAT`, `SUBSTRING`, `UPPER`, etc. |
| **Int64** | Integer; returned by `YEAR`, `MONTH`, `DAY`, `LENGTH` |
| **Float64** | Decimal; used in arithmetic and `CAST(... AS Float64)` |
| **Date** | Calendar date; accepted by `YEAR`, `MONTH`, `DAY`, `MONTH_NAME` |
| **Array** | List of strings; produced by `SPLIT`, consumed by `[index]` |

---

## Function Reference

### String functions

#### `CONCAT(str, str, ...)`

Concatenates one or more values as strings. Each argument is cast to String
before joining; NULL arguments are treated as empty strings.

```
CONCAT('Hello', ' ', 'World')  →  'Hello World'
CONCAT(day, ' ', month)        →  '20 Sep'
```

---

#### `SUBSTRING(str, start, length)`

Extracts a substring. `start` is **0-indexed**.

```
SUBSTRING('hello', 1, 3)   →  'ell'
SUBSTRING(ref, 0, 2)       →  first two characters of ref
```

---

#### `UPPER(str)`

Converts to uppercase.

```
UPPER('hello')  →  'HELLO'
```

---

#### `LOWER(str)`

Converts to lowercase.

```
LOWER('HELLO')  →  'hello'
```

---

#### `TRIM(str)`

Removes leading and trailing whitespace.

```
TRIM('  hello  ')  →  'hello'
```

---

#### `LTRIM(str)`

Removes leading whitespace only.

```
LTRIM('  hello  ')  →  'hello  '
```

---

#### `RTRIM(str)`

Removes trailing whitespace only.

```
RTRIM('  hello  ')  →  '  hello'
```

---

#### `LENGTH(str)`

Returns the number of characters.

```
LENGTH('hello')    →  5
LENGTH(date_str)   →  varies per row
```

---

#### `REPLACE(str, old, new)`

Replaces every occurrence of `old` with `new`.

```
REPLACE('1,234.56', ',', '')   →  '1234.56'
REPLACE('hello', 'l', 'L')    →  'heLLo'
```

---

#### `SPLIT(str, delimiter)`

Splits a string on a delimiter and returns an array. Access elements with
`[index]` (0-based; `-1` is the last element).

```
SPLIT('a-b-c', '-')      →  ['a', 'b', 'c']
SPLIT('a-b-c', '-')[0]   →  'a'
SPLIT('a-b-c', '-')[-1]  →  'c'
SPLIT('20 Sep', ' ')[1]  →  'Sep'
```

**Common use case:** Extracting the month abbreviation from a `"DD MMM"` field
where the day may be one or two digits. Using `SPLIT(date, ' ')[1]` is more
robust than `SUBSTRING` with a position calculation because it does not depend
on knowing the day width in advance.

---

### Date functions

All date functions accept either a `Date`-typed column **or** an ISO-format
string column (`'YYYY-MM-DD'`).

#### `YEAR(date_or_str)`

Returns the year as Int64.

```
YEAR('2024-09-20')     →  2024
YEAR(STD_STATEMENT_DATE)  →  2026
```

---

#### `MONTH(date_or_str)`

Returns the month as Int64 (1–12).

```
MONTH('2024-09-20')       →  9
MONTH(STD_STATEMENT_DATE) →  1
```

---

#### `DAY(date_or_str)`

Returns the day of month as Int64.

```
DAY('2024-09-20')  →  20
```

---

#### `MONTH_NAME(int_or_str)`

Returns the 3-letter English month abbreviation, always title-case. Accepts an
integer (1–12), a 3-letter abbreviation, or a full month name.

```
MONTH_NAME(1)           →  'Jan'
MONTH_NAME(12)          →  'Dec'
MONTH_NAME('Sep')       →  'Sep'   (passthrough)
MONTH_NAME('September') →  'Sep'
```

**Common use case:** Normalising a month string extracted from a PDF so it can
be reliably compared in a `CASE/WHEN ... IN (...)` condition.

---

#### `CAST(value AS type)`

Explicitly converts a value to a type. Valid types: `String`, `Int64`,
`Float64`, `Date`.

```
CAST(YEAR(STD_STATEMENT_DATE) AS String)   →  '2026'
CAST('1234.56' AS Float64)                 →  1234.56
CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String) →  '2025'
```

**Important:** `YEAR()` returns an Int64. When you need to include the year in a
`CONCAT`, you must `CAST(... AS String)` first; otherwise Polars raises a schema
error because `CONCAT` expects all arguments to be of type String.

---

### Utility

#### `COALESCE(value1, value2, ...)`

Returns the first non-NULL argument.

```
COALESCE(alt_date, primary_date, '1970-01-01')
```

---

#### `IN` (membership test)

Used inside `CASE/WHEN` conditions to check whether a value belongs to a list
of literals.

```
MONTH_NAME(SPLIT(date, ' ')[1]) IN ('Oct', 'Nov', 'Dec')
MONTH(STD_STATEMENT_DATE) IN (1, 2, 3)
```

---

## Type System and Coercion

The evaluator performs automatic type coercion in arithmetic and comparison
contexts, following this chain:

1. Try native types (e.g. both operands already numeric).
2. Cast String to `Float64`, then `Int64` if applicable.
3. Fall back to NULL if conversion fails (logged at INFO level).

Use `CAST` for explicit, predictable conversion rather than relying on
automatic coercion.

**Config-load-time errors:** `ConfigError` is raised immediately (before any
PDF is processed) if the expression contains a syntax error, an unknown
function name, or a reference to a column that is not present in the extraction
schema.

**Runtime errors:** A `ValueError` is raised with the full expression text in
the error message, making it straightforward to identify which configuration
entry caused the problem.

---

## Null Handling Strategies

Add `null_strategy` and (when needed) `null_default_value` alongside
`expression` in the `std_refs` entry.

### `return_null` (default)

NULL propagates through the expression. Any operation involving NULL typically
produces NULL, which flows into the output column.

```toml
{statement_type="My Bank", expression="UPPER(TRIM(description))"}
```

Use when a missing field should leave the output column NULL (downstream
checks & balances validation will flag incomplete rows).

---

### `default_value`

NULL is replaced with the string in `null_default_value` before evaluation.
The fallback is cast to the expected output type at runtime.

```toml
{statement_type="My Bank", expression="UPPER(TRIM(description))",
 null_strategy="default_value", null_default_value="UNKNOWN"}
```

Use when a sensible fallback exists (e.g. `"0"` for a missing amount,
`"1 Jan 2000"` as a sentinel date). Be aware that defaulting numeric fields
to `0` can make checks & balances appear to pass even when data is missing.

---

### `skip_row`

The row is excluded from the output after all expressions are evaluated.
This happens *post-loop* (not mid-loop) so that statement-level totals used by
checks & balances are not affected.

```toml
{statement_type="My Bank", expression="UPPER(TRIM(description))",
 null_strategy="skip_row"}
```

Use when the field is so critical that a row without it is meaningless and
should be silently dropped rather than flagged.

---

## Practical Examples

### Normalise a description field

```toml
{statement_type="My Bank", expression="UPPER(TRIM(description))"}
```

Strips whitespace and upper-cases the description — useful before merchant
categorisation or duplicate detection.

---

### Build a date from separate day, month, year columns

```toml
{statement_type="My Bank",
 expression="CONCAT(day, ' ', MONTH_NAME(month), ' ', CAST(year AS String))",
 format="%d %b %Y"}
```

Combines three separate extracted fields into a parseable date string.

---

### Strip a currency symbol from an amount field

```toml
{statement_type="My Bank",
 expression="CAST(REPLACE(TRIM(SUBSTRING(amount, 1, LENGTH(amount))), ',', '') AS Float64)",
 null_strategy="default_value", null_default_value="0"}
```

Removes the first character (the currency symbol), trims spaces, strips
thousands separators, then casts to Float64.

---

### Handle two date formats in the same bank config

```toml
{statement_type="My Bank",
 expression="CASE WHEN LENGTH(date) > 10 THEN SUBSTRING(date, 0, 10) ELSE date END",
 format="%Y-%m-%d"}
```

Detects whether a field looks like a full ISO timestamp and truncates it to
the date portion. Useful when a bank has changed its PDF format over time.

---

## Troubleshooting

### `polars.exceptions.SchemaError: type String is incompatible with expected type Date`

You are passing a non-String value to `CONCAT`. The most common cause is
`YEAR()` returning an Int64:

```
# Wrong:
CONCAT(date, ' ', YEAR(STD_STATEMENT_DATE))

# Correct:
CONCAT(date, ' ', CAST(YEAR(STD_STATEMENT_DATE) AS String))
```

---

### `ConfigError: column 'xyz' not found in schema`

The column name in the expression does not match any column in the extraction
context. Check:

- The field name in `statement_tables.toml` (raw extracted column names).
- That the column you need is in the same section (`header` or `lines`).
- For header-section values referenced in a lines expression, that the field
  is listed in `header_scalars` (see [new-bank-config.md](new-bank-config.md)).

---

### SPLIT index out of range returns NULL

`SPLIT(str, delimiter)[n]` returns NULL if the index is out of range (not an
error). If you depend on a specific element being present, consider using
`null_strategy="default_value"` or `null_strategy="skip_row"` to handle the
case where the split does not produce enough elements.

---

## Complex Example: Santander UK Current Account

### Problem

Santander PDFs contain transaction rows where the `date` field holds only the
day and month — for example `"20 Sep"` or `"5 Dec"` — with no year. The year
must be inferred from the statement date (a header field).

The complication: Santander sometimes groups December, November, and October
transactions onto a January, February, or March statement of the following
year. A transaction dated `"20 Dec"` on a statement dated `2026-01-15` actually
occurred on `20 Dec 2025`, not `20 Dec 2026`.

**Rule:** If the statement month is in January–March **AND** the transaction
month is in October–December, subtract 1 from the statement year. Otherwise
use the statement year as-is.

---

### Why SPLIT, not SUBSTRING

The `date` field is `"DD MMM"` or `"D MMM"` — the day can be one or two
digits, so the month abbreviation starts at a variable position. Using
`SUBSTRING` would require a fragile position calculation.

`SPLIT(date, ' ')[1]` is simpler and correct for all day widths:

```
SPLIT('20 Dec', ' ')[1]  →  'Dec'
SPLIT('5 Nov', ' ')[1]   →  'Nov'
```

---

### Expression

```
CONCAT(
  date, ' ',
  CASE
    WHEN MONTH(STD_STATEMENT_DATE) <= 3
     AND MONTH_NAME(SPLIT(date, ' ')[1]) IN ('Oct', 'Nov', 'Dec')
    THEN CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String)
    ELSE CAST(YEAR(STD_STATEMENT_DATE) AS String)
  END
)
```

**Step by step:**

1. `SPLIT(date, ' ')[1]` — extract the month abbreviation from the `date` field
   regardless of whether the day is one or two digits.
2. `MONTH_NAME(...)` — normalise the abbreviation to title-case (`'Dec'`, `'Nov'`,
   `'Oct'`) so the `IN` test is reliable.
3. `MONTH(STD_STATEMENT_DATE) <= 3` — check whether the statement falls in Q1
   (January, February, or March).
4. `CASE WHEN ... AND ...` — both conditions must be true to trigger the
   year-decrement path.
5. `CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String)` — decrement the year and cast
   to String for `CONCAT`.
6. `ELSE CAST(YEAR(STD_STATEMENT_DATE) AS String)` — otherwise use the statement
   year unchanged.
7. `CONCAT(date, ' ', ...)` — combine the original `date` string with a space
   and the computed year to produce e.g. `"20 Dec 2025"`.

---

### TOML configuration

In `project/config/import/standard_fields.toml`, under `[STD_TRANSACTION_DATE]`:

```toml
{statement_type="Santander UK Current Account",
 expression="CONCAT(date,' ',CASE WHEN MONTH(STD_STATEMENT_DATE) <= 3 AND MONTH_NAME(SPLIT(date, ' ')[1]) IN ('Oct', 'Nov', 'Dec') THEN CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String) ELSE CAST(YEAR(STD_STATEMENT_DATE) AS String) END)",
 format="%d %b %Y"}
```

`STD_STATEMENT_DATE` is a header field. For it to be available inside a lines
expression, it must be passed as a header scalar by the calling code — see
`new-bank-config.md` for how `header_scalars` works.

The `format` string `"%d %b %Y"` is used to parse the constructed string
(e.g. `"20 Dec 2025"` or `"5 Nov 2025"`) back into a Polars `Date`. Note
that `%d` accepts both `"5"` and `"20"` as valid day values — there is no
zero-padding requirement.

---

### Verifying with a pytest test

The following test covers all six boundary conditions:

```python
import polars as pl
from datetime import date
from bank_statement_parser.modules.data import StandardFields, StdRefs
from bank_statement_parser.modules.statement_functions import get_standard_fields

EXPR = (
    "CONCAT(date,' ',"
    "CASE WHEN MONTH(STD_STATEMENT_DATE) <= 3"
    " AND MONTH_NAME(SPLIT(date, ' ')[1]) IN ('Oct', 'Nov', 'Dec')"
    " THEN CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String)"
    " ELSE CAST(YEAR(STD_STATEMENT_DATE) AS String)"
    " END)"
)


def test_santander_date_year_inference():
    """Six cases covering all month-boundary conditions."""
    ref = StdRefs(
        statement_type="Santander UK Current Account",
        field=None,
        concat_fields=None,
        format="%d %b %Y",
        default=None,
        expression=EXPR,
        null_strategy="default_value",
        null_default_value="1 Jan 2000",
    )
    std_fields = {
        "STD_TRANSACTION_DATE": StandardFields(
            section="lines", type="string", vital=False, std_refs=[ref]
        )
    }
    df = pl.DataFrame(
        {
            "date": ["20 Dec", "5 Nov", "15 Oct", "20 Sep", "15 Dec", "1 Mar"],
            "STD_STATEMENT_DATE": [
                date(2026, 1, 15),   # Q4 txn on Q1 stmt → 2025
                date(2026, 2, 1),    # Q4 txn on Q1 stmt → 2025
                date(2026, 3, 10),   # Q4 txn on Q1 stmt → 2025
                date(2026, 1, 15),   # non-Q4 txn on Q1 stmt → 2026
                date(2025, 12, 20),  # Q4 txn on Q4 stmt → 2025
                date(2026, 4, 10),   # non-Q4 txn on Q2 stmt → 2026
            ],
        }
    ).with_columns(
        pl.col("STD_STATEMENT_DATE").cast(pl.Date),
        pl.lit(1).alias("transaction_number"),
        pl.lit(1).alias("page"),
        pl.lit(0).cast(pl.Decimal(scale=4)).alias("STD_TRANSACTION_PAYMENTS_IN"),
        pl.lit(0).cast(pl.Decimal(scale=4)).alias("STD_TRANSACTION_PAYMENTS_OUT"),
    )
    checks = pl.DataFrame({"STD_OPENING_BALANCE": pl.Series([0.0], dtype=pl.Float64)})
    result = get_standard_fields(
        df, "lines", std_fields, "Santander UK Current Account", checks
    )
    dates = result["STD_TRANSACTION_DATE"].to_list()
    assert dates == [
        "20 Dec 2025",  # Q4 on Q1 → prior year
        "5 Nov 2025",   # Q4 on Q1 → prior year
        "15 Oct 2025",  # Q4 on Q1 → prior year
        "20 Sep 2026",  # Sep not in Q4 → same year
        "15 Dec 2025",  # Dec on Dec stmt (month > 3) → same year
        "1 Mar 2026",   # Mar not in Q4 → same year
    ]
```

Run with:

```bash
pytest tests/test_expression_evaluator.py::TestGetStandardFieldsExpression::test_santander_date_year_inference -v
```

---

## Further Reading

- **[Adding a New Bank](new-bank-config.md)** — full TOML configuration
  walkthrough; the `std_refs` entry options (including `expression`) are
  documented in the [StdRefs reference](new-bank-config.md#stdRefs) section.
- **`expression_evaluator.py`** — source for the Lexer, Parser, AST, and
  Evaluator; relevant for contributors wanting to extend the function registry.
