"""Generate the 'Adding a New Bank' documentation from data.py source.

Parses ``src/bank_statement_parser/modules/data.py`` using the ``ast`` and
``tokenize`` modules to extract dataclass docstrings and per-field inline
comments.  Combines these with static prose and real TOML examples from the
shipped ``HSBC_UK`` config folder to produce ``docs/guides/new-bank-config.md``.

Run from the repository root::

    python scripts/generate_docs.py
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (relative to repository root)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DATA_PY = _REPO_ROOT / "src" / "bank_statement_parser" / "modules" / "data.py"
_CONFIG_DIR = _REPO_ROOT / "src" / "bank_statement_parser" / "project" / "config"
_HSBC_DIR = _CONFIG_DIR / "HSBC_UK"
_OUTPUT = _REPO_ROOT / "docs" / "guides" / "new-bank-config.md"


# ---------------------------------------------------------------------------
# Data structures for extracted info
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FieldInfo:
    """One field extracted from a dataclass in data.py."""

    name: str
    type_annotation: str
    status: str  # "ACTIVE" or "STUB"
    description: str  # cleaned-up inline comment text


@dataclass(slots=True)
class ClassInfo:
    """One dataclass extracted from data.py."""

    name: str
    docstring: str
    fields: list[FieldInfo]


# ---------------------------------------------------------------------------
# AST + tokenize extraction
# ---------------------------------------------------------------------------


def _extract_field_comments(source: str) -> dict[tuple[int, str], tuple[str, str]]:
    """Extract inline comments for dataclass fields from source code.

    Uses ``tokenize`` to read the raw comments that ``ast`` discards.  Returns
    a mapping of ``(line_number, field_name)`` -> ``(status, description)``
    where *status* is ``"ACTIVE"`` or ``"STUB"`` and *description* is the
    cleaned comment text.

    For multi-line comments (continuation lines starting with ``#``), all
    lines are joined into a single description string.
    """
    lines = source.splitlines()
    result: dict[tuple[int, str], tuple[str, str]] = {}

    # Regex-scan lines for comments
    comment_map: dict[int, str] = {}
    for lineno_0, line in enumerate(lines):
        lineno = lineno_0 + 1
        m = re.search(r"#\s*(.*)", line)
        if m:
            comment_map[lineno] = m.group(1).strip()

    # Walk through lines to associate comments with fields.
    # Pattern: a field assignment line, then one or more comment lines below it.
    # Field lines look like:  "    field_name: Type" or "    field_name: Type = default"
    field_line_re = re.compile(r"^\s+(\w+)\s*:\s*(.+?)(?:\s*=\s*(.+))?$")

    i = 0
    while i < len(lines):
        line = lines[i]
        fm = field_line_re.match(line)
        if fm:
            field_name = fm.group(1)
            # Check if the next line(s) are comment lines
            comment_lines: list[str] = []
            j = i + 1
            while j < len(lines):
                stripped = lines[j].strip()
                if stripped.startswith("#"):
                    comment_lines.append(stripped.lstrip("# ").strip())
                    j += 1
                else:
                    break
            if comment_lines:
                full_comment = " ".join(comment_lines)
                # Extract status tag
                status_m = re.match(r"\[(\w+)\]\s*[—–-]\s*(.*)", full_comment)
                if status_m:
                    status = status_m.group(1)
                    desc = status_m.group(2).strip()
                else:
                    status = "ACTIVE"
                    desc = full_comment
                result[(i + 1, field_name)] = (status, desc)
        i += 1

    return result


def _extract_classes(source: str) -> list[ClassInfo]:
    """Parse data.py and return structured info for every dataclass."""
    tree = ast.parse(source)
    field_comments = _extract_field_comments(source)

    classes: list[ClassInfo] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        # Only process @dataclass-decorated classes
        is_dataclass = any(
            (isinstance(d, ast.Name) and d.id == "dataclass")
            or (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == "dataclass")
            for d in node.decorator_list
        )
        if not is_dataclass:
            continue

        docstring = ast.get_docstring(node) or ""
        fields: list[FieldInfo] = []

        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                fname = item.target.id
                # Reconstruct type annotation as source text
                ftype = ast.unparse(item.annotation) if item.annotation else "Any"
                # Clean up Optional display
                ftype = ftype.replace("Optional[", "").rstrip("]") if "Optional[" in ftype else ftype

                # Look up the comment for this field
                matched = None
                for (lineno, cfield), (status, desc) in field_comments.items():
                    if cfield == fname and abs(lineno - item.lineno) <= 1:
                        matched = (status, desc)
                        break
                if matched:
                    status, desc = matched
                else:
                    status, desc = "ACTIVE", ""

                fields.append(FieldInfo(name=fname, type_annotation=ftype, status=status, description=desc))

        classes.append(ClassInfo(name=node.name, docstring=docstring, fields=fields))

    return classes


# ---------------------------------------------------------------------------
# TOML example loading
# ---------------------------------------------------------------------------


def _load_example(path: Path) -> str:
    """Read a TOML file and return its content as a fenced code block."""
    return path.read_text(encoding="utf-8").strip()


# ---------------------------------------------------------------------------
# Markdown generation
# ---------------------------------------------------------------------------


def _field_table(fields: list[FieldInfo]) -> str:
    """Render a list of FieldInfo as a Markdown table."""
    rows = ["| Field | Type | Status | Description |", "| --- | --- | --- | --- |"]
    for f in fields:
        esc_desc = f.description.replace("|", "\\|")
        status_badge = f.status
        rows.append(f"| `{f.name}` | `{f.type_annotation}` | {status_badge} | {esc_desc} |")
    return "\n".join(rows)


def _class_section(ci: ClassInfo, heading_level: int = 3) -> str:
    """Render a ClassInfo as a Markdown section."""
    hashes = "#" * heading_level
    parts = [f"{hashes} `{ci.name}`", ""]
    if ci.docstring:
        # Take just the first paragraph of the docstring
        paragraphs = ci.docstring.split("\n\n")
        first_para = paragraphs[0].strip().replace("\n", " ")
        parts.append(first_para)
        parts.append("")
    if ci.fields:
        parts.append(_field_table(ci.fields))
        parts.append("")
    return "\n".join(parts)


def generate() -> str:
    """Generate the full Markdown document and return it as a string."""
    source = _DATA_PY.read_text(encoding="utf-8")
    classes = _extract_classes(source)

    # Build a lookup by class name
    class_map = {c.name: c for c in classes}

    # Load TOML examples
    companies_toml = _load_example(_HSBC_DIR / "companies.toml")
    accounts_toml = _load_example(_HSBC_DIR / "accounts.toml")
    statement_types_toml = _load_example(_HSBC_DIR / "statement_types.toml")
    statement_tables_toml = _load_example(_HSBC_DIR / "statement_tables.toml")
    account_types_toml = _load_example(_CONFIG_DIR / "account_types.toml")
    standard_fields_toml = _load_example(_CONFIG_DIR / "standard_fields.toml")

    # --- Build document ---
    doc = []

    def w(text: str = "") -> None:
        doc.append(text)

    w("<!-- DO NOT EDIT — this file is auto-generated by scripts/generate_docs.py -->")
    w("<!-- Regenerate with: python scripts/generate_docs.py -->")
    w()
    w("# Adding a New Bank")
    w()
    w("This guide walks through the process of configuring bank_statement_parser to")
    w("parse PDF statements from a new bank. The configuration is entirely TOML-based")
    w("and does not require writing any Python code.")
    w()
    w("## Overview")
    w()
    w("Adding support for a new bank involves creating and editing several TOML files")
    w("that describe how to identify the bank's PDFs, locate tables on each page,")
    w("extract field values, and map them to standard output columns.")
    w()
    w("The configuration lives in two places:")
    w()
    w("| Location | Purpose |")
    w("| --- | --- |")
    w("| `project/config/<BANK_COUNTRY>/` | Bank-specific config folder (4 TOML files) |")
    w("| `project/config/account_types.toml` | Shared account type registry |")
    w("| `project/config/standard_fields.toml` | Shared standard field mappings |")
    w()
    w("### Bank config folder structure")
    w()
    w("Each bank has its own subfolder named in `SCREAMING_SNAKE_CASE` (e.g. `HSBC_UK`,")
    w("`TSB_UK`). A complete folder contains exactly four files:")
    w()
    w("| File | Purpose | Key Dataclass |")
    w("| --- | --- | --- |")
    w("| `companies.toml` | Bank identification (name + PDF detection rule) | [`Company`](#company) |")
    w("| `accounts.toml` | Account definitions (one per product/card type) | [`Account`](#account) |")
    w("| `statement_types.toml` | Statement layout definitions (header + lines extraction) | [`StatementType`](#statementtype) |")
    w("| `statement_tables.toml` | Physical table extraction rules (locations, fields, bookends) | [`StatementTable`](#statementtable) |")
    w()
    w("### Processing pipeline")
    w()
    w("Understanding the processing order helps when writing config:")
    w()
    w("1. **Company identification** — the `Company.config` extraction is run against page 1")
    w("   to determine which bank issued the PDF.")
    w("2. **Account identification** — each `Account.config` is tried until one matches,")
    w("   identifying the specific account product.")
    w("3. **Header extraction** — the `StatementType.header` configs run to extract")
    w("   statement-level metadata (dates, balances, account details).")
    w("4. **Lines extraction** — the `StatementType.lines` configs run per-page to extract")
    w("   transaction rows.")
    w("5. **Standard field mapping** — raw extracted fields are mapped to `STD_*` output")
    w("   columns via `standard_fields.toml`.")
    w("6. **Checks & balances** — opening balance + payments in - payments out = closing")
    w("   balance is validated.")
    w()

    # ----- Step 1: Account Types -----
    w("## Step 1: Register the Account Type")
    w()
    w("If your bank uses an account type not already in `account_types.toml`, add a new")
    w("entry. Most banks will use the existing types (`CRD`, `CUR`, `SAV`, `ISA`).")
    w()
    w("**File:** `project/config/account_types.toml`")
    w()
    w("```toml")
    w(account_types_toml)
    w("```")
    w()
    if "AccountType" in class_map:
        w(_class_section(class_map["AccountType"], heading_level=3))
    w()

    # ----- Step 2: Create folder -----
    w("## Step 2: Create the Bank Config Folder")
    w()
    w("Create a new subfolder under `project/config/` using the naming convention")
    w("`<BANK>_<COUNTRY>` in SCREAMING_SNAKE_CASE:")
    w()
    w("```")
    w("project/config/")
    w("  HSBC_UK/          # existing")
    w("  TSB_UK/           # existing")
    w("  NEWBANK_UK/       # <- your new folder")
    w("    companies.toml")
    w("    accounts.toml")
    w("    statement_types.toml")
    w("    statement_tables.toml")
    w("```")
    w()

    # ----- Step 3: companies.toml -----
    w("## Step 3: Define the Company")
    w()
    w("Create `companies.toml` in your new folder. This file identifies the bank by")
    w("extracting a distinguishing piece of text from page 1 of the PDF (typically a")
    w("website URL or bank name).")
    w()
    w("**Example** (`HSBC_UK/companies.toml`):")
    w()
    w("```toml")
    w(companies_toml)
    w("```")
    w()
    w("**How it works:** The `config` block defines a small extraction region on page 1.")
    w("The `field` spec extracts text from that region and checks it against")
    w("`string_pattern`. If the pattern matches, this company is selected. Multiple")
    w("`locations` can be provided — the pipeline tries each until one succeeds.")
    w()
    w("### Key dataclasses")
    w()
    for cls_name in ["Company", "Config", "Location", "Field"]:
        if cls_name in class_map:
            w(_class_section(class_map[cls_name], heading_level=4))

    # ----- Step 4: statement_tables.toml -----
    w("## Step 4: Define Statement Tables")
    w()
    w("Create `statement_tables.toml` to define how tables are physically extracted from")
    w("the PDF pages. This is usually the most complex configuration file, as it requires")
    w("understanding the precise layout of the bank's PDF statements.")
    w()
    w("Each table entry defines:")
    w()
    w("- **Where** on the page to look (bounding box coordinates, vertical column dividers)")
    w("- **What** fields to extract (column indices or cell addresses, data types, patterns)")
    w("- **How** to handle multi-row transactions (bookend detection, field merging)")
    w()
    w("### Table types")
    w()
    w("There are three table types, determined by the presence of `transaction_spec`:")
    w()
    w("| Type | Use Case | Field Addressing | Has `transaction_spec`? |")
    w("| --- | --- | --- | --- |")
    w("| `summary` | Account balances, totals | `cell = {row, col}` | No |")
    w("| `detail` | Account holder info, sort codes | `cell = {row, col}` | No |")
    w("| `transaction` | Transaction line items | `column = N` | Yes |")
    w()

    # Summary table example
    w("### Summary table example")
    w()
    w("A summary table extracts fixed values from known cell positions (e.g. opening")
    w("balance at row 1, column 1):")
    w()
    w("```toml")
    # Extract just the first summary table from the TOML
    summary_lines = []
    in_section = False
    for line in statement_tables_toml.splitlines():
        if line.startswith("[HSBC_UK_CUR_ACCT_SUM]"):
            in_section = True
        elif in_section and line.startswith("[") and "HSBC_UK_CUR_ACCT_SUM" not in line:
            break
        if in_section:
            summary_lines.append(line)
    w("\n".join(summary_lines).strip())
    w("```")
    w()

    # Transaction table example
    w("### Transaction table example")
    w()
    w("A transaction table extracts rows of variable length, using bookend detection")
    w("to identify where each transaction starts and ends:")
    w()
    w("```toml")
    txn_lines = []
    in_section = False
    for line in statement_tables_toml.splitlines():
        if line.startswith("[HSBC_UK_CUR_TRANSACTIONS]"):
            in_section = True
        elif in_section and line.startswith("[") and "HSBC_UK_CUR_TRANSACTIONS" not in line:
            break
        if in_section:
            txn_lines.append(line)
    w("\n".join(txn_lines).strip())
    w("```")
    w()

    w("### Key dataclasses")
    w()
    for cls_name in [
        "StatementTable",
        "Location",
        "DynamicLineSpec",
        "Field",
        "Cell",
        "NumericModifier",
        "FieldOffset",
        "CurrencySpec",
        "TransactionSpec",
        "TransactionBookend",
        "FieldValidation",
        "MergeFields",
    ]:
        if cls_name in class_map:
            w(_class_section(class_map[cls_name], heading_level=4))

    # ----- Step 5: statement_types.toml -----
    w("## Step 5: Define Statement Types")
    w()
    w("Create `statement_types.toml` to define the extraction workflow for each distinct")
    w("statement layout. A single bank may have multiple statement types (e.g. current")
    w("account vs. credit card) if their PDF layouts differ.")
    w()
    w("Each statement type groups extraction into two sections:")
    w()
    w("- **`header`** — runs once per statement to extract metadata (dates, balances, account info)")
    w("- **`lines`** — runs per-page to extract transaction rows")
    w()
    w("Configs within each section either reference a `statement_table_key` from")
    w("`statement_tables.toml` or define an inline single-field extraction.")
    w()
    w("**Example** (`HSBC_UK/statement_types.toml`):")
    w()
    w("```toml")
    w(statement_types_toml)
    w("```")
    w()
    w("### Key dataclasses")
    w()
    for cls_name in ["StatementType", "ConfigGroup", "Config"]:
        if cls_name in class_map:
            w(_class_section(class_map[cls_name], heading_level=4))

    # ----- Step 6: accounts.toml -----
    w("## Step 6: Define Accounts")
    w()
    w("Create `accounts.toml` to define each account product offered by the bank. Each")
    w("account links together a company, an account type, and a statement type, plus")
    w("defines a PDF detection rule to identify which account a given statement belongs to.")
    w()
    w("**Example** (`HSBC_UK/accounts.toml` — first entry shown):")
    w()
    w("```toml")
    # Show just the first account entry
    acct_lines = []
    entry_count = 0
    for line in accounts_toml.splitlines():
        if line.startswith("[") and not line.startswith("[HSBC_UK_CRD_RCC"):
            if entry_count >= 1:
                break
        if line.startswith("[HSBC_UK_CRD_RCC"):
            entry_count = 1
        if entry_count >= 1:
            acct_lines.append(line)
    w("\n".join(acct_lines).strip())
    w("```")
    w()
    w("**Key fields:**")
    w()
    w("- `company_key` — must match a key in your `companies.toml`")
    w("- `account_type_key` — must match a key in the shared `account_types.toml` (e.g. `CRD`, `CUR`)")
    w("- `statement_type_key` — must match a key in your `statement_types.toml`")
    w("- `exclude_last_n_pages` — number of trailing pages to skip (terms & conditions, etc.)")
    w("- `config` — inline extraction rule to identify this account from page 1 of the PDF")
    w()
    w("### Key dataclasses")
    w()
    for cls_name in ["Account"]:
        if cls_name in class_map:
            w(_class_section(class_map[cls_name], heading_level=4))

    # ----- Step 7: standard_fields.toml -----
    w("## Step 7: Register Standard Field Mappings")
    w()
    w("Finally, add entries for your new statement type(s) to the shared")
    w("`standard_fields.toml`. This file maps bank-specific raw field names to")
    w("standardised output columns (`STD_*`).")
    w()
    w("For each `STD_*` field, add a new `std_refs` entry with your statement type's")
    w("name and the corresponding raw field name from your `statement_tables.toml`.")
    w()
    w("**File:** `project/config/standard_fields.toml`")
    w()
    w("**Example** (showing `STD_OPENING_BALANCE` with entries for multiple banks):")
    w()
    w("```toml")
    # Extract just the STD_OPENING_BALANCE section
    std_lines = []
    in_section = False
    for line in standard_fields_toml.splitlines():
        if line.startswith("[STD_OPENING_BALANCE]"):
            in_section = True
        elif in_section and line.startswith("[") and "STD_OPENING_BALANCE" not in line:
            break
        if in_section:
            std_lines.append(line)
    w("\n".join(std_lines).strip())
    w("```")
    w()
    w("### Standard fields reference")
    w()
    w("The following standard fields must be mapped for each statement type. Fields")
    w("marked `vital = true` will raise a `ConfigError` if no mapping is found.")
    w()
    w("| Standard Field | Section | Type | Vital | Purpose |")
    w("| --- | --- | --- | --- | --- |")
    w("| `STD_STATEMENT_DATE` | header | date | Yes | Statement period end date |")
    w("| `STD_SORTCODE` | header | string | No | Bank sort code |")
    w("| `STD_ACCOUNT_NUMBER` | header | string | Yes | Account or card number |")
    w("| `STD_ACCOUNT_HOLDER` | header | string | No | Account holder name |")
    w("| `STD_OPENING_BALANCE` | header | numeric | Yes | Opening balance |")
    w("| `STD_CLOSING_BALANCE` | header | numeric | Yes | Closing balance |")
    w("| `STD_PAYMENTS_IN` | header | numeric | Yes | Total credits in period |")
    w("| `STD_PAYMENTS_OUT` | header | numeric | Yes | Total debits in period |")
    w("| `STD_TRANSACTION_DATE` | lines | date | Yes | Individual transaction date |")
    w("| `STD_TRANSACTION_TYPE` | lines | str | Yes | Payment type code |")
    w("| `STD_TRANSACTION_DESC` | lines | string | Yes | Transaction description |")
    w("| `STD_PAYMENT_IN` | lines | numeric | Yes | Credit amount per transaction |")
    w("| `STD_PAYMENT_OUT` | lines | numeric | Yes | Debit amount per transaction |")
    w()
    w("### `std_refs` entry options")
    w()
    w("Each `std_refs` entry supports the following options:")
    w()
    if "StdRefs" in class_map:
        w(_class_section(class_map["StdRefs"], heading_level=4))
    if "StandardFields" in class_map:
        w(_class_section(class_map["StandardFields"], heading_level=4))
    w()

    # ----- Checklist -----
    w("## Configuration Checklist")
    w()
    w("Use this checklist to verify your configuration is complete:")
    w()
    w("- [ ] Account type registered in `account_types.toml` (or existing type reused)")
    w("- [ ] Bank config folder created: `project/config/<BANK_COUNTRY>/`")
    w("- [ ] `companies.toml` — company key, name, and PDF detection rule")
    w("- [ ] `statement_tables.toml` — all table extraction rules (summary, detail, transaction)")
    w("- [ ] `statement_types.toml` — header and lines config groups referencing your table keys")
    w("- [ ] `accounts.toml` — account entries linking company, type, and statement type")
    w("- [ ] `standard_fields.toml` — `std_refs` entries added for all 13 standard fields")
    w("- [ ] Test with a real PDF: `bsp process --pdfs /path/to/statements`")
    w("- [ ] Verify checks & balances pass (opening + payments_in - payments_out = closing)")
    w()

    # ----- Dataclass Reference -----
    w("## Dataclass Reference")
    w()
    w("Complete reference for all configuration dataclasses defined in")
    w("`bank_statement_parser.modules.data`. Fields marked **STUB** are declared but")
    w("not currently read by the pipeline — they are reserved for future use.")
    w()

    # Render in a logical order
    order = [
        "Company",
        "Account",
        "AccountType",
        "StatementType",
        "ConfigGroup",
        "Config",
        "StatementTable",
        "Location",
        "DynamicLineSpec",
        "Field",
        "Cell",
        "FieldOffset",
        "NumericModifier",
        "CurrencySpec",
        "TransactionSpec",
        "TransactionBookend",
        "FieldValidation",
        "MergeFields",
        "StandardFields",
        "StdRefs",
        "Test",
    ]
    for name in order:
        if name in class_map:
            w(_class_section(class_map[name], heading_level=3))

    return "\n".join(doc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Generate the documentation file."""
    _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    content = generate()
    _OUTPUT.write_text(content, encoding="utf-8")
    print(f"Generated {_OUTPUT.relative_to(_REPO_ROOT)}")


if __name__ == "__main__":
    main()
