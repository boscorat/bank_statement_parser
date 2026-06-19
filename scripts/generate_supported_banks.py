"""Generate supported banks and accounts table from config files.

Scans the config/import/ directory and extracts bank names and account
product names from TOML configuration files. Generates a Markdown table
for inclusion in the documentation.

Run from the repository root::

    python scripts/generate_supported_banks.py <config_dir>
"""

import tomllib
from pathlib import Path


def extract_banks(config_dir: Path) -> list[tuple[str, list[str]]]:
    """Extract bank names and account lists from config folder.

    Scans config_dir for folders matching <BANK>_<COUNTRY> pattern.
    For each folder, reads companies.toml (bank name) and accounts.toml
    (account product names).

    Args:
        config_dir: Path to config/import/ directory.

    Returns:
        List of (bank_name, [account_names]) tuples, sorted by bank name.
    """
    banks: list[tuple[str, list[str]]] = []

    # Find all bank config folders (directories named like HSBC_UK, TSB_UK, etc.)
    for bank_folder in sorted(config_dir.iterdir()):
        if not bank_folder.is_dir() or "_" not in bank_folder.name:
            continue

        # Skip non-bank folders (those without underscore pattern)
        parts = bank_folder.name.split("_")
        if len(parts) < 2:
            continue

        companies_file = bank_folder / "companies.toml"
        accounts_file = bank_folder / "accounts.toml"

        # Both files must exist
        if not companies_file.exists() or not accounts_file.exists():
            continue

        # Extract bank name from companies.toml
        try:
            with open(companies_file, "rb") as f:
                companies_data = tomllib.load(f)
        except Exception:
            continue

        # Find the company name (first [[section]] in companies.toml)
        bank_name = None
        for section_name, section_data in companies_data.items():
            if isinstance(section_data, dict) and "name" in section_data:
                bank_name = section_data["name"]
                break

        if not bank_name:
            # Fallback: use folder name with underscores as spaces
            bank_name = bank_folder.name.replace("_", " ")

        # Extract account names from accounts.toml
        try:
            with open(accounts_file, "rb") as f:
                accounts_data = tomllib.load(f)
        except Exception:
            accounts = []
        else:
            accounts = []
            for section_name, section_data in accounts_data.items():
                if isinstance(section_data, dict) and "account" in section_data:
                    account_name = section_data["account"]
                    if account_name and account_name not in accounts:
                        accounts.append(account_name)

        if accounts or bank_name:
            banks.append((bank_name, accounts))

    return banks


def generate_banks_table(config_dir: Path) -> str:
    """Generate Markdown table of supported banks and accounts.

    Args:
        config_dir: Path to config/import/ directory.

    Returns:
        Markdown table string (including header and separator rows).
    """
    banks = extract_banks(config_dir)

    if not banks:
        return ""

    lines: list[str] = [
        "| Bank | Supported accounts |",
        "|---|---|",
    ]

    for bank_name, accounts in banks:
        # Bold the bank name, join accounts with commas
        accounts_str = ", ".join(accounts) if accounts else "*(no accounts configured)*"
        lines.append(f"| **{bank_name}** | {accounts_str} |")

    return "\n".join(lines)


def update_index_md(index_path: Path, table: str) -> None:
    """Replace the banks table in docs/index.md.

    Finds the line "## Supported banks and accounts" and replaces
    the Markdown table that follows it with the generated table.

    Args:
        index_path: Path to docs/index.md.
        table: Generated Markdown table string.
    """
    if not index_path.exists():
        raise FileNotFoundError(f"docs/index.md not found at {index_path}")

    content = index_path.read_text(encoding="utf-8")
    lines = content.splitlines(keepends=True)

    # Find the section header
    section_idx = None
    for i, line in enumerate(lines):
        if "## Supported banks and accounts" in line:
            section_idx = i
            break

    if section_idx is None:
        raise ValueError("Could not find '## Supported banks and accounts' section in docs/index.md")

    # Find the start of the table (first | line after the section)
    table_start = None
    for i in range(section_idx + 1, len(lines)):
        if lines[i].strip().startswith("|"):
            table_start = i
            break

    if table_start is None:
        raise ValueError("Could not find table start in docs/index.md")

    # Find the end of the table (first non-table line after start)
    table_end = None
    for i in range(table_start, len(lines)):
        if lines[i].strip() and not lines[i].strip().startswith("|"):
            table_end = i
            break

    if table_end is None:
        table_end = len(lines)

    # Replace the table
    new_lines = lines[:table_start] + [table + "\n"] + lines[table_end:]
    index_path.write_text("".join(new_lines), encoding="utf-8")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python scripts/generate_supported_banks.py <config_dir>")
        sys.exit(1)

    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"Error: config directory not found: {config_path}")
        sys.exit(1)

    table = generate_banks_table(config_path)
    print(table)
