import sqlite3
from pathlib import Path

import polars as pl


def polars_to_sqlite_type(pl_type):
    type_str = str(pl_type)
    if "Utf8" in type_str:
        return "TEXT"
    elif "Int" in type_str or "UInt" in type_str:
        return "INTEGER"
    elif "Float" in type_str:
        return "REAL"
    elif "Boolean" in type_str:
        return "INTEGER"
    elif "Date" in type_str or "Datetime" in type_str:
        return "TEXT"
    elif "Decimal" in type_str:
        return "REAL"
    else:
        return "TEXT"


SCHEMAS = {
    "checks_and_balances": pl.DataFrame(
        orient="row",
        schema={
            "ID_CAB": pl.Utf8,
            "ID_STATEMENT": pl.Utf8,
            "ID_BATCH": pl.Utf8,
            "HAS_TRANSACTIONS": pl.Boolean,
            "STD_OPENING_BALANCE_HEADS": pl.Decimal(16, 4),
            "STD_PAYMENTS_IN_HEADS": pl.Decimal(16, 4),
            "STD_PAYMENTS_OUT_HEADS": pl.Decimal(16, 4),
            "STD_MOVEMENT_HEADS": pl.Decimal(16, 4),
            "STD_CLOSING_BALANCE_HEADS": pl.Decimal(16, 4),
            "STD_OPENING_BALANCE_LINES": pl.Decimal(16, 4),
            "STD_PAYMENTS_IN_LINES": pl.Decimal(16, 4),
            "STD_PAYMENTS_OUT_LINES": pl.Decimal(16, 4),
            "STD_MOVEMENT_LINES": pl.Decimal(16, 4),
            "STD_CLOSING_BALANCE_LINES": pl.Decimal(16, 4),
            "CHECK_PAYMENTS_IN": pl.Boolean,
            "CHECK_PAYMENTS_OUT": pl.Boolean,
            "CHECK_MOVEMENT": pl.Boolean,
            "CHECK_CLOSING": pl.Boolean,
        },
    ),
    "statement_heads": pl.DataFrame(
        orient="row",
        schema={
            "ID_STATEMENT": pl.Utf8,
            "ID_BATCH": pl.Utf8,
            "ID_ACCOUNT": pl.Utf8,
            "STD_COMPANY": pl.Utf8,
            "STD_STATEMENT_TYPE": pl.Utf8,
            "STD_ACCOUNT": pl.Utf8,
            "STD_SORTCODE": pl.Utf8,
            "STD_ACCOUNT_NUMBER": pl.Utf8,
            "STD_ACCOUNT_HOLDER": pl.Utf8,
            "STD_STATEMENT_DATE": pl.Date,
            "STD_OPENING_BALANCE": pl.Decimal(16, 4),
            "STD_PAYMENTS_IN": pl.Decimal(16, 4),
            "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
            "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
        },
    ),
    "statement_lines": pl.DataFrame(
        orient="row",
        schema={
            "ID_TRANSACTION": pl.Utf8,
            "ID_STATEMENT": pl.Utf8,
            "STD_PAGE_NUMBER": pl.Int32,
            "STD_TRANSACTION_DATE": pl.Date,
            "STD_TRANSACTION_NUMBER": pl.UInt32,
            "STD_CD": pl.Utf8,
            "STD_TRANSACTION_TYPE": pl.Utf8,
            "STD_TRANSACTION_TYPE_CD": pl.Utf8,
            "STD_TRANSACTION_DESC": pl.Utf8,
            "STD_OPENING_BALANCE": pl.Decimal(16, 4),
            "STD_PAYMENTS_IN": pl.Decimal(16, 4),
            "STD_PAYMENTS_OUT": pl.Decimal(16, 4),
            "STD_CLOSING_BALANCE": pl.Decimal(16, 4),
        },
    ),
    "batch_heads": pl.DataFrame(
        orient="row",
        schema={
            "ID_BATCH": pl.Utf8,
            "STD_PATH": pl.Utf8,
            "STD_COMPANY": pl.Utf8,
            "STD_ACCOUNT": pl.Utf8,
            "STD_PDF_COUNT": pl.Int64,
            "STD_ERROR_COUNT": pl.Int64,
            "STD_DURATION_SECS": pl.Float64,
            "STD_UPDATETIME": pl.Datetime,
        },
    ),
    "batch_lines": pl.DataFrame(
        orient="row",
        schema={
            "ID_BATCH": pl.Utf8,
            "ID_BATCHLINE": pl.Utf8,
            "ID_STATEMENT": pl.Utf8,
            "STD_BATCH_LINE": pl.Int64,
            "STD_FILENAME": pl.Utf8,
            "STD_ACCOUNT": pl.Utf8,
            "STD_DURATION_SECS": pl.Float64,
            "STD_UPDATETIME": pl.Datetime,
            "STD_SUCCESS": pl.Boolean,
            "STD_ERROR_MESSAGE": pl.Utf8,
            "ERROR_CAB": pl.Boolean,
            "ERROR_CONFIG": pl.Boolean,
        },
    ),
}

FOREIGN_KEYS = {
    "checks_and_balances": [
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT)",
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH)",
    ],
    "statement_heads": [
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH)",
    ],
    "statement_lines": [
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT)",
    ],
    "batch_lines": [
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH)",
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT)",
    ],
}


def create_table(conn, table_name, schema: pl.DataFrame, with_fk: bool = False):
    columns = list(schema.schema.items())
    col_defs = []
    for col_name, col_type in columns:
        sqlite_type = polars_to_sqlite_type(col_type)
        col_defs.append(f'"{col_name}" {sqlite_type}')

    if with_fk and table_name in FOREIGN_KEYS:
        col_defs.extend(FOREIGN_KEYS[table_name])

    create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"
    print(f"Creating table: {table_name}")
    print(create_sql)
    print()
    conn.execute(create_sql)


def main(with_fk: bool = False):
    db_name = "v2.db" if with_fk else "v1.db"
    db_path = Path(__file__).parent / db_name
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    if with_fk:
        conn.execute("PRAGMA foreign_keys = ON;")

    table_order = ["batch_heads", "statement_heads", "checks_and_balances", "statement_lines", "batch_lines"]

    for table_name in table_order:
        create_table(conn, table_name, SCHEMAS[table_name], with_fk)

    conn.commit()
    conn.close()
    print(f"Database created: {db_path}")


if __name__ == "__main__":
    main(with_fk=False)
    main(with_fk=True)
