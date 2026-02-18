import sqlite3
from pathlib import Path

from bank_statement_parser.data.create_project_db_views import create_views

SCHEMAS = {
    "checks_and_balances": {
        "ID_CAB": "TEXT",
        "ID_STATEMENT": "TEXT",
        "ID_BATCH": "TEXT",
        "HAS_TRANSACTIONS": "INTEGER",
        "STD_OPENING_BALANCE_HEADS": "REAL",
        "STD_PAYMENTS_IN_HEADS": "REAL",
        "STD_PAYMENTS_OUT_HEADS": "REAL",
        "STD_MOVEMENT_HEADS": "REAL",
        "STD_CLOSING_BALANCE_HEADS": "REAL",
        "STD_OPENING_BALANCE_LINES": "REAL",
        "STD_PAYMENTS_IN_LINES": "REAL",
        "STD_PAYMENTS_OUT_LINES": "REAL",
        "STD_MOVEMENT_LINES": "REAL",
        "STD_CLOSING_BALANCE_LINES": "REAL",
        "CHECK_PAYMENTS_IN": "INTEGER",
        "CHECK_PAYMENTS_OUT": "INTEGER",
        "CHECK_MOVEMENT": "INTEGER",
        "CHECK_CLOSING": "INTEGER",
    },
    "statement_heads": {
        "ID_STATEMENT": "TEXT",
        "ID_BATCH": "TEXT",
        "ID_ACCOUNT": "TEXT",
        "STD_COMPANY": "TEXT",
        "STD_STATEMENT_TYPE": "TEXT",
        "STD_ACCOUNT": "TEXT",
        "STD_SORTCODE": "TEXT",
        "STD_ACCOUNT_NUMBER": "TEXT",
        "STD_ACCOUNT_HOLDER": "TEXT",
        "STD_STATEMENT_DATE": "TEXT",
        "STD_OPENING_BALANCE": "REAL",
        "STD_PAYMENTS_IN": "REAL",
        "STD_PAYMENTS_OUT": "REAL",
        "STD_CLOSING_BALANCE": "REAL",
    },
    "statement_lines": {
        "ID_TRANSACTION": "TEXT",
        "ID_STATEMENT": "TEXT",
        "STD_PAGE_NUMBER": "INTEGER",
        "STD_TRANSACTION_DATE": "TEXT",
        "STD_TRANSACTION_NUMBER": "INTEGER",
        "STD_CD": "TEXT",
        "STD_TRANSACTION_TYPE": "TEXT",
        "STD_TRANSACTION_TYPE_CD": "TEXT",
        "STD_TRANSACTION_DESC": "TEXT",
        "STD_OPENING_BALANCE": "REAL",
        "STD_PAYMENTS_IN": "REAL",
        "STD_PAYMENTS_OUT": "REAL",
        "STD_CLOSING_BALANCE": "REAL",
    },
    "batch_heads": {
        "ID_BATCH": "TEXT",
        "STD_PATH": "TEXT",
        "STD_COMPANY": "TEXT",
        "STD_ACCOUNT": "TEXT",
        "STD_PDF_COUNT": "INTEGER",
        "STD_ERROR_COUNT": "INTEGER",
        "STD_DURATION_SECS": "REAL",
        "STD_UPDATETIME": "TEXT",
    },
    "batch_lines": {
        "ID_BATCH": "TEXT",
        "ID_BATCHLINE": "TEXT",
        "ID_STATEMENT": "TEXT",
        "STD_BATCH_LINE": "INTEGER",
        "STD_FILENAME": "TEXT",
        "STD_ACCOUNT": "TEXT",
        "STD_DURATION_SECS": "REAL",
        "STD_UPDATETIME": "TEXT",
        "STD_SUCCESS": "INTEGER",
        "STD_ERROR_MESSAGE": "TEXT",
        "ERROR_CAB": "INTEGER",
        "ERROR_CONFIG": "INTEGER",
    },
}

FOREIGN_KEYS = {
    "checks_and_balances": [
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT) ON UPDATE CASCADE ON DELETE CASCADE",
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH) ON UPDATE CASCADE ON DELETE CASCADE",
    ],
    "statement_heads": [
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH) ON UPDATE CASCADE ON DELETE CASCADE",
    ],
    "statement_lines": [
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT) ON UPDATE CASCADE ON DELETE CASCADE",
    ],
    "batch_lines": [
        "FOREIGN KEY (ID_BATCH) REFERENCES batch_heads(ID_BATCH) ON UPDATE CASCADE ON DELETE CASCADE",
        "FOREIGN KEY (ID_STATEMENT) REFERENCES statement_heads(ID_STATEMENT) ON UPDATE CASCADE ON DELETE CASCADE",
    ],
}


PRIMARY_KEYS = {
    "checks_and_balances": "ID_CAB",
    "statement_heads": "ID_STATEMENT",
    "statement_lines": "ID_TRANSACTION",
    "batch_heads": "ID_BATCH",
    "batch_lines": "ID_BATCHLINE",
}


def create_table(conn, table_name, schema: dict, with_fk: bool = False):
    col_defs = []
    for col_name, col_type in schema.items():
        if with_fk and table_name in PRIMARY_KEYS and PRIMARY_KEYS[table_name] == col_name:
            col_defs.append(f'"{col_name}" {col_type} NOT NULL PRIMARY KEY')
        else:
            col_defs.append(f'"{col_name}" {col_type}')

    if with_fk and table_name in FOREIGN_KEYS:
        col_defs.extend(FOREIGN_KEYS[table_name])

    create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"
    print(f"Creating table: {table_name}")
    print(create_sql)
    print()
    conn.execute(create_sql)


def main(db_path: Path, with_fk: bool = False):
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

    create_views(db_path)


if __name__ == "__main__":
    # main(db_name="project_basic.db", with_fk=False)
    main(db_path=Path(__file__).parent.joinpath("project.db"), with_fk=False)
