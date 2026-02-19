import sqlite3
from pathlib import Path


def create_views(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # GapReport
    # Identifies missing statements (gaps between consecutive closing /
    # opening balances) in the raw source tables.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS GapReport")
    cursor.execute("""
        CREATE VIEW GapReport AS
        WITH ordered_statements AS (
            SELECT
                STD_ACCOUNT          AS account_type,
                STD_ACCOUNT_NUMBER   AS account_number,
                STD_ACCOUNT_HOLDER   AS account_holder,
                STD_STATEMENT_DATE   AS statement_date,
                CAST(STD_OPENING_BALANCE AS REAL) AS opening_balance,
                CAST(STD_CLOSING_BALANCE AS REAL) AS closing_balance,
                ROW_NUMBER() OVER (
                    PARTITION BY STD_ACCOUNT, STD_ACCOUNT_NUMBER
                    ORDER BY STD_ACCOUNT, STD_ACCOUNT_NUMBER, STD_STATEMENT_DATE
                ) AS row_num
            FROM statement_heads
        ),
        with_prev AS (
            SELECT
                account_type,
                account_number,
                account_holder,
                statement_date,
                opening_balance,
                closing_balance,
                LAG(closing_balance) OVER (
                    PARTITION BY account_type, account_number
                    ORDER BY statement_date
                ) AS prev_closing_balance,
                CASE
                    WHEN account_type || account_number =
                         LAG(account_type || account_number) OVER (
                             PARTITION BY account_type, account_number
                             ORDER BY statement_date
                         )
                    THEN 0 ELSE 1
                END AS account_change
            FROM ordered_statements
        )
        SELECT
            account_type,
            account_number,
            account_holder,
            statement_date,
            opening_balance,
            closing_balance,
            CASE
                WHEN account_change = 1                          THEN ''
                WHEN opening_balance = prev_closing_balance      THEN ''
                ELSE 'GAP'
            END AS gap_flag
        FROM with_prev
    """)
    print("Created view: GapReport")

    # ------------------------------------------------------------------
    # FlatTransaction
    # Denormalised view for reporting.  References the mart tables
    # (populated by build_datamart.py) rather than raw source tables.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS FlatTransaction")
    cursor.execute("""
        CREATE VIEW FlatTransaction AS
        SELECT
            ft.id_date          AS transaction_date,
            ds.statement_date,
            ds.filename,
            da.company,
            da.account_type,
            da.account_number,
            da.sortcode,
            da.account_holder,
            ft.transaction_number,
            ft.transaction_credit_or_debit  AS CD,
            ft.transaction_type             AS type,
            ft.transaction_desc,
            SUBSTR(ft.transaction_desc, 1, 25) AS short_desc,
            ft.value_in,
            ft.value_out,
            ft.value
        FROM FactTransaction ft
        INNER JOIN DimStatement ds ON ft.statement_id = ds.statement_id
        INNER JOIN DimAccount   da ON ft.account_id   = da.account_id
    """)
    print("Created view: FlatTransaction")

    conn.commit()
    conn.close()
    print(f"\nAll views created successfully in {db_path}")


if __name__ == "__main__":
    create_views(Path(__file__).parent.joinpath("project.db"))
