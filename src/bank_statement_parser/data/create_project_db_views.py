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
        INNER JOIN DimStatement ds ON ft.statement_int = ds.statement_int
        INNER JOIN DimAccount   da ON ft.account_int   = da.account_int
    """)
    print("Created view: FlatTransaction")

    # ------------------------------------------------------------------
    # DimStatementBatch
    # DimStatement with id_batch replaced by batch_id from batch_lines,
    # so a statement appears once per distinct batch it was processed in.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS DimStatementBatch")
    cursor.execute("""
        CREATE VIEW DimStatementBatch AS
        SELECT DISTINCT
            bl.ID_BATCH          AS batch_id,
            ds.statement_int,
            ds.id_statement,
            ds.account_int,
            ds.id_account,
            ds.company,
            ds.account_type,
            ds.account_number,
            ds.sortcode,
            ds.account_holder,
            ds.statement_date,
            ds.opening_balance,
            ds.payments_in,
            ds.payments_out,
            ds.closing_balance,
            ds.statement_type,
            ds.filename,
            ds.batch_time
        FROM DimStatement ds
        INNER JOIN batch_lines bl ON ds.id_statement = bl.ID_STATEMENT
    """)
    print("Created view: DimStatementBatch")

    # ------------------------------------------------------------------
    # FactTransactionBatch
    # FactTransaction rows annotated with the batch_id they belong to.
    # A transaction fans out if its statement appears in multiple batches.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS FactTransactionBatch")
    cursor.execute("""
        CREATE VIEW FactTransactionBatch AS
        SELECT
            dsb.batch_id,
            ft.transaction_int,
            ft.id_transaction,
            ft.statement_int,
            ft.account_int,
            ft.date_int,
            ft.id_date,
            ft.id_account,
            ft.id_statement,
            ft.transaction_number,
            ft.transaction_credit_or_debit,
            ft.transaction_type,
            ft.transaction_type_cd,
            ft.transaction_desc,
            ft.opening_balance,
            ft.value_in,
            ft.value_out,
            ft.value
        FROM FactTransaction ft
        INNER JOIN DimStatementBatch dsb ON ft.statement_int = dsb.statement_int
    """)
    print("Created view: FactTransactionBatch")

    # ------------------------------------------------------------------
    # DimAccountBatch
    # Distinct accounts contained within each batch.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS DimAccountBatch")
    cursor.execute("""
        CREATE VIEW DimAccountBatch AS
        SELECT DISTINCT
            dsb.batch_id,
            da.account_int,
            da.id_account,
            da.company,
            da.account_type,
            da.account_number,
            da.sortcode,
            da.account_holder
        FROM DimAccount da
        INNER JOIN DimStatementBatch dsb ON da.account_int = dsb.account_int
    """)
    print("Created view: DimAccountBatch")

    # ------------------------------------------------------------------
    # DimDateBatch
    # Calendar rows scoped to each batch's date range
    # (min transaction date → max statement date per batch_id).
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS DimDateBatch")
    cursor.execute("""
        CREATE VIEW DimDateBatch AS
        SELECT
            dd.*,
            r.batch_id
        FROM DimDate dd
        INNER JOIN (
            SELECT
                ftb.batch_id,
                MIN(ftb.id_date)  AS min_date,
                MAX(dsb.statement_date) AS max_date
            FROM FactTransactionBatch ftb
            INNER JOIN DimStatementBatch dsb
                   ON ftb.statement_int = dsb.statement_int
                  AND ftb.batch_id      = dsb.batch_id
            GROUP BY ftb.batch_id
        ) r ON dd.id_date BETWEEN r.min_date AND r.max_date
    """)
    print("Created view: DimDateBatch")

    # ------------------------------------------------------------------
    # FactBalanceBatch
    # FactBalance rows restricted to the accounts and date range of each
    # batch.  Joins FactBalance to DimAccountBatch (accounts in batch) and
    # DimTimeBatch (dates in batch), then adds batch_id.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS FactBalanceBatch")
    cursor.execute("""
        CREATE VIEW FactBalanceBatch AS
        SELECT
            dab.batch_id,
            fb.date_int,
            fb.account_int,
            fb.id_date,
            fb.id_account,
            fb.opening_balance,
            fb.closing_balance,
            fb.movement,
            fb.outside_date
        FROM FactBalance fb
        INNER JOIN DimAccountBatch dab ON fb.account_int = dab.account_int
        INNER JOIN DimDateBatch    ddb ON fb.date_int    = ddb.date_int
                                     AND dab.batch_id   = ddb.batch_id
    """)
    print("Created view: FactBalanceBatch")

    # ------------------------------------------------------------------
    # FlatTransactionBatch
    # Denormalised batch-scoped equivalent of FlatTransaction.
    # ------------------------------------------------------------------
    cursor.execute("DROP VIEW IF EXISTS FlatTransactionBatch")
    cursor.execute("""
        CREATE VIEW FlatTransactionBatch AS
        SELECT
            ftb.batch_id,
            ftb.id_date             AS transaction_date,
            dsb.statement_date,
            dsb.filename,
            dab.company,
            dab.account_type,
            dab.account_number,
            dab.sortcode,
            dab.account_holder,
            ftb.transaction_number,
            ftb.transaction_credit_or_debit  AS CD,
            ftb.transaction_type             AS type,
            ftb.transaction_desc,
            SUBSTR(ftb.transaction_desc, 1, 25) AS short_desc,
            ftb.value_in,
            ftb.value_out,
            ftb.value
        FROM FactTransactionBatch ftb
        INNER JOIN DimStatementBatch dsb ON ftb.statement_int = dsb.statement_int
                                        AND ftb.batch_id      = dsb.batch_id
        INNER JOIN DimAccountBatch   dab ON ftb.account_int   = dab.account_int
                                        AND ftb.batch_id      = dab.batch_id
    """)
    print("Created view: FlatTransactionBatch")

    conn.commit()
    conn.close()
    print(f"\nAll views created successfully in {db_path}")


if __name__ == "__main__":
    from bank_statement_parser.modules.paths import ProjectPaths  # noqa: PLC0415

    create_views(ProjectPaths.resolve().project_db)
