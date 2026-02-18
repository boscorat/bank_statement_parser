import sqlite3
from pathlib import Path


def create_views(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("DROP VIEW IF EXISTS DimStatement")
    cursor.execute("""
        CREATE VIEW DimStatement AS
        SELECT 
            sh.ID_STATEMENT AS id_statement,
            sh.ID_BATCH AS id_batch,
            bl.rowid AS id_statement_int,
            sh.ID_ACCOUNT AS id_account,
            sh.STD_COMPANY AS company,
            sh.STD_ACCOUNT AS account_type,
            sh.STD_ACCOUNT_NUMBER AS account_number,
            sh.STD_SORTCODE AS sortcode,
            sh.STD_ACCOUNT_HOLDER AS account_holder,
            sh.STD_STATEMENT_DATE AS statement_date,
            sh.STD_OPENING_BALANCE AS opening_balance,
            sh.STD_PAYMENTS_IN AS payments_in,
            sh.STD_PAYMENTS_OUT AS payments_out,
            sh.STD_CLOSING_BALANCE AS closing_balance,
            sh.STD_STATEMENT_TYPE AS statement_type,
            bl.STD_FILENAME AS filename,
            bl.STD_UPDATETIME AS batch_time
        FROM statement_heads sh
        INNER JOIN batch_lines bl ON sh.ID_STATEMENT = bl.ID_STATEMENT AND sh.ID_BATCH = bl.ID_BATCH
    """)
    print("Created view: DimStatement")

    cursor.execute("DROP VIEW IF EXISTS DimAccount")
    cursor.execute("""
        CREATE VIEW DimAccount AS
        SELECT 
            id_account,
            company,
            account_type,
            account_number,
            sortcode,
            account_holder
        FROM (
            SELECT 
                sh.ID_ACCOUNT AS id_account,
                sh.STD_COMPANY AS company,
                sh.STD_ACCOUNT AS account_type,
                sh.STD_ACCOUNT_NUMBER AS account_number,
                sh.STD_SORTCODE AS sortcode,
                sh.STD_ACCOUNT_HOLDER AS account_holder,
                sh.STD_STATEMENT_DATE AS statement_date,
                ROW_NUMBER() OVER (PARTITION BY sh.ID_ACCOUNT ORDER BY sh.STD_STATEMENT_DATE DESC) AS rn
            FROM statement_heads sh
            WHERE sh.ID_ACCOUNT IS NOT NULL
        )
        WHERE rn = 1
    """)
    print("Created view: DimAccount")

    cursor.execute("DROP VIEW IF EXISTS DimTime")
    cursor.execute("""
        CREATE VIEW DimTime AS
        WITH date_range AS (
            SELECT 
                MIN(sl.STD_TRANSACTION_DATE) AS min_date,
                MAX(sh.STD_STATEMENT_DATE) AS max_date
            FROM statement_heads sh
            INNER JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
        ),
        recursive_dates AS (
            SELECT date(min_date) as id_date FROM date_range
            UNION ALL
            SELECT date(id_date, '+1 day') FROM recursive_dates, date_range
            WHERE id_date < date(max_date)
        )
        SELECT 
            id_date,
            strftime('%x', id_date) AS date_local_format,
            CAST(strftime('%Y%m%d', id_date) AS INTEGER) AS date_integer,
            CAST(strftime('%Y', id_date) AS INTEGER) AS year,
            CAST(strftime('%y', id_date) AS INTEGER) AS year_short,
            CAST(strftime('%m', id_date) AS INTEGER) AS quarter,
            'Q' || CAST(strftime('%m', id_date) AS INTEGER) AS quarter_name,
            CAST(strftime('%m', id_date) AS INTEGER) AS month_number,
            strftime('%m', id_date) AS month_number_padded,
            strftime('%B', id_date) AS month_name,
            strftime('%b', id_date) AS month_abbrv,
            CAST(strftime('%Y%m', id_date) AS INTEGER) AS period,
            CAST(strftime('%W', id_date) AS INTEGER) AS week,
            CAST(strftime('%Y%W', id_date) AS INTEGER) AS year_week,
            CAST(strftime('%d', id_date) AS INTEGER) AS day_of_month,
            CAST(strftime('%j', id_date) AS INTEGER) AS day_of_year,
            CAST(strftime('%w', id_date) AS INTEGER) + 1 AS day_of_week,
            strftime('%A', id_date) AS weekday,
            strftime('%a', id_date) AS weekday_abbrv,
            substr(strftime('%a', id_date), 1, 1) AS weekday_initial,
            CASE 
                WHEN CAST(strftime('%d', id_date) AS INTEGER) = CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                THEN 1 ELSE 0 
            END AS is_last_day_of_month,
            CASE 
                WHEN CAST(strftime('%d', id_date) AS INTEGER) = CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                AND CAST(strftime('%m', id_date) AS INTEGER) % 3 = 0
                THEN 1 ELSE 0 
            END AS is_last_day_of_quarter,
            CASE 
                WHEN CAST(strftime('%d', id_date) AS INTEGER) = CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                AND CAST(strftime('%m', id_date) AS INTEGER) = 12
                THEN 1 ELSE 0 
            END AS is_last_day_of_year,
            CASE 
                WHEN strftime('%w', id_date) NOT IN ('0', '6') THEN 1 ELSE 0 
            END AS is_weekday
        FROM recursive_dates
    """)
    print("Created view: DimTime")

    cursor.execute("DROP VIEW IF EXISTS FactTransaction")
    cursor.execute("""
        CREATE VIEW FactTransaction AS
        SELECT 
            sh.ID_STATEMENT AS id_statement,
            sh.ID_ACCOUNT AS id_account,
            sh.ID_BATCH AS id_batch,
            sl.ID_TRANSACTION AS id_transaction,
            sl.STD_TRANSACTION_DATE AS id_date,
            sl.STD_TRANSACTION_NUMBER AS transaction_number,
            sl.STD_CD AS transaction_credit_or_debit,
            sl.STD_TRANSACTION_TYPE AS transaction_type,
            sl.STD_TRANSACTION_TYPE_CD AS transaction_type_cd,
            sl.STD_TRANSACTION_DESC AS transaction_desc,
            sl.STD_OPENING_BALANCE AS opening_balance,
            CAST(sl.STD_PAYMENTS_IN AS REAL) AS value_in,
            CAST(sl.STD_PAYMENTS_OUT AS REAL) AS value_out,
            CAST(sl.STD_PAYMENTS_IN AS REAL) - CAST(sl.STD_PAYMENTS_OUT AS REAL) AS value
        FROM statement_heads sh
        INNER JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
    """)
    print("Created view: FactTransaction")

    cursor.execute("DROP VIEW IF EXISTS FactBalance")
    cursor.execute("""
        CREATE VIEW FactBalance AS
        WITH account_dates AS (
            SELECT 
                sh.ID_ACCOUNT AS id_account,
                sh.ID_STATEMENT AS id_statement,
                sl.STD_TRANSACTION_DATE AS id_date,
                sl.STD_TRANSACTION_NUMBER AS trnno,
                sl.STD_OPENING_BALANCE AS opening_balance,
                sl.STD_CLOSING_BALANCE AS closing_balance,
                CAST(sl.STD_PAYMENTS_IN AS REAL) - CAST(sl.STD_PAYMENTS_OUT AS REAL) AS movement
            FROM statement_heads sh
            INNER JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
        ),
        aggregated AS (
            SELECT 
                id_account,
                id_date,
                FIRST_VALUE(opening_balance) OVER (PARTITION BY id_account, id_date ORDER BY trnno) AS opening_balance,
                LAST_VALUE(closing_balance) OVER (PARTITION BY id_account, id_date ORDER BY trnno) AS closing_balance,
                SUM(movement) OVER (PARTITION BY id_account, id_date) AS movement
            FROM account_dates
        ),
        account_bookends AS (
            SELECT 
                id_account,
                MIN(id_date) AS first_day,
                MAX(id_date) AS last_day
            FROM aggregated
            GROUP BY id_account
        ),
        date_account_grid AS (
            SELECT DISTINCT
                dt.id_date,
                ab.id_account
            FROM DimTime dt
            CROSS JOIN (SELECT DISTINCT id_account FROM statement_heads WHERE ID_ACCOUNT IS NOT NULL) ab
        ),
        filled AS (
            SELECT 
                dag.id_date,
                dag.id_account,
                ab.first_day,
                ab.last_day,
                agg.closing_balance,
                agg.movement,
                CASE WHEN dag.id_date < ab.first_day THEN 1 ELSE 0 END AS pre_date,
                CASE WHEN dag.id_date > ab.last_day THEN 1 ELSE 0 END AS post_date
            FROM date_account_grid dag
            LEFT JOIN aggregated agg ON dag.id_date = agg.id_date AND dag.id_account = agg.id_account
            LEFT JOIN account_bookends ab ON dag.id_account = ab.id_account
        ),
        with_forward_fill AS (
            SELECT 
                id_date,
                id_account,
                first_day,
                last_day,
                pre_date,
                post_date,
                CASE 
                    WHEN closing_balance IS NOT NULL THEN closing_balance
                    WHEN pre_date = 1 THEN NULL
                    ELSE (
                        SELECT closing_balance 
                        FROM filled f2 
                        WHERE f2.id_account = filled.id_account 
                        AND f2.id_date < filled.id_date 
                        AND f2.closing_balance IS NOT NULL 
                        ORDER BY f2.id_date DESC 
                        LIMIT 1
                    )
                END AS closing_balance_filled,
                COALESCE(movement, 0) AS movement
            FROM filled
        ),
        with_opening AS (
            SELECT 
                id_date,
                id_account,
                first_day,
                last_day,
                pre_date,
                post_date,
                CASE 
                    WHEN closing_balance_filled IS NOT NULL THEN closing_balance_filled
                    ELSE (
                        SELECT closing_balance_filled 
                        FROM with_forward_fill f2 
                        WHERE f2.id_account = with_forward_fill.id_account 
                        AND f2.id_date > with_forward_fill.id_date 
                        AND f2.closing_balance_filled IS NOT NULL 
                        ORDER BY f2.id_date ASC 
                        LIMIT 1
                    )
                END AS opening_balance,
                closing_balance_filled AS closing_balance,
                movement
            FROM with_forward_fill
        )
        SELECT 
            id_date,
            id_account,
            opening_balance,
            closing_balance,
            CASE WHEN pre_date = 1 OR post_date = 1 THEN 1 ELSE 0 END AS outside_date
        FROM with_opening
    """)
    print("Created view: FactBalance")

    cursor.execute("DROP VIEW IF EXISTS GapReport")
    cursor.execute("""
        CREATE VIEW GapReport AS
        WITH ordered_statements AS (
            SELECT 
                STD_ACCOUNT AS account_type,
                STD_ACCOUNT_NUMBER AS account_number,
                STD_ACCOUNT_HOLDER AS account_holder,
                STD_STATEMENT_DATE AS statement_date,
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
                WHEN account_change = 1 THEN ''
                WHEN opening_balance = prev_closing_balance THEN ''
                ELSE 'GAP'
            END AS gap_flag
        FROM with_prev
    """)
    print("Created view: GapReport")

    cursor.execute("DROP VIEW IF EXISTS FlatTransaction")
    cursor.execute("""
        CREATE VIEW FlatTransaction AS
        SELECT 
            ft.id_date AS transaction_date,
            ds.statement_date,
            ds.filename,
            da.company,
            da.account_type,
            da.account_number,
            da.sortcode,
            da.account_holder,
            ft.transaction_number,
            ft.transaction_credit_or_debit AS CD,
            ft.transaction_type AS type,
            ft.transaction_desc,
            SUBSTR(ft.transaction_desc, 1, 25) AS short_desc,
            ft.value_in,
            ft.value_out,
            ft.value
        FROM FactTransaction ft
        INNER JOIN DimTime dt ON ft.id_date = dt.id_date
        INNER JOIN DimAccount da ON ft.id_account = da.id_account
        INNER JOIN DimStatement ds ON ft.id_statement = ds.id_statement
    """)
    print("Created view: FlatTransaction")

    conn.commit()
    conn.close()
    print(f"\nAll views created successfully in {db_path}")


if __name__ == "__main__":
    create_views(Path(__file__).parent.joinpath("project.db"))
