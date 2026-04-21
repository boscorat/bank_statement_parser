import argparse
import sqlite3
import time
from pathlib import Path

# Whitelist of mart object names that _drop_mart_objects is permitted to DROP.
# This guards against a tampered sqlite_master injecting unexpected object names.
_ALLOWED_MART_NAMES: frozenset[str] = frozenset({"FactBalance", "FactTransaction", "DimStatement", "DimAccount", "DimDate"})


# ---------------------------------------------------------------------------
# Mart table DDL
# ---------------------------------------------------------------------------

_DDL_DIM_DATE = """
    CREATE TABLE DimDate (
        date_int             INTEGER NOT NULL PRIMARY KEY,
        id_date              TEXT    NOT NULL UNIQUE,
        date_local_format    TEXT,
        date_integer         INTEGER,
        year                 INTEGER,
        year_short           INTEGER,
        quarter              INTEGER,
        quarter_name         TEXT,
        month_number         INTEGER,
        month_number_padded  TEXT,
        month_name           TEXT,
        month_abbrv          TEXT,
        period               INTEGER,
        week                 INTEGER,
        year_week            INTEGER,
        day_of_month         INTEGER,
        day_of_year          INTEGER,
        day_of_week          INTEGER,
        weekday              TEXT,
        weekday_abbrv        TEXT,
        weekday_initial      TEXT,
        is_last_day_of_month    INTEGER NOT NULL DEFAULT 0,
        is_last_day_of_quarter  INTEGER NOT NULL DEFAULT 0,
        is_last_day_of_year     INTEGER NOT NULL DEFAULT 0,
        is_weekday              INTEGER NOT NULL DEFAULT 0
    )
"""

_DDL_DIM_ACCOUNT = """
    CREATE TABLE DimAccount (
        account_int     INTEGER NOT NULL PRIMARY KEY,
        id_account      TEXT    NOT NULL UNIQUE,
        company         TEXT,
        account_type    TEXT,
        account_number  TEXT,
        sortcode        TEXT,
        account_holder  TEXT,
        currency        TEXT
    )
"""

_DDL_DIM_STATEMENT = """
    CREATE TABLE DimStatement (
        statement_int   INTEGER NOT NULL PRIMARY KEY,
        id_statement    TEXT    NOT NULL UNIQUE,
        account_int     INTEGER NOT NULL REFERENCES DimAccount(account_int),
        id_account      TEXT    NOT NULL,
        id_batch        TEXT,
        company         TEXT,
        account_type    TEXT,
        account_number  TEXT,
        sortcode        TEXT,
        account_holder  TEXT,
        statement_date  TEXT,
        opening_balance REAL,
        payments_in     REAL,
        payments_out    REAL,
        closing_balance REAL,
        statement_type  TEXT,
        currency        TEXT,
        filename        TEXT,
        batch_time      TEXT
    )
"""

_DDL_FACT_TRANSACTION = """
    CREATE TABLE FactTransaction (
        transaction_int             INTEGER NOT NULL PRIMARY KEY,
        id_transaction              TEXT    NOT NULL UNIQUE,
        statement_int               INTEGER NOT NULL REFERENCES DimStatement(statement_int),
        account_int                 INTEGER NOT NULL REFERENCES DimAccount(account_int),
        date_int                    INTEGER NOT NULL REFERENCES DimDate(date_int),
        id_date                     TEXT    NOT NULL,
        id_account                  TEXT    NOT NULL,
        id_statement                TEXT    NOT NULL,
        transaction_number          INTEGER,
        transaction_credit_or_debit TEXT,
        transaction_type            TEXT,
        transaction_type_cd         TEXT,
        transaction_desc            TEXT,
        opening_balance             REAL,
        value_in                    REAL,
        value_out                   REAL,
        value                       REAL
    )
"""

_DDL_FACT_BALANCE = """
    CREATE TABLE FactBalance (
        date_int         INTEGER NOT NULL REFERENCES DimDate(date_int),
        account_int      INTEGER NOT NULL REFERENCES DimAccount(account_int),
        id_date          TEXT    NOT NULL,
        id_account       TEXT    NOT NULL,
        opening_balance  REAL,
        closing_balance  REAL,
        movement         REAL    NOT NULL DEFAULT 0,
        outside_date     INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (date_int, account_int)
    )
"""


# ---------------------------------------------------------------------------
# Ensure-DDL: CREATE TABLE IF NOT EXISTS variants used by _ensure_mart_structure.
# These are separate from the _DDL_* constants above, which are used by the
# drop-and-recreate build steps and intentionally omit IF NOT EXISTS.
# ---------------------------------------------------------------------------

_DDL_DIM_DATE_ENSURE = """
    CREATE TABLE IF NOT EXISTS DimDate (
        date_int             INTEGER NOT NULL PRIMARY KEY,
        id_date              TEXT    NOT NULL UNIQUE,
        date_local_format    TEXT,
        date_integer         INTEGER,
        year                 INTEGER,
        year_short           INTEGER,
        quarter              INTEGER,
        quarter_name         TEXT,
        month_number         INTEGER,
        month_number_padded  TEXT,
        month_name           TEXT,
        month_abbrv          TEXT,
        period               INTEGER,
        week                 INTEGER,
        year_week            INTEGER,
        day_of_month         INTEGER,
        day_of_year          INTEGER,
        day_of_week          INTEGER,
        weekday              TEXT,
        weekday_abbrv        TEXT,
        weekday_initial      TEXT,
        is_last_day_of_month    INTEGER NOT NULL DEFAULT 0,
        is_last_day_of_quarter  INTEGER NOT NULL DEFAULT 0,
        is_last_day_of_year     INTEGER NOT NULL DEFAULT 0,
        is_weekday              INTEGER NOT NULL DEFAULT 0
    )
"""

_DDL_DIM_ACCOUNT_ENSURE = """
    CREATE TABLE IF NOT EXISTS DimAccount (
        account_int     INTEGER NOT NULL PRIMARY KEY,
        id_account      TEXT    NOT NULL UNIQUE,
        company         TEXT,
        account_type    TEXT,
        account_number  TEXT,
        sortcode        TEXT,
        account_holder  TEXT,
        currency        TEXT
    )
"""

_DDL_DIM_STATEMENT_ENSURE = """
    CREATE TABLE IF NOT EXISTS DimStatement (
        statement_int   INTEGER NOT NULL PRIMARY KEY,
        id_statement    TEXT    NOT NULL UNIQUE,
        account_int     INTEGER NOT NULL REFERENCES DimAccount(account_int),
        id_account      TEXT    NOT NULL,
        id_batch        TEXT,
        company         TEXT,
        account_type    TEXT,
        account_number  TEXT,
        sortcode        TEXT,
        account_holder  TEXT,
        statement_date  TEXT,
        opening_balance REAL,
        payments_in     REAL,
        payments_out    REAL,
        closing_balance REAL,
        statement_type  TEXT,
        currency        TEXT,
        filename        TEXT,
        batch_time      TEXT
    )
"""

_DDL_FACT_TRANSACTION_ENSURE = """
    CREATE TABLE IF NOT EXISTS FactTransaction (
        transaction_int             INTEGER NOT NULL PRIMARY KEY,
        id_transaction              TEXT    NOT NULL UNIQUE,
        statement_int               INTEGER NOT NULL REFERENCES DimStatement(statement_int),
        account_int                 INTEGER NOT NULL REFERENCES DimAccount(account_int),
        date_int                    INTEGER NOT NULL REFERENCES DimDate(date_int),
        id_date                     TEXT    NOT NULL,
        id_account                  TEXT    NOT NULL,
        id_statement                TEXT    NOT NULL,
        transaction_number          INTEGER,
        transaction_credit_or_debit TEXT,
        transaction_type            TEXT,
        transaction_type_cd         TEXT,
        transaction_desc            TEXT,
        opening_balance             REAL,
        value_in                    REAL,
        value_out                   REAL,
        value                       REAL
    )
"""

_DDL_FACT_BALANCE_ENSURE = """
    CREATE TABLE IF NOT EXISTS FactBalance (
        date_int         INTEGER NOT NULL REFERENCES DimDate(date_int),
        account_int      INTEGER NOT NULL REFERENCES DimAccount(account_int),
        id_date          TEXT    NOT NULL,
        id_account       TEXT    NOT NULL,
        opening_balance  REAL,
        closing_balance  REAL,
        movement         REAL    NOT NULL DEFAULT 0,
        outside_date     INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (date_int, account_int)
    )
"""

# Indexes that belong to the mart tables.  All use IF NOT EXISTS so running
# _ensure_mart_structure() on an already-populated database is a no-op.
_MART_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_dd_id_date      ON DimDate (id_date)",
    "CREATE INDEX IF NOT EXISTS idx_ds_id_statement ON DimStatement (id_statement)",
    "CREATE INDEX IF NOT EXISTS idx_ds_account_int  ON DimStatement (account_int)",
    "CREATE INDEX IF NOT EXISTS idx_ft_account_date ON FactTransaction (account_int, date_int)",
    "CREATE INDEX IF NOT EXISTS idx_ft_date_int     ON FactTransaction (date_int)",
    "CREATE INDEX IF NOT EXISTS idx_ft_statement_int ON FactTransaction (statement_int)",
    "CREATE INDEX IF NOT EXISTS idx_fb_account_date ON FactBalance (account_int, date_int)",
    "CREATE INDEX IF NOT EXISTS idx_fb_date_int     ON FactBalance (date_int)",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _drop_mart_objects(conn: sqlite3.Connection) -> None:
    """Drop all mart tables/views (both old view and new table forms)."""
    for name in ("FactBalance", "FactTransaction", "DimStatement", "DimAccount", "DimDate"):
        row = conn.execute("SELECT type FROM sqlite_master WHERE name = ?", (name,)).fetchone()
        if row is not None:
            if name not in _ALLOWED_MART_NAMES:  # defence-in-depth: name is a literal above
                raise ValueError(f"Unexpected mart object name {name!r}; refusing to DROP.")
            kw = "VIEW" if row[0] == "view" else "TABLE"
            conn.execute(f"DROP {kw} IF EXISTS {name}")  # noqa: S608


def _ensure_mart_structure(conn: sqlite3.Connection) -> None:
    """Create mart tables and indexes if they do not already exist.

    This is a structural bootstrap used by the migration path.  It creates
    empty tables so that the rest of the codebase can safely reference them
    (returning empty results) before the first :func:`build_datamart` run.

    The function is idempotent — running it against a database that already
    has all mart tables populated is a no-op (every statement uses
    ``IF NOT EXISTS``).

    The build steps in :func:`build_datamart` still use drop-then-recreate
    for performance; this function is *not* called from there.

    Args:
        conn: Open SQLite connection with write access.
    """
    for ddl in (
        _DDL_DIM_DATE_ENSURE,
        _DDL_DIM_ACCOUNT_ENSURE,
        _DDL_DIM_STATEMENT_ENSURE,
        _DDL_FACT_TRANSACTION_ENSURE,
        _DDL_FACT_BALANCE_ENSURE,
    ):
        conn.execute(ddl)
    for idx_sql in _MART_INDEXES:
        conn.execute(idx_sql)


# ---------------------------------------------------------------------------
# Build steps
# ---------------------------------------------------------------------------


def _build_dim_date(conn: sqlite3.Connection, verbose: bool) -> float:
    t0 = time.monotonic()

    conn.execute(_DDL_DIM_DATE)
    conn.execute("""
        INSERT INTO DimDate (
            date_int, id_date, date_local_format, date_integer,
            year, year_short, quarter, quarter_name,
            month_number, month_number_padded, month_name, month_abbrv,
            period, week, year_week,
            day_of_month, day_of_year, day_of_week,
            weekday, weekday_abbrv, weekday_initial,
            is_last_day_of_month, is_last_day_of_quarter, is_last_day_of_year,
            is_weekday
        )
        WITH date_range AS (
            SELECT
                MIN(sl.STD_TRANSACTION_DATE) AS min_date,
                MAX(sh.STD_STATEMENT_DATE)   AS max_date
            FROM statement_heads sh
            INNER JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
        ),
        recursive_dates AS (
            SELECT date(min_date) AS id_date FROM date_range
            UNION ALL
            SELECT date(id_date, '+1 day')
            FROM recursive_dates, date_range
            WHERE id_date < date(max_date)
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY id_date)                            AS date_int,
            id_date,
            -- %x / %y / %B / %b / %A / %a are not supported by SQLite's strftime;
            -- they return NULL.  Use CASE expressions and arithmetic instead.
            strftime('%d/%m/%Y', id_date)                                   AS date_local_format,
            CAST(strftime('%Y%m%d', id_date) AS INTEGER)                    AS date_integer,
            CAST(strftime('%Y', id_date) AS INTEGER)                        AS year,
            CAST(strftime('%Y', id_date) AS INTEGER) % 100                  AS year_short,
            -- NOTE: quarter/quarter_name deliberately use month number to
            -- preserve parity with the original DimDate view behaviour.
            CAST(strftime('%m', id_date) AS INTEGER)                        AS quarter,
            'Q' || CAST(strftime('%m', id_date) AS INTEGER)                 AS quarter_name,
            CAST(strftime('%m', id_date) AS INTEGER)                        AS month_number,
            strftime('%m', id_date)                                         AS month_number_padded,
            CASE CAST(strftime('%m', id_date) AS INTEGER)
                WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
                WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
                WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
                WHEN 9  THEN 'September' WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
            END                                                             AS month_name,
            CASE CAST(strftime('%m', id_date) AS INTEGER)
                WHEN 1  THEN 'Jan' WHEN 2  THEN 'Feb' WHEN 3  THEN 'Mar'
                WHEN 4  THEN 'Apr' WHEN 5  THEN 'May' WHEN 6  THEN 'Jun'
                WHEN 7  THEN 'Jul' WHEN 8  THEN 'Aug' WHEN 9  THEN 'Sep'
                WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
            END                                                             AS month_abbrv,
            CAST(strftime('%Y%m', id_date) AS INTEGER)                      AS period,
            CAST(strftime('%W', id_date) AS INTEGER)                        AS week,
            CAST(strftime('%Y%W', id_date) AS INTEGER)                      AS year_week,
            CAST(strftime('%d', id_date) AS INTEGER)                        AS day_of_month,
            CAST(strftime('%j', id_date) AS INTEGER)                        AS day_of_year,
            CAST(strftime('%w', id_date) AS INTEGER) + 1                    AS day_of_week,
            CASE CAST(strftime('%w', id_date) AS INTEGER)
                WHEN 0 THEN 'Sunday'    WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'   WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'  WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END                                                             AS weekday,
            CASE CAST(strftime('%w', id_date) AS INTEGER)
                WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
                WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri'
                WHEN 6 THEN 'Sat'
            END                                                             AS weekday_abbrv,
            CASE CAST(strftime('%w', id_date) AS INTEGER)
                WHEN 0 THEN 'S' WHEN 1 THEN 'M' WHEN 2 THEN 'T'
                WHEN 3 THEN 'W' WHEN 4 THEN 'T' WHEN 5 THEN 'F'
                WHEN 6 THEN 'S'
            END                                                             AS weekday_initial,
            CASE
                WHEN CAST(strftime('%d', id_date) AS INTEGER) =
                     CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                THEN 1 ELSE 0
            END AS is_last_day_of_month,
            CASE
                WHEN CAST(strftime('%d', id_date) AS INTEGER) =
                     CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                AND CAST(strftime('%m', id_date) AS INTEGER) % 3 = 0
                THEN 1 ELSE 0
            END AS is_last_day_of_quarter,
            CASE
                WHEN CAST(strftime('%d', id_date) AS INTEGER) =
                     CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                AND CAST(strftime('%m', id_date) AS INTEGER) = 12
                THEN 1 ELSE 0
            END AS is_last_day_of_year,
            CASE WHEN strftime('%w', id_date) NOT IN ('0', '6') THEN 1 ELSE 0 END AS is_weekday
        FROM recursive_dates
    """)
    conn.execute("CREATE INDEX idx_dd_id_date ON DimDate (id_date)")

    elapsed = time.monotonic() - t0
    n = conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
    if verbose:
        print(f"  [1/5] DimDate ({n:,} rows):          {elapsed:.2f}s")
    return elapsed


def _build_dim_account(conn: sqlite3.Connection, verbose: bool) -> float:
    t0 = time.monotonic()

    conn.execute(_DDL_DIM_ACCOUNT)
    conn.execute("""
        INSERT INTO DimAccount (account_int, id_account, company, account_type,
                                account_number, sortcode, account_holder, currency)
        SELECT
            ROW_NUMBER() OVER (ORDER BY id_account) AS account_int,
            id_account, company, account_type, account_number, sortcode, account_holder, currency
        FROM (
            SELECT
                sh.ID_ACCOUNT           AS id_account,
                sh.STD_COMPANY          AS company,
                sh.STD_ACCOUNT          AS account_type,
                sh.STD_ACCOUNT_NUMBER   AS account_number,
                sh.STD_SORTCODE         AS sortcode,
                sh.STD_ACCOUNT_HOLDER   AS account_holder,
                sh.STD_CURRENCY         AS currency,
                ROW_NUMBER() OVER (
                    PARTITION BY sh.ID_ACCOUNT
                    ORDER BY sh.STD_STATEMENT_DATE DESC
                ) AS rn
            FROM statement_heads sh
            WHERE sh.ID_ACCOUNT IS NOT NULL
        )
        WHERE rn = 1
    """)

    elapsed = time.monotonic() - t0
    n = conn.execute("SELECT COUNT(*) FROM DimAccount").fetchone()[0]
    if verbose:
        print(f"  [2/5] DimAccount ({n:,} rows):       {elapsed:.2f}s")
    return elapsed


def _build_dim_statement(conn: sqlite3.Connection, verbose: bool) -> float:
    t0 = time.monotonic()

    conn.execute(_DDL_DIM_STATEMENT)
    conn.execute("""
        INSERT INTO DimStatement (
            statement_int, id_statement, account_int, id_account, id_batch,
            company, account_type, account_number, sortcode, account_holder,
            statement_date, opening_balance, payments_in, payments_out,
            closing_balance, statement_type, currency, filename, batch_time
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY sh.ID_STATEMENT) AS statement_int,
            sh.ID_STATEMENT,
            da.account_int,
            sh.ID_ACCOUNT,
            bl.ID_BATCH,
            sh.STD_COMPANY,
            sh.STD_ACCOUNT,
            sh.STD_ACCOUNT_NUMBER,
            sh.STD_SORTCODE,
            sh.STD_ACCOUNT_HOLDER,
            sh.STD_STATEMENT_DATE,
            sh.STD_OPENING_BALANCE,
            sh.STD_PAYMENTS_IN,
            sh.STD_PAYMENTS_OUT,
            sh.STD_CLOSING_BALANCE,
            sh.STD_STATEMENT_TYPE,
            sh.STD_CURRENCY,
            bl.STD_FILENAME,
            bl.STD_UPDATETIME
        FROM statement_heads sh
        INNER JOIN batch_lines bl ON sh.ID_BATCHLINE = bl.ID_BATCHLINE
        INNER JOIN DimAccount da ON sh.ID_ACCOUNT = da.id_account
    """)
    conn.execute("CREATE INDEX idx_ds_id_statement ON DimStatement (id_statement)")
    conn.execute("CREATE INDEX idx_ds_account_int  ON DimStatement (account_int)")

    elapsed = time.monotonic() - t0
    n = conn.execute("SELECT COUNT(*) FROM DimStatement").fetchone()[0]
    if verbose:
        print(f"  [3/5] DimStatement ({n:,} rows):     {elapsed:.2f}s")
    return elapsed


def _build_fact_transaction(conn: sqlite3.Connection, verbose: bool) -> float:
    t0 = time.monotonic()

    conn.execute(_DDL_FACT_TRANSACTION)
    conn.execute("""
        INSERT INTO FactTransaction (
            transaction_int, id_transaction,
            statement_int, account_int, date_int,
            id_date, id_account, id_statement,
            transaction_number, transaction_credit_or_debit,
            transaction_type, transaction_type_cd, transaction_desc,
            opening_balance, value_in, value_out, value
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY sl.ID_TRANSACTION) AS transaction_int,
            sl.ID_TRANSACTION,
            ds.statement_int,
            da.account_int,
            dd.date_int,
            sl.STD_TRANSACTION_DATE,
            sh.ID_ACCOUNT,
            sh.ID_STATEMENT,
            sl.STD_TRANSACTION_NUMBER,
            sl.STD_CD,
            sl.STD_TRANSACTION_TYPE,
            sl.STD_TRANSACTION_TYPE_CD,
            sl.STD_TRANSACTION_DESC,
            sl.STD_OPENING_BALANCE,
            CAST(sl.STD_PAYMENTS_IN  AS REAL),
            CAST(sl.STD_PAYMENTS_OUT AS REAL),
            CAST(sl.STD_PAYMENTS_IN  AS REAL) - CAST(sl.STD_PAYMENTS_OUT AS REAL)
        FROM statement_lines sl
        INNER JOIN statement_heads sh ON sl.ID_STATEMENT  = sh.ID_STATEMENT
        INNER JOIN DimStatement    ds ON sh.ID_STATEMENT  = ds.id_statement
        INNER JOIN DimAccount      da ON sh.ID_ACCOUNT    = da.id_account
        INNER JOIN DimDate         dd ON sl.STD_TRANSACTION_DATE = dd.id_date
    """)
    conn.execute("CREATE INDEX idx_ft_account_date ON FactTransaction (account_int, date_int)")
    conn.execute("CREATE INDEX idx_ft_date_int      ON FactTransaction (date_int)")
    conn.execute("CREATE INDEX idx_ft_statement_int ON FactTransaction (statement_int)")

    elapsed = time.monotonic() - t0
    n = conn.execute("SELECT COUNT(*) FROM FactTransaction").fetchone()[0]
    if verbose:
        print(f"  [4/5] FactTransaction ({n:,} rows): {elapsed:.2f}s")
    return elapsed


def _build_fact_balance(conn: sqlite3.Connection, verbose: bool) -> float:
    """
    Build FactBalance using the fill-group trick (no correlated subqueries,
    no IGNORE NULLS — which SQLite does not support).

    The grid is built from already-materialised mart tables (DimDate, DimAccount)
    rather than raw source tables, which avoids re-scanning statement_heads /
    statement_lines and benefits from the indexes already built on the mart tables.

    Forward fill:
        fwd_group = COUNT(non-null closing_balance) OVER (PARTITION BY account_int
                    ORDER BY date_int ROWS UNBOUNDED PRECEDING)
        MAX(closing_balance) OVER (PARTITION BY account_int, fwd_group) gives the
        last known balance carried forward — correct even for negative values
        because each group contains exactly one non-null row.

    Backward fill (opening_balance before the first transaction):
        bwd_group = same but ORDER BY date_int DESC
        MAX(closing_balance) OVER (PARTITION BY account_int, bwd_group)
    """
    t0 = time.monotonic()

    # ------------------------------------------------------------------
    # Temp 1: aggregate FactTransaction to one row per (account_int, date_int)
    # ------------------------------------------------------------------
    conn.execute("DROP TABLE IF EXISTS _fb_agg")
    conn.execute("""
        CREATE TEMP TABLE _fb_agg AS
        SELECT
            account_int,
            date_int,
            id_date,
            id_account,
            MAX(STD_CLOSING_BALANCE_FROM_SRC)   AS closing_balance,
            SUM(value)                           AS movement
        FROM (
            -- Pull closing_balance from statement_lines via the transaction
            SELECT
                ft.account_int,
                ft.date_int,
                ft.id_date,
                ft.id_account,
                sl.STD_CLOSING_BALANCE              AS STD_CLOSING_BALANCE_FROM_SRC,
                ft.value
            FROM FactTransaction ft
            INNER JOIN statement_lines sl ON ft.id_transaction = sl.ID_TRANSACTION
        )
        GROUP BY account_int, date_int, id_date, id_account
    """)
    conn.execute("CREATE INDEX idx_fb_agg ON _fb_agg (account_int, date_int)")

    # ------------------------------------------------------------------
    # Temp 2: account bookends from _fb_agg (first/last date_int per account)
    # ------------------------------------------------------------------
    conn.execute("DROP TABLE IF EXISTS _fb_bk")
    conn.execute("""
        CREATE TEMP TABLE _fb_bk AS
        SELECT account_int, MIN(date_int) AS first_did, MAX(date_int) AS last_did
        FROM _fb_agg
        GROUP BY account_int
    """)
    conn.execute("CREATE INDEX idx_fb_bk ON _fb_bk (account_int)")

    # ------------------------------------------------------------------
    # Temp 3: full grid with fill groups
    #
    # Cross-join DimDate × DimAccount (already small, indexed tables)
    # then left-join the aggregated actuals.
    # ------------------------------------------------------------------
    conn.execute("DROP TABLE IF EXISTS _fb_grid")
    conn.execute("""
        CREATE TEMP TABLE _fb_grid AS
        WITH grid AS (
            SELECT
                dd.date_int,
                dd.id_date,
                da.account_int,
                da.id_account,
                CASE WHEN dd.date_int < bk.first_did THEN 1 ELSE 0 END  AS pre_date,
                CASE WHEN dd.date_int > bk.last_did  THEN 1 ELSE 0 END  AS post_date,
                ag.closing_balance,
                COALESCE(ag.movement, 0.0)                               AS movement
            FROM DimDate dd
            CROSS JOIN DimAccount da
            LEFT JOIN _fb_bk bk ON da.account_int = bk.account_int
            LEFT JOIN _fb_agg ag
                   ON dd.date_int    = ag.date_int
                  AND da.account_int = ag.account_int
        )
        SELECT
            date_int, id_date, account_int, id_account,
            pre_date, post_date, closing_balance, movement,
            COUNT(CASE WHEN closing_balance IS NOT NULL THEN 1 END)
                OVER (PARTITION BY account_int ORDER BY date_int
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fwd_group,
            COUNT(CASE WHEN closing_balance IS NOT NULL THEN 1 END)
                OVER (PARTITION BY account_int ORDER BY date_int DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bwd_group
        FROM grid
    """)
    conn.execute("CREATE INDEX idx_fb_grid ON _fb_grid (account_int, fwd_group, bwd_group)")

    # ------------------------------------------------------------------
    # Populate FactBalance
    # ------------------------------------------------------------------
    conn.execute(_DDL_FACT_BALANCE)
    conn.execute("""
        INSERT INTO FactBalance (
            date_int, account_int, id_date, id_account,
            opening_balance, closing_balance, movement, outside_date
        )
        SELECT
            g.date_int,
            g.account_int,
            g.id_date,
            g.id_account,
            CASE WHEN g.pre_date = 1 THEN NULL
                 ELSE MAX(g.closing_balance)
                      OVER (PARTITION BY g.account_int, g.bwd_group)
            END                                                              AS opening_balance,
            CASE WHEN g.pre_date = 1 THEN NULL
                 ELSE MAX(g.closing_balance)
                      OVER (PARTITION BY g.account_int, g.fwd_group)
            END                                                              AS closing_balance,
            g.movement,
            CASE WHEN g.pre_date = 1 OR g.post_date = 1 THEN 1 ELSE 0 END  AS outside_date
        FROM _fb_grid g
    """)
    conn.execute("CREATE INDEX idx_fb_account_date ON FactBalance (account_int, date_int)")
    conn.execute("CREATE INDEX idx_fb_date_int      ON FactBalance (date_int)")

    # Cleanup temp tables
    _ALLOWED_TEMP: frozenset[str] = frozenset({"_fb_agg", "_fb_bk", "_fb_grid"})
    for tbl in ("_fb_agg", "_fb_bk", "_fb_grid"):
        if tbl not in _ALLOWED_TEMP:
            raise ValueError(f"Unexpected temp table name {tbl!r}; refusing to DROP.")
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")  # noqa: S608

    elapsed = time.monotonic() - t0
    n = conn.execute("SELECT COUNT(*) FROM FactBalance").fetchone()[0]
    if verbose:
        print(f"  [5/5] FactBalance ({n:,} rows):      {elapsed:.2f}s")
    return elapsed


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def build_datamart(db_path: Path, verbose: bool = True) -> dict:
    """
    Empty and rebuild all mart tables (DimDate, DimAccount, DimStatement,
    FactTransaction, FactBalance) from the raw source tables.

    Each mart table is a real SQLite table with an integer surrogate primary key.
    FactBalance is built last and references the already-populated mart tables
    for the CROSS JOIN grid, avoiding re-scans of the raw source tables.

    Returns a dict with per-step timings and the total elapsed time.
    """
    t_total = time.monotonic()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -65536")  # 64 MB

    if verbose:
        print(f"Building data mart in {db_path} ...")

    _drop_mart_objects(conn)

    timings: dict[str, float] = {}
    timings["DimDate"] = _build_dim_date(conn, verbose)
    timings["DimAccount"] = _build_dim_account(conn, verbose)
    timings["DimStatement"] = _build_dim_statement(conn, verbose)
    timings["FactTransaction"] = _build_fact_transaction(conn, verbose)
    timings["FactBalance"] = _build_fact_balance(conn, verbose)

    conn.commit()
    # Checkpoint the WAL fully so the .db file is self-contained and the
    # -wal / -shm side-files are left empty (or absent) after the build.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    timings["total"] = time.monotonic() - t_total
    if verbose:
        print(f"\n  Total time: {timings['total']:.2f}s")

    return timings


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from bank_statement_parser.modules.paths import ProjectPaths  # noqa: PLC0415

    parser = argparse.ArgumentParser(description="Build (or rebuild) the data mart tables from raw source data.")
    parser.add_argument(
        "--db",
        type=Path,
        default=ProjectPaths.resolve().project_db,
        help="Path to the SQLite database (default: project.db)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )
    args = parser.parse_args()

    if not args.db.exists():
        raise FileNotFoundError(f"Database not found: {args.db}")

    result = build_datamart(db_path=args.db, verbose=not args.quiet)
    print("Done.")
