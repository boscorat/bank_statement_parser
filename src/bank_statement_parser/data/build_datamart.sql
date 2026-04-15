-- =============================================================================
-- build_datamart.sql
-- Equivalent of build_datamart.py — drops and rebuilds all mart tables.
-- Run directly against the SQLite database, e.g.:
--   sqlite3 project.db < build_datamart.sql
--   sqlite3 project.db ".read build_datamart.sql"
-- =============================================================================

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -65536;  -- 64 MB


-- ---------------------------------------------------------------------------
-- Drop existing mart objects (in reverse dependency order)
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS FactBalance;
DROP TABLE IF EXISTS FactTransaction;
DROP TABLE IF EXISTS DimStatement;
DROP TABLE IF EXISTS DimAccount;
DROP TABLE IF EXISTS DimDate;

-- Also drop in case any of the above were previously created as VIEWs
DROP VIEW IF EXISTS FactBalance;
DROP VIEW IF EXISTS FactTransaction;
DROP VIEW IF EXISTS DimStatement;
DROP VIEW IF EXISTS DimAccount;
DROP VIEW IF EXISTS DimDate;


-- ---------------------------------------------------------------------------
-- [1/5] DimDate
-- ---------------------------------------------------------------------------

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
);

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
    END                                                             AS is_last_day_of_month,
    CASE
        WHEN CAST(strftime('%d', id_date) AS INTEGER) =
             CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
        AND CAST(strftime('%m', id_date) AS INTEGER) % 3 = 0
        THEN 1 ELSE 0
    END                                                             AS is_last_day_of_quarter,
    CASE
        WHEN CAST(strftime('%d', id_date) AS INTEGER) =
             CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
        AND CAST(strftime('%m', id_date) AS INTEGER) = 12
        THEN 1 ELSE 0
    END                                                             AS is_last_day_of_year,
    CASE WHEN strftime('%w', id_date) NOT IN ('0', '6') THEN 1 ELSE 0 END AS is_weekday
FROM recursive_dates;

CREATE INDEX idx_dt_id_date ON DimDate (id_date);


-- ---------------------------------------------------------------------------
-- [2/5] DimAccount
-- ---------------------------------------------------------------------------

CREATE TABLE DimAccount (
    account_int     INTEGER NOT NULL PRIMARY KEY,
    id_account      TEXT    NOT NULL UNIQUE,
    company         TEXT,
    account_type    TEXT,
    account_number  TEXT,
    sortcode        TEXT,
    account_holder  TEXT,
    currency        TEXT
);

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
WHERE rn = 1;


-- ---------------------------------------------------------------------------
-- [3/5] DimStatement
-- ---------------------------------------------------------------------------

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
);

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
INNER JOIN DimAccount da ON sh.ID_ACCOUNT = da.id_account;

CREATE INDEX idx_ds_id_statement ON DimStatement (id_statement);
CREATE INDEX idx_ds_account_int  ON DimStatement (account_int);


-- ---------------------------------------------------------------------------
-- [4/5] FactTransaction
-- ---------------------------------------------------------------------------

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
);

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
INNER JOIN DimDate         dd ON sl.STD_TRANSACTION_DATE = dd.id_date;

CREATE INDEX idx_ft_account_date ON FactTransaction (account_int, date_int);
CREATE INDEX idx_ft_date_int     ON FactTransaction (date_int);
CREATE INDEX idx_ft_statement_int ON FactTransaction (statement_int);


-- ---------------------------------------------------------------------------
-- [5/5] FactBalance
--
-- Uses the fill-group trick to forward/backward fill closing balances across
-- the full DimDate x DimAccount grid without correlated subqueries or
-- IGNORE NULLS (unsupported in SQLite).
-- ---------------------------------------------------------------------------

-- Temp 1: aggregate FactTransaction to one row per (account_int, date_int)
DROP TABLE IF EXISTS _fb_agg;
CREATE TEMP TABLE _fb_agg AS
SELECT
    account_int,
    date_int,
    id_date,
    id_account,
    MAX(STD_CLOSING_BALANCE_FROM_SRC)   AS closing_balance,
    SUM(value)                           AS movement
FROM (
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
GROUP BY account_int, date_int, id_date, id_account;

CREATE INDEX idx_fb_agg ON _fb_agg (account_int, date_int);

-- Temp 2: account bookends (first/last date_int per account)
DROP TABLE IF EXISTS _fb_bk;
CREATE TEMP TABLE _fb_bk AS
SELECT account_int, MIN(date_int) AS first_did, MAX(date_int) AS last_did
FROM _fb_agg
GROUP BY account_int;

CREATE INDEX idx_fb_bk ON _fb_bk (account_int);

-- Temp 3: full grid with fill groups
DROP TABLE IF EXISTS _fb_grid;
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
FROM grid;

CREATE INDEX idx_fb_grid ON _fb_grid (account_int, fwd_group, bwd_group);

-- Populate FactBalance
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
);

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
FROM _fb_grid g;

CREATE INDEX idx_fb_account_date ON FactBalance (account_int, date_int);
CREATE INDEX idx_fb_date_int     ON FactBalance (date_int);

-- Cleanup temp tables
DROP TABLE IF EXISTS _fb_agg;
DROP TABLE IF EXISTS _fb_bk;
DROP TABLE IF EXISTS _fb_grid;


-- ---------------------------------------------------------------------------
-- Commit and checkpoint
-- ---------------------------------------------------------------------------

COMMIT;
PRAGMA wal_checkpoint(TRUNCATE);
