"""
Tests that validate the data mart (DimTime, DimAccount, DimStatement,
FactTransaction, FactBalance) against the raw source tables.

Each test class covers one mart table or one cross-cutting concern.
The tests require a populated project.db (run create_project_db.py,
mock_project_data.py, and build_datamart.py before running the suite).

Run with:
    pytest tests/test_datamart.py -v
"""

import datetime
import sqlite3
from pathlib import Path
from typing import Any

import pytest

DB_PATH = Path(__file__).parent.parent / "src" / "bank_statement_parser" / "data" / "project.db"
FLOAT_TOL = 0.005  # absolute tolerance for monetary comparisons


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    """Read-only connection to project.db, shared across all tests in the module."""
    if not DB_PATH.exists():
        pytest.skip(f"project.db not found at {DB_PATH} — run the setup scripts first")
    connection = sqlite3.connect(str(DB_PATH))
    yield connection
    connection.close()


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# DimTime
# ---------------------------------------------------------------------------


class TestDimTime:
    def test_row_count_matches_date_range(self, conn):
        """DimTime has exactly one row per calendar day from the earliest
        transaction date to the latest statement date (inclusive)."""
        min_date_str = _scalar(conn, "SELECT MIN(STD_TRANSACTION_DATE) FROM statement_lines")
        max_date_str = _scalar(conn, "SELECT MAX(STD_STATEMENT_DATE) FROM statement_heads")
        expected = (datetime.date.fromisoformat(max_date_str) - datetime.date.fromisoformat(min_date_str)).days + 1
        actual = _scalar(conn, "SELECT COUNT(*) FROM DimTime")
        assert actual == expected

    def test_date_spine_is_contiguous(self, conn):
        """There are no missing days in the DimTime date spine."""
        gaps = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT id_date,
                       LAG(id_date) OVER (ORDER BY id_date) AS prev_date
                FROM DimTime
            )
            WHERE prev_date IS NOT NULL
            AND julianday(id_date) - julianday(prev_date) != 1
        """,
        )
        assert gaps == 0

    def test_min_date_equals_earliest_transaction(self, conn):
        raw_min = _scalar(conn, "SELECT MIN(STD_TRANSACTION_DATE) FROM statement_lines")
        mart_min = _scalar(conn, "SELECT MIN(id_date) FROM DimTime")
        assert mart_min == raw_min

    def test_max_date_equals_latest_statement(self, conn):
        raw_max = _scalar(conn, "SELECT MAX(STD_STATEMENT_DATE) FROM statement_heads")
        mart_max = _scalar(conn, "SELECT MAX(id_date) FROM DimTime")
        assert mart_max == raw_max

    def test_no_null_columns(self, conn):
        """Every column in DimTime is fully populated — no NULLs anywhere."""
        cols = [c[1] for c in conn.execute("PRAGMA table_info(DimTime)").fetchall()]
        null_counts = {col: _scalar(conn, f"SELECT COUNT(*) FROM DimTime WHERE {col} IS NULL") for col in cols}
        nulls = {col: n for col, n in null_counts.items() if n > 0}
        assert nulls == {}, f"Columns with NULLs: {nulls}"

    def test_time_id_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)          FROM DimTime")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT time_id) FROM DimTime")
        assert total == distinct

    def test_id_date_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)           FROM DimTime")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT id_date) FROM DimTime")
        assert total == distinct

    def test_year_derived_correctly(self, conn):
        """year column matches the year portion of id_date for every row."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE year != CAST(strftime('%Y', id_date) AS INTEGER)
        """,
        )
        assert mismatches == 0

    def test_year_short_derived_correctly(self, conn):
        """year_short == year % 100 for every row."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE year_short != year % 100
        """,
        )
        assert mismatches == 0

    def test_month_number_matches_strftime(self, conn):
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE month_number != CAST(strftime('%m', id_date) AS INTEGER)
        """,
        )
        assert mismatches == 0

    def test_month_name_matches_month_number(self, conn):
        """month_name is consistent with month_number for all 12 months."""
        expected = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }
        rows = conn.execute("SELECT DISTINCT month_number, month_name FROM DimTime ORDER BY month_number").fetchall()
        actual = {r[0]: r[1] for r in rows}
        for num, name in expected.items():
            if num in actual:
                assert actual[num] == name, f"month {num}: expected {name!r}, got {actual[num]!r}"

    def test_month_abbrv_matches_month_number(self, conn):
        expected = {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
        }
        rows = conn.execute("SELECT DISTINCT month_number, month_abbrv FROM DimTime ORDER BY month_number").fetchall()
        actual = {r[0]: r[1] for r in rows}
        for num, abbrv in expected.items():
            if num in actual:
                assert actual[num] == abbrv, f"month {num}: expected {abbrv!r}, got {actual[num]!r}"

    def test_weekday_name_matches_strftime_w(self, conn):
        """weekday name is consistent with strftime('%w') day-of-week number."""
        # %w: 0=Sunday, 1=Monday, ..., 6=Saturday
        expected = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday"}
        rows = conn.execute("""
            SELECT DISTINCT CAST(strftime('%w', id_date) AS INTEGER) AS dow, weekday
            FROM DimTime ORDER BY dow
        """).fetchall()
        for dow, name in rows:
            assert name == expected[dow], f"dow {dow}: expected {expected[dow]!r}, got {name!r}"

    def test_weekday_abbrv_is_first_three_chars_of_weekday(self, conn):
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE weekday_abbrv != SUBSTR(weekday, 1, 3)
        """,
        )
        assert mismatches == 0

    def test_day_of_week_is_one_indexed(self, conn):
        """day_of_week runs 1–7 (Sunday=1 through Saturday=7)."""
        min_dow = _scalar(conn, "SELECT MIN(day_of_week) FROM DimTime")
        max_dow = _scalar(conn, "SELECT MAX(day_of_week) FROM DimTime")
        assert min_dow == 1
        assert max_dow == 7

    def test_is_weekday_consistent_with_day_of_week(self, conn):
        """is_weekday=1 iff day_of_week is 2–6 (Mon–Fri)."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE is_weekday != CASE WHEN day_of_week BETWEEN 2 AND 6 THEN 1 ELSE 0 END
        """,
        )
        assert mismatches == 0

    def test_is_last_day_of_month_correct(self, conn):
        """is_last_day_of_month=1 iff day_of_month equals the last day of that month."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimTime
            WHERE is_last_day_of_month != CASE
                WHEN day_of_month =
                     CAST(strftime('%d', date(id_date, 'start of month', '+1 month', '-1 day')) AS INTEGER)
                THEN 1 ELSE 0
            END
        """,
        )
        assert mismatches == 0


# ---------------------------------------------------------------------------
# DimAccount
# ---------------------------------------------------------------------------


class TestDimAccount:
    def test_row_count_matches_distinct_accounts(self, conn):
        """One DimAccount row per unique ID_ACCOUNT in statement_heads."""
        raw = _scalar(conn, "SELECT COUNT(DISTINCT ID_ACCOUNT) FROM statement_heads WHERE ID_ACCOUNT IS NOT NULL")
        mart = _scalar(conn, "SELECT COUNT(*) FROM DimAccount")
        assert mart == raw

    def test_account_id_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)               FROM DimAccount")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT account_id) FROM DimAccount")
        assert total == distinct

    def test_id_account_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)                FROM DimAccount")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT id_account) FROM DimAccount")
        assert total == distinct

    def test_no_null_id_account(self, conn):
        nulls = _scalar(conn, "SELECT COUNT(*) FROM DimAccount WHERE id_account IS NULL")
        assert nulls == 0

    def test_all_raw_accounts_present(self, conn):
        """Every ID_ACCOUNT from statement_heads appears in DimAccount."""
        missing = _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT sh.ID_ACCOUNT)
            FROM statement_heads sh
            LEFT JOIN DimAccount da ON sh.ID_ACCOUNT = da.id_account
            WHERE sh.ID_ACCOUNT IS NOT NULL AND da.id_account IS NULL
        """,
        )
        assert missing == 0

    def test_per_account_statement_counts(self, conn):
        """DimStatement count per account matches statement_heads count per account."""
        raw_counts = dict(
            conn.execute("""
            SELECT ID_ACCOUNT, COUNT(*) FROM statement_heads
            GROUP BY ID_ACCOUNT
        """).fetchall()
        )
        mart_counts = dict(
            conn.execute("""
            SELECT da.id_account, COUNT(*)
            FROM DimStatement ds
            JOIN DimAccount da ON ds.account_id = da.account_id
            GROUP BY da.id_account
        """).fetchall()
        )
        assert mart_counts == raw_counts


# ---------------------------------------------------------------------------
# DimStatement
# ---------------------------------------------------------------------------


class TestDimStatement:
    def test_row_count_matches_statement_heads(self, conn):
        raw = _scalar(conn, "SELECT COUNT(*) FROM statement_heads")
        mart = _scalar(conn, "SELECT COUNT(*) FROM DimStatement")
        assert mart == raw

    def test_statement_id_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)                  FROM DimStatement")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT statement_id) FROM DimStatement")
        assert total == distinct

    def test_id_statement_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)                   FROM DimStatement")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT id_statement) FROM DimStatement")
        assert total == distinct

    def test_all_raw_statements_present(self, conn):
        missing = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM statement_heads sh
            LEFT JOIN DimStatement ds ON sh.ID_STATEMENT = ds.id_statement
            WHERE ds.id_statement IS NULL
        """,
        )
        assert missing == 0

    def test_total_payments_in_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(STD_PAYMENTS_IN), 4) FROM statement_heads")
        mart = _scalar(conn, "SELECT ROUND(SUM(payments_in),  4) FROM DimStatement")
        assert abs(raw - mart) < FLOAT_TOL

    def test_total_payments_out_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(STD_PAYMENTS_OUT), 4) FROM statement_heads")
        mart = _scalar(conn, "SELECT ROUND(SUM(payments_out),   4) FROM DimStatement")
        assert abs(raw - mart) < FLOAT_TOL

    def test_total_opening_balance_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(STD_OPENING_BALANCE), 4) FROM statement_heads")
        mart = _scalar(conn, "SELECT ROUND(SUM(opening_balance),   4) FROM DimStatement")
        assert abs(raw - mart) < FLOAT_TOL

    def test_total_closing_balance_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(STD_CLOSING_BALANCE), 4) FROM statement_heads")
        mart = _scalar(conn, "SELECT ROUND(SUM(closing_balance),   4) FROM DimStatement")
        assert abs(raw - mart) < FLOAT_TOL

    def test_payments_in_per_account_matches_raw(self, conn):
        raw = dict(
            conn.execute("""
            SELECT ID_ACCOUNT, ROUND(SUM(STD_PAYMENTS_IN), 4)
            FROM statement_heads GROUP BY ID_ACCOUNT
        """).fetchall()
        )
        mart = dict(
            conn.execute("""
            SELECT da.id_account, ROUND(SUM(ds.payments_in), 4)
            FROM DimStatement ds
            JOIN DimAccount da ON ds.account_id = da.account_id
            GROUP BY da.id_account
        """).fetchall()
        )
        for acct, raw_val in raw.items():
            assert acct in mart, f"Account {acct} missing from mart"
            assert abs(raw_val - mart[acct]) < FLOAT_TOL, f"payments_in mismatch for {acct}: raw={raw_val}, mart={mart[acct]}"

    def test_all_statements_linked_to_known_account(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimStatement ds
            LEFT JOIN DimAccount da ON ds.account_id = da.account_id
            WHERE da.account_id IS NULL
        """,
        )
        assert orphans == 0


# ---------------------------------------------------------------------------
# FactTransaction
# ---------------------------------------------------------------------------


class TestFactTransaction:
    def test_row_count_matches_statement_lines(self, conn):
        raw = _scalar(conn, "SELECT COUNT(*) FROM statement_lines")
        mart = _scalar(conn, "SELECT COUNT(*) FROM FactTransaction")
        assert mart == raw

    def test_transaction_id_is_unique(self, conn):
        total = _scalar(conn, "SELECT COUNT(*)                     FROM FactTransaction")
        distinct = _scalar(conn, "SELECT COUNT(DISTINCT transaction_id) FROM FactTransaction")
        assert total == distinct

    def test_id_transaction_covers_all_raw(self, conn):
        """Every ID_TRANSACTION from statement_lines appears in FactTransaction."""
        missing = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM statement_lines sl
            LEFT JOIN FactTransaction ft ON sl.ID_TRANSACTION = ft.id_transaction
            WHERE ft.id_transaction IS NULL
        """,
        )
        assert missing == 0

    def test_total_value_in_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(CAST(STD_PAYMENTS_IN  AS REAL)), 4) FROM statement_lines")
        mart = _scalar(conn, "SELECT ROUND(SUM(value_in),  4) FROM FactTransaction")
        assert abs(raw - mart) < FLOAT_TOL

    def test_total_value_out_matches_raw(self, conn):
        raw = _scalar(conn, "SELECT ROUND(SUM(CAST(STD_PAYMENTS_OUT AS REAL)), 4) FROM statement_lines")
        mart = _scalar(conn, "SELECT ROUND(SUM(value_out), 4) FROM FactTransaction")
        assert abs(raw - mart) < FLOAT_TOL

    def test_total_net_value_equals_in_minus_out(self, conn):
        total_in = _scalar(conn, "SELECT SUM(value_in)  FROM FactTransaction")
        total_out = _scalar(conn, "SELECT SUM(value_out) FROM FactTransaction")
        total_val = _scalar(conn, "SELECT SUM(value)     FROM FactTransaction")
        assert abs(total_val - (total_in - total_out)) < FLOAT_TOL

    def test_value_in_per_account_matches_raw(self, conn):
        raw = dict(
            conn.execute("""
            SELECT sh.ID_ACCOUNT, ROUND(SUM(CAST(sl.STD_PAYMENTS_IN AS REAL)), 4)
            FROM statement_lines sl
            JOIN statement_heads sh ON sl.ID_STATEMENT = sh.ID_STATEMENT
            GROUP BY sh.ID_ACCOUNT
        """).fetchall()
        )
        mart = dict(
            conn.execute("""
            SELECT da.id_account, ROUND(SUM(ft.value_in), 4)
            FROM FactTransaction ft
            JOIN DimAccount da ON ft.account_id = da.account_id
            GROUP BY da.id_account
        """).fetchall()
        )
        for acct, raw_val in raw.items():
            assert acct in mart, f"Account {acct} missing from mart"
            assert abs(raw_val - mart[acct]) < FLOAT_TOL, f"value_in mismatch for {acct}: raw={raw_val}, mart={mart[acct]}"

    def test_value_out_per_account_matches_raw(self, conn):
        raw = dict(
            conn.execute("""
            SELECT sh.ID_ACCOUNT, ROUND(SUM(CAST(sl.STD_PAYMENTS_OUT AS REAL)), 4)
            FROM statement_lines sl
            JOIN statement_heads sh ON sl.ID_STATEMENT = sh.ID_STATEMENT
            GROUP BY sh.ID_ACCOUNT
        """).fetchall()
        )
        mart = dict(
            conn.execute("""
            SELECT da.id_account, ROUND(SUM(ft.value_out), 4)
            FROM FactTransaction ft
            JOIN DimAccount da ON ft.account_id = da.account_id
            GROUP BY da.id_account
        """).fetchall()
        )
        for acct, raw_val in raw.items():
            assert acct in mart, f"Account {acct} missing from mart"
            assert abs(raw_val - mart[acct]) < FLOAT_TOL, f"value_out mismatch for {acct}: raw={raw_val}, mart={mart[acct]}"

    def test_transaction_count_per_account_matches_raw(self, conn):
        raw = dict(
            conn.execute("""
            SELECT sh.ID_ACCOUNT, COUNT(*)
            FROM statement_lines sl
            JOIN statement_heads sh ON sl.ID_STATEMENT = sh.ID_STATEMENT
            GROUP BY sh.ID_ACCOUNT
        """).fetchall()
        )
        mart = dict(
            conn.execute("""
            SELECT da.id_account, COUNT(*)
            FROM FactTransaction ft
            JOIN DimAccount da ON ft.account_id = da.account_id
            GROUP BY da.id_account
        """).fetchall()
        )
        assert mart == raw

    def test_transaction_count_per_month_matches_raw(self, conn):
        """Transaction count per year-month matches between raw and mart."""
        raw = dict(
            conn.execute("""
            SELECT CAST(strftime('%Y%m', STD_TRANSACTION_DATE) AS INTEGER) AS period,
                   COUNT(*)
            FROM statement_lines
            GROUP BY period
        """).fetchall()
        )
        mart = dict(
            conn.execute("""
            SELECT dt.period, COUNT(*)
            FROM FactTransaction ft
            JOIN DimTime dt ON ft.time_id = dt.time_id
            GROUP BY dt.period
        """).fetchall()
        )
        assert mart == raw

    def test_fk_account_id_no_orphans(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            LEFT JOIN DimAccount da ON ft.account_id = da.account_id
            WHERE da.account_id IS NULL
        """,
        )
        assert orphans == 0

    def test_fk_time_id_no_orphans(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            LEFT JOIN DimTime dt ON ft.time_id = dt.time_id
            WHERE dt.time_id IS NULL
        """,
        )
        assert orphans == 0

    def test_fk_statement_id_no_orphans(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            LEFT JOIN DimStatement ds ON ft.statement_id = ds.statement_id
            WHERE ds.statement_id IS NULL
        """,
        )
        assert orphans == 0

    def test_id_date_consistent_with_time_id(self, conn):
        """id_date on FactTransaction matches the id_date of the joined DimTime row."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            JOIN DimTime dt ON ft.time_id = dt.time_id
            WHERE ft.id_date != dt.id_date
        """,
        )
        assert mismatches == 0


# ---------------------------------------------------------------------------
# FactBalance
# ---------------------------------------------------------------------------


class TestFactBalance:
    def test_row_count_equals_accounts_times_date_spine(self, conn):
        """FactBalance has exactly one row per (account, day) across the full spine."""
        n_accounts = _scalar(conn, "SELECT COUNT(*) FROM DimAccount")
        n_days = _scalar(conn, "SELECT COUNT(*) FROM DimTime")
        expected = n_accounts * n_days
        actual = _scalar(conn, "SELECT COUNT(*) FROM FactBalance")
        assert actual == expected

    def test_no_duplicate_time_account_pairs(self, conn):
        dups = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT time_id, account_id, COUNT(*) AS c
                FROM FactBalance
                GROUP BY time_id, account_id
                HAVING c > 1
            )
        """,
        )
        assert dups == 0

    def test_fk_account_id_no_orphans(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance fb
            LEFT JOIN DimAccount da ON fb.account_id = da.account_id
            WHERE da.account_id IS NULL
        """,
        )
        assert orphans == 0

    def test_fk_time_id_no_orphans(self, conn):
        orphans = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance fb
            LEFT JOIN DimTime dt ON fb.time_id = dt.time_id
            WHERE dt.time_id IS NULL
        """,
        )
        assert orphans == 0

    def test_no_null_closing_balance_outside_pre_date_rows(self, conn):
        """Closing balance is NULL only on pre-date rows (before first transaction)."""
        bad = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance
            WHERE closing_balance IS NULL AND outside_date = 0
        """,
        )
        assert bad == 0

    def test_movement_sum_per_account_matches_fact_transaction(self, conn):
        """SUM(movement) per account in FactBalance equals SUM(value) in FactTransaction."""
        fb = dict(
            conn.execute("""
            SELECT id_account, ROUND(SUM(movement), 4) FROM FactBalance GROUP BY id_account
        """).fetchall()
        )
        ft = dict(
            conn.execute("""
            SELECT id_account, ROUND(SUM(value), 4) FROM FactTransaction GROUP BY id_account
        """).fetchall()
        )
        for acct, ft_val in ft.items():
            assert acct in fb, f"Account {acct} missing from FactBalance"
            assert abs(ft_val - fb[acct]) < FLOAT_TOL, f"movement mismatch for {acct}: FactTransaction={ft_val}, FactBalance={fb[acct]}"

    def test_closing_balance_on_last_transaction_date_matches_raw(self, conn):
        """FactBalance closing_balance on each account's last transaction date
        matches MAX(STD_CLOSING_BALANCE) from statement_lines on that same date."""
        rows = conn.execute("""
            WITH last_dates AS (
                SELECT sh.ID_ACCOUNT, MAX(sl.STD_TRANSACTION_DATE) AS last_date
                FROM statement_lines sl
                JOIN statement_heads sh ON sl.ID_STATEMENT = sh.ID_STATEMENT
                GROUP BY sh.ID_ACCOUNT
            )
            SELECT ld.ID_ACCOUNT, ld.last_date,
                   MAX(sl.STD_CLOSING_BALANCE) AS raw_closing
            FROM last_dates ld
            JOIN statement_heads sh ON ld.ID_ACCOUNT = sh.ID_ACCOUNT
            JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
                                    AND sl.STD_TRANSACTION_DATE = ld.last_date
            GROUP BY ld.ID_ACCOUNT, ld.last_date
        """).fetchall()

        for id_account, last_date, raw_closing in rows:
            mart_closing = _scalar(
                conn,
                """
                SELECT fb.closing_balance
                FROM FactBalance fb
                JOIN DimAccount da ON fb.account_id = da.account_id
                JOIN DimTime    dt ON fb.time_id    = dt.time_id
                WHERE da.id_account = ? AND dt.id_date = ?
            """,
                (id_account, last_date),
            )
            assert mart_closing is not None, f"No FactBalance row for {id_account} on {last_date}"
            assert abs(raw_closing - mart_closing) < FLOAT_TOL, (
                f"closing_balance mismatch for {id_account} on {last_date}: raw={raw_closing}, mart={mart_closing}"
            )

    def test_closing_balance_on_first_transaction_date_matches_raw(self, conn):
        """FactBalance closing_balance on each account's first transaction date
        matches MAX(STD_CLOSING_BALANCE) from statement_lines on that date."""
        rows = conn.execute("""
            WITH first_dates AS (
                SELECT sh.ID_ACCOUNT, MIN(sl.STD_TRANSACTION_DATE) AS first_date
                FROM statement_lines sl
                JOIN statement_heads sh ON sl.ID_STATEMENT = sh.ID_STATEMENT
                GROUP BY sh.ID_ACCOUNT
            )
            SELECT fd.ID_ACCOUNT, fd.first_date,
                   MAX(sl.STD_CLOSING_BALANCE) AS raw_closing
            FROM first_dates fd
            JOIN statement_heads sh ON fd.ID_ACCOUNT = sh.ID_ACCOUNT
            JOIN statement_lines sl ON sh.ID_STATEMENT = sl.ID_STATEMENT
                                    AND sl.STD_TRANSACTION_DATE = fd.first_date
            GROUP BY fd.ID_ACCOUNT, fd.first_date
        """).fetchall()

        for id_account, first_date, raw_closing in rows:
            mart_closing = _scalar(
                conn,
                """
                SELECT fb.closing_balance
                FROM FactBalance fb
                JOIN DimAccount da ON fb.account_id = da.account_id
                JOIN DimTime    dt ON fb.time_id    = dt.time_id
                WHERE da.id_account = ? AND dt.id_date = ?
            """,
                (id_account, first_date),
            )
            assert mart_closing is not None, f"No FactBalance row for {id_account} on {first_date}"
            assert abs(raw_closing - mart_closing) < FLOAT_TOL, (
                f"closing_balance mismatch for {id_account} on {first_date}: raw={raw_closing}, mart={mart_closing}"
            )

    def test_no_movement_on_gap_days(self, conn):
        """Days with no transactions in FactTransaction have movement=0 in FactBalance."""
        bad = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance fb
            WHERE fb.movement != 0
            AND NOT EXISTS (
                SELECT 1 FROM FactTransaction ft
                WHERE ft.account_id = fb.account_id
                AND ft.time_id = fb.time_id
            )
        """,
        )
        assert bad == 0

    def test_outside_date_flag_set_correctly(self, conn):
        """outside_date=1 only for rows before the first or after the last
        transaction date of that account."""
        bad = _scalar(
            conn,
            """
            WITH bookends AS (
                SELECT account_id,
                       MIN(time_id) AS first_tid,
                       MAX(time_id) AS last_tid
                FROM FactTransaction
                GROUP BY account_id
            )
            SELECT COUNT(*) FROM FactBalance fb
            JOIN bookends bk ON fb.account_id = bk.account_id
            WHERE fb.outside_date != CASE
                WHEN fb.time_id < bk.first_tid OR fb.time_id > bk.last_tid
                THEN 1 ELSE 0
            END
        """,
        )
        assert bad == 0

    def test_id_date_consistent_with_time_id(self, conn):
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance fb
            JOIN DimTime dt ON fb.time_id = dt.time_id
            WHERE fb.id_date != dt.id_date
        """,
        )
        assert mismatches == 0

    def test_id_account_consistent_with_account_id(self, conn):
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactBalance fb
            JOIN DimAccount da ON fb.account_id = da.account_id
            WHERE fb.id_account != da.id_account
        """,
        )
        assert mismatches == 0


# ---------------------------------------------------------------------------
# Cross-mart surrogate key consistency
# ---------------------------------------------------------------------------


class TestSurrogateKeyConsistency:
    def test_fact_transaction_account_id_matches_dim_account(self, conn):
        """FactTransaction.id_account matches DimAccount.id_account via account_id FK."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            JOIN DimAccount da ON ft.account_id = da.account_id
            WHERE ft.id_account != da.id_account
        """,
        )
        assert mismatches == 0

    def test_fact_transaction_id_statement_matches_dim_statement(self, conn):
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM FactTransaction ft
            JOIN DimStatement ds ON ft.statement_id = ds.statement_id
            WHERE ft.id_statement != ds.id_statement
        """,
        )
        assert mismatches == 0

    def test_dim_statement_account_id_matches_dim_account(self, conn):
        """Every DimStatement.account_id resolves to the correct id_account."""
        mismatches = _scalar(
            conn,
            """
            SELECT COUNT(*) FROM DimStatement ds
            JOIN DimAccount da ON ds.account_id = da.account_id
            JOIN statement_heads sh ON ds.id_statement = sh.ID_STATEMENT
            WHERE da.id_account != sh.ID_ACCOUNT
        """,
        )
        assert mismatches == 0
