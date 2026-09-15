"""
Tests for the opening_balance_source = "closing_minus_movements" feature.

Validates that _apply_opening_balance_correction correctly derives the true
opening balance when the statement header's opening balance is unreliable
(e.g. Halifax PDFs showing end-of-day-1 balance instead of start-of-period).
"""

import polars as pl
import pytest

from bank_statement_parser.modules.data import (
    ConfigGroup,
    StatementType,
)
from bank_statement_parser.modules.statements import Statement


class TestOpeningBalanceCorrection:
    """Tests for Statement._apply_opening_balance_correction."""

    def _make_statement_with_correction(
        self,
        closing: float,
        transactions: list[tuple[float, float]],
        incorrect_opening: float | None = None,
    ) -> Statement:
        """Create a minimal Statement-like object with correction enabled.

        Simulates the Halifax failure mode where the summary table's "opening
        balance" is actually the end-of-day-1 balance (after day-1 transactions
        have been applied), not the true start-of-period balance.

        Args:
            closing: The stated closing balance from the header.
            transactions: List of (payments_in, payments_out) per transaction row.
            incorrect_opening: The wrong opening balance the PDF reports.  When
                ``None``, defaults to ``closing`` which simulates the Halifax
                bug (the summary table shows a value unrelated to the true
                opening).

        Returns:
            A Statement with checks_and_balances and lines_results pre-populated.
        """
        stmt = object.__new__(Statement)
        total_in = sum(t[0] for t in transactions)
        total_out = sum(t[1] for t in transactions)
        total_movement = total_in - total_out

        # The "incorrect" opening — the wrong value the PDF reports.
        # Default: closing (simulates Halifax where the summary table shows
        # a value that is NOT the true start-of-period balance).
        if incorrect_opening is None:
            incorrect_opening = closing

        stmt.checks_and_balances = pl.DataFrame(
            {
                "STD_CLOSING_BALANCE": [closing],
                "STD_OPENING_BALANCE": [incorrect_opening],
                "STD_PAYMENTS_IN": [total_in],
                "STD_PAYMENTS_OUT": [total_out],
                "STD_MOVEMENT": [closing - incorrect_opening],
                "STD_BALANCE_OF_PAYMENTS": [total_in - total_out],
                "STD_TRANSACTION_PAYMENTS_IN": [total_in],
                "STD_TRANSACTION_PAYMENTS_OUT": [total_out],
                "STD_TRANSACTION_MOVEMENT": [total_movement],
                "STD_RUNNING_BALANCE": [closing],
            }
        )

        # Build a LazyFrame of transaction lines with running balance seeded from incorrect opening
        movements = [t[0] - t[1] for t in transactions]
        running = []
        bal = incorrect_opening
        for m in movements:
            bal += m
            running.append(bal)

        stmt.lines_results = pl.DataFrame(
            {
                "STD_TRANSACTION_MOVEMENT": movements,
                "STD_RUNNING_BALANCE": running,
            }
        ).lazy()

        stmt.header_results = pl.DataFrame(
            {
                "STD_OPENING_BALANCE": [incorrect_opening],
            }
        ).lazy()

        # Config with correction enabled
        config_group = ConfigGroup(configs=[])
        stmt.config = type(
            "Config",
            (),
            {
                "statement_type": StatementType(
                    statement_type="Halifax UK Current Account",
                    header=config_group,
                    lines=config_group,
                    opening_balance_source="closing_minus_movements",
                )
            },
        )()

        return stmt

    def test_correction_derives_true_opening(self):
        """True opening = closing - sum(movements), regardless of wrong header value."""
        # closing=55, one transaction of in=0 out=10 → movement=-10
        # true_opening = 55 - (-10) = 65
        # incorrect_opening defaults to closing=55 (simulates Halifax bug)
        stmt = self._make_statement_with_correction(closing=55.0, transactions=[(0.0, 10.0)])
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_OPENING_BALANCE").item() == pytest.approx(65.0)
        assert result.select("STD_MOVEMENT").item() == pytest.approx(-10.0)

    def test_correction_recomputes_running_balance(self):
        """Running balance should be recomputed from the corrected opening."""
        # closing=30, transactions: in=0 out=10, in=0 out=5
        # true_opening = 30 - (-15) = 45
        stmt = self._make_statement_with_correction(closing=30.0, transactions=[(0.0, 10.0), (0.0, 5.0)])
        stmt._apply_opening_balance_correction()

        running = stmt.lines_results.select("STD_RUNNING_BALANCE").collect().to_series().to_list()
        # true_opening=45, then: 45+(-10)=35, 35+(-5)=30
        assert running == pytest.approx([35.0, 30.0])

    def test_correction_updates_checks_and_balances_running_balance(self):
        """checks_and_balances.STD_RUNNING_BALANCE should match last transaction running balance."""
        # closing=55, transactions: in=0 out=10, in=20 out=0
        # sum(movements) = -10 + 20 = 10
        # true_opening = 55 - 10 = 45
        stmt = self._make_statement_with_correction(closing=55.0, transactions=[(0.0, 10.0), (20.0, 0.0)])
        stmt._apply_opening_balance_correction()

        last_running = stmt.lines_results.select(pl.last("STD_RUNNING_BALANCE")).collect().item()
        cb_running = stmt.checks_and_balances.select("STD_RUNNING_BALANCE").item()
        assert cb_running == pytest.approx(last_running)
        assert cb_running == pytest.approx(55.0)

    def test_correction_updates_std_opening_balance_scalar(self):
        """Statement.std_opening_balance should reflect the corrected value after scalar is derived from header_results."""
        # closing=40, transactions: in=0 out=10
        # true_opening = 40 - (-10) = 50
        stmt = self._make_statement_with_correction(closing=40.0, transactions=[(0.0, 10.0)])
        stmt._apply_opening_balance_correction()

        # Scalar is derived from header_results downstream in Statement.__init__
        stmt.std_opening_balance = stmt.header_results.select("STD_OPENING_BALANCE").collect().item()
        assert stmt.std_opening_balance == pytest.approx(50.0)

    def test_correction_updates_header_results(self):
        """header_results should carry the corrected opening balance for parquet/SQLite."""
        # closing=55, transactions: in=0 out=10
        # true_opening = 55 - (-10) = 65
        stmt = self._make_statement_with_correction(closing=55.0, transactions=[(0.0, 10.0)])
        stmt._apply_opening_balance_correction()

        header_opening = stmt.header_results.select("STD_OPENING_BALANCE").collect().item()
        assert header_opening == pytest.approx(65.0)

    def test_correction_with_halifax_realistic_scenario(self):
        """Realistic Halifax scenario: Jan closing=50, Feb has day-1 debit of 10, closing=55."""
        # True opening = 50 (Jan closing)
        # Day 1 transaction: -10
        # Halifax summary "opening": 40 (end-of-day-1) — this is the bug
        # Further transactions bring balance to 55
        # Total movement = -10 + 15 = 5
        # true_opening = closing - total_movement = 55 - 5 = 50 ✓
        stmt = self._make_statement_with_correction(
            closing=55.0,
            transactions=[(0.0, 10.0), (25.0, 10.0)],
            incorrect_opening=40.0,  # the Halifax bug value
        )
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_OPENING_BALANCE").item() == pytest.approx(50.0)
        assert result.select("STD_MOVEMENT").item() == pytest.approx(5.0)

        # Scalar is derived from header_results downstream in Statement.__init__
        stmt.std_opening_balance = stmt.header_results.select("STD_OPENING_BALANCE").collect().item()
        assert stmt.std_opening_balance == pytest.approx(50.0)

        running = stmt.lines_results.select("STD_RUNNING_BALANCE").collect().to_series().to_list()
        # true_opening=50: 50+(-10)=40, 40+(25-10)=55
        assert running == pytest.approx([40.0, 55.0])

    def test_correction_preserves_payments(self):
        """Payments in/out should not be altered."""
        stmt = self._make_statement_with_correction(closing=40.0, transactions=[(100.0, 60.0)])
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_PAYMENTS_IN").item() == pytest.approx(100.0)
        assert result.select("STD_PAYMENTS_OUT").item() == pytest.approx(60.0)

    def test_no_correction_when_flag_not_set(self):
        """Method should be a no-op when opening_balance_source is not set."""
        stmt = object.__new__(Statement)
        stmt.config = None
        stmt.checks_and_balances = pl.DataFrame({"STD_OPENING_BALANCE": [40.0]})
        config_group = ConfigGroup(configs=[])
        stmt.config = type(
            "Config",
            (),
            {
                "statement_type": StatementType(
                    statement_type="HSBC UK Current Account",
                    header=config_group,
                    lines=config_group,
                )
            },
        )()
        stmt.lines_results = pl.DataFrame({"STD_RUNNING_BALANCE": [40.0]}).lazy()

        stmt._apply_opening_balance_correction()

        # Opening balance should be unchanged
        assert stmt.checks_and_balances.select("STD_OPENING_BALANCE").item() == 40.0

    def test_no_correction_when_no_config(self):
        """Method should be a no-op when config is None."""
        stmt = object.__new__(Statement)
        stmt.config = None
        stmt.checks_and_balances = pl.DataFrame({"STD_OPENING_BALANCE": [40.0]})
        stmt.lines_results = pl.DataFrame({"STD_RUNNING_BALANCE": [40.0]}).lazy()

        stmt._apply_opening_balance_correction()

        assert stmt.checks_and_balances.select("STD_OPENING_BALANCE").item() == 40.0

    def test_no_correction_when_checks_and_balances_empty(self):
        """Method should be a no-op when checks_and_balances is empty."""
        stmt = object.__new__(Statement)
        stmt.config = None
        stmt.checks_and_balances = pl.DataFrame()
        stmt.lines_results = pl.DataFrame({"STD_RUNNING_BALANCE": [40.0]}).lazy()

        stmt._apply_opening_balance_correction()

        assert stmt.checks_and_balances.is_empty()

    def test_correction_with_credits_and_debits(self):
        """Works correctly with mixed credits and debits."""
        # closing=60, transactions: in=20 out=10, in=0 out=5, in=15 out=0
        # sum(movements) = 10 + (-5) + 15 = 20
        # true_opening = 60 - 20 = 40
        stmt = self._make_statement_with_correction(closing=60.0, transactions=[(20.0, 10.0), (0.0, 5.0), (15.0, 0.0)])
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_OPENING_BALANCE").item() == pytest.approx(40.0)
        assert result.select("STD_MOVEMENT").item() == pytest.approx(20.0)

        running = stmt.lines_results.select("STD_RUNNING_BALANCE").collect().to_series().to_list()
        # 40+10=50, 50+(-5)=45, 45+15=60
        assert running == pytest.approx([50.0, 45.0, 60.0])
