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

    def _make_statement_with_correction(self, closing: float, transactions: list[tuple[float, float]]) -> Statement:
        """Create a minimal Statement-like object with correction enabled.

        Args:
            closing: The stated closing balance from the header.
            transactions: List of (payments_in, payments_out) per transaction row.

        Returns:
            A Statement with checks_and_balances and lines_results pre-populated.
        """
        stmt = object.__new__(Statement)
        total_in = sum(t[0] for t in transactions)
        total_out = sum(t[1] for t in transactions)
        total_movement = total_in - total_out

        # Simulate a post-day-1 "opening" (what Halifax reports)
        incorrect_opening = closing - total_movement

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
        """True opening = closing - sum(movements)."""
        # closing=40, one transaction of in=0 out=10 → movement=-10
        # incorrect_opening = 40 - (-10) = 50  (post-day-1)
        # true_opening = 40 - (-10) = 50 ... wait, let me recalculate.
        # sum(movements) = 0 - 10 = -10
        # true_opening = closing - sum(movements) = 40 - (-10) = 50
        stmt = self._make_statement_with_correction(closing=40.0, transactions=[(0.0, 10.0)])
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_OPENING_BALANCE").item() == pytest.approx(50.0)
        assert result.select("STD_MOVEMENT").item() == pytest.approx(-10.0)

    def test_correction_recomputes_running_balance(self):
        """Running balance should be recomputed from the corrected opening."""
        # closing=30, transactions: in=0 out=10, in=0 out=5
        # sum(movements) = -10 + -5 = -15
        # true_opening = 30 - (-15) = 45
        stmt = self._make_statement_with_correction(closing=30.0, transactions=[(0.0, 10.0), (0.0, 5.0)])
        stmt._apply_opening_balance_correction()

        running = stmt.lines_results.select("STD_RUNNING_BALANCE").collect().to_series().to_list()
        # true_opening=45, then: 45+(-10)=35, 35+(-5)=30
        assert running == pytest.approx([35.0, 30.0])

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
        stmt.checks_and_balances = pl.DataFrame({"STD_OPENING_BALANCE": [40.0]})
        stmt.config = None
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

    def test_correction_with_multiple_transactions(self):
        """Works correctly with multiple transactions including credits."""
        # closing=55, transactions: in=0 out=10, in=20 out=0, in=0 out=5
        # sum(movements) = -10 + 20 + -5 = 5
        # true_opening = 55 - 5 = 50
        stmt = self._make_statement_with_correction(closing=55.0, transactions=[(0.0, 10.0), (20.0, 0.0), (0.0, 5.0)])
        stmt._apply_opening_balance_correction()

        result = stmt.checks_and_balances
        assert result.select("STD_OPENING_BALANCE").item() == pytest.approx(50.0)
        assert result.select("STD_MOVEMENT").item() == pytest.approx(5.0)

        running = stmt.lines_results.select("STD_RUNNING_BALANCE").collect().to_series().to_list()
        # 50 + (-10) = 40, 40 + 20 = 60, 60 + (-5) = 55
        assert running == pytest.approx([40.0, 60.0, 55.0])
