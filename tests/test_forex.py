"""
Tests for the forex exchange-rate module.

All tests are fully unit-tested with mocked HTTP and SQLite — no live API
calls and no dependency on a real project database.

Run with:
    pytest tests/test_forex.py -v
"""

import sqlite3
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bank_statement_parser.modules.forex import (
    _forward_fill,
    _load_forex_config,
    _provider_frankfurter,
    _provider_exchangerate_api,
    get_exchange_rates,
)
from bank_statement_parser.modules.data import ForexApiConfig
from bank_statement_parser.modules.errors import ProjectDatabaseMissing


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> Path:
    """Create a minimal in-memory-ish SQLite database with required tables."""
    db_path = tmp_path / "project.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE exchange_rates (
            "id_date"   TEXT NOT NULL,
            "currency"  TEXT NOT NULL,
            "rate_USD"  REAL NOT NULL,
            PRIMARY KEY (id_date, currency)
        );
        CREATE TABLE DimAccount (
            account_id INTEGER PRIMARY KEY,
            currency   TEXT
        );
        CREATE TABLE DimTime (
            time_id  INTEGER PRIMARY KEY,
            id_date  TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# ForexApiConfig defaults
# ---------------------------------------------------------------------------


class TestForexApiConfig:
    def test_defaults(self):
        cfg = ForexApiConfig()
        assert cfg.provider == "frankfurter"
        assert cfg.api_key == ""
        assert cfg.base_currency == "USD"
        assert cfg.extra_currencies == []

    def test_custom_values(self):
        cfg = ForexApiConfig(provider="exchangerate-api", api_key="secret", base_currency="USD", extra_currencies=["AED"])
        assert cfg.provider == "exchangerate-api"
        assert cfg.api_key == "secret"
        assert cfg.extra_currencies == ["AED"]


# ---------------------------------------------------------------------------
# _load_forex_config
# ---------------------------------------------------------------------------


class TestLoadForexConfig:
    def test_returns_defaults_when_file_absent(self, tmp_path):
        (tmp_path / "config").mkdir()
        # Point to a project path that has a config dir but no forex_api_config.toml.
        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            cfg = _load_forex_config(tmp_path)
        assert cfg == ForexApiConfig()

    def test_loads_toml_when_present(self, tmp_path):
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        toml_file = config_dir / "forex_api_config.toml"
        toml_file.write_text('provider = "exchangerate-api"\napi_key = "abc"\nbase_currency = "USD"\nextra_currencies = ["AED"]\n')

        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.forex_config = toml_file
            mock_resolve.return_value = mock_paths
            cfg = _load_forex_config(tmp_path)

        assert cfg.provider == "exchangerate-api"
        assert cfg.api_key == "abc"
        assert cfg.extra_currencies == ["AED"]


# ---------------------------------------------------------------------------
# _forward_fill
# ---------------------------------------------------------------------------


class TestForwardFill:
    def test_fills_weekend_gaps(self):
        # Friday rate only; Saturday and Sunday should be filled forward.
        records = [("2024-03-15", "GBP", 1.265), ("2024-03-18", "GBP", 1.270)]
        all_dates = ["2024-03-15", "2024-03-16", "2024-03-17", "2024-03-18"]
        filled = _forward_fill(records, all_dates, ["GBP"])
        assert len(filled) == 4
        assert filled[0] == ("2024-03-15", "GBP", 1.265)
        assert filled[1] == ("2024-03-16", "GBP", 1.265)  # Saturday — filled
        assert filled[2] == ("2024-03-17", "GBP", 1.265)  # Sunday — filled
        assert filled[3] == ("2024-03-18", "GBP", 1.270)

    def test_skips_leading_dates_with_no_data(self):
        # No data on first date — should not appear until first known rate.
        records = [("2024-03-18", "EUR", 1.08)]
        all_dates = ["2024-03-15", "2024-03-16", "2024-03-17", "2024-03-18"]
        filled = _forward_fill(records, all_dates, ["EUR"])
        assert len(filled) == 1
        assert filled[0] == ("2024-03-18", "EUR", 1.08)

    def test_multiple_currencies_filled_independently(self):
        records = [("2024-01-02", "GBP", 1.27), ("2024-01-02", "EUR", 1.09)]
        all_dates = ["2024-01-02", "2024-01-03"]
        filled = _forward_fill(records, all_dates, ["GBP", "EUR"])
        assert len(filled) == 4
        gbp_dates = [(d, r) for (d, c, r) in filled if c == "GBP"]
        eur_dates = [(d, r) for (d, c, r) in filled if c == "EUR"]
        assert gbp_dates == [("2024-01-02", 1.27), ("2024-01-03", 1.27)]
        assert eur_dates == [("2024-01-02", 1.09), ("2024-01-03", 1.09)]

    def test_empty_records_returns_empty(self):
        filled = _forward_fill([], ["2024-01-01", "2024-01-02"], ["GBP"])
        assert filled == []


# ---------------------------------------------------------------------------
# _provider_frankfurter
# ---------------------------------------------------------------------------


class TestProviderFrankfurter:
    def _mock_response(self, data: dict) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = data
        resp.raise_for_status = MagicMock()
        return resp

    def test_converts_rates_to_usd_multiplier(self):
        """Frankfurter returns units-per-USD; we invert to get USD-per-unit."""
        mock_data = {
            "rates": {
                "2024-01-02": {"GBP": 0.7874},  # 1 USD = 0.7874 GBP → rate_USD = 1/0.7874 ≈ 1.2699
            }
        }
        with patch("requests.get", return_value=self._mock_response(mock_data)):
            records = _provider_frankfurter(["GBP"], "2024-01-02", "2024-01-02", "")

        assert len(records) == 1
        id_date, currency, rate = records[0]
        assert id_date == "2024-01-02"
        assert currency == "GBP"
        assert abs(rate - (1.0 / 0.7874)) < 0.0001

    def test_skips_unsupported_currencies(self):
        """AED and SAR are not supported by Frankfurter; they should be excluded from the request."""
        mock_data = {"rates": {"2024-01-02": {"GBP": 0.79}}}
        with patch("requests.get", return_value=self._mock_response(mock_data)) as mock_get:
            _provider_frankfurter(["GBP", "AED", "SAR"], "2024-01-02", "2024-01-02", "")
        called_url: str = mock_get.call_args[0][0]
        assert "AED" not in called_url
        assert "SAR" not in called_url
        assert "GBP" in called_url

    def test_returns_empty_if_all_unsupported(self):
        with patch("requests.get") as mock_get:
            records = _provider_frankfurter(["AED", "SAR"], "2024-01-02", "2024-01-02", "")
        mock_get.assert_not_called()
        assert records == []

    def test_skips_zero_rate(self):
        mock_data = {"rates": {"2024-01-02": {"GBP": 0, "EUR": 1.08}}}
        with patch("requests.get", return_value=self._mock_response(mock_data)):
            records = _provider_frankfurter(["GBP", "EUR"], "2024-01-02", "2024-01-02", "")
        currencies = [r[1] for r in records]
        assert "GBP" not in currencies
        assert "EUR" in currencies


# ---------------------------------------------------------------------------
# _provider_exchangerate_api (stub)
# ---------------------------------------------------------------------------


class TestProviderExchangeRateApi:
    def test_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            _provider_exchangerate_api(["AED"], "2024-01-02", "2024-01-02", "mykey")


# ---------------------------------------------------------------------------
# get_exchange_rates
# ---------------------------------------------------------------------------


class TestGetExchangeRates:
    def test_raises_if_db_missing(self, tmp_path):
        with pytest.raises(ProjectDatabaseMissing):
            get_exchange_rates(project_path=tmp_path)

    def test_no_op_if_dimtime_empty(self, tmp_path):
        db_path = _make_db(tmp_path)
        # DimTime is empty — function should print a message and return cleanly.
        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.project_db = db_path
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            # No exception should be raised.
            get_exchange_rates(project_path=tmp_path)

    def test_fetches_and_persists_rates(self, tmp_path):
        """End-to-end: DimAccount has GBP, DimTime spans 3 days, rates fetched and written."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO DimAccount (account_id, currency) VALUES (1, 'GBP')")
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (1, '2024-01-02')")
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (2, '2024-01-03')")
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (3, '2024-01-04')")
        conn.commit()
        conn.close()

        frankfurter_data = {
            "rates": {
                "2024-01-02": {"GBP": 0.79},
                "2024-01-04": {"GBP": 0.785},
            }
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = frankfurter_data
        mock_resp.raise_for_status = MagicMock()

        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.project_db = db_path
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            with patch("requests.get", return_value=mock_resp):
                get_exchange_rates(project_path=tmp_path, extra_currencies=None)

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT id_date, currency, rate_USD FROM exchange_rates ORDER BY id_date").fetchall()
        conn.close()

        assert len(rows) == 3
        dates = [r[0] for r in rows]
        assert "2024-01-02" in dates
        assert "2024-01-03" in dates  # forward-filled from 2024-01-02
        assert "2024-01-04" in dates

        # 2024-01-03 should be filled from 2024-01-02 rate
        rate_jan2 = next(r[2] for r in rows if r[0] == "2024-01-02")
        rate_jan3 = next(r[2] for r in rows if r[0] == "2024-01-03")
        assert abs(rate_jan3 - rate_jan2) < 0.0001

    def test_no_op_if_rates_already_present(self, tmp_path):
        """If all (date, currency) pairs already exist, nothing should be fetched."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO DimAccount (account_id, currency) VALUES (1, 'GBP')")
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (1, '2024-01-02')")
        conn.execute("INSERT INTO exchange_rates (id_date, currency, rate_USD) VALUES ('2024-01-02', 'GBP', 1.27)")
        conn.commit()
        conn.close()

        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.project_db = db_path
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            with patch("requests.get") as mock_get:
                get_exchange_rates(project_path=tmp_path)
            mock_get.assert_not_called()

    def test_warns_for_unsupported_currencies(self, tmp_path):
        """Currencies unsupported by Frankfurter and with no secondary provider should warn."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO DimAccount (account_id, currency) VALUES (1, 'AED')")
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (1, '2024-01-02')")
        conn.commit()
        conn.close()

        frankfurter_data = {"rates": {}}  # AED not returned

        mock_resp = MagicMock()
        mock_resp.json.return_value = frankfurter_data
        mock_resp.raise_for_status = MagicMock()

        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.project_db = db_path
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                with patch("requests.get", return_value=mock_resp):
                    get_exchange_rates(project_path=tmp_path)

        messages = [str(w.message) for w in caught]
        assert any("AED" in m for m in messages)

    def test_extra_currencies_arg(self, tmp_path):
        """extra_currencies passed to get_exchange_rates are fetched even if not in DimAccount."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(db_path)
        # No currencies in DimAccount — only extra_currencies.
        conn.execute("INSERT INTO DimTime (time_id, id_date) VALUES (1, '2024-01-02')")
        conn.commit()
        conn.close()

        frankfurter_data = {"rates": {"2024-01-02": {"EUR": 1.09}}}
        mock_resp = MagicMock()
        mock_resp.json.return_value = frankfurter_data
        mock_resp.raise_for_status = MagicMock()

        with patch("bank_statement_parser.modules.forex.ProjectPaths.resolve") as mock_resolve:
            mock_paths = MagicMock()
            mock_paths.project_db = db_path
            mock_paths.forex_config = tmp_path / "config" / "forex_api_config.toml"
            mock_resolve.return_value = mock_paths
            with patch("requests.get", return_value=mock_resp):
                get_exchange_rates(project_path=tmp_path, extra_currencies=["EUR"])

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT currency FROM exchange_rates").fetchall()
        conn.close()
        assert any(r[0] == "EUR" for r in rows)
