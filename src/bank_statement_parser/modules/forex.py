"""Forex exchange-rate fetching and persistence module.

Provides :func:`get_exchange_rates` — the public entry point for fetching
daily USD-based exchange rates for all currencies found in ``DimAccount``
(plus any extras), forward-filling weekend and holiday gaps, and persisting
the results into the ``exchange_rates`` SQLite table.

Provider chain
--------------
1. **Primary** (default: Frankfurter) — free, no API key, ECB data, ~30
   currencies.  Covers all currencies in ``currency.py`` except AED and SAR.
2. **Secondary** (optional: ExchangeRate-API) — scaffolded; activated when
   ``provider = "exchangerate-api"`` in ``forex_api_config.toml``.  Handles
   currencies unsupported by Frankfurter.
3. Any currency still unresolved after both providers → ``warnings.warn`` and
   skip.

Gap-filling strategy
--------------------
Frankfurter (and most providers) only return business-day rates.  After
fetching, :func:`_forward_fill` extends the raw records to cover every date
in the ``DimTime`` range by propagating the most recent known rate forward
across weekends and holidays.
"""

import re
import sqlite3
import warnings
from datetime import date, timedelta
from pathlib import Path

import requests

from bank_statement_parser.modules.data import ForexApiConfig
from bank_statement_parser.modules.errors import ProjectDatabaseMissing
from bank_statement_parser.modules.paths import ProjectPaths

# ---------------------------------------------------------------------------
# Frankfurter: currencies NOT supported (absent from ECB basket)
# ---------------------------------------------------------------------------

_FRANKFURTER_UNSUPPORTED: frozenset[str] = frozenset({"AED", "SAR", "PHP", "IDR", "SGD", "HKD", "CNY", "NZD"})

_FRANKFURTER_BASE_URL = "https://api.frankfurter.dev/v1"

# ISO 4217 currency code pattern: exactly 3 uppercase ASCII letters.
_ISO4217_RE = re.compile(r"^[A-Z]{3}$")


def _validate_currency_code(code: str) -> None:
    """Raise ValueError if *code* is not a valid ISO 4217 currency code format.

    Args:
        code: The currency code to validate.

    Raises:
        ValueError: If *code* does not match the ``[A-Z]{3}`` pattern.
    """
    if not _ISO4217_RE.match(code):
        raise ValueError(f"Currency code {code!r} is not a valid ISO 4217 code (expected 3 uppercase letters).")


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def _load_forex_config(project_path: Path | None) -> ForexApiConfig:
    """Load ``forex_api_config.toml`` from the project config directory if present.

    Falls back to a default :class:`~bank_statement_parser.modules.data.ForexApiConfig`
    (Frankfurter, no key, USD base, no extras) when the file is absent.

    Args:
        project_path: Project root directory, or ``None`` for the default bundled project.

    Returns:
        A :class:`~bank_statement_parser.modules.data.ForexApiConfig` instance.
    """
    import tomllib  # noqa: PLC0415  — stdlib, Python ≥ 3.11

    import dacite  # noqa: PLC0415

    paths = ProjectPaths.resolve(project_path)
    config_file = paths.forex_config

    if not config_file.exists():
        return ForexApiConfig()

    with open(config_file, "rb") as fh:
        raw = tomllib.load(fh)

    return dacite.from_dict(ForexApiConfig, raw)


# ---------------------------------------------------------------------------
# Provider: Frankfurter
# ---------------------------------------------------------------------------


def _provider_frankfurter(
    currencies: list[str],
    date_from: str,
    date_to: str,
    api_key: str,  # noqa: ARG001 — Frankfurter does not use an API key
) -> list[tuple[str, str, float]]:
    """Fetch USD-based rates from Frankfurter for a date range.

    Only currencies supported by Frankfurter (i.e. not in
    :data:`_FRANKFURTER_UNSUPPORTED`) are fetched; unsupported ones are
    silently excluded from the request and the caller must handle them.

    The response only contains business days; weekend/holiday gaps must be
    filled by the caller via :func:`_forward_fill`.

    Args:
        currencies: ISO 4217 currency codes to fetch (e.g. ``["GBP", "EUR"]``).
        date_from: ISO date string for the start of the range (``"YYYY-MM-DD"``).
        date_to: ISO date string for the end of the range (``"YYYY-MM-DD"``).
        api_key: Ignored for Frankfurter; present for interface consistency.

    Returns:
        List of ``(id_date, currency, rate_USD)`` tuples where ``rate_USD``
        is the multiplier to convert from *currency* to USD.

    Raises:
        requests.HTTPError: If the Frankfurter API returns a non-2xx status.
    """
    supported = [c for c in currencies if c not in _FRANKFURTER_UNSUPPORTED]
    if not supported:
        return []

    # Validate all codes before interpolating into the URL to prevent query-string injection.
    for code in supported:
        _validate_currency_code(code)

    symbols = ",".join(supported)
    url = f"{_FRANKFURTER_BASE_URL}/{date_from}..{date_to}?base=USD&symbols={symbols}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    records: list[tuple[str, str, float]] = []

    # Frankfurter returns rates as: how many units of each currency equal 1 USD.
    # We want rate_USD = multiplier to go FROM currency TO USD.
    # If 1 USD = 0.79 GBP, then rate_USD for GBP = 1 / 0.79 ≈ 1.2658.
    # But our design says rate_USD = "multiply to convert to USD", so:
    # £100 × rate_USD_GBP = $X  →  rate_USD_GBP = USD / GBP = 1 / (GBP per USD).
    for id_date, day_rates in data.get("rates", {}).items():
        for currency, rate_per_usd in day_rates.items():
            if rate_per_usd and rate_per_usd != 0:
                records.append((id_date, currency, 1.0 / rate_per_usd))

    return records


# ---------------------------------------------------------------------------
# Provider: ExchangeRate-API (stub)
# ---------------------------------------------------------------------------


def _provider_exchangerate_api(
    currencies: list[str],
    date_from: str,
    date_to: str,
    api_key: str,
) -> list[tuple[str, str, float]]:
    """Fetch USD-based rates from ExchangeRate-API for a date range.

    .. note::
        This provider is **not yet implemented**.  Calling it raises
        :exc:`NotImplementedError` with a helpful message.  To use it,
        set ``provider = "exchangerate-api"`` and supply your API key in
        ``forex_api_config.toml``.

    Args:
        currencies: ISO 4217 currency codes to fetch.
        date_from: ISO date string for the start of the range.
        date_to: ISO date string for the end of the range.
        api_key: Your ExchangeRate-API key.

    Raises:
        NotImplementedError: Always — this provider stub is not yet implemented.
    """
    raise NotImplementedError(
        "ExchangeRate-API provider is not yet implemented. "
        "To add support, implement _provider_exchangerate_api() in forex.py. "
        "See https://www.exchangerate-api.com/docs/overview for the API reference."
    )


# ---------------------------------------------------------------------------
# Gap filling
# ---------------------------------------------------------------------------


def _forward_fill(
    records: list[tuple[str, str, float]],
    all_dates: list[str],
    currencies: list[str],
) -> list[tuple[str, str, float]]:
    """Forward-fill missing dates (weekends/holidays) for each currency.

    Takes a sparse list of ``(id_date, currency, rate_USD)`` tuples — as
    returned by a provider which only covers business days — and produces a
    dense list covering every date in *all_dates*.  For each currency the most
    recently seen rate is propagated forward until a new rate is available.

    Currencies that have no data at all in *records* are silently omitted from
    the output (the caller handles the warning).

    Args:
        records: Sparse ``(id_date, currency, rate_USD)`` tuples from a provider.
        all_dates: Sorted list of ISO date strings covering the full target range.
        currencies: Full list of ISO 4217 codes we intended to fetch (used to
            order output and detect completely missing currencies).

    Returns:
        Dense list of ``(id_date, currency, rate_USD)`` tuples covering every
        date in *all_dates* for every currency that had at least one data point.
    """
    # Build a lookup: {currency: {id_date: rate_USD}}
    rate_map: dict[str, dict[str, float]] = {c: {} for c in currencies}
    for id_date, currency, rate in records:
        if currency in rate_map:
            rate_map[currency][id_date] = rate

    filled: list[tuple[str, str, float]] = []

    for currency in currencies:
        last_rate: float | None = None
        daily = rate_map[currency]
        for d in all_dates:
            if d in daily:
                last_rate = daily[d]
            if last_rate is not None:
                filled.append((d, currency, last_rate))
            # If last_rate is still None (no data yet), skip — currency may
            # not have existed before the first available date.

    return filled


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def get_exchange_rates(
    project_path: Path | None = None,
    extra_currencies: list[str] | None = None,
    api_key: str | None = None,
) -> None:
    """Fetch daily USD-based exchange rates and persist them to ``exchange_rates``.

    Reads the currencies required from ``DimAccount`` (all unique ``currency``
    values), merges with *extra_currencies* and any extras in
    ``forex_api_config.toml``, determines which ``(date, currency)`` pairs are
    missing from the ``exchange_rates`` table, fetches the gaps from the
    configured provider(s), forward-fills weekends and holidays, and writes the
    results via ``INSERT OR REPLACE``.

    This function is **not** called automatically by ``update_db()``.  Call it
    explicitly to populate or refresh exchange-rate data.

    Provider chain:

    1. Primary provider (Frankfurter by default) handles all supported currencies.
    2. Any currencies unsupported by the primary are tried on the secondary
       (ExchangeRate-API if ``provider = "exchangerate-api"``; currently a stub).
    3. Any still-unresolved currencies trigger ``warnings.warn`` and are skipped.

    Args:
        project_path: Project root directory.  ``None`` uses the default bundled
            project.  The database must already exist (call
            :func:`~bank_statement_parser.modules.paths.validate_or_initialise_project`
            beforehand if needed).
        extra_currencies: Additional ISO 4217 codes to fetch beyond those detected
            from ``DimAccount``.  Combined with ``extra_currencies`` from
            ``forex_api_config.toml``.  ``None`` is treated as an empty list.
        api_key: Override API key; takes precedence over the value in
            ``forex_api_config.toml``.

    Raises:
        ProjectDatabaseMissing: If the project database does not exist.
        requests.HTTPError: If a provider returns a non-2xx response.
    """
    paths = ProjectPaths.resolve(project_path)
    db_path = paths.project_db

    if not db_path.exists():
        raise ProjectDatabaseMissing(db_path)

    config = _load_forex_config(project_path)
    effective_api_key = api_key if api_key is not None else config.api_key

    # -- collect currencies to fetch -----------------------------------------
    conn = sqlite3.connect(db_path)
    try:
        dim_currencies: list[str] = []
        try:
            rows = conn.execute("SELECT DISTINCT currency FROM DimAccount WHERE currency IS NOT NULL").fetchall()
            dim_currencies = [r[0] for r in rows]
        except sqlite3.OperationalError:
            # DimAccount may not exist yet if the mart has never been built.
            pass

        all_extra = list(extra_currencies or []) + list(config.extra_currencies)
        # Validate all caller-supplied currency codes before use.
        for code in all_extra:
            _validate_currency_code(code)
        currencies: list[str] = sorted(set(dim_currencies + all_extra))

        if not currencies:
            print("[forex] No currencies found in DimAccount and no extra_currencies specified. Nothing to fetch.")
            conn.close()
            return

        # -- determine date range from DimTime --------------------------------
        try:
            date_rows = conn.execute("SELECT MIN(id_date), MAX(id_date) FROM DimTime").fetchone()
        except sqlite3.OperationalError:
            print("[forex] DimTime table not found. Run bsp process first to populate the mart.")
            conn.close()
            return

        if not date_rows or date_rows[0] is None:
            print("[forex] DimTime is empty. Run bsp process first to populate the mart.")
            conn.close()
            return

        date_min_str: str = date_rows[0]
        date_max_str: str = date_rows[1]

        # Build the full list of calendar dates in the DimTime range.
        d_start = date.fromisoformat(date_min_str)
        d_end = date.fromisoformat(date_max_str)
        all_dates: list[str] = []
        cursor_date = d_start
        while cursor_date <= d_end:
            all_dates.append(cursor_date.isoformat())
            cursor_date += timedelta(days=1)

        # -- find already-existing (date, currency) pairs ---------------------
        existing: set[tuple[str, str]] = set()
        try:
            ex_rows = conn.execute("SELECT id_date, currency FROM exchange_rates").fetchall()
            existing = {(r[0], r[1]) for r in ex_rows}
        except sqlite3.OperationalError:
            pass  # table may not exist in very old DBs; migration handles it

        # Determine which (date, currency) pairs we still need.
        needed: dict[str, set[str]] = {}  # currency -> set of dates needed
        for d in all_dates:
            for c in currencies:
                if (d, c) not in existing:
                    needed.setdefault(c, set()).add(d)

        if not needed:
            print("[forex] Exchange rates are already up to date. Nothing to fetch.")
            conn.close()
            return

        # -- derive contiguous date ranges per currency -----------------------
        # Simplest approach: use the global min/max of needed dates per currency.
        currencies_to_fetch = sorted(needed.keys())
        fetch_date_from = min(d for dates in needed.values() for d in dates)
        fetch_date_to = max(d for dates in needed.values() for d in dates)

        print(
            f"[forex] Fetching rates for {len(currencies_to_fetch)} currency(ies) "
            f"from {fetch_date_from} to {fetch_date_to} via {config.provider}."
        )

        # -- primary provider -------------------------------------------------
        primary_currencies = currencies_to_fetch[:]
        records: list[tuple[str, str, float]] = []

        if config.provider == "frankfurter":
            unsupported_by_primary = [c for c in primary_currencies if c in _FRANKFURTER_UNSUPPORTED]
            supported_by_primary = [c for c in primary_currencies if c not in _FRANKFURTER_UNSUPPORTED]
        else:
            # For any other provider, attempt all currencies (stub will raise).
            supported_by_primary = primary_currencies
            unsupported_by_primary = []

        if supported_by_primary:
            if config.provider == "frankfurter":
                fetched = _provider_frankfurter(supported_by_primary, fetch_date_from, fetch_date_to, effective_api_key)
            elif config.provider == "exchangerate-api":
                fetched = _provider_exchangerate_api(supported_by_primary, fetch_date_from, fetch_date_to, effective_api_key)
            else:
                warnings.warn(
                    f"[forex] Unknown provider '{config.provider}'. No rates fetched.",
                    UserWarning,
                    stacklevel=2,
                )
                fetched = []
            records.extend(fetched)

        # -- secondary provider for unsupported currencies --------------------
        if unsupported_by_primary:
            if config.provider == "frankfurter" and config.api_key:
                # User has configured exchangerate-api as fallback via api_key presence.
                try:
                    fallback = _provider_exchangerate_api(unsupported_by_primary, fetch_date_from, fetch_date_to, effective_api_key)
                    records.extend(fallback)
                    unsupported_by_primary = []
                except NotImplementedError as exc:
                    warnings.warn(str(exc), UserWarning, stacklevel=2)
            for c in unsupported_by_primary:
                warnings.warn(
                    f"[forex] Currency '{c}' is not supported by provider '{config.provider}' "
                    f"and no secondary provider is configured. Skipping.",
                    UserWarning,
                    stacklevel=2,
                )

        if not records:
            print("[forex] No rates fetched.")
            conn.close()
            return

        # -- forward-fill to cover every date in range ------------------------
        fetched_currencies = sorted({r[1] for r in records})
        filled = _forward_fill(records, all_dates, fetched_currencies)

        # -- filter to only the pairs we actually need -------------------------
        needed_filled = [(d, c, r) for (d, c, r) in filled if (d, c) not in existing]

        if not needed_filled:
            print("[forex] All required rates already present after fill. Nothing to write.")
            conn.close()
            return

        # -- write to exchange_rates ------------------------------------------
        conn.executemany(
            'INSERT OR REPLACE INTO exchange_rates (id_date, currency, "rate_USD") VALUES (?, ?, ?)',
            needed_filled,
        )
        conn.commit()
        conn.close()

        print(f"[forex] Written {len(needed_filled)} rate row(s) to exchange_rates.")

    except Exception:
        conn.close()
        raise
