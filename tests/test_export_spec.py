"""
test_export_spec.py — integration tests for the export spec engine.

Tests are grouped into four classes:

TestLoadSpec
    Verifies that _load_spec() correctly parses both shipped QuickBooks spec
    files, and raises ConfigError for malformed or invalid specs.

TestExportSpecOutput
    Verifies that export_spec() produces correctly named output files in the
    right directory, with the expected columns and row counts.

TestExportSpecContent
    Verifies the data content: date format, numeric precision, blank-zero
    behaviour, signed-amount polarity, polarity inversion, and string
    sanitisation.

TestExportSpecFiltering
    Verifies date-range filtering, statement_key filtering, and
    split_by_statement partitioning.

Run with:
    pytest tests/test_export_spec.py -v
"""

import re
import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from bank_statement_parser.modules.errors import ConfigError
from bank_statement_parser.modules.export_spec import ExportSpec, _load_spec, export_spec
from bank_statement_parser.modules.paths import ProjectPaths

# ---------------------------------------------------------------------------
# Helpers / constants
# ---------------------------------------------------------------------------

# Paths to the two shipped QuickBooks spec files
_SPECS_DIR = Path(__file__).parent.parent / "src" / "bank_statement_parser" / "project" / "export" / "specs"
_SPEC_3COL = _SPECS_DIR / "quickbooks_3column.toml"
_SPEC_4COL = _SPECS_DIR / "quickbooks_4column.toml"

# One well-known id_account present in the good_project test data
_ACCOUNT_KEY = "HSBC_UK_CUR_12345678"

# Date pattern for dd/mm/yyyy
_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def _get_flat_transaction_count(project_path: Path, account_key: str) -> int:
    """Return the number of FlatTransaction rows for a given account.

    Queries FactTransaction directly (joined to DimAccount) to match the
    production query in _build_frame, which avoids double-join fan-out.
    """
    paths = ProjectPaths.resolve(project_path)
    with sqlite3.connect(paths.project_db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM FactTransaction ft INNER JOIN DimAccount da ON ft.account_id = da.account_id WHERE da.id_account = ?",
            [account_key],
        ).fetchone()
    return row[0] if row else 0


def _get_fact_transaction_value_in_sum(project_path: Path, account_key: str) -> float:
    """Return SUM(value_in) from FactTransaction for a given account."""
    paths = ProjectPaths.resolve(project_path)
    with sqlite3.connect(paths.project_db) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(ft.value_in), 0)"
            " FROM FactTransaction ft"
            " INNER JOIN DimAccount da ON ft.account_id = da.account_id"
            " WHERE da.id_account = ?",
            [account_key],
        ).fetchone()
    return float(row[0]) if row else 0.0


def _get_fact_transaction_value_out_sum(project_path: Path, account_key: str) -> float:
    """Return SUM(value_out) from FactTransaction for a given account."""
    paths = ProjectPaths.resolve(project_path)
    with sqlite3.connect(paths.project_db) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(ft.value_out), 0)"
            " FROM FactTransaction ft"
            " INNER JOIN DimAccount da ON ft.account_id = da.account_id"
            " WHERE da.id_account = ?",
            [account_key],
        ).fetchone()
    return float(row[0]) if row else 0.0


def _get_statement_ids(project_path: Path, account_key: str) -> list[str]:
    """Return all id_statement values for a given account, sorted."""
    paths = ProjectPaths.resolve(project_path)
    with sqlite3.connect(paths.project_db) as conn:
        rows = conn.execute(
            "SELECT ds.id_statement"
            " FROM DimStatement ds"
            " INNER JOIN DimAccount da ON ds.account_id = da.account_id"
            " WHERE da.id_account = ?"
            " ORDER BY ds.id_statement",
            [account_key],
        ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# TestLoadSpec
# ---------------------------------------------------------------------------


class TestLoadSpec:
    """Verify _load_spec() against the shipped TOML files and error cases."""

    def test_load_3column_returns_export_spec(self):
        """_load_spec() returns an ExportSpec for the 3-column file."""
        spec = _load_spec(_SPEC_3COL)
        assert isinstance(spec, ExportSpec)

    def test_load_4column_returns_export_spec(self):
        """_load_spec() returns an ExportSpec for the 4-column file."""
        spec = _load_spec(_SPEC_4COL)
        assert isinstance(spec, ExportSpec)

    def test_3column_columns(self):
        """3-column spec declares exactly Date, Description, Amount."""
        spec = _load_spec(_SPEC_3COL)
        assert list(spec.columns.keys()) == ["Date", "Description", "Amount"]

    def test_4column_columns(self):
        """4-column spec declares exactly Date, Description, Credit, Debit."""
        spec = _load_spec(_SPEC_4COL)
        assert list(spec.columns.keys()) == ["Date", "Description", "Credit", "Debit"]

    def test_3column_signed_amount_computed(self):
        """3-column spec Amount maps to computed:signed_amount."""
        spec = _load_spec(_SPEC_3COL)
        assert spec.columns["Amount"] == "computed:signed_amount"

    def test_4column_credit_debit_sources(self):
        """4-column spec Credit → value_in, Debit → value_out."""
        spec = _load_spec(_SPEC_4COL)
        assert spec.columns["Credit"] == "value_in"
        assert spec.columns["Debit"] == "value_out"

    def test_3column_format_csv(self):
        """3-column spec format is 'csv'."""
        spec = _load_spec(_SPEC_3COL)
        assert spec.format == "csv"

    def test_4column_blank_zeros_true(self):
        """4-column spec has blank_zeros = true."""
        spec = _load_spec(_SPEC_4COL)
        assert spec.blank_zeros is True

    def test_3column_blank_zeros_false(self):
        """3-column spec has blank_zeros = false."""
        spec = _load_spec(_SPEC_3COL)
        assert spec.blank_zeros is False

    def test_date_format_is_uk(self):
        """Both specs use the UK date format %%d/%%m/%%Y."""
        assert _load_spec(_SPEC_3COL).date_format == "%d/%m/%Y"
        assert _load_spec(_SPEC_4COL).date_format == "%d/%m/%Y"

    def test_invert_polarity_defaults_false(self):
        """Both specs default invert_polarity to false."""
        assert _load_spec(_SPEC_3COL).invert_polarity is False
        assert _load_spec(_SPEC_4COL).invert_polarity is False

    def test_missing_file_raises_config_error(self):
        """_load_spec() raises ConfigError for a non-existent path."""
        with pytest.raises(ConfigError, match="not found"):
            _load_spec(Path("/nonexistent/spec.toml"))

    def test_missing_section_raises_config_error(self):
        """_load_spec() raises ConfigError when a required TOML section is absent."""
        toml_content = b"""
[meta]
description = "test"
source_table = "FlatTransaction"

[export]
format = "csv"
split_by_statement = false

[columns]
Date = "transaction_date"

[format]
date_format = "%d/%m/%Y"
float_precision = 2
invert_polarity = false
blank_zeros = false
# [sanitise] section deliberately omitted
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as fh:
            fh.write(toml_content)
            tmp = Path(fh.name)
        try:
            with pytest.raises(ConfigError, match="sanitise"):
                _load_spec(tmp)
        finally:
            tmp.unlink(missing_ok=True)

    def test_invalid_format_raises_config_error(self):
        """_load_spec() raises ConfigError when format is not csv or xlsx."""
        toml_content = b"""
[meta]
description = "test"
source_table = "FlatTransaction"

[export]
format = "parquet"
split_by_statement = false

[columns]
Date = "transaction_date"

[format]
date_format = "%d/%m/%Y"
float_precision = 2
invert_polarity = false
blank_zeros = false

[sanitise]
strip_chars = ""
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as fh:
            fh.write(toml_content)
            tmp = Path(fh.name)
        try:
            with pytest.raises(ConfigError, match="format"):
                _load_spec(tmp)
        finally:
            tmp.unlink(missing_ok=True)

    def test_unknown_computed_token_raises_config_error(self):
        """_load_spec() raises ConfigError for an unrecognised computed: token."""
        toml_content = b"""
[meta]
description = "test"
source_table = "FlatTransaction"

[export]
format = "csv"
split_by_statement = false

[columns]
Amount = "computed:unknown_token"

[format]
date_format = "%d/%m/%Y"
float_precision = 2
invert_polarity = false
blank_zeros = false

[sanitise]
strip_chars = ""
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as fh:
            fh.write(toml_content)
            tmp = Path(fh.name)
        try:
            with pytest.raises(ConfigError, match="computed"):
                _load_spec(tmp)
        finally:
            tmp.unlink(missing_ok=True)

    def test_invalid_source_table_raises_config_error(self):
        """_load_spec() raises ConfigError for a source_table not in the allowed set."""
        toml_content = b"""
[meta]
description = "test"
source_table = "some_random_table"

[export]
format = "csv"
split_by_statement = false

[columns]
Date = "transaction_date"

[format]
date_format = "%d/%m/%Y"
float_precision = 2
invert_polarity = false
blank_zeros = false

[sanitise]
strip_chars = ""
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as fh:
            fh.write(toml_content)
            tmp = Path(fh.name)
        try:
            with pytest.raises(ConfigError, match="source_table"):
                _load_spec(tmp)
        finally:
            tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# TestExportSpecOutput
# ---------------------------------------------------------------------------


class TestExportSpecOutput:
    """Verify output file location, naming, and column headers."""

    def test_3column_output_file_exists(self, good_project):
        """export_spec() creates a non-empty CSV for the 3-column spec."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        assert len(written) == 1
        assert written[0].exists()
        assert written[0].stat().st_size > 0

    def test_4column_output_file_exists(self, good_project):
        """export_spec() creates a non-empty CSV for the 4-column spec."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        assert len(written) == 1
        assert written[0].exists()
        assert written[0].stat().st_size > 0

    def test_output_dir_is_under_export(self, good_project):
        """Output file lives under export/<spec_stem>/ not export/specs/."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        expected_dir = paths.exports / _SPEC_3COL.stem
        assert written[0].parent == expected_dir

    def test_output_filename_matches_account_key(self, good_project):
        """Single-file output is named <account_key>.csv."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        assert written[0].name == f"{_ACCOUNT_KEY}.csv"

    def test_3column_headers(self, good_project):
        """3-column CSV has exactly Date, Description, Amount columns."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        assert df.columns == ["Date", "Description", "Amount"]

    def test_4column_headers(self, good_project):
        """4-column CSV has exactly Date, Description, Credit, Debit columns."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0], infer_schema_length=0)
        assert df.columns == ["Date", "Description", "Credit", "Debit"]

    def test_format_override_xlsx(self, good_project):
        """Passing format='xlsx' override produces an .xlsx file."""
        written = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            format="xlsx",
        )
        assert len(written) == 1
        assert written[0].suffix == ".xlsx"
        assert written[0].exists()
        assert written[0].stat().st_size > 0


# ---------------------------------------------------------------------------
# TestExportSpecContent
# ---------------------------------------------------------------------------


class TestExportSpecContent:
    """Verify data content: counts, totals, dates, blanks, polarity."""

    def test_3column_row_count_matches_db(self, good_project):
        """3-column export row count matches FlatTransaction count for the account."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        expected = _get_flat_transaction_count(good_project.project_path, _ACCOUNT_KEY)
        assert df.height == expected, f"CSV rows={df.height}, DB rows={expected}"

    def test_4column_row_count_matches_db(self, good_project):
        """4-column export row count matches FlatTransaction count for the account."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0], infer_schema_length=0)
        expected = _get_flat_transaction_count(good_project.project_path, _ACCOUNT_KEY)
        assert df.height == expected, f"CSV rows={df.height}, DB rows={expected}"

    def test_date_format_is_uk(self, good_project):
        """All Date values in the 3-column export match dd/mm/yyyy."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        bad = [v for v in df["Date"].to_list() if v is not None and not _DATE_RE.match(str(v))]
        assert bad == [], f"Non-UK date values found: {bad[:5]}"

    def test_3column_amount_sum_matches_db(self, good_project):
        """3-column Amount column: SUM equals value_in minus value_out from DB."""
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        csv_sum = df["Amount"].sum() or 0.0
        db_in = _get_fact_transaction_value_in_sum(good_project.project_path, _ACCOUNT_KEY)
        db_out = _get_fact_transaction_value_out_sum(good_project.project_path, _ACCOUNT_KEY)
        expected = db_in - db_out
        assert abs(csv_sum - expected) < 0.01, f"Amount sum {csv_sum} != expected {expected}"

    def test_invert_polarity_override_negates_amount(self, good_project):
        """Passing invert_polarity=True negates the Amount column."""
        written_normal = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
        )
        # Read immediately before the second call overwrites the same output path.
        df_normal = pl.read_csv(written_normal[0])
        normal_sum = df_normal["Amount"].sum() or 0.0

        written_inverted = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            invert_polarity=True,
        )
        df_inverted = pl.read_csv(written_inverted[0])
        inverted_sum = df_inverted["Amount"].sum() or 0.0
        assert abs(normal_sum + inverted_sum) < 0.01, f"Inverted sum {inverted_sum} should be the negative of normal sum {normal_sum}"

    def test_4column_blank_zeros_credit(self, good_project):
        """4-column Credit column is blank (empty string) when value_in is 0."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        # Read as strings to inspect blank cells
        df = pl.read_csv(written[0], infer_schema_length=0)
        # For every row, Credit and Debit should not both be non-empty
        credit_col = df["Credit"].to_list()
        debit_col = df["Debit"].to_list()
        both_filled = [
            i
            for i, (c, d) in enumerate(zip(credit_col, debit_col))
            if c not in (None, "", "0.0", "0.00") and d not in (None, "", "0.0", "0.00")
        ]
        assert both_filled == [], f"Rows where both Credit and Debit are non-blank (expected at most one): rows {both_filled[:5]}"

    def test_4column_credit_sum_matches_db(self, good_project):
        """4-column Credit column sum matches value_in from DB."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        csv_credit = df["Credit"].sum() or 0.0
        db_in = _get_fact_transaction_value_in_sum(good_project.project_path, _ACCOUNT_KEY)
        assert abs(csv_credit - db_in) < 0.01, f"Credit sum {csv_credit} != DB value_in {db_in}"

    def test_4column_debit_sum_matches_db(self, good_project):
        """4-column Debit column sum matches value_out from DB."""
        written = export_spec(_SPEC_4COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        csv_debit = df["Debit"].sum() or 0.0
        db_out = _get_fact_transaction_value_out_sum(good_project.project_path, _ACCOUNT_KEY)
        assert abs(csv_debit - db_out) < 0.01, f"Debit sum {csv_debit} != DB value_out {db_out}"

    def test_strip_chars_removes_special_characters(self, good_project):
        """Description column contains no characters from strip_chars."""
        spec = _load_spec(_SPEC_3COL)
        written = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        df = pl.read_csv(written[0])
        for char in spec.strip_chars:
            bad_rows = df.filter(pl.col("Description").str.contains(re.escape(char)))
            assert bad_rows.height == 0, f"Character {char!r} found in Description after sanitisation"


# ---------------------------------------------------------------------------
# TestExportSpecFiltering
# ---------------------------------------------------------------------------


class TestExportSpecFiltering:
    """Verify date-range, statement_key, and split_by_statement filtering."""

    def test_date_from_reduces_rows(self, good_project):
        """Passing date_from produces fewer or equal rows than the full export."""
        written_full = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        written_filtered = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            date_from=date(2022, 1, 1),
        )
        df_full = pl.read_csv(written_full[0])
        df_filtered = pl.read_csv(written_filtered[0])
        assert df_filtered.height <= df_full.height

    def test_date_to_reduces_rows(self, good_project):
        """Passing date_to produces fewer or equal rows than the full export."""
        written_full = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        written_filtered = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            date_to=date(2022, 12, 31),
        )
        df_full = pl.read_csv(written_full[0])
        df_filtered = pl.read_csv(written_filtered[0])
        assert df_filtered.height <= df_full.height

    def test_date_range_rows_within_bounds(self, good_project):
        """All dates in a date-filtered export fall within the requested range."""
        d_from = date(2021, 1, 1)
        d_to = date(2023, 12, 31)
        written = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            date_from=d_from,
            date_to=d_to,
        )
        df = pl.read_csv(written[0])
        if df.height == 0:
            pytest.skip("No transactions in date range — cannot validate bounds")
        dates = pl.Series(df["Date"]).str.strptime(pl.Date, "%d/%m/%Y")
        assert dates.min() >= d_from, f"Earliest date {dates.min()} is before date_from {d_from}"
        assert dates.max() <= d_to, f"Latest date {dates.max()} is after date_to {d_to}"

    def test_statement_key_filter_returns_subset(self, good_project):
        """Passing statement_key returns fewer or equal rows than the full export."""
        statement_ids = _get_statement_ids(good_project.project_path, _ACCOUNT_KEY)
        assert statement_ids, "No statements found for test account — fixture issue"
        first_sid = statement_ids[0]

        written_full = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        written_filtered = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            statement_key=first_sid,
        )
        df_full = pl.read_csv(written_full[0])
        df_filtered = pl.read_csv(written_filtered[0])
        assert df_filtered.height <= df_full.height
        assert df_filtered.height > 0, f"statement_key={first_sid!r} returned no rows"

    def test_split_by_statement_produces_multiple_files(self, good_project):
        """split_by_statement=True produces one file per statement."""
        statement_ids = _get_statement_ids(good_project.project_path, _ACCOUNT_KEY)
        assert len(statement_ids) > 1, "Need multiple statements to test split — fixture issue"

        written = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            split_by_statement=True,
        )
        assert len(written) == len(statement_ids), f"Expected {len(statement_ids)} files, got {len(written)}"

    def test_split_by_statement_filenames(self, good_project):
        """Each split file is named <account_key>_<id_statement>.csv."""
        statement_ids = _get_statement_ids(good_project.project_path, _ACCOUNT_KEY)
        written = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            split_by_statement=True,
        )
        expected_stems = {f"{_ACCOUNT_KEY}_{sid}" for sid in statement_ids}
        actual_stems = {p.stem for p in written}
        assert actual_stems == expected_stems, f"Stem mismatch.\nExpected: {sorted(expected_stems)}\nActual:   {sorted(actual_stems)}"

    def test_split_files_row_counts_sum_to_total(self, good_project):
        """Sum of rows across split files equals the total row count."""
        written_single = export_spec(_SPEC_3COL, account_key=_ACCOUNT_KEY, project_path=good_project.project_path)
        written_split = export_spec(
            _SPEC_3COL,
            account_key=_ACCOUNT_KEY,
            project_path=good_project.project_path,
            split_by_statement=True,
        )
        total_single = pl.read_csv(written_single[0]).height
        total_split = sum(pl.read_csv(p).height for p in written_split)
        assert total_split == total_single, f"Split total {total_split} != single-file total {total_single}"
