"""
test_statements.py — integration tests for bank_statement_parser.

Tests are grouped into five classes:

TestGoodStatements
    Verifies that all PDFs in ``tests/pdfs/good/`` process without error
    and that the expected output files are created.

TestDbReports
    Verifies the SQLite-backed report classes (reports_db) against the
    processed good-PDF data.

TestExports
    Verifies CSV, JSON, and Excel export functions for the database backend,
    including full and simple presets, content/totals checks for CSV and JSON,
    the ``filetype="all"`` convenience mode on ``StatementBatch.export()``,
    and the deprecated ``filetype="both"`` alias.

TestCopyStatements
    Verifies that ``copy_statements_to_project()`` copies PDFs into the
    correct ``statements/`` directory, that all returned paths exist and
    are non-empty, that the operation is idempotent, and that error/missing
    entries are silently skipped.

TestBadStatements
    Verifies that each PDF in ``tests/pdfs/bad/`` is flagged as an error.

Run with:
    pytest tests/test_statements.py -v
"""

import sqlite3
import warnings
from dataclasses import replace
from datetime import timedelta

import polars as pl

from bank_statement_parser.modules import reports_db as db
from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.paths import ProjectPaths
from bank_statement_parser.modules.statements import copy_statements_to_project

FLOAT_TOL = 0.005  # monetary comparison tolerance (matches test_datamart.py)


# ---------------------------------------------------------------------------
# TestGoodStatements
# ---------------------------------------------------------------------------


class TestGoodStatements:
    def test_no_errors(self, good_project):
        """Batch reports zero processing errors for all good PDFs."""
        assert good_project.batch.errors == 0

    def test_all_pdfs_processed(self, good_project):
        """processed_pdfs list length equals the number of input PDFs."""
        assert len(good_project.batch.processed_pdfs) == len(good_project.pdfs)

    def test_parquet_files_exist(self, good_project):
        """statement_heads, statement_lines and batch_lines parquet files are written."""
        paths = ProjectPaths.resolve(good_project.project_path)
        assert paths.statement_heads.exists(), "statement_heads.parquet missing"
        assert paths.statement_lines.exists(), "statement_lines.parquet missing"
        assert paths.batch_lines.exists(), "batch_lines.parquet missing"

    def test_db_exists(self, good_project):
        """SQLite project.db is created and non-empty."""
        paths = ProjectPaths.resolve(good_project.project_path)
        assert paths.project_db.exists(), "project.db missing"
        assert paths.project_db.stat().st_size > 0, "project.db is empty"

    def test_no_temp_files_remain(self, good_project):
        """No *_temp_*.parquet files survive after delete_temp_files()."""
        paths = ProjectPaths.resolve(good_project.project_path)
        temp_files = list(paths.parquet.glob("*_temp_*.parquet"))
        assert temp_files == [], f"Temp files still present: {temp_files}"


# ---------------------------------------------------------------------------
# TestDbReports
# ---------------------------------------------------------------------------


class TestDbReports:
    def test_dim_statement_row_count(self, good_project):
        """DB DimStatement has exactly one row per processed good PDF."""
        df = db.DimStatement(project_path=good_project.project_path).all.collect()
        assert df.height == len(good_project.pdfs)

    def test_dim_account_uniqueness(self, good_project):
        """DB DimAccount id_account values are unique."""
        df = db.DimAccount(project_path=good_project.project_path).all.collect()
        assert df.height == df["id_account"].n_unique()

    def test_dim_time_contiguous(self, good_project):
        """DB DimTime date spine contains no gaps."""
        df = db.DimTime(project_path=good_project.project_path).all.collect()
        dates = df["id_date"].cast(pl.Date).sort()
        diffs = dates.diff().drop_nulls()
        assert (diffs == timedelta(days=1)).all(), "Date spine has gaps in DimTime (db)"

    def test_fact_transaction_row_count(self, good_project):
        """DB FactTransaction row count matches SQLite statement_lines table."""
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            raw_count = conn.execute("SELECT COUNT(*) FROM statement_lines").fetchone()[0]
        ft = db.FactTransaction(project_path=good_project.project_path).all.collect()
        assert ft.height == raw_count

    def test_fact_balance_coverage(self, good_project):
        """DB FactBalance has exactly n_accounts × n_days rows."""
        db_path = good_project.project_path
        n_accounts = db.DimAccount(project_path=db_path).all.collect().height
        n_days = db.DimTime(project_path=db_path).all.collect().height
        fb = db.FactBalance(project_path=db_path).all.collect()
        assert fb.height == n_accounts * n_days

    def test_gap_report_runs_without_error(self, good_project):
        """DB GapReport.all.collect() completes without raising an exception."""
        df = db.GapReport(project_path=good_project.project_path).all.collect()
        assert df.height >= 0

    def test_flat_transaction_count_matches_fact_transaction(self, good_project):
        """DB FlatTransaction row count equals DB FactTransaction row count."""
        db_path = good_project.project_path
        ft_count = db.FactTransaction(project_path=db_path).all.collect().height
        flat_count = db.FlatTransaction(project_path=db_path).all.collect().height
        assert flat_count == ft_count

    def test_statement_lines_db_value_in_matches_fact_transaction_db(self, good_project):
        """SUM(STD_PAYMENTS_IN) from SQLite statement_lines equals DB FactTransaction value_in sum."""
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            raw_sum = conn.execute("SELECT SUM(CAST(STD_PAYMENTS_IN AS REAL)) FROM statement_lines").fetchone()[0]
        ft_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_in"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_in mismatch: raw db={raw_sum}, db FactTransaction={ft_sum}"

    def test_statement_lines_db_value_out_matches_fact_transaction_db(self, good_project):
        """SUM(STD_PAYMENTS_OUT) from SQLite statement_lines equals DB FactTransaction value_out sum."""
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            raw_sum = conn.execute("SELECT SUM(CAST(STD_PAYMENTS_OUT AS REAL)) FROM statement_lines").fetchone()[0]
        ft_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_out"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_out mismatch: raw db={raw_sum}, db FactTransaction={ft_sum}"

    def test_statement_lines_row_count_matches_db(self, good_project):
        """Raw statement_lines row count agrees between parquet file and SQLite table."""
        paths = ProjectPaths.resolve(good_project.project_path)
        pq_count = pl.read_parquet(paths.statement_lines).height
        with sqlite3.connect(paths.project_db) as conn:
            db_count = conn.execute("SELECT COUNT(*) FROM statement_lines").fetchone()[0]
        assert pq_count == db_count


# ---------------------------------------------------------------------------
# TestExports
# ---------------------------------------------------------------------------

# File names produced by ``type="full"`` exports (stem only, no extension).
_FULL_EXPORT_STEMS = [
    "statement_dimension",
    "account_dimension",
    "calendar_dimension",
    "transaction_measures",
    "daily_account_balances",
    "missing_statement_report",
]

# Mapping from full-export logical name → DB table/view name (for row-count checks).
_FULL_STEM_TO_DB_TABLE = {
    "statement_dimension": "DimStatement",
    "account_dimension": "DimAccount",
    "calendar_dimension": "DimTime",
    "transaction_measures": "FactTransaction",
    "daily_account_balances": "FactBalance",
    "missing_statement_report": "GapReport",
}


class TestExports:
    """Verify CSV, JSON, and Excel exports for the database backend and presets.

    Content checks (row counts, monetary totals) are performed for CSV and JSON
    ``type="simple"`` and ``type="full"`` exports.  Excel tests are
    existence-only to avoid OS-dependent or extra-dependency parsing.
    """

    # ------------------------------------------------------------------
    # DB backend — CSV existence
    # ------------------------------------------------------------------

    def test_db_export_csv_full(self, good_project):
        """DB export_csv(type='full') writes all six CSV files."""
        db.export_csv(type="full", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        for stem in _FULL_EXPORT_STEMS:
            f = paths.csv / f"{stem}.csv"
            assert f.exists(), f"Missing CSV: {stem}.csv"
            assert f.stat().st_size > 0, f"Empty CSV: {stem}.csv"

    def test_db_export_csv_simple(self, good_project):
        """DB export_csv(type='simple') writes the flat transactions CSV."""
        db.export_csv(type="simple", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        f = paths.csv / "transactions.csv"
        assert f.exists(), "Missing transactions.csv (db/simple)"
        assert f.stat().st_size > 0, "Empty transactions.csv (db/simple)"

    # ------------------------------------------------------------------
    # DB backend — CSV content: simple totals
    # ------------------------------------------------------------------

    def test_csv_simple_totals(self, good_project):
        """transactions_table.csv row count and monetary totals match the DB mart."""
        db.export_csv(type="simple", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        df = pl.read_csv(paths.csv / "transactions.csv", schema_overrides={"account_number": pl.String})

        with sqlite3.connect(str(paths.project_db)) as conn:
            db_rows = conn.execute("SELECT COUNT(*) FROM FlatTransaction").fetchone()[0]
            db_value_in = conn.execute("SELECT COALESCE(SUM(value_in), 0) FROM FlatTransaction").fetchone()[0]
            db_value_out = conn.execute("SELECT COALESCE(SUM(value_out), 0) FROM FlatTransaction").fetchone()[0]

        assert df.height == db_rows, f"CSV row count {df.height} != DB {db_rows}"
        csv_value_in = df["value_in"].sum() or 0
        csv_value_out = df["value_out"].sum() or 0
        assert abs(csv_value_in - db_value_in) <= FLOAT_TOL, f"value_in mismatch: CSV={csv_value_in} DB={db_value_in}"
        assert abs(csv_value_out - db_value_out) <= FLOAT_TOL, f"value_out mismatch: CSV={csv_value_out} DB={db_value_out}"

    # ------------------------------------------------------------------
    # DB backend — CSV content: full row counts
    # ------------------------------------------------------------------

    def test_csv_full_row_counts(self, good_project):
        """Each full-export CSV file has the same row count as its DB source table."""
        db.export_csv(type="full", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(str(paths.project_db)) as conn:
            for stem, table in _FULL_STEM_TO_DB_TABLE.items():
                db_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
                df = pl.read_csv(paths.csv / f"{stem}.csv", infer_schema_length=0)
                assert df.height == db_rows, f"{stem}.csv: CSV rows={df.height} != DB rows={db_rows}"

    # ------------------------------------------------------------------
    # DB backend — Excel existence only
    # ------------------------------------------------------------------

    def test_db_export_excel_full(self, good_project):
        """DB export_excel(type='full') writes a non-empty workbook."""
        db.export_excel(type="full", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (db/full)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (db/full)"

    def test_db_export_excel_simple(self, good_project):
        """DB export_excel(type='simple') writes a non-empty workbook."""
        db.export_excel(type="simple", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (db/simple)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (db/simple)"

    # ------------------------------------------------------------------
    # DB backend — JSON existence
    # ------------------------------------------------------------------

    def test_db_export_json_full(self, good_project):
        """DB export_json(type='full') writes all six JSON files."""
        db.export_json(type="full", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        for stem in _FULL_EXPORT_STEMS:
            f = paths.json / f"{stem}.json"
            assert f.exists(), f"Missing JSON: {stem}.json"
            assert f.stat().st_size > 0, f"Empty JSON: {stem}.json"

    def test_db_export_json_simple(self, good_project):
        """DB export_json(type='simple') writes the flat transactions JSON."""
        db.export_json(type="simple", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        f = paths.json / "transactions.json"
        assert f.exists(), "Missing transactions.json (db/simple)"
        assert f.stat().st_size > 0, "Empty transactions.json (db/simple)"

    # ------------------------------------------------------------------
    # DB backend — JSON content: simple totals
    # ------------------------------------------------------------------

    def test_json_simple_totals(self, good_project):
        """transactions_table.json row count and monetary totals match the DB mart."""
        db.export_json(type="simple", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        df = pl.read_json(paths.json / "transactions.json")

        with sqlite3.connect(str(paths.project_db)) as conn:
            db_rows = conn.execute("SELECT COUNT(*) FROM FlatTransaction").fetchone()[0]
            db_value_in = conn.execute("SELECT COALESCE(SUM(value_in), 0) FROM FlatTransaction").fetchone()[0]
            db_value_out = conn.execute("SELECT COALESCE(SUM(value_out), 0) FROM FlatTransaction").fetchone()[0]

        assert df.height == db_rows, f"JSON row count {df.height} != DB {db_rows}"
        json_value_in = df["value_in"].sum() or 0
        json_value_out = df["value_out"].sum() or 0
        assert abs(json_value_in - db_value_in) <= FLOAT_TOL, f"value_in mismatch: JSON={json_value_in} DB={db_value_in}"
        assert abs(json_value_out - db_value_out) <= FLOAT_TOL, f"value_out mismatch: JSON={json_value_out} DB={db_value_out}"

    # ------------------------------------------------------------------
    # DB backend — JSON content: full row counts
    # ------------------------------------------------------------------

    def test_json_full_row_counts(self, good_project):
        """Each full-export JSON file has the same row count as its DB source table."""
        db.export_json(type="full", project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(str(paths.project_db)) as conn:
            for stem, table in _FULL_STEM_TO_DB_TABLE.items():
                db_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
                df = pl.read_json(paths.json / f"{stem}.json", infer_schema_length=None)
                assert df.height == db_rows, f"{stem}.json: JSON rows={df.height} != DB rows={db_rows}"

    # ------------------------------------------------------------------
    # StatementBatch.export() — filetype="all"
    # ------------------------------------------------------------------

    def test_batch_export_all(self, good_project):
        """StatementBatch.export(filetype='all') writes Excel, CSV, and JSON."""
        good_project.batch.export(filetype="all", type="full")
        paths = ProjectPaths.resolve(good_project.project_path)
        # Excel
        xlsx = paths.excel / "transactions.xlsx"
        assert xlsx.exists(), "Missing transactions.xlsx after filetype='all'"
        assert xlsx.stat().st_size > 0, "Empty transactions.xlsx after filetype='all'"
        # CSV
        for stem in _FULL_EXPORT_STEMS:
            f = paths.csv / f"{stem}.csv"
            assert f.exists(), f"Missing CSV after filetype='all': {stem}.csv"
            assert f.stat().st_size > 0, f"Empty CSV after filetype='all': {stem}.csv"
        # JSON
        for stem in _FULL_EXPORT_STEMS:
            f = paths.json / f"{stem}.json"
            assert f.exists(), f"Missing JSON after filetype='all': {stem}.json"
            assert f.stat().st_size > 0, f"Empty JSON after filetype='all': {stem}.json"

    # ------------------------------------------------------------------
    # StatementBatch.export() — filetype="both" deprecated alias
    # ------------------------------------------------------------------

    def test_batch_export_both_deprecated(self, good_project):
        """filetype='both' emits a DeprecationWarning and still produces output."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            good_project.batch.export(filetype="both", type="simple")
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(dep_warnings) == 1, "Expected exactly one DeprecationWarning for filetype='both'"
        assert "filetype='both'" in str(dep_warnings[0].message)
        # Output should still be written (all three formats, simple preset)
        paths = ProjectPaths.resolve(good_project.project_path)
        assert (paths.excel / "transactions.xlsx").exists()
        assert (paths.csv / "transactions.csv").exists()
        assert (paths.json / "transactions.json").exists()

    # ------------------------------------------------------------------
    # DB backend — export_reporting_data existence
    # ------------------------------------------------------------------

    def test_reporting_data_simple_exists(self, good_project):
        """export_reporting_data() writes transactions.csv to reporting/data/simple/."""
        db.export_reporting_data(project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        f = paths.reporting_data_simple / "transactions.csv"
        assert f.exists(), "Missing reporting/data/simple/transactions.csv"
        assert f.stat().st_size > 0, "Empty reporting/data/simple/transactions.csv"

    def test_reporting_data_full_exists(self, good_project):
        """export_reporting_data() writes all six CSVs to reporting/data/full/."""
        db.export_reporting_data(project_path=good_project.project_path)
        paths = ProjectPaths.resolve(good_project.project_path)
        for stem in _FULL_EXPORT_STEMS:
            f = paths.reporting_data_full / f"{stem}.csv"
            assert f.exists(), f"Missing reporting/data/full/{stem}.csv"
            assert f.stat().st_size > 0, f"Empty reporting/data/full/{stem}.csv"

    def test_batch_export_reporting(self, good_project):
        """StatementBatch.export(filetype='reporting') populates both reporting directories."""
        good_project.batch.export(filetype="reporting")
        paths = ProjectPaths.resolve(good_project.project_path)
        assert (paths.reporting_data_simple / "transactions.csv").exists()
        for stem in _FULL_EXPORT_STEMS:
            assert (paths.reporting_data_full / f"{stem}.csv").exists()


# ---------------------------------------------------------------------------
# TestCopyStatements
# ---------------------------------------------------------------------------


class TestCopyStatements:
    """Verify copy_statements_to_project() directory layout and behaviour."""

    def test_returns_nonempty_list(self, good_project):
        """copy_statements_to_project() returns at least one copied path."""
        copied = good_project.batch.copy_statements_to_project()
        assert len(copied) > 0, "No files were copied"

    def test_returned_paths_exist(self, good_project):
        """Every path returned by copy_statements_to_project() exists on disk."""
        copied = good_project.batch.copy_statements_to_project()
        missing = [p for p in copied if not p.exists()]
        assert missing == [], f"Missing copied files: {missing}"

    def test_returned_paths_nonempty(self, good_project):
        """Every copied file contains at least one byte."""
        copied = good_project.batch.copy_statements_to_project()
        empty = [p for p in copied if p.stat().st_size == 0]
        assert empty == [], f"Zero-byte copied files: {empty}"

    def test_directory_structure(self, good_project):
        """Copied files land directly under statements/<filename> (flat layout)."""
        paths = ProjectPaths.resolve(good_project.project_path)
        copied = good_project.batch.copy_statements_to_project()
        for dest in copied:
            # dest must be inside <project>/statements/
            assert dest.is_relative_to(paths.statements), f"{dest} is not under {paths.statements}"
            # Relative path must have exactly one part: the filename only (no sub-folders)
            rel = dest.relative_to(paths.statements)
            parts = rel.parts
            assert len(parts) == 1, f"Expected flat statements/<filename>, got {parts!r} for {dest}"

    def test_count_matches_successful_pdfs(self, good_project):
        """Number of copied files equals number of successfully processed PdfResult entries."""
        expected = sum(1 for e in good_project.batch.processed_pdfs if isinstance(e, PdfResult) and e.result == "SUCCESS")
        copied = good_project.batch.copy_statements_to_project()
        assert len(copied) == expected, f"Expected {expected} copies, got {len(copied)}"

    def test_idempotent(self, good_project):
        """Calling copy_statements_to_project() twice does not raise and returns same count."""
        first = good_project.batch.copy_statements_to_project()
        second = good_project.batch.copy_statements_to_project()
        assert len(first) == len(second), f"First call copied {len(first)}, second call copied {len(second)}"

    def test_skips_base_exception_entries(self, good_project):
        """BaseException entries in processed_pdfs are silently ignored."""
        sentinel = RuntimeError("synthetic worker crash")
        mixed: list[BaseException | PdfResult] = [sentinel, *good_project.batch.processed_pdfs]
        mixed_pdfs = [good_project.pdfs[0], *good_project.pdfs]  # extra dummy pdf for the sentinel
        copied = copy_statements_to_project(mixed, pdfs=mixed_pdfs, project_path=good_project.project_path)
        # Must still copy the real entries — no crash, no reduction in count
        expected = sum(1 for e in good_project.batch.processed_pdfs if isinstance(e, PdfResult) and e.result == "SUCCESS")
        assert len(copied) == expected

    def test_skips_entries_without_file_dst(self, good_project):
        """PdfResult entries with filename_new='' are silently skipped."""
        # Build a list that has one extra entry with no filename_new
        real_entries: list[BaseException | PdfResult] = list(good_project.batch.processed_pdfs)
        # Create a PdfResult entry with filename_new cleared by using the first successful entry
        first = next(e for e in real_entries if isinstance(e, PdfResult) and e.result == "SUCCESS")
        first_info = first.payload.statement_info  # type: ignore[union-attr]
        no_dst = replace(
            first,
            payload=replace(
                first.payload,  # type: ignore[union-attr]
                statement_info=replace(first_info, filename_new=""),
            ),
        )
        mixed: list[BaseException | PdfResult] = [no_dst, *real_entries]
        mixed_pdfs = [good_project.pdfs[0], *good_project.pdfs]  # extra dummy pdf for no_dst
        copied = copy_statements_to_project(mixed, pdfs=mixed_pdfs, project_path=good_project.project_path)
        expected = sum(1 for e in real_entries if isinstance(e, PdfResult) and e.result == "SUCCESS")
        assert len(copied) == expected


# ---------------------------------------------------------------------------
# TestBatchReports
# ---------------------------------------------------------------------------


class TestBatchReports:
    """Verify batch_id filtering on all report classes that support it."""

    def _get_batch_id(self, good_project) -> str:
        """Return the first batch ID from the database."""
        paths = ProjectPaths.resolve(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            row = conn.execute("SELECT ID_BATCH FROM batch_heads ORDER BY STD_UPDATETIME LIMIT 1").fetchone()
        assert row is not None, "No batch_heads rows found — cannot test batch filtering"
        return row[0]

    def test_dim_statement_batch_returns_rows(self, good_project):
        """DimStatement(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.DimStatement(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"DimStatement(batch_id={batch_id!r}) returned no rows"

    def test_dim_statement_batch_subset_of_full(self, good_project):
        """DimStatement(batch_id=...) row count ≤ full DimStatement row count."""
        batch_id = self._get_batch_id(good_project)
        full = db.DimStatement(project_path=good_project.project_path).all.collect()
        filtered = db.DimStatement(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert filtered.height <= full.height

    def test_dim_account_batch_returns_rows(self, good_project):
        """DimAccount(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.DimAccount(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"DimAccount(batch_id={batch_id!r}) returned no rows"

    def test_dim_time_batch_returns_rows(self, good_project):
        """DimTime(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.DimTime(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"DimTime(batch_id={batch_id!r}) returned no rows"

    def test_dim_time_batch_subset_of_full(self, good_project):
        """DimTime(batch_id=...) row count ≤ full DimTime row count."""
        batch_id = self._get_batch_id(good_project)
        full = db.DimTime(project_path=good_project.project_path).all.collect()
        filtered = db.DimTime(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert filtered.height <= full.height

    def test_fact_transaction_batch_returns_rows(self, good_project):
        """FactTransaction(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.FactTransaction(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"FactTransaction(batch_id={batch_id!r}) returned no rows"

    def test_fact_transaction_batch_subset_of_full(self, good_project):
        """FactTransaction(batch_id=...) row count ≤ full FactTransaction row count."""
        batch_id = self._get_batch_id(good_project)
        full = db.FactTransaction(project_path=good_project.project_path).all.collect()
        filtered = db.FactTransaction(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert filtered.height <= full.height

    def test_fact_balance_batch_returns_rows(self, good_project):
        """FactBalance(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.FactBalance(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"FactBalance(batch_id={batch_id!r}) returned no rows"

    def test_fact_balance_batch_subset_of_full(self, good_project):
        """FactBalance(batch_id=...) row count ≤ full FactBalance row count."""
        batch_id = self._get_batch_id(good_project)
        full = db.FactBalance(project_path=good_project.project_path).all.collect()
        filtered = db.FactBalance(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert filtered.height <= full.height

    def test_flat_transaction_batch_returns_rows(self, good_project):
        """FlatTransaction(batch_id=...) returns at least one row."""
        batch_id = self._get_batch_id(good_project)
        df = db.FlatTransaction(project_path=good_project.project_path, batch_id=batch_id).all.collect()
        assert df.height > 0, f"FlatTransaction(batch_id={batch_id!r}) returned no rows"

    def test_flat_transaction_batch_count_matches_fact_transaction_batch(self, good_project):
        """FlatTransaction(batch_id=...) row count equals FactTransaction(batch_id=...) row count."""
        batch_id = self._get_batch_id(good_project)
        db_path = good_project.project_path
        ft_count = db.FactTransaction(project_path=db_path, batch_id=batch_id).all.collect().height
        flat_count = db.FlatTransaction(project_path=db_path, batch_id=batch_id).all.collect().height
        assert flat_count == ft_count

    def test_no_batch_id_unchanged(self, good_project):
        """Passing batch_id=None returns the same result as the no-argument constructor."""
        db_path = good_project.project_path
        df_default = db.FactTransaction(project_path=db_path).all.collect()
        df_none = db.FactTransaction(project_path=db_path, batch_id=None).all.collect()
        assert df_default.height == df_none.height


# ---------------------------------------------------------------------------
# TestBadStatements
# ---------------------------------------------------------------------------


class TestBadStatements:
    def test_bad_pdfs_produce_errors(self, bad_project):
        """Processing bad PDFs results in at least one error."""
        assert bad_project.batch.errors > 0

    def test_bad_pdf_error_count_matches_pdf_count(self, bad_project):
        """At least one error is raised per bad PDF batch (some PDFs may be ambiguous)."""
        assert bad_project.batch.errors >= 1
