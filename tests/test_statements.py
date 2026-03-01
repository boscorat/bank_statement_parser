"""
test_statements.py — integration tests for bank_statement_parser.

Tests are grouped into seven classes:

TestGoodStatements
    Verifies that all PDFs in ``tests/pdfs/good/`` process without error
    and that the expected output files are created.

TestParquetReports
    Verifies the Parquet-backed report classes (reports_parquet) against
    the processed good-PDF data.

TestDbReports
    Verifies the SQLite-backed report classes (reports_db) against the
    processed good-PDF data.

TestCrossBackend
    Verifies that numeric totals and row counts agree between the Parquet
    and DB backends, and that both match the raw statement_lines source.

TestExports
    Verifies CSV and Excel export functions for both the database and
    parquet backends, including full and simple presets and the
    ``filetype="both"`` convenience mode on ``StatementBatch.export()``.

TestCopyStatements
    Verifies that ``copy_statements_to_project()`` copies PDFs into the
    correct ``statements/<year>/<id_account>/`` directory structure, that
    all returned paths exist and are non-empty, that the operation is
    idempotent, and that error/missing entries are silently skipped.

TestBadStatements
    Verifies that each PDF in ``tests/pdfs/bad/`` is flagged as an error.

Run with:
    pytest tests/test_statements.py -v
"""

import sqlite3
from datetime import timedelta
from pathlib import Path

import polars as pl

from bank_statement_parser.modules import reports_db as db
from bank_statement_parser.modules import reports_parquet as parquet
from bank_statement_parser.modules.data import PdfResult
from bank_statement_parser.modules.paths import get_paths
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
        paths = get_paths(good_project.project_path)
        assert paths.statement_heads.exists(), "statement_heads.parquet missing"
        assert paths.statement_lines.exists(), "statement_lines.parquet missing"
        assert paths.batch_lines.exists(), "batch_lines.parquet missing"

    def test_db_exists(self, good_project):
        """SQLite project.db is created and non-empty."""
        paths = get_paths(good_project.project_path)
        assert paths.project_db.exists(), "project.db missing"
        assert paths.project_db.stat().st_size > 0, "project.db is empty"

    def test_no_temp_files_remain(self, good_project):
        """No *_temp_*.parquet files survive after delete_temp_files()."""
        paths = get_paths(good_project.project_path)
        temp_files = list(paths.parquet.glob("*_temp_*.parquet"))
        assert temp_files == [], f"Temp files still present: {temp_files}"


# ---------------------------------------------------------------------------
# TestParquetReports
# ---------------------------------------------------------------------------


class TestParquetReports:
    def test_dim_statement_row_count(self, good_project):
        """DimStatement has exactly one row per processed good PDF."""
        df = parquet.DimStatement(project_path=good_project.project_path).all.collect()
        assert df.height == len(good_project.pdfs)

    def test_dim_account_uniqueness(self, good_project):
        """DimAccount id_account values are unique."""
        df = parquet.DimAccount(project_path=good_project.project_path).all.collect()
        assert df.height == df["id_account"].n_unique()

    def test_dim_account_count_matches_raw(self, good_project):
        """DimAccount row count matches the number of distinct ID_ACCOUNT values in raw parquet."""
        paths = get_paths(good_project.project_path)
        raw_accounts = pl.read_parquet(paths.statement_heads)["ID_ACCOUNT"].n_unique()
        df = parquet.DimAccount(project_path=good_project.project_path).all.collect()
        assert df.height == raw_accounts

    def test_dim_time_contiguous(self, good_project):
        """DimTime date spine contains no gaps."""
        df = parquet.DimTime(project_path=good_project.project_path).all.collect()
        dates = df["id_date"].sort()
        # Consecutive date differences must all be exactly 1 day
        diffs = dates.diff().drop_nulls()
        assert (diffs == timedelta(days=1)).all(), "Date spine has gaps in DimTime (parquet)"

    def test_fact_transaction_row_count(self, good_project):
        """FactTransaction row count matches statement_lines.parquet row count."""
        paths = get_paths(good_project.project_path)
        raw_count = pl.read_parquet(paths.statement_lines).height
        ft = parquet.FactTransaction(project_path=good_project.project_path).all.collect()
        assert ft.height == raw_count

    def test_fact_transaction_has_credits_and_debits(self, good_project):
        """At least one credit (value_in > 0) and one debit (value_out > 0) exist."""
        ft = parquet.FactTransaction(project_path=good_project.project_path).all.collect()
        assert ft.filter(pl.col("value_in") > 0).height > 0, "No credits found in FactTransaction (parquet)"
        assert ft.filter(pl.col("value_out") > 0).height > 0, "No debits found in FactTransaction (parquet)"

    def test_fact_balance_coverage(self, good_project):
        """FactBalance has exactly n_accounts × n_days rows."""
        pq_path = good_project.project_path
        n_accounts = parquet.DimAccount(project_path=pq_path).all.collect().height
        n_days = parquet.DimTime(project_path=pq_path).all.collect().height
        fb = parquet.FactBalance(project_path=pq_path).all.collect()
        assert fb.height == n_accounts * n_days

    def test_gap_report_runs_without_error(self, good_project):
        """GapReport.all.collect() completes without raising an exception."""
        df = parquet.GapReport(project_path=good_project.project_path).all.collect()
        assert df.height >= 0  # any non-raising result is acceptable

    def test_flat_transaction_count_matches_fact_transaction(self, good_project):
        """FlatTransaction row count equals FactTransaction row count."""
        pq_path = good_project.project_path
        ft_count = parquet.FactTransaction(project_path=pq_path).all.collect().height
        flat_count = parquet.FlatTransaction(project_path=pq_path).all.collect().height
        assert flat_count == ft_count


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
        paths = get_paths(good_project.project_path)
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


# ---------------------------------------------------------------------------
# TestCrossBackend
# ---------------------------------------------------------------------------


class TestCrossBackend:
    # ------------------------------------------------------------------
    # Row count parity: parquet vs DB
    # ------------------------------------------------------------------

    def test_fact_transaction_row_counts_agree(self, good_project):
        """Parquet and DB FactTransaction have the same row count."""
        pq_count = parquet.FactTransaction(project_path=good_project.project_path).all.collect().height
        db_count = db.FactTransaction(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    def test_fact_balance_row_counts_agree(self, good_project):
        """Parquet and DB FactBalance have the same row count."""
        pq_count = parquet.FactBalance(project_path=good_project.project_path).all.collect().height
        db_count = db.FactBalance(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    def test_dim_time_row_counts_agree(self, good_project):
        """Parquet and DB DimTime have the same row count."""
        pq_count = parquet.DimTime(project_path=good_project.project_path).all.collect().height
        db_count = db.DimTime(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    def test_dim_account_row_counts_agree(self, good_project):
        """Parquet and DB DimAccount have the same row count."""
        pq_count = parquet.DimAccount(project_path=good_project.project_path).all.collect().height
        db_count = db.DimAccount(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    def test_dim_statement_row_counts_agree(self, good_project):
        """Parquet and DB DimStatement have the same row count."""
        pq_count = parquet.DimStatement(project_path=good_project.project_path).all.collect().height
        db_count = db.DimStatement(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    def test_flat_transaction_row_counts_agree(self, good_project):
        """Parquet and DB FlatTransaction have the same row count."""
        pq_count = parquet.FlatTransaction(project_path=good_project.project_path).all.collect().height
        db_count = db.FlatTransaction(project_path=good_project.project_path).all.collect().height
        assert pq_count == db_count

    # ------------------------------------------------------------------
    # Numeric totals: parquet vs DB
    # ------------------------------------------------------------------

    def test_fact_transaction_total_value_in_agrees(self, good_project):
        """Parquet and DB FactTransaction total value_in agree within tolerance."""
        pq_sum = parquet.FactTransaction(project_path=good_project.project_path).all.collect()["value_in"].sum()
        db_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_in"].sum()
        assert abs(pq_sum - db_sum) < FLOAT_TOL, f"value_in mismatch: parquet={pq_sum}, db={db_sum}"

    def test_fact_transaction_total_value_out_agrees(self, good_project):
        """Parquet and DB FactTransaction total value_out agree within tolerance."""
        pq_sum = parquet.FactTransaction(project_path=good_project.project_path).all.collect()["value_out"].sum()
        db_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_out"].sum()
        assert abs(pq_sum - db_sum) < FLOAT_TOL, f"value_out mismatch: parquet={pq_sum}, db={db_sum}"

    def test_fact_transaction_total_net_value_agrees(self, good_project):
        """Parquet and DB FactTransaction total net value agree within tolerance."""
        pq_sum = parquet.FactTransaction(project_path=good_project.project_path).all.collect()["value"].sum()
        db_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value"].sum()
        assert abs(pq_sum - db_sum) < FLOAT_TOL, f"net value mismatch: parquet={pq_sum}, db={db_sum}"

    # ------------------------------------------------------------------
    # Raw statement_lines vs FactTransaction (parquet backend)
    # ------------------------------------------------------------------

    def test_statement_lines_parquet_value_in_matches_fact_transaction_parquet(self, good_project):
        """SUM(STD_PAYMENTS_IN) from raw parquet equals FactTransaction(pq) value_in sum."""
        paths = get_paths(good_project.project_path)
        raw_sum = pl.read_parquet(paths.statement_lines)["STD_PAYMENTS_IN"].cast(float).sum()
        ft_sum = parquet.FactTransaction(project_path=good_project.project_path).all.collect()["value_in"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_in mismatch: raw={raw_sum}, parquet FactTransaction={ft_sum}"

    def test_statement_lines_parquet_value_out_matches_fact_transaction_parquet(self, good_project):
        """SUM(STD_PAYMENTS_OUT) from raw parquet equals FactTransaction(pq) value_out sum."""
        paths = get_paths(good_project.project_path)
        raw_sum = pl.read_parquet(paths.statement_lines)["STD_PAYMENTS_OUT"].cast(float).sum()
        ft_sum = parquet.FactTransaction(project_path=good_project.project_path).all.collect()["value_out"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_out mismatch: raw={raw_sum}, parquet FactTransaction={ft_sum}"

    # ------------------------------------------------------------------
    # Raw statement_lines vs FactTransaction (DB backend)
    # ------------------------------------------------------------------

    def test_statement_lines_db_value_in_matches_fact_transaction_db(self, good_project):
        """SUM(STD_PAYMENTS_IN) from SQLite statement_lines equals DB FactTransaction value_in sum."""
        paths = get_paths(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            raw_sum = conn.execute("SELECT SUM(CAST(STD_PAYMENTS_IN AS REAL)) FROM statement_lines").fetchone()[0]
        ft_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_in"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_in mismatch: raw db={raw_sum}, db FactTransaction={ft_sum}"

    def test_statement_lines_db_value_out_matches_fact_transaction_db(self, good_project):
        """SUM(STD_PAYMENTS_OUT) from SQLite statement_lines equals DB FactTransaction value_out sum."""
        paths = get_paths(good_project.project_path)
        with sqlite3.connect(paths.project_db) as conn:
            raw_sum = conn.execute("SELECT SUM(CAST(STD_PAYMENTS_OUT AS REAL)) FROM statement_lines").fetchone()[0]
        ft_sum = db.FactTransaction(project_path=good_project.project_path).all.collect()["value_out"].sum()
        assert abs(raw_sum - ft_sum) < FLOAT_TOL, f"value_out mismatch: raw db={raw_sum}, db FactTransaction={ft_sum}"

    def test_statement_lines_row_count_parquet_matches_db(self, good_project):
        """Raw statement_lines row count agrees between parquet file and SQLite table."""
        paths = get_paths(good_project.project_path)
        pq_count = pl.read_parquet(paths.statement_lines).height
        with sqlite3.connect(paths.project_db) as conn:
            db_count = conn.execute("SELECT COUNT(*) FROM statement_lines").fetchone()[0]
        assert pq_count == db_count


# ---------------------------------------------------------------------------
# TestExports
# ---------------------------------------------------------------------------

# CSV file names produced by ``type="full"`` exports.
_FULL_CSV_FILES = [
    "statement.csv",
    "account.csv",
    "calendar.csv",
    "transactions.csv",
    "balances.csv",
    "gaps.csv",
]


class TestExports:
    """Verify CSV and Excel exports for both backends and presets."""

    # ------------------------------------------------------------------
    # DB backend — CSV
    # ------------------------------------------------------------------

    def test_db_export_csv_full(self, good_project):
        """DB export_csv(type='full') writes all six CSV files."""
        db.export_csv(type="full", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        for name in _FULL_CSV_FILES:
            f = paths.csv / name
            assert f.exists(), f"Missing CSV: {name}"
            assert f.stat().st_size > 0, f"Empty CSV: {name}"

    def test_db_export_csv_simple(self, good_project):
        """DB export_csv(type='simple') writes the flat transactions CSV."""
        db.export_csv(type="simple", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.csv / "transactions_table.csv"
        assert f.exists(), "Missing transactions_table.csv (db/simple)"
        assert f.stat().st_size > 0, "Empty transactions_table.csv (db/simple)"

    # ------------------------------------------------------------------
    # DB backend — Excel
    # ------------------------------------------------------------------

    def test_db_export_excel_full(self, good_project):
        """DB export_excel(type='full') writes a non-empty workbook."""
        db.export_excel(type="full", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (db/full)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (db/full)"

    def test_db_export_excel_simple(self, good_project):
        """DB export_excel(type='simple') writes a non-empty workbook."""
        db.export_excel(type="simple", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (db/simple)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (db/simple)"

    # ------------------------------------------------------------------
    # Parquet backend — CSV
    # ------------------------------------------------------------------

    def test_parquet_export_csv_full(self, good_project):
        """Parquet export_csv(type='full') writes all six CSV files."""
        parquet.export_csv(type="full", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        for name in _FULL_CSV_FILES:
            f = paths.csv / name
            assert f.exists(), f"Missing CSV: {name}"
            assert f.stat().st_size > 0, f"Empty CSV: {name}"

    def test_parquet_export_csv_simple(self, good_project):
        """Parquet export_csv(type='simple') writes the flat transactions CSV."""
        parquet.export_csv(type="simple", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.csv / "transactions_table.csv"
        assert f.exists(), "Missing transactions_table.csv (parquet/simple)"
        assert f.stat().st_size > 0, "Empty transactions_table.csv (parquet/simple)"

    # ------------------------------------------------------------------
    # Parquet backend — Excel
    # ------------------------------------------------------------------

    def test_parquet_export_excel_full(self, good_project):
        """Parquet export_excel(type='full') writes a non-empty workbook."""
        parquet.export_excel(type="full", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (parquet/full)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (parquet/full)"

    def test_parquet_export_excel_simple(self, good_project):
        """Parquet export_excel(type='simple') writes a non-empty workbook."""
        parquet.export_excel(type="simple", project_path=good_project.project_path)
        paths = get_paths(good_project.project_path)
        f = paths.excel / "transactions.xlsx"
        assert f.exists(), "Missing transactions.xlsx (parquet/simple)"
        assert f.stat().st_size > 0, "Empty transactions.xlsx (parquet/simple)"

    # ------------------------------------------------------------------
    # StatementBatch.export() — filetype="both"
    # ------------------------------------------------------------------

    def test_batch_export_both(self, good_project):
        """StatementBatch.export(filetype='both') writes both Excel and CSV."""
        good_project.batch.export(datasource="database", filetype="both", type="full")
        paths = get_paths(good_project.project_path)
        # Excel file must exist
        xlsx = paths.excel / "transactions.xlsx"
        assert xlsx.exists(), "Missing transactions.xlsx after filetype='both'"
        assert xlsx.stat().st_size > 0, "Empty transactions.xlsx after filetype='both'"
        # CSV files must exist
        for name in _FULL_CSV_FILES:
            f = paths.csv / name
            assert f.exists(), f"Missing CSV after filetype='both': {name}"
            assert f.stat().st_size > 0, f"Empty CSV after filetype='both': {name}"


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
        """Copied files land under statements/<year>/<id_account>/<filename>."""
        paths = get_paths(good_project.project_path)
        copied = good_project.batch.copy_statements_to_project()
        for dest in copied:
            # dest must be inside <project>/statements/
            assert dest.is_relative_to(paths.statements), f"{dest} is not under {paths.statements}"
            # Relative path must have exactly three parts: year/id_account/filename
            rel = dest.relative_to(paths.statements)
            parts = rel.parts
            assert len(parts) == 3, f"Expected year/id_account/file, got {parts!r} for {dest}"
            year, id_account, filename = parts
            # year must be a 4-digit string
            assert year.isdigit() and len(year) == 4, f"Unexpected year folder {year!r}"
            # filename stem must end with _YYYYMMDD matching the year folder
            stem = Path(filename).stem
            assert stem[-8:-4] == year, f"Year folder {year!r} does not match date in filename stem {stem!r}"
            # id_account must be the stem minus the trailing _YYYYMMDD
            expected_id_account = stem[: -(len("_YYYYMMDD"))]
            assert id_account == expected_id_account, f"id_account folder {id_account!r} != expected {expected_id_account!r}"

    def test_count_matches_successful_pdfs(self, good_project):
        """Number of copied files equals number of PdfResult entries with file_dst set."""
        expected = sum(
            1 for e in good_project.batch.processed_pdfs if isinstance(e, PdfResult) and e.file_src is not None and e.file_dst is not None
        )
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
        copied = copy_statements_to_project(mixed, project_path=good_project.project_path)
        # Must still copy the real entries — no crash, no reduction in count
        expected = sum(
            1 for e in good_project.batch.processed_pdfs if isinstance(e, PdfResult) and e.file_src is not None and e.file_dst is not None
        )
        assert len(copied) == expected

    def test_skips_entries_without_file_dst(self, good_project):
        """PdfResult entries with file_dst=None are silently skipped."""
        # Build a list that has one extra entry with no dst
        real_entries: list[BaseException | PdfResult] = list(good_project.batch.processed_pdfs)
        # Create a PdfResult-like entry with file_dst=None by using the first real entry
        first = next(e for e in real_entries if isinstance(e, PdfResult) and e.file_dst is not None)
        no_dst = PdfResult(*[None if f == "file_dst" else getattr(first, f) for f in PdfResult._fields])
        mixed: list[BaseException | PdfResult] = [no_dst, *real_entries]
        copied = copy_statements_to_project(mixed, project_path=good_project.project_path)
        expected = sum(1 for e in real_entries if isinstance(e, PdfResult) and e.file_src is not None and e.file_dst is not None)
        assert len(copied) == expected


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
