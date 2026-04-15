"""
SQLite-backed report classes and export helpers.

Functions:
    export_csv: Write all report CSVs to a folder (defaults to project export/csv/).
    export_excel: Write all report sheets to a single Excel workbook
        (defaults to project export/excel/transactions.xlsx).
    export_json: Write all report JSON files to a folder (defaults to project export/json/).
    export_reporting_data: Write CSV reporting feeds to reporting/data/single/ and
        reporting/data/multi/ inside the project directory.
    export_spec: Load a TOML export spec and write filtered, formatted export file(s).

Classes:
    FlatTransaction, FactBalance, DimTime, DimStatement, DimAccount,
    FactTransaction, GapReport: Report tables/views backed by SQLite.
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl
from xlsxwriter import Workbook

from bank_statement_parser.modules.errors import ProjectDatabaseMissing
from bank_statement_parser.modules.paths import ProjectPaths


def _require_db(paths) -> None:
    """Raise appropriate errors if the database directory or file are missing.

    Args:
        paths: A :class:`~bank_statement_parser.modules.paths.ProjectPaths` instance.

    Raises:
        ProjectSubFolderNotFound: If the ``database/`` directory does not exist.
        ProjectDatabaseMissing: If ``database/project.db`` does not exist.
    """
    paths.require_subdir_for_read(paths.database)
    if not paths.project_db.exists():
        raise ProjectDatabaseMissing(paths.project_db)


def _read_data(db_path: Path, table_name: str) -> pl.LazyFrame:
    """Read all rows from a SQLite table or view into a LazyFrame.

    Args:
        db_path: Path to the SQLite database file.
        table_name: Name of the table or view to read.

    Returns:
        A :class:`pl.LazyFrame` containing all rows from the table/view.
    """
    query = f"SELECT * FROM {table_name}"
    with sqlite3.connect(db_path) as conn:
        return pl.read_database(query, connection=conn, infer_schema_length=None).lazy()


def _read_data_filtered(db_path: Path, table_name: str, batch_table: str, batch_id: str | None) -> pl.LazyFrame:
    """Read rows from a SQLite table/view, optionally filtered to a single batch.

    When *batch_id* is ``None`` the full *table_name* is returned unchanged.
    When *batch_id* is provided, *batch_table* is queried and filtered to rows
    matching that ``batch_id``.

    Args:
        db_path: Path to the SQLite database file.
        table_name: Name of the unfiltered table or view to read when *batch_id*
            is ``None``.
        batch_table: Name of the batch-scoped view to query when *batch_id* is
            provided.
        batch_id: Optional batch identifier to filter by.

    Returns:
        A :class:`pl.LazyFrame` containing the matching rows.
    """
    if batch_id is None:
        return _read_data(db_path, table_name)
    query = f"SELECT * FROM {batch_table} WHERE batch_id = ?"
    with sqlite3.connect(db_path) as conn:
        return pl.read_database(query, connection=conn, execute_options={"parameters": [batch_id]}, infer_schema_length=None).lazy()


# Names of the numeric (float) tables in the full export — used by export_excel
# to pass float_precision=2.
_FLOAT_TABLES: frozenset[str] = frozenset({"transaction_measures", "daily_account_balances"})


def _ts() -> str:
    """Return the current local datetime as a human-readable timestamp string.

    Returns:
        Datetime formatted as ``"yyyymmddHHMMSS"``, e.g. ``"20250331143022"``.
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")


def _collect_report_frames(
    type: Literal["single", "multi"],
    project_path: Path | None,
    batch_id: str | None = None,
) -> list[tuple[str, pl.DataFrame]]:
    """Collect and return report DataFrames for the given export type.

    Each entry in the returned list is a ``(logical_name, DataFrame)`` pair.
    The logical name is used by callers to derive file names or worksheet names.

    Args:
        type: Export preset — ``"single"`` (flat transactions table only) or
            ``"multi"`` (all six star-schema tables).
        project_path: Optional project root directory.  Falls back to the
            bundled default project when ``None``.
        batch_id: Optional batch identifier to filter all report tables to a
            single batch.  When ``None`` all rows are returned.

    Returns:
        A list of ``(name, DataFrame)`` tuples in export order.
    """
    if type == "multi":
        return [
            ("statement_dimension", DimStatement(project_path, batch_id).all.collect()),
            ("account_dimension", DimAccount(project_path, batch_id).all.collect()),
            ("calendar_dimension", DimTime(project_path, batch_id).all.collect()),
            ("transaction_measures", FactTransaction(project_path, batch_id).all.collect()),
            ("daily_account_balances", FactBalance(project_path, batch_id).all.collect()),
            ("missing_statement_report", GapReport(project_path).all.collect()),
        ]
    # type == "single"
    return [
        ("transactions", FlatTransaction(project_path, batch_id).all.collect()),
    ]


def export_csv(
    folder: Path | None = None,
    type: Literal["single", "multi"] = "single",
    project_path: Path | None = None,
    batch_id: str | None = None,
    filename_timestamp: bool = False,
) -> None:
    """
    Write report data to CSV files in *folder*.

    Each table is written as a separate ``.csv`` file named after its logical
    table name (e.g. ``transactions.csv``, or ``statement_dimension.csv``,
    ``account_dimension.csv``, etc. for ``type="multi"``).

    When *filename_timestamp* is ``True``:

    - ``type="single"``: the timestamp is appended to the filename, e.g.
      ``transactions_20250331143022.csv``.
    - ``type="multi"``: files are written into a ``multi_20250331143022/``
      sub-folder inside *folder* with their original names.

    When *filename_timestamp* is ``False``:

    - ``type="single"``: files are written directly to *folder* with their
      original names, e.g. ``transactions.csv``.
    - ``type="multi"``: files are written into a ``multi/`` sub-folder inside
      *folder* with their original names.

    Args:
        folder: Directory to write CSV files into.  When ``None`` the project's
            ``export/csv/`` directory (resolved via *project_path*) is used and
            created automatically if absent.
        type: Export preset — ``"single"`` (flat transactions table) or
            ``"multi"`` (separate star-schema tables for loading into a
            database).  Defaults to ``"single"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
        batch_id: Optional batch identifier to filter report data to a single
            batch.  When ``None`` all rows are exported.
        filename_timestamp: When ``True``, append a human-readable timestamp
            (``yyyymmddHHMMSS``) to the filename (single) or create a
            timestamped sub-folder (multi).  Defaults to ``False``.
    """
    if folder is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.csv)
        folder = paths.csv
    frames = _collect_report_frames(type, project_path, batch_id)
    if type == "multi":
        out_folder = folder / (f"multi_{_ts()}" if filename_timestamp else "multi")
        out_folder.mkdir(parents=True, exist_ok=True)
        for name, df in frames:
            df.write_csv(
                file=out_folder / f"{name}.csv",
                separator=",",
                include_header=True,
                quote_style="non_numeric",
                float_precision=2,
            )
    elif filename_timestamp:
        ts = _ts()
        for name, df in frames:
            df.write_csv(
                file=folder / f"{name}_{ts}.csv",
                separator=",",
                include_header=True,
                quote_style="non_numeric",
                float_precision=2,
            )
    else:
        for name, df in frames:
            df.write_csv(
                file=folder / f"{name}.csv",
                separator=",",
                include_header=True,
                quote_style="non_numeric",
                float_precision=2,
            )


def export_excel(
    path: Path | None = None,
    type: Literal["single", "multi"] = "single",
    project_path: Path | None = None,
    batch_id: str | None = None,
    filename_timestamp: bool = False,
) -> None:
    """
    Write report data to an Excel workbook at *path*.

    Each table is written as a separate worksheet.  For ``type="single"`` a
    single ``transactions`` sheet is written; for ``type="multi"`` six sheets
    are written (``statement_dimension``, ``account_dimension``,
    ``calendar_dimension``, ``transaction_measures``,
    ``daily_account_balances``, ``missing_statement_report``).

    Filename conventions:

    - ``type="single"``, no timestamp: ``transactions.xlsx``
    - ``type="single"``, with timestamp: ``transactions_20250331143022.xlsx``
    - ``type="multi"``, no timestamp: ``transactions_multi.xlsx``
    - ``type="multi"``, with timestamp: ``transactions_multi_20250331143022.xlsx``

    Worksheet names are never modified by the timestamp or type logic.

    Args:
        path: Full file path for the output ``.xlsx`` workbook.  When ``None``
            the file is written to ``export/excel/transactions.xlsx`` inside the
            project directory resolved via *project_path*.
        type: Export preset — ``"single"`` (flat transactions table) or
            ``"multi"`` (separate star-schema sheets for loading into a
            database).  Defaults to ``"single"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
        batch_id: Optional batch identifier to filter report data to a single
            batch.  When ``None`` all rows are exported.
        filename_timestamp: When ``True``, append a human-readable timestamp
            (``yyyymmddHHMMSS``) to the workbook filename.  Worksheet names
            are unaffected.  Defaults to ``False``.
    """
    if path is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.excel)
        path = paths.excel / "transactions.xlsx"
    if type == "multi":
        if filename_timestamp:
            path = path.parent / f"{path.stem}_multi_{_ts()}{path.suffix}"
        else:
            path = path.parent / f"{path.stem}_multi{path.suffix}"
    elif filename_timestamp:
        path = path.parent / f"{path.stem}_{_ts()}{path.suffix}"
    with Workbook(str(path)) as wb:
        for name, df in _collect_report_frames(type, project_path, batch_id):
            extra: dict = {"float_precision": 2} if name in _FLOAT_TABLES else {}
            df.write_excel(
                workbook=wb,
                worksheet=name,
                autofit=False,
                table_name=name,
                table_style="Table Style Medium 4",
                **extra,
            )


def export_json(
    folder: Path | None = None,
    type: Literal["single", "multi"] = "single",
    project_path: Path | None = None,
    batch_id: str | None = None,
    filename_timestamp: bool = False,
) -> None:
    """
    Write report data to JSON files in *folder*.

    Each table is written as a separate ``.json`` file containing a JSON array
    of row objects, named after its logical table name (e.g.
    ``transactions.json``, or ``statement_dimension.json``,
    ``account_dimension.json``, etc. for ``type="multi"``).

    When *filename_timestamp* is ``True``:

    - ``type="single"``: the timestamp is appended to the filename, e.g.
      ``transactions_20250331143022.json``.
    - ``type="multi"``: files are written into a ``multi_20250331143022/``
      sub-folder inside *folder* with their original names.

    When *filename_timestamp* is ``False``:

    - ``type="single"``: files are written directly to *folder* with their
      original names, e.g. ``transactions.json``.
    - ``type="multi"``: files are written into a ``multi/`` sub-folder inside
      *folder* with their original names.

    Args:
        folder: Directory to write JSON files into.  When ``None`` the
            project's ``export/json/`` directory (resolved via *project_path*)
            is used and created automatically if absent.
        type: Export preset — ``"single"`` (flat transactions table) or
            ``"multi"`` (separate star-schema tables for loading into a
            database).  Defaults to ``"single"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
        batch_id: Optional batch identifier to filter report data to a single
            batch.  When ``None`` all rows are exported.
        filename_timestamp: When ``True``, append a human-readable timestamp
            (``yyyymmddHHMMSS``) to the filename (single) or create a
            timestamped sub-folder (multi).  Defaults to ``False``.
    """
    if folder is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.json)
        folder = paths.json
    frames = _collect_report_frames(type, project_path, batch_id)
    if type == "multi":
        out_folder = folder / (f"multi_{_ts()}" if filename_timestamp else "multi")
        out_folder.mkdir(parents=True, exist_ok=True)
        for name, df in frames:
            df.write_json(out_folder / f"{name}.json")
    elif filename_timestamp:
        ts = _ts()
        for name, df in frames:
            df.write_json(folder / f"{name}_{ts}.json")
    else:
        for name, df in frames:
            df.write_json(folder / f"{name}.json")


def export_reporting_data(project_path: Path | None = None) -> None:
    """
    Write CSV reporting feeds to the project's ``reporting/data/`` sub-directories.

    Calls :func:`export_csv` twice — once with ``type="single"`` writing to
    ``reporting/data/single/`` and once with ``type="multi"`` writing to
    ``reporting/data/multi/`` (created as a sub-folder of ``reporting/data/``
    by the multi-export logic).  Both directories are created automatically if
    absent.

    This produces a stable set of CSV files that external reporting tools
    (e.g. Power BI, Tableau, Excel) can point at directly without needing
    to know about the full export machinery.

    Args:
        project_path: Optional project root directory.  Falls back to the
            bundled default project when ``None``.

    Example::

        import bank_statement_parser as bsp
        from pathlib import Path

        bsp.db.export_reporting_data(project_path=Path("/my/project"))
        # Writes:
        #   /my/project/reporting/data/single/transactions.csv
        #   /my/project/reporting/data/multi/statement_dimension.csv
        #   /my/project/reporting/data/multi/account_dimension.csv
        #   /my/project/reporting/data/multi/calendar_dimension.csv
        #   /my/project/reporting/data/multi/transaction_measures.csv
        #   /my/project/reporting/data/multi/daily_account_balances.csv
        #   /my/project/reporting/data/multi/missing_statement_report.csv
    """
    paths = ProjectPaths.resolve(project_path)
    paths.ensure_subdir_for_write(paths.reporting_data_single)
    paths.ensure_subdir_for_write(paths.reporting_data)
    export_csv(folder=paths.reporting_data_single, type="single", project_path=project_path)
    export_csv(folder=paths.reporting_data, type="multi", project_path=project_path)


class FlatTransaction:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "FlatTransaction", "FlatTransactionBatch", batch_id)


class FactBalance:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "FactBalance", "FactBalanceBatch", batch_id)


class DimTime:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "DimDate", "DimDateBatch", batch_id)


class DimStatement:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "DimStatement", "DimStatementBatch", batch_id)


class DimAccount:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "DimAccount", "DimAccountBatch", batch_id)


class FactTransaction:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None, batch_id: str | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data_filtered(paths.project_db, "FactTransaction", "FactTransactionBatch", batch_id)


class GapReport:
    __slots__ = ("all", "gaps")

    def __init__(self, project_path: Path | None = None) -> None:
        paths = ProjectPaths.resolve(project_path)
        _require_db(paths)
        self.all = _read_data(paths.project_db, "GapReport")
        self.gaps = self.all.filter(pl.col("gap_flag") == "GAP")


if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    export_excel(ProjectPaths.resolve().excel.joinpath("test_db.xlsx"), type="single")
