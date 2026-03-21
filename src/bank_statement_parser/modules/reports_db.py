"""
SQLite-backed report classes and export helpers.

Functions:
    export_csv: Write all report CSVs to a folder (defaults to project export/csv/).
    export_excel: Write all report sheets to a single Excel workbook
        (defaults to project export/excel/transactions.xlsx).
    export_json: Write all report JSON files to a folder (defaults to project export/json/).
    export_reporting_data: Write CSV reporting feeds to reporting/data/simple/ and
        reporting/data/full/ inside the project directory.
    export_spec: Load a TOML export spec and write filtered, formatted export file(s).

Classes:
    FlatTransaction, FactBalance, DimTime, DimStatement, DimAccount,
    FactTransaction, GapReport: Report tables/views backed by SQLite.
"""

import sqlite3
from pathlib import Path

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


def _collect_report_frames(
    type: str,
    project_path: Path | None,
) -> list[tuple[str, pl.DataFrame]]:
    """Collect and return report DataFrames for the given export type.

    Each entry in the returned list is a ``(logical_name, DataFrame)`` pair.
    The logical name is used by callers to derive file names or worksheet names.

    Args:
        type: Export preset — ``"simple"`` (flat transactions table only) or
            ``"full"`` (all six star-schema tables).
        project_path: Optional project root directory.  Falls back to the
            bundled default project when ``None``.

    Returns:
        A list of ``(name, DataFrame)`` tuples in export order.
    """
    if type == "full":
        return [
            ("statement_dimension", DimStatement(project_path).all.collect()),
            ("account_dimension", DimAccount(project_path).all.collect()),
            ("calendar_dimension", DimTime(project_path).all.collect()),
            ("transaction_measures", FactTransaction(project_path).all.collect()),
            ("daily_account_balances", FactBalance(project_path).all.collect()),
            ("missing_statement_report", GapReport(project_path).all.collect()),
        ]
    # type == "simple"
    return [
        ("transactions", FlatTransaction(project_path).all.collect()),
    ]


def export_csv(
    folder: Path | None = None,
    type: str = "simple",
    project_path: Path | None = None,
) -> None:
    """
    Write report data to CSV files in *folder*.

    Each table is written as a separate ``.csv`` file named after its logical
    table name (e.g. ``transactions_table.csv``, or ``statement.csv``,
    ``account.csv``, etc. for ``type="full"``).

    Args:
        folder: Directory to write CSV files into.  When ``None`` the project's
            ``export/csv/`` directory (resolved via *project_path*) is used and
            created automatically if absent.
        type: Export preset — ``"simple"`` (flat transactions table) or
            ``"full"`` (separate star-schema tables for loading into a
            database).  Defaults to ``"simple"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
    """
    if folder is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.csv)
        folder = paths.csv
    for name, df in _collect_report_frames(type, project_path):
        df.write_csv(
            file=folder / f"{name}.csv",
            separator=",",
            include_header=True,
            quote_style="non_numeric",
            float_precision=2,
        )


def export_excel(
    path: Path | None = None,
    type: str = "simple",
    project_path: Path | None = None,
) -> None:
    """
    Write report data to an Excel workbook at *path*.

    Each table is written as a separate worksheet.  For ``type="simple"`` a
    single ``transactions_table`` sheet is written; for ``type="full"`` six
    sheets are written (``statement``, ``account``, ``calendar``,
    ``transactions``, ``balances``, ``gaps``).

    Args:
        path: Full file path for the output ``.xlsx`` workbook.  When ``None``
            the file is written to ``export/excel/transactions.xlsx`` inside the
            project directory resolved via *project_path*.
        type: Export preset — ``"simple"`` (flat transactions table) or
            ``"full"`` (separate star-schema sheets for loading into a
            database).  Defaults to ``"simple"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
    """
    if path is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.excel)
        path = paths.excel / "transactions.xlsx"
    with Workbook(str(path)) as wb:
        for name, df in _collect_report_frames(type, project_path):
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
    type: str = "simple",
    project_path: Path | None = None,
) -> None:
    """
    Write report data to JSON files in *folder*.

    Each table is written as a separate ``.json`` file containing a JSON array
    of row objects, named after its logical table name (e.g.
    ``transactions_table.json``, or ``statement.json``, ``account.json``, etc.
    for ``type="full"``).

    Args:
        folder: Directory to write JSON files into.  When ``None`` the
            project's ``export/json/`` directory (resolved via *project_path*)
            is used and created automatically if absent.
        type: Export preset — ``"simple"`` (flat transactions table) or
            ``"full"`` (separate star-schema tables for loading into a
            database).  Defaults to ``"simple"``.
        project_path: Optional project root used to resolve the default export
            folder and data sources.  Falls back to the bundled default project
            when ``None``.
    """
    if folder is None:
        paths = ProjectPaths.resolve(project_path)
        paths.ensure_subdir_for_write(paths.json)
        folder = paths.json
    for name, df in _collect_report_frames(type, project_path):
        df.write_json(folder / f"{name}.json")


def export_reporting_data(project_path: Path | None = None) -> None:
    """
    Write CSV reporting feeds to the project's ``reporting/data/`` sub-directories.

    Calls :func:`export_csv` twice — once with ``type="simple"`` writing to
    ``reporting/data/simple/`` and once with ``type="full"`` writing to
    ``reporting/data/full/``.  Both directories are created automatically if
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
        #   /my/project/reporting/data/simple/transactions_table.csv
        #   /my/project/reporting/data/full/statement.csv
        #   /my/project/reporting/data/full/account.csv
        #   /my/project/reporting/data/full/calendar.csv
        #   /my/project/reporting/data/full/transactions.csv
        #   /my/project/reporting/data/full/balances.csv
        #   /my/project/reporting/data/full/gaps.csv
    """
    paths = ProjectPaths.resolve(project_path)
    paths.ensure_subdir_for_write(paths.reporting_data_simple)
    paths.ensure_subdir_for_write(paths.reporting_data_full)
    export_csv(folder=paths.reporting_data_simple, type="simple", project_path=project_path)
    export_csv(folder=paths.reporting_data_full, type="full", project_path=project_path)


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
        self.all = _read_data_filtered(paths.project_db, "DimTime", "DimTimeBatch", batch_id)


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
    export_excel(ProjectPaths.resolve().excel.joinpath("test_db.xlsx"), type="simple")
