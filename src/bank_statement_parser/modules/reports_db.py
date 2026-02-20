from pathlib import Path

import polars as pl
import sqlite3
from xlsxwriter import Workbook

from bank_statement_parser.modules.errors import ProjectDatabaseMissing
from bank_statement_parser.modules.paths import get_paths


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


def _read_view(db_path: Path, view_name: str, id_batch: str | None = None) -> pl.LazyFrame:
    query = f"SELECT * FROM {view_name}"
    if id_batch and view_name in ("DimStatement", "FactTransaction", "FlatTransaction"):
        query += f" WHERE id_batch = '{id_batch}'"
    with sqlite3.connect(db_path) as conn:
        return pl.read_database(query, connection=conn).lazy()


def export_csv(
    folder: Path,
    type: str = "full",
    id_batch: str | None = None,
    project_path: Path | None = None,
):
    if type == "full":
        DimStatement(id_batch, project_path).all.collect().write_csv(
            file=folder.joinpath("statement.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        DimAccount(project_path).all.collect().write_csv(
            file=folder.joinpath("account.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        DimTime(project_path).all.collect().write_csv(
            file=folder.joinpath("calendar.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        FactTransaction(id_batch, project_path).all.collect().write_csv(
            file=folder.joinpath("transactions.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        FactBalance(project_path).all.collect().write_csv(
            file=folder.joinpath("balances.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        GapReport(project_path).all.collect().write_csv(
            file=folder.joinpath("gaps.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
        FlatTransaction(id_batch, project_path).all.collect().write_csv(
            file=folder.joinpath("flat.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )
    elif type == "simple":
        FlatTransaction(id_batch, project_path).all.collect().write_csv(
            file=folder.joinpath("transactions_table.csv"), separator=",", include_header=True, quote_style="non_numeric", float_precision=2
        )


def export_excel(
    path: Path,
    type: str = "full",
    id_batch: str | None = None,
    project_path: Path | None = None,
):
    with Workbook(str(path)) as wb:
        if type == "full":
            DimStatement(id_batch, project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="statement",
                autofit=False,
                table_name="statement",
                table_style="Table Style Medium 4",
            )
            DimAccount(project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="account",
                autofit=False,
                table_name="account",
                table_style="Table Style Medium 4",
            )
            DimTime(project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="calendar",
                autofit=False,
                table_name="calendar",
                table_style="Table Style Medium 4",
            )
            FactTransaction(id_batch, project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="transactions",
                autofit=False,
                table_name="transactions",
                table_style="Table Style Medium 4",
                float_precision=2,
            )
            FactBalance(project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="balances",
                autofit=False,
                table_name="balances",
                table_style="Table Style Medium 4",
                float_precision=2,
            )
            GapReport(project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="gaps",
                autofit=False,
                table_name="gaps",
                table_style="Table Style Medium 4",
            )
            FlatTransaction(id_batch, project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="flat",
                autofit=False,
                table_name="flat",
                table_style="Table Style Medium 4",
            )
        elif type == "simple":
            FlatTransaction(id_batch, project_path).all.collect().write_excel(
                workbook=wb,
                worksheet="transactions_table",
                autofit=False,
                table_name="flat",
                table_style="Table Style Medium 4",
            )


class FlatTransaction:
    __slots__ = ("all",)

    def __init__(self, id_batch: str | None = None, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "FlatTransaction", id_batch)


class FactBalance:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "FactBalance")


class DimTime:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "DimTime")


class DimStatement:
    __slots__ = ("all",)

    def __init__(self, id_batch: str | None = None, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "DimStatement", id_batch)


class DimAccount:
    __slots__ = ("all",)

    def __init__(self, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "DimAccount")


class FactTransaction:
    __slots__ = ("all",)

    def __init__(self, id_batch: str | None = None, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "FactTransaction", id_batch)


class GapReport:
    __slots__ = ("all", "gaps")

    def __init__(self, project_path: Path | None = None) -> None:
        paths = get_paths(project_path)
        _require_db(paths)
        self.all = _read_view(paths.project_db, "GapReport")
        self.gaps = self.all.filter(pl.col("gap_flag") == "GAP")


if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    export_excel(get_paths().excel.joinpath("test_db.xlsx"), type="simple")
