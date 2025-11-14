from pathlib import Path

import polars as pl
from xlsxwriter import Workbook

import bank_statement_parser.modules.paths as pt


def export_excel(path: Path):
    with Workbook(str(path)) as wb:
        DimStatement().all.write_excel(
            workbook=wb,
            worksheet="statement",
            autofit=False,
            table_name="statement",
            table_style="Table Style Medium 4",
        )
        DimAccount().all.write_excel(
            workbook=wb,
            worksheet="account",
            autofit=False,
            table_name="account",
            table_style="Table Style Medium 4",
        )
        DimTime().all.write_excel(
            workbook=wb,
            worksheet="calendar",
            autofit=False,
            table_name="calendar",
            table_style="Table Style Medium 4",
        )
        FactTransaction().all.write_excel(
            workbook=wb,
            worksheet="transactions",
            autofit=False,
            table_name="transactions",
            table_style="Table Style Medium 4",
            float_precision=2,
        )
        FactBalance().all.write_excel(
            workbook=wb, worksheet="balances", autofit=False, table_name="balances", table_style="Table Style Medium 4", float_precision=2
        )
        GapReport().all.write_excel(
            workbook=wb,
            worksheet="gaps",
            autofit=False,
            table_name="gaps",
            table_style="Table Style Medium 4",
        )


def main():
    export_excel(pt.EXCEL.joinpath("test.xlsx"))
    # print(pl.read_parquet(pt.STATEMENT_LINES))


class FactBalance:
    def __init__(self) -> None:
        self._cartesian_date_account: pl.DataFrame = (
            DimTime().all.select("id_date").join(pl.read_parquet(pt.STATEMENT_HEADS).select(id_account="ID_ACCOUNT").unique(), how="cross")
        )
        self._account_days: pl.DataFrame = (
            pl.read_parquet(pt.STATEMENT_HEADS)
            .select(id_statement="ID_STATEMENT", id_account="ID_ACCOUNT")
            .join(
                pl.read_parquet(pt.STATEMENT_LINES).select(
                    id_statement="ID_STATEMENT",
                    id_date="STD_TRANSACTION_DATE",
                    trnno="STD_TRANSACTION_NUMBER",
                    opening_balance="STD_OPENING_BALANCE",
                    closing_balance="STD_CLOSING_BALANCE",
                    movement=pl.col("STD_PAYMENTS_IN").sub(pl.col("STD_PAYMENTS_OUT")),
                ),
                how="inner",
                on="id_statement",
            )
            .sort("id_account", "id_date", "trnno")
            .group_by("id_account", "id_date")
            .agg(
                opening_balance=pl.col("opening_balance").first(),
                closing_balance=pl.col("closing_balance").last(),
                movement=pl.col("movement").sum(),
            )
            .sort("id_account", "id_date")
        )
        self._bookends: pl.DataFrame = self._account_days.group_by("id_account").agg(
            first_day=pl.col("id_date").min(), last_day=pl.col("id_date").max()
        )
        self.raw: pl.DataFrame = self._cartesian_date_account.join(self._bookends, how="inner", on="id_account", validate="m:1").join(
            self._account_days, on=["id_account", "id_date"], how="left", validate="1:1"
        )
        self.all: pl.DataFrame = pl.DataFrame()

        for acct in self.raw.partition_by("id_account"):
            self.all.vstack(
                acct.sort("id_date")
                .with_columns(
                    opening_balance=pl.col("closing_balance")
                    .fill_null(strategy="forward")
                    .sub(pl.col("movement").fill_null(0.0000))
                    .cast(float),
                    closing_balance=pl.col("closing_balance").fill_null(strategy="forward").cast(float),
                    pre_date=pl.when(pl.col("id_date") < pl.col("first_day")).then(pl.lit(True)).otherwise(pl.lit(False)),
                    post_date=pl.when(pl.col("id_date") > pl.col("last_day")).then(pl.lit(True)).otherwise(pl.lit(False)),
                )
                .with_columns(outside_date=pl.col("pre_date").or_(pl.col("post_date")))
                .with_columns(
                    pl.col("opening_balance").fill_null(strategy="backward"), pl.col("closing_balance").fill_null(strategy="backward")
                )
                .drop("movement"),
                in_place=True,
            )


class DimTime:
    def __init__(self) -> None:
        self.start_date: str = pl.read_parquet(pt.STATEMENT_LINES).select("STD_TRANSACTION_DATE").min().item()
        self.end_date: str = pl.read_parquet(pt.STATEMENT_HEADS).select("STD_STATEMENT_DATE").max().item()
        self.date_series: pl.Series = pl.Series("ID_DATE", pl.date_ranges(self.start_date, self.end_date, eager=True)).explode()
        self.all: pl.DataFrame = pl.DataFrame(self.date_series).select(
            id_date="ID_DATE",
            date_local_format=pl.col("ID_DATE").dt.to_string("%x"),
            date_integer=pl.col("ID_DATE").dt.year().mul(100).add(pl.col("ID_DATE").dt.month()).mul(100).add(pl.col("ID_DATE").dt.day()),
            year=pl.col("ID_DATE").dt.year(),
            year_short=pl.col("ID_DATE").dt.to_string("%y"),
            quarter=pl.col("ID_DATE").dt.quarter(),
            quarter_name=pl.lit("Q").add(pl.col("ID_DATE").dt.quarter().cast(str)),
            month_number=pl.col("ID_DATE").dt.month(),
            month_number_padded=pl.col("ID_DATE").dt.to_string("%m"),
            month_name=pl.col("ID_DATE").dt.to_string("%B"),
            month_abbrv=pl.col("ID_DATE").dt.to_string("%b"),
            period=pl.col("ID_DATE").dt.year().mul(100).add(pl.col("ID_DATE").dt.month()),
            week=pl.col("ID_DATE").dt.week(),
            year_week=pl.col("ID_DATE").dt.year().mul(100).add(pl.col("ID_DATE").dt.week()),
            day_of_month=pl.col("ID_DATE").dt.day(),
            day_of_year=pl.col("ID_DATE").dt.to_string("%-j"),
            day_of_week=pl.col("ID_DATE").dt.to_string("%w").cast(int).add(1),
            weekday=pl.col("ID_DATE").dt.to_string("%A"),
            weekday_abbrv=pl.col("ID_DATE").dt.to_string("%a"),
            weekday_initial=pl.col("ID_DATE").dt.to_string("%a").str.head(1),
            is_last_day_of_month=pl.when(pl.col("ID_DATE").dt.days_in_month() == pl.col("ID_DATE").dt.day())
            .then(pl.lit(True))
            .otherwise(pl.lit(False)),
            is_last_day_of_quarter=pl.when(
                (pl.col("ID_DATE").dt.days_in_month() == pl.col("ID_DATE").dt.day()) & (pl.col("ID_DATE").dt.month() % 3 == 0)
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False)),
            is_last_day_of_year=pl.when(
                (pl.col("ID_DATE").dt.days_in_month() == pl.col("ID_DATE").dt.day()) & (pl.col("ID_DATE").dt.month() == 12)
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False)),
            is_weekday=pl.col("ID_DATE").dt.is_business_day(),
        )


class DimStatement:
    def __init__(self) -> None:
        self.raw: pl.DataFrame = pl.read_parquet(pt.STATEMENT_HEADS).join(
            other=pl.read_parquet(pt.BATCH_LINES), on=["ID_STATEMENT", "ID_BATCH"], how="inner", validate="1:1", coalesce=True
        )
        self.all: pl.DataFrame = self.raw.select(
            id_statement="ID_STATEMENT",
            # id_account="ID_ACCOUNT",
            id_statement_int="index",
            # company="STD_COMPANY",
            # account_type="STD_ACCOUNT_NUMBER",
            # account="STD_ACCOUNT",
            # account_number="STD_ACCOUNT_NUMBER",
            # account_holder="STD_ACCOUNT_HOLDER",
            statement_date="STD_STATEMENT_DATE",
            filename="STD_FILENAME",
            batch_time="STD_UPDATETIME",
        )


class DimAccount:
    def __init__(self) -> None:
        self.raw: pl.DataFrame = pl.read_parquet(pt.STATEMENT_HEADS).join(
            other=pl.read_parquet(pt.BATCH_LINES), on=["ID_STATEMENT", "ID_BATCH"], how="inner", validate="1:1", coalesce=True
        )
        self.all: pl.DataFrame = (
            self.raw.select(
                # id_statement="ID_STATEMENT",
                id_account="ID_ACCOUNT",
                # id_statement_int="index",
                company="STD_COMPANY",
                # account_type="STD_ACCOUNT_TYPE",
                account_type="STD_ACCOUNT",
                account_number="STD_ACCOUNT_NUMBER",
                account_holder="STD_ACCOUNT_HOLDER",
                # statement_date="STD_STATEMENT_DATE",
                # filename="STD_FILENAME",
                # batch_time="STD_UPDATETIME",
            )
            .group_by("id_account")
            .last()
        )


class FactTransaction:
    def __init__(self) -> None:
        self.raw: pl.DataFrame = pl.read_parquet(pt.STATEMENT_HEADS).join(
            other=pl.read_parquet(pt.STATEMENT_LINES), on="ID_STATEMENT", how="inner", validate="1:m", coalesce=True
        )
        self.all: pl.DataFrame = self.raw.select(
            id_transaction="ID_TRANSACTION",
            id_transaction_int="index_right",
            id_statement="ID_STATEMENT",
            id_statement_int="index",
            id_account="ID_ACCOUNT",
            id_date="STD_TRANSACTION_DATE",
            transaction_number="STD_TRANSACTION_NUMBER",
            transaction_credit_or_debit="STD_CD",
            transaction_type="STD_TRANSACTION_TYPE",
            transaction_type_cd="STD_TRANSACTION_TYPE_CD",
            transaction_desc="STD_TRANSACTION_DESC",
            value_in=pl.col("STD_PAYMENTS_IN_right").cast(float),
            value_out=pl.col("STD_PAYMENTS_OUT_right").cast(float),
            value=pl.col("STD_PAYMENTS_IN_right").add(pl.col("STD_PAYMENTS_OUT_right").mul(-1)).cast(float),
        )


class GapReport:
    def __init__(self):
        self.raw: pl.DataFrame = (
            pl.read_parquet(pt.STATEMENT_HEADS)
            .select(
                "STD_ACCOUNT",
                "STD_ACCOUNT_NUMBER",
                "STD_ACCOUNT_HOLDER",
                "STD_STATEMENT_DATE",
                "STD_OPENING_BALANCE",
                "STD_CLOSING_BALANCE",
            )
            .sort("STD_ACCOUNT", "STD_ACCOUNT_NUMBER", "STD_STATEMENT_DATE")
            .with_row_index()
            .with_columns(
                gap=pl.when(pl.col("index") == 0)
                .then(pl.lit(False))
                .otherwise(
                    pl.when(
                        pl.col("STD_ACCOUNT").add(pl.col("STD_ACCOUNT_NUMBER"))
                        != pl.col("STD_ACCOUNT").shift().add(pl.col("STD_ACCOUNT_NUMBER").shift())
                    )
                    .then(pl.lit(False))
                    .otherwise(
                        pl.when(pl.col("STD_OPENING_BALANCE") == pl.col("STD_CLOSING_BALANCE").shift())
                        .then(pl.lit(False))
                        .otherwise(pl.lit(True))
                    )
                )
            )
        )
        self.all: pl.DataFrame = self.raw.select(
            account_type="STD_ACCOUNT",
            account_number="STD_ACCOUNT_NUMBER",
            account_holder="STD_ACCOUNT_HOLDER",
            statement_date="STD_STATEMENT_DATE",
            opening_balance=pl.col("STD_OPENING_BALANCE").cast(float),
            closing_balance=pl.col("STD_CLOSING_BALANCE").cast(float),
            gap_flag=pl.when(pl.col("gap")).then(pl.lit("GAP")).otherwise(pl.lit("")),
        )
        self.gaps: pl.DataFrame = self.all.join(
            other=self.all.filter(pl.col("gap_flag") == "GAP").select("account_type", "account_number").unique(),
            on=["account_type", "account_number"],
        )


if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
