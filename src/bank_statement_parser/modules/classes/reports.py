import polars as pl

import bank_statement_parser.modules.classes.database as db
import bank_statement_parser.modules.paths as pt


class DimTime:
    def __init__(self) -> None:
        self.start_date: str = pl.read_parquet(pt.STATEMENT_LINES).select("STD_TRANSACTION_DATE").min().item()
        self.end_date: str = pl.read_parquet(pt.STATEMENT_HEADS).select("STD_STATEMENT_DATE").max().item()
        self.date_series: pl.Series = pl.Series("ID_DATE", pl.date_ranges(self.start_date, self.end_date, eager=True)).explode()
        self.all: pl.DataFrame = pl.DataFrame(self.date_series).select(
            date="ID_DATE",
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
        )


class DimStatement:
    def __init__(self) -> None:
        self.raw: pl.DataFrame = pl.read_parquet(pt.STATEMENT_HEADS).join(
            other=pl.read_parquet(pt.BATCH_LINES), on=["ID_STATEMENT", "ID_BATCH"], how="inner", validate="1:1", coalesce=True
        )
        self.all: pl.DataFrame = self.raw.select(
            id_statement="ID_STATEMENT",
            id_statement_int="index",
            company="STD_COMPANY",
            account_type="STD_ACCOUNT_NUMBER",
            account="STD_ACCOUNT",
            account_number="STD_ACCOUNT_NUMBER",
            account_holder="STD_ACCOUNT_HOLDER",
            statement_date="STD_STATEMENT_DATE",
            filename="STD_FILENAME",
            batch_time="STD_UPDATETIME",
        )


class FactStatement:
    def __init__(self) -> None:
        self.raw: pl.DataFrame = pl.read_parquet(pt.STATEMENT_HEADS).join(
            other=pl.read_parquet(pt.STATEMENT_LINES), on="ID_STATEMENT", how="inner", validate="1:m", coalesce=True
        )
        self.all: pl.DataFrame = self.raw.select(
            id_transaction="ID_TRANSACTION",
            id_transaction_int="index_right",
            id_statement="ID_STATEMENT",
            id_statement_int="index",
            id_date="STD_TRANSACTION_DATE",
            transaction_number="STD_TRANSACTION_NUMBER",
            transaction_desc="STD_TRANSACTION_DESC",
            value_in="STD_PAYMENTS_IN_right",
            value_out="STD_PAYMENTS_OUT_right",
            value=pl.col("STD_PAYMENTS_IN_right").add(pl.col("STD_PAYMENTS_OUT_right").mul(-1)),
        )
        print(self.all)
        ...


def main():
    FactStatement()


class GapReport:
    def __init__(self):
        self.raw = (
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
        self.all = self.raw.select(
            account_type="STD_ACCOUNT",
            account_number="STD_ACCOUNT_NUMBER",
            account_holder="STD_ACCOUNT_HOLDER",
            statement_date="STD_STATEMENT_DATE",
            opening_balance="STD_OPENING_BALANCE",
            closing_balance="STD_CLOSING_BALANCE",
            gap_flag=pl.when(pl.col("gap")).then(pl.lit("GAP")).otherwise(pl.lit("")),
        )
        self.gaps = self.all.join(
            other=self.all.filter(pl.col("gap_flag") == "GAP").select("account_type", "account_number").unique(),
            on=["account_type", "account_number"],
        )


# class FACT_Balance:
#     def __init__(self, account_type: str | None = None, account_number: str | None = None):
#         self.account_type = account_type
#         self.account_number = account_number
#         self._data_head: pl.DataFrame = (
#             pl.read_parquet(DIM_STATEMENT)
#             .filter(pl.col("STD_ACCOUNT") == self.account_type)
#             .select("ID_STATEMENT", "STD_STATEMENT_DATE", "STD_ACCOUNT_NUMBER")
#         )
#         self._data_lines: pl.DataFrame = (
#             pl.read_parquet(FACT_TRANSACTION)
#             .join(self._data_head, on="ID_STATEMENT", coalesce=True, maintain_order="left")
#             .sort("STD_ACCOUNT_NUMBER", "STD_STATEMENT_DATE", "STD_TRANSACTION_DATE")
#             .select(
#                 "STD_ACCOUNT_NUMBER",
#                 "ID_STATEMENT",
#                 "STD_STATEMENT_DATE",
#                 "ID_TRANSACTION",
#                 "STD_TRANSACTION_DATE",
#                 "STD_RUNNING_BALANCE",
#                 "STD_MOVEMENT",
#                 "index",
#             )
#         )
#         self._date_range: pl.DataFrame = (
#             self._data_lines.group_by("STD_ACCOUNT_NUMBER")
#             .agg(pl.col("STD_STATEMENT_DATE").max().alias("date_max"), pl.col("STD_TRANSACTION_DATE").min().alias("date_min"))
#             .with_columns(ID_DAY=pl.date_ranges("date_min", "date_max"))
#             .explode("ID_DAY")
#             .drop("date_min", "date_max")
#         )
#         self.daily = self._date_range.join(
#             other=self._data_lines.group_by("STD_ACCOUNT_NUMBER", "STD_TRANSACTION_DATE").agg(
#                 pl.col("STD_MOVEMENT").sum(), pl.col("STD_RUNNING_BALANCE").last()
#             ),
#             left_on=["STD_ACCOUNT_NUMBER", "ID_DAY"],
#             right_on=["STD_ACCOUNT_NUMBER", "STD_TRANSACTION_DATE"],
#             how="left",
#             validate="1:1",
#             maintain_order="left",
#         ).with_columns(pl.col("STD_MOVEMENT").fill_null(0), pl.col("STD_RUNNING_BALANCE").fill_null(strategy="forward"))
#         self.monthly = (
#             self.daily.group_by("STD_ACCOUNT_NUMBER", pl.col("ID_DAY").dt.month_end().alias("ID_MONTH"))
#             .agg(pl.col("STD_MOVEMENT").sum(), pl.col("STD_RUNNING_BALANCE").last())
#             .sort("STD_ACCOUNT_NUMBER", "ID_MONTH")
#             .with_columns(PERIOD=pl.col("ID_MONTH").dt.year().mul(100).add(pl.col("ID_MONTH").dt.month()))
#         )
#         self.yearly = ()


if __name__ == "__main__":
    pl.Config.set_tbl_rows(50)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
