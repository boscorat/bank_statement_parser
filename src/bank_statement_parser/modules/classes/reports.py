import polars as pl

from bank_statement_parser.modules.paths import PATH_DIM_STATEMENT, PATH_FACT_TRANSACTION


class GapReport:
    def __init__(self):
        self.dataframe = (
            pl.read_parquet(PATH_DIM_STATEMENT)
            .select("STD_FILENAME", "STD_STATEMENT_DATE", "STD_ACCOUNT", "STD_OPENING_BALANCE", "STD_CLOSING_BALANCE")
            .sort("STD_ACCOUNT", "STD_STATEMENT_DATE")
            .with_row_index()
            .with_columns(
                gap=pl.when(pl.col("index") == 0)
                .then(pl.lit(False))
                .otherwise(
                    pl.when(pl.col("STD_ACCOUNT") != pl.col("STD_ACCOUNT").shift())
                    .then(pl.lit(False))
                    .otherwise(
                        pl.when(pl.col("STD_OPENING_BALANCE") == pl.col("STD_CLOSING_BALANCE").shift())
                        .then(pl.lit(False))
                        .otherwise(pl.lit(True))
                    )
                )
            )
        )


class FACT_Balance:
    def __init__(self, account_type: str | None = None, account_number: str | None = None):
        self.account_type = account_type
        self.account_number = account_number
        self._data_head: pl.DataFrame = (
            pl.read_parquet(PATH_DIM_STATEMENT)
            .filter(pl.col("STD_ACCOUNT") == self.account_type)
            .select("ID_STATEMENT", "STD_STATEMENT_DATE", "STD_ACCOUNT_NUMBER")
        )
        self._data_lines: pl.DataFrame = (
            pl.read_parquet(PATH_FACT_TRANSACTION)
            .join(self._data_head, on="ID_STATEMENT", coalesce=True, maintain_order="left")
            .sort("STD_ACCOUNT_NUMBER", "STD_STATEMENT_DATE", "STD_TRANSACTION_DATE")
            .select(
                "STD_ACCOUNT_NUMBER",
                "ID_STATEMENT",
                "STD_STATEMENT_DATE",
                "ID_TRANSACTION",
                "STD_TRANSACTION_DATE",
                "STD_RUNNING_BALANCE",
                "STD_MOVEMENT",
                "index",
            )
        )
        self._date_range: pl.DataFrame = (
            self._data_lines.group_by("STD_ACCOUNT_NUMBER")
            .agg(pl.col("STD_STATEMENT_DATE").max().alias("date_max"), pl.col("STD_TRANSACTION_DATE").min().alias("date_min"))
            .with_columns(ID_DAY=pl.date_ranges("date_min", "date_max"))
            .explode("ID_DAY")
            .drop("date_min", "date_max")
        )
        self.daily = self._date_range.join(
            other=self._data_lines.group_by("STD_ACCOUNT_NUMBER", "STD_TRANSACTION_DATE").agg(
                pl.col("STD_MOVEMENT").sum(), pl.col("STD_RUNNING_BALANCE").last()
            ),
            left_on=["STD_ACCOUNT_NUMBER", "ID_DAY"],
            right_on=["STD_ACCOUNT_NUMBER", "STD_TRANSACTION_DATE"],
            how="left",
            validate="1:1",
            maintain_order="left",
        ).with_columns(pl.col("STD_MOVEMENT").fill_null(0), pl.col("STD_RUNNING_BALANCE").fill_null(strategy="forward"))
        self.monthly = (
            self.daily.group_by("STD_ACCOUNT_NUMBER", pl.col("ID_DAY").dt.month_end().alias("ID_MONTH"))
            .agg(pl.col("STD_MOVEMENT").sum(), pl.col("STD_RUNNING_BALANCE").last())
            .sort("STD_ACCOUNT_NUMBER", "ID_MONTH")
            .with_columns(PERIOD=pl.col("ID_MONTH").dt.year().mul(100).add(pl.col("ID_MONTH").dt.month()))
        )
        self.yearly = ()


def main():
    db = FACT_Balance(account_type="Flexible Saver")
    # print(db._data_head)
    # print(db._data_lines)
    # print(db._date_range)
    # print(db.daily)
    print(db.monthly)


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(25)
    main()
