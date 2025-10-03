import polars as pl

from bank_statement_parser.modules.config import get_config_from_account, get_config_from_company, get_config_from_statement
from bank_statement_parser.modules.functions.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.functions.statement_functions import get_field_values


class Statement:
    def __init__(self, file_path: str, key_company: str | None = None, key_account: str | None = None):
        self.file_path = file_path
        self.key_company = key_company
        self.key_account = key_account
        self.pdf = pdf_open(file_path)
        self.config = self.get_config()
        self.company = self.config.company.company if self.config.company and hasattr(self.config.company, "company") else ""
        self.account = self.config.account if self.config and hasattr(self.config, "account") else ""
        self.statement_type = (
            self.config.statement_type.statement_type
            if self.config.statement_type and hasattr(self.config.statement_type, "statement_type")
            else None
        )
        self.config_header = (
            self.config.statement_type.header.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "header")
            else None
        )
        self.config_pages = (
            self.config.statement_type.pages.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "pages")
            else None
        )
        self.config_lines = (
            self.config.statement_type.lines.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "lines")
            else None
        )
        # del self.config
        self.header_results = get_field_values(
            self.config_header, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        )
        self.page_results = get_field_values(
            self.config_pages, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        )
        self.lines_results = get_field_values(
            self.config_lines, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        )
        # # pivoted dataframes
        # self.lines_df = self.lines_results.results_clean.pivot(
        #     values="value", index=["config", "page_number", "table_row", "id_row", "transaction_start", "transaction_end"], on="field"
        # )
        # self.transactions = self.get_transactions()

    # def get_transactions(self):
    #     # remove rows before first transaction_start and after last transaction_end
    #     first_id_row = self.lines_df.filter(pl.col("transaction_start")).select(pl.col("id_row")).min()[0, 0]
    #     last_id_row = self.lines_df.filter(pl.col("transaction_end")).select(pl.col("id_row")).max()[0, 0]
    #     transactions = self.lines_df.filter((pl.col("id_row") >= first_id_row) & (pl.col("id_row") <= last_id_row))

    #     # transaction modifications
    #     mods = self.lines_results.transaction_mods

    #     if fill_forward_fields := mods["fill_forward_fields"] if "fill_forward_fields" in mods.keys() else None:
    #         for field in fill_forward_fields:
    #             if field in transactions.columns:
    #                 transactions = transactions.with_columns(pl.col(field).fill_null(strategy="forward"))

    #     if merge_fields := mods["merge_fields"] if "merge_fields" in mods.keys() else None:
    #         for field in merge_fields:
    #             for _ in range(10):  # repeat multiple times to ensure all rows are merged - can handle up to 10 rows per transaction
    #                 transactions = (
    #                     transactions.with_columns(
    #                         pl.when(
    #                             pl.col("transaction_end") & ~pl.col("transaction_start")
    #                         )  # if a row is a continuation of the previous transaction
    #                         .then(
    #                             pl.concat_str(transactions.shift(1)[field], pl.col(field), separator=" | ", ignore_nulls=True)
    #                         )  # merge with previous row
    #                         .otherwise(pl.col(field))  # else keep the same
    #                         .alias(f"{field}_merge"),  # new column with merged values
    #                         pl.when(
    #                             ~pl.col("transaction_end") & pl.col("transaction_start") & transactions.shift(-1)["transaction_end"]
    #                         )  # if a row is the start of a transaction, but not the end, and the next row is the end of a transaction we can delete this row
    #                         .then(True)
    #                         .otherwise(False)
    #                         .alias(f"{field}_delete_row"),
    #                     )
    #                     .drop(field)
    #                     .rename({f"{field}_merge": field})
    #                 )
    #                 transactions = transactions.filter(~pl.col(f"{field}_delete_row")).drop(
    #                     f"{field}_delete_row"
    #                 )  # drop rows that have been merged into the following row

    #     transactions = transactions.filter(
    #         pl.col("transaction_end")
    #     )  # keep only rows where transaction ends, all transaction rows above have been merged into these rows

    #     # get standard fields
    #     # credit & debit columns first so these can be used in the movement column
    #     transactions = transactions.with_columns(
    #         std_credit=pl.when(
    #             pl.col("amount").str.ends_with("CR")
    #             & pl.col("amount").str.starts_with("")
    #             & ~pl.col("amount").cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean)
    #         )
    #         .then(pl.col("amount").str.strip_chars_end("CR").str.strip_chars_start(""))
    #         .otherwise(0.00)
    #         .cast(pl.Float64)
    #         .mul(1)
    #         .round(2),
    #         std_debit=pl.when(
    #             pl.col("amount").str.ends_with("")
    #             & pl.col("amount").str.starts_with("")
    #             & pl.col("amount").cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean)
    #         )
    #         .then(pl.col("amount").str.strip_chars_end("").str.strip_chars_start(""))
    #         .otherwise(0.00)
    #         .cast(pl.Float64)
    #         .mul(-1)
    #         .round(2),
    #     )
    #     # and then the other standard fields
    #     transactions = transactions.with_columns(
    #         std_date=pl.col("transaction_date").str.to_date(format="%d %b %y", strict=True),
    #         std_description=pl.col("details").str.to_titlecase().str.slice(0, 100).str.strip_chars_start(")"),
    #         std_movement=(pl.col("std_credit") + pl.col("std_debit")).round(2),
    #     )

    #     return transactions

    def get_config(self):
        config = None
        if self.key_account:
            config = get_config_from_account(self.key_account, self.file_path)
        elif self.key_company:
            config = get_config_from_company(self.key_company, self.pdf, self.file_path)
        else:
            config = get_config_from_statement(self.pdf, self.file_path)
        return config

    def close_pdf(self):
        pdf_close(self.pdf)
        self.pdf = None


stmt = Statement("/home/boscorat/Downloads/2025-07-12_Statement_Rewards_Credit_Card.pdf")
# stmt = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Advance_Account.pdf")
# stmt = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Flexible_Saver.pdf")

with pl.Config(tbl_cols=-1, tbl_rows=-1):
    print(f"\n\n{(stmt.company + '---' + stmt.account).center(80, '=')}")
    # print(f"HEADER:\n{stmtCC.header_results.results_field}")
    # print(f"PAGES:\n{stmtCC.page_results.results_field}")
    # print(f"LINES:\n{stmt.lines_results.results_field}")
    # pivoted_lines = stmt.lines_results.results_field.pivot(values="value", index=["config", "page_number", "table_row"], columns="field")
    # print(f"LINES PIVOTED:\n{pivoted_lines}")
    # print(f"HEADER:\n{stmt.header_results.results_field}")
    # print(f"PAGES:\n{stmt.page_results.results_field}")
    print(f"Results Full:\n{stmt.lines_results.results_full}")
    print(f"Results Clean:\n{stmt.lines_results.results_clean}")
    print(f"Results Transactions:\n{stmt.lines_results.results_transactions}")

    for tr in stmt.lines_results.results_transactions:
        print(f"transaction_count: {tr.select('std_movement').count()}")
        print(f"transaction movements: {tr.select('std_credit', 'std_debit', 'std_movement').sum()}")

    # print(f"\n\n{(stmtAdv.company + '---' + stmtAdv.account).center(80, '=')}")
    # print(f"HEADER:\n{stmtAdv.header_results.results_field}")
    # print(f"PAGES:\n{stmtAdv.page_results.results_field}")

    # print(f"\n\n{(stmtFlex.company + '---' + stmtFlex.account).center(80, '=')}")
    # print(f"HEADER:\n{stmtFlex.header_results.results_field}")
    # print(f"PAGES:\n{stmtFlex.page_results.results_field}")

stmt.close_pdf()
# stmtAdv.close_pdf()
# stmtFlex.close_pdf()

# del stmtCC
# del stmtAdv
# del stmtFlex

print()


# data = {"Column1": [2, 4, 6, 8, 10], "Column2": [3, 5, 7, 9, 11]}
# data2 = {"Column1": [1, 2, 3, 4, 5], "Column2": [10, 20, "big", "head", 50]}
# df = pl.DataFrame(data)
# print("Original DataFrame:\n", df)
# df2 = df.with_columns(df - df.shift(1))
# print("DataFrame after subtracting previous row:\n", df2)

# jf = pl.DataFrame(data2, strict=False)
# print("Original DataFrame:\n", jf)
# jf2 = jf.with_columns(jf.shift(1))
# print("DataFrame after shifting down:\n", jf2)
# jf3 = jf.with_columns(pl.concat_str(jf["Column2"], jf.shift(2)["Column2"], separator=" ", ignore_nulls=True).alias("merged"))
# print("DataFrame after merging:\n", jf3)

# jf = jf.with_columns(
#     pl.when(jf.shift(1)["Column1"] == 3)
#     .then(pl.concat_str(jf.shift(1)["Column2"], jf["Column2"], separator=" | ", ignore_nulls=True))
#     .otherwise(pl.col("Column2"))
#     .alias("Col2")
# )

# # for i, row in enumerate(jf.iter_rows(named=True)):
# #     lc2 = jf.shift(1)["Column2"]
# #     print(lc2)
# #     jf = (
# #         jf.with_columns(
# #             pl.when(pl.col("Column1") == 4)
# #             .then(pl.concat_str(lc2, pl.col("Column2"), separator=" | ", ignore_nulls=True))
# #             .otherwise(pl.col("Column2"))
# #             .alias("Col2")
# #         )
# #         .head(4)
# #         .tail(1)
# #     )

# print("DataFrame after changing row:\n", jf)  # does not change original df

# # print("DataFrame after selecting rows:\n", jf.select(["Column1", "Column2"]).head(4).tail(1))  # does not change original df
# # f = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
# # f.with_columns(pl.when(pl.col("a") <= 3).then(pl.col("b") // 10).otherwise(pl.col("b")))
