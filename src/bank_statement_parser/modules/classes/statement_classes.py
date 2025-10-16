from datetime import datetime

import polars as pl

from bank_statement_parser.modules.classes.data_definitions import Cell, Field, Location, StatementTable
from bank_statement_parser.modules.config import get_config_from_account, get_config_from_company, get_config_from_statement
from bank_statement_parser.modules.functions.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.functions.statement_functions import get_field_values


class Statement:
    def __init__(self, file_path: str, company_key: str | None = None, account_key: str | None = None):
        self.file_path = file_path
        self.company_key = company_key
        self.account_key = account_key
        self.pdf = pdf_open(file_path)
        # self.config = self.get_config()
        # self.company = self.config.company.company if self.config.company and hasattr(self.config.company, "company") else ""
        # self.account = self.config.account if self.config and hasattr(self.config, "account") else ""
        # self.statement_type = (
        #     self.config.statement_type.statement_type
        #     if self.config.statement_type and hasattr(self.config.statement_type, "statement_type")
        #     else None
        # )
        # self.config_header = (
        #     self.config.statement_type.header.configs
        #     if self.config.statement_type and hasattr(self.config.statement_type, "header")
        #     else None
        # )
        # self.config_pages = (
        #     self.config.statement_type.pages.configs
        #     if self.config.statement_type and hasattr(self.config.statement_type, "pages")
        #     else None
        # )
        # self.config_lines = (
        #     self.config.statement_type.lines.configs
        #     if self.config.statement_type and hasattr(self.config.statement_type, "lines")
        #     else None
        # )
        # # del self.config
        # self.header_results = get_field_values(
        #     self.config_header, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        # )
        # self.page_results = get_field_values(
        #     self.config_pages, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        # )
        # self.lines_results = get_field_values(
        #     self.config_lines, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        # )
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
        if self.account_key:
            config = get_config_from_account(self.account_key, self.file_path)
        elif self.company_key:
            config = get_config_from_company(self.company_key, self.pdf, self.file_path)
        else:
            config = get_config_from_statement(self.pdf, self.file_path)
        return config

    def close_pdf(self):
        pdf_close(self.pdf)
        self.pdf = None


# for _ in range(1):
#     stmt = Statement("/home/boscorat/Downloads/2025-07-12_Statement_Rewards_Credit_Card.pdf")
#     # stmt = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Advance_Account.pdf")
#     # stmt = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Flexible_Saver.pdf")
#     with pl.Config(tbl_cols=-1, tbl_rows=-1):
#         print(f"\n\n{(stmt.company + '---' + stmt.account).center(80, '=')}")
#         # print(f"HEADER:\n{stmtCC.header_results.results_field}")
#         # print(f"PAGES:\n{stmtCC.page_results.results_field}")
#         # print(f"LINES:\n{stmt.lines_results.results_field}")
#         # pivoted_lines = stmt.lines_results.results_field.pivot(values="value", index=["config", "page_number", "table_row"], columns="field")
#         # print(f"LINES PIVOTED:\n{pivoted_lines}")
#         # print(f"HEADER:\n{stmt.header_results.results_field}")
#         # print(f"PAGES:\n{stmt.page_results.results_field}")
#         print(f"Results Full:\n{stmt.lines_results.results_full}")
#         print(f"Results Clean:\n{stmt.lines_results.results_clean}")
#         print(f"Results Transactions:\n{stmt.lines_results.results_transactions}")
#         for tr in stmt.lines_results.results_transactions:
#             print(f"transaction_count: {tr.select('std_movement').count()}")
#             print(f"transaction movements: {tr.select('std_credit', 'std_debit', 'std_movement').sum()}")
#         # print(f"\n\n{(stmtAdv.company + '---' + stmtAdv.account).center(80, '=')}")
#         # print(f"HEADER:\n{stmtAdv.header_results.results_field}")
#         # print(f"PAGES:\n{stmtAdv.page_results.results_field}")
#         # print(f"\n\n{(stmtFlex.company + '---' + stmtFlex.account).center(80, '=')}")
#         # print(f"HEADER:\n{stmtFlex.header_results.results_field}")
#         # print(f"PAGES:\n{stmtFlex.page_results.results_field}")
#     stmt.close_pdf()
#     # stmtAdv.close_pdf()
#     # stmtFlex.close_pdf()
#     # del stmtCC
#     # del stmtAdv
#     # del stmtFlex
#     print()
# table = [["Credit Limit", "£10,500.00"], ["APR", "23.9"], ["PreviousBalance", "1,090.67"]]
# column_names = ["col_" + str(i) for i in range(len(table[0]))] if table else []
# table_df = pl.DataFrame(table[0:], schema=column_names) if table else pl.DataFrame()
# result_df = pl.DataFrame()
# result = pl.DataFrame()
# statement_table = StatementTable(
#     statement_table="Account Summary",
#     locations=[Location(page_number=1, top_left=[335, 180], bottom_right=[575, 380], vertical_lines=None)],
#     fields=[
#         Field(
#             field="credit_limit",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=0, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=False,
#             type="float",
#         ),
#         Field(
#             field="credit_limit_2",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=0, col=2),
#             strip=["£", " ", ",", "\\n"],
#             vital=False,
#             type="float",
#         ),
#         Field(
#             field="apr", pattern="^[0-9]*\\.[0-9]{1}[D]?$", cell=Cell(row=1, col=1), strip=["%", " ", ",", "\\n"], vital=False, type="float"
#         ),
#         Field(
#             field="previous_balance",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=2, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=False,
#             type="float",
#         ),
#         Field(
#             field="debits",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=3, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=True,
#             type="float",
#         ),
#         Field(
#             field="credits",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=4, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=True,
#             type="float",
#         ),
#         Field(
#             field="new_balance",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=5, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=True,
#             type="float",
#         ),
#         Field(
#             field="transaction_balance",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=6, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=True,
#             type="float",
#         ),
#         Field(
#             field="minimum_payment",
#             pattern="^[0-9]*\\.[0-9]{2}[D]?$",
#             cell=Cell(row=7, col=1),
#             strip=["£", " ", ",", "\\n"],
#             vital=False,
#             type="float",
#         ),
#     ],
#     table_columns=None,
#     table_rows=None,
#     row_spacing=7,
#     tests=None,
#     delete_success_false=None,
#     delete_cast_success_false=None,
#     delete_rows_with_missing_vital_fields=None,
#     transaction_mods=None,
# )
# fields_df = pl.DataFrame(statement_table.fields)
# print(fields_df)
# if table_df.height > 0:
#     for field in statement_table.fields:
#         if field.cell.row is not None and field.cell.row < table_df.height:  # type: ignore
#             result = (
#                 table_df.head(field.cell.row + 1)
#                 .tail(1)
#                 .with_columns(value=pl.col("col_1").str.replace_all("[£,\\s]", ""))
#                 .with_columns(valid=pl.col("value").str.contains("^[0-9]*\\.[0-9]{1}[D]?$"))
#             )  # type: ignore
#             result_df = result_df.vstack(result)
# print(result_df)

# table_df = table_df.with_columns(row=pl.row_index())
# fields_df = fields_df.unnest("cell")

# for i, col in enumerate(table_df.iter_columns()):
#     table_df = table_df.with_columns(pl.lit("new").alias(f"{col.name}_config"))
# print(table_df)
# # print(fields_df)
