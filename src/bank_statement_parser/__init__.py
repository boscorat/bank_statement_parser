# import re
# from os import listdir
# from typing import Generator, Iterator

# import camelot

# import config

# STATEMENT_DIRECTORY = "/home/boscorat/Downloads/Statements/"
# STATEMENT_DIRECTORY_CC = "/home/boscorat/Downloads/Statements/CC/"

# files = listdir(STATEMENT_DIRECTORY)  # List of PDF files in the statements directory
# files_CC = listdir(STATEMENT_DIRECTORY_CC)  # as above, but for credit card
# # Filter out non-PDF files using a list comprehension
# files = [f"{STATEMENT_DIRECTORY}/{filename}" for filename in files if filename.endswith(".pdf")]
# files_CC = [f"{STATEMENT_DIRECTORY_CC}/{filename}" for filename in files_CC if filename.endswith(".pdf")]
# files = files + files_CC

# # # files = [file for file in files if file == "/home/boscorat/Downloads/Statements//2019-08-08_Statement.pdf"]


# files: list = [
#     # "/home/boscorat/Downloads/Statements/CC/2022-12-08_Statement.pdf",  # CRD
#     "/home/boscorat/Downloads/Statements/2025-05-08_Statement.pdf",  # CUR
#     # "/home/boscorat/Downloads/Statements/2025-02-28_Statement.pdf",  # SAV
# ]

# current_pdf = ""


# def debit_check(value: str):
#     try:
#         return round(float(value), 2)
#     except ValueError:
#         try:
#             return round(float(value[:-1]) * -1, 2)
#         except ValueError:
#             raise Exception("Incompatible value")


# def balance_check(open: float | str, close: float | str, credit: float, debit: float, account_type: str):
#     if account_type in ["CRD"]:
#         open = open * -1
#         close = close * -1
#     return round(close - open, 2) == round(credit + (debit * -1), 2)

# def extract_tables(file: str, fixed_area: list[str] | None, fixed_columns: list[str] | None) -> list[dict]:
#     global current_pdf
#     current_pdf = file
#     if fixed_area and fixed_columns:
#         raw = camelot.read_pdf(file, pages="1-end", flavor="stream", table_areas=fixed_area, columns=fixed_columns)
#     elif fixed_area:
#         raw = camelot.read_pdf(file, pages="1", flavor="stream", table_areas=fixed_area)
#     else:
#         raw = camelot.read_pdf(file, pages="1-end", flavor="stream")
#     tables: list = [{"table_number": id, "page": table.page, "data": table.data} for id, table in enumerate(raw)]
#     for table in tables:
#         table_text = ""
#         for row in table["data"]:
#             table_text += "|".join(row)
#         table["text"] = table_text
#         table["text_strip"] = table_text.replace(" ", "")
#     current_pdf = file
#     return tables

# def extract_tables_batch(files: list[str]) -> Generator[list[dict], None, None]:
#     for file in files:
#         tables = extract_tables(file, None, None)
#         yield tables


# print()
# bank_name: str = "<no bank name>"
# account_name: str = "<no account name>"
# account_type: str = "<bank account type>"


# def search_data(config_list: list, pdf_tables: list) -> dict | None:
#     for row in config_list.values():
#         ref_results: list = []
#         for ref in row["refs"]:
#             ref = ref.replace(" ", "") if row["refs_strip"] else ref
#             text = ""
#             for table_text in pdf_tables:
#                 if table_text["page"] == row["page"]:
#                     text += table_text["text_strip"] if row["refs_strip"] else table_text["text"]
#             ref_results.append((ref, ref in text))
#         if len(ref_results) > 0:  # we've got some ref result
#             matches = sum(1 for result in ref_results if result[1])
#             if matches > 0:
#                 if matches == len(ref_results) or not row["refs_all"]:
#                     return row
#                     break
#     return None


# def cell_data(config_list: list, pdf_tables: list, field: str) -> dict | None:
#     try:
#         sc = config_list[field]
#     except KeyError:  # field isn't specified in the specs so can be safely skipped
#         return None
#     for ref in sc["refs"]:
#         try:
#             val = str([r for r in pdf_tables[ref["table"]]["data"]][ref["row"]][ref["cell"]])
#         except IndexError:  # the ref can't be found, but a later ref may work so we contine
#             continue
#         if subs := sc["re_subs"]:
#             for sub in subs:
#                 val = re.sub(sub["pattern"], sub["replacement"], val)
#         if match := re.search(rf"{ref['re_search']}", val):
#             return val[match.start() : match.end()]
#         else:
#             continue
#     raise ValueError(
#         f"No matching ref for {bank_name} - {account_name} - {account_type}\nFailure Field: {field}, file: {current_pdf}"
#     )  # if we get this far we've not managed to match any refs

# def table_data(config_list: list, pdf_tables: list) -> list[dict]:
#     try:
#         fixed_area = config_list["fixed_area"]
#         fixed_column = config_list["fixed_column"]
#         if fixed_area:
#             fixed_area = config_list["area_spec"]
#             if fixed_column:
#                 fixed_column = config_list["column_spec"]
#             pdf_tables = extract_tables(current_pdf, fixed_area, fixed_column)
#         header_spec = config_list["header_line"]
#         lines_spec = config_list["valid_transactions"]
#     except KeyError as e:
#         raise KeyError(f"Missing transaction config: {e}")
#     transaction_tables: list[dict] = []
#     # identify the tables containing a header line
#     for table in pdf_tables:
#         if table["table_number"] < config_list["min_table_number"]: # skip tables before the minimum table number
#             continue
#         for id, row in enumerate(table["data"]):
#             if row == header_spec:
#                 table["data"] = table["data"][id + 1:] # remove all lines before and including the header line
#                 transaction_tables.append(table)
#                 break
#     # if transactions span multiple pages, withouth their own header line, we need to add them
#     if config_list["table_spans_pages"]:
#         possible_tables = [table for table in pdf_tables if table not in transaction_tables\
#                            and table["table_number"] >= config_list["min_table_number"]]

#         for table in possible_tables:
#             for id, row in enumerate(table["data"]):
#                 row_OK = False
#                 spec_OK = True
#                 for spec in lines_spec:
#                     for id, cell in enumerate(row):
#                         if not re.search(spec[id], cell):
#                             spec_OK = False
#                             break
#                     if spec_OK:
#                         row_OK = True
#                         break
#                 if row_OK:
#                     # if we get here, the row matches one of the line specs so we can add it to the headed tables
#                     table["data"] = table["data"][id:]
#                     transaction_tables.append(table)
#                     break

#     # now we have the transaction tables we can remove any invalid transactions
#     for table in transaction_tables:
#         for id, row in enumerate(table["data"]):
#             row_OK = False
#             spec_OK = True
#             for spec in lines_spec:
#                 for id, cell in enumerate(row):
#                     if not re.search(spec[id], cell):
#                         spec_OK = False
#                         break
#                 if spec_OK:
#                     row_OK = True
#                     break
#             if not row_OK:
#                 # if we get here, the row does not match any of the line specs so we can remove it
#                 table["data"].pop(id)

#     return transaction_tables


# results: list[tuple[str, str, str, str, str, float, float, float, float, bool]] = []


# for tables in extract_tables_batch(files):
#     company_data = search_data(config_list=config.companies, pdf_tables=tables)
#     company_name = company_data["name"]
#     account_data = search_data(config_list=company_data["accounts"], pdf_tables=tables)
#     account_name = account_data["name"]
#     account_type = account_data["account_type"]["key"]
#     account_type_name = account_data["account_type"]["name"]

#     #     except Exception:
#     #         return None
#     # except (KeyError, IndexError):
#     #     return None

#     # statements
#     spec = account_data["statement"]
#     sort_code = cell_data(spec, tables, "sort_code")
#     account_number = cell_data(spec, tables, "account_number")
#     card_number = cell_data(spec, tables, "card_number")
#     account_name = cell_data(spec, tables, "account_name")
#     opening_balance = float(debit_check(cell_data(spec, tables, "opening_balance")))
#     closing_balance = float(debit_check(cell_data(spec, tables, "closing_balance")))
#     payments_in = float(cell_data(spec, tables, "payments_in"))
#     payments_out = float(cell_data(spec, tables, "payments_out"))

#     if balance_check(
#         opening_balance, closing_balance, payments_in, payments_out, account_type
#     ):
#         transaction_tables = table_data(spec["transaction"], tables)

#     results.append(
#         (
#             current_pdf,
#             sort_code,
#             account_number,
#             card_number,
#             account_name,
#             opening_balance,
#             closing_balance,
#             payments_in,
#             payments_out,
#             balance_check(opening_balance, closing_balance, payments_in, payments_out, account_type),
#         )
#     )


# print("matched: ")
# for result in results:
#     if result[-1]:
#         print(result[0], ": ", result[5:])

# print("unmatched: ")
# for result in results:
#     if not result[-1]:
#         print(result[0], ": ", result[5:])

# print(len(files), len(results))
# # print("sort_code: ", sort_code)
# # print("account: ", account_number)
# # print("card: ", card_number)
# # print("name: ", account_name)
# # print("opening", opening_balance)
# # print("closing", closing_balance)
# # print("in", payments_in)
# # print("out", payments_out)
# # print()


# # for b in config.banks_base:
# #     bank_match = False
# #     print(b["name"])
# #     ref_results: list = []
# #     for ref in b["refs"]:
# #         ref = ref.replace(" ", "") if b["refs_strip"] else ref
# #         text = tables[b["ref_table"]]["text_strip"] if b["refs_strip"] else tables[b["ref_table"]]["text"]
# #         ref_results.append((ref, ref in text))
# #     if len(ref_results) > 0:  # we've got some ref result
# #         matches = sum(1 for result in ref_results if result[1])
# #         if matches > 0 and (not b["refs_all"] or matches == len(ref_results)):
# #             bank_match = True
# #     if bank_match:
# #         bank_name = b["name"]
# #         break


# # print(bank_name)
# # def ref_cell_match(cell: str, ref: str, strip_spaces: bool = True) -> bool:
# #     if strip_spaces:
# #         cell = cell.replace(" ", "")
# #         validator = ref.replace(" ", "")
# #     return cell == validator

# # for b in config.banks:
# #     bank_valid = False
# #     for ref in b["refs"]:
# #         ref_valid = False
# #         for row in tables[b["ref_table"]].data:
# #             if ref_valid:
# #                 break
# #             for cell in row:
# #                 valid = ref_cell_match(cell, ref, b["refs_strip"])
# #         if ref_valid:
# #             bank_valid = True
# #         else:
# #             break
# #     if bank_valid:
# #         bank_name = b["bank"]
# #         for a in b["accounts"]:
# #             account_valid = False
# #             for v in a["validators"]:
# #                 valid = False
# #                 for row in tables[int(v["table"])].data:
# #                     if valid:
# #                         break
# #                     for cell in row:
# #                         valid = validator_cell_match(cell, v["validator"])
# #                 if valid:
# #                     account_valid = True
# #                 else:
# #                     break
# #             if account_valid:
# #                 account_name = a["account"]
# #                 account_type = a["type"]

# # print(bank_name)
# # print(account_name)
# # print(account_type)
