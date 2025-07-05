import re
from os import listdir

import camelot

import config

STATEMENT_DIRECTORY = "/home/boscorat/Downloads/Statements/"
STATEMENT_DIRECTORY_CC = "/home/boscorat/Downloads/Statements/CC/"

files = listdir(STATEMENT_DIRECTORY)  # List of PDF files in the statements directory
files_CC = listdir(STATEMENT_DIRECTORY_CC)  # as above, but for credit card
# Filter out non-PDF files using a list comprehension
files = [f"{STATEMENT_DIRECTORY}/{filename}" for filename in files if filename.endswith(".pdf")]
files_CC = [f"{STATEMENT_DIRECTORY_CC}/{filename}" for filename in files_CC if filename.endswith(".pdf")]
files = files + files_CC

# # files = [file for file in files if file == "/home/boscorat/Downloads/Statements//2019-08-08_Statement.pdf"]


# files: list = [
#     "/home/boscorat/repos/bstec/statements/2022-12-08_Statement.pdf",  # CRD
#     "/home/boscorat/Downloads/Statements/2025-05-08_Statement.pdf",  # CUR
#     "/home/boscorat/Downloads/Statements/2025-02-28_Statement.pdf",  # SAV
# ]

current_pdf = ""


def debit_check(value: str):
    try:
        return round(float(value), 2)
    except ValueError:
        try:
            return round(float(value[:-1]) * -1, 2)
        except ValueError:
            raise Exception("Incompatible value")


def balance_check(open: float | str, close: float | str, credit: float, debit: float, account_type: str):
    if account_type in ["CRD"]:
        open = open * -1
        close = close * -1
    return round(close - open, 2) == round(credit + (debit * -1), 2)


def extract_pdf_tables(files: str) -> tuple():
    global current_pdf
    for file in files:
        current_pdf = file
        raw = camelot.read_pdf(file, pages="1-end", flavor="stream")
        tables: list = [{"page": table.page, "data": table.data} for table in raw]
        for table in tables:
            table_text = ""
            for row in table["data"]:
                table_text += "|".join(row)
            table["text"] = table_text
            table["text_strip"] = table_text.replace(" ", "")
        current_pdf = file
        yield tables


print()
bank_name: str = "<no bank name>"
account_name: str = "<no account name>"
account_type: str = "<bank account type>"


def ref_accounts(config_list: list, pdf_tables: list) -> dict | None:
    for id, row in config_list.items():
        ref_results: list = []
        for ref in row["refs"]:
            ref = ref.replace(" ", "") if row["refs_strip"] else ref
            text = ""
            for table_text in pdf_tables:
                if table_text["page"] == row["page"]:
                    text += table_text["text_strip"] if row["refs_strip"] else table_text["text"]
            ref_results.append((ref, ref in text))
        if len(ref_results) > 0:  # we've got some ref result
            matches = sum(1 for result in ref_results if result[1])
            if matches > 0:
                if matches == len(ref_results) or not row["refs_all"]:
                    return row
                    break
    return None


def ref_specs(config_list: list, pdf_tables: list, field: str = "account_number") -> dict | None:
    try:
        sc = config_list[field]
    except KeyError:  # field isn't specified in the specs so can be safely skipped
        return None
    for ref in sc["refs"]:
        try:
            val = str([r for r in pdf_tables[ref["table"]]["data"]][ref["row"]][ref["cell"]])
        except IndexError:  # the ref can't be found, but a later ref may work so we contine
            continue
        if subs := sc["re_subs"]:
            for sub in subs:
                val = re.sub(sub["pattern"], sub["replacement"], val)
        if match := re.search(rf"{ref['re_search']}", val):
            return val[match.start() : match.end()]
        else:
            continue
    raise ValueError(
        f"No matching ref for {bank_name} - {account_name} - {account_type}\nFailure Field: {field}, file: {current_pdf}"
    )  # if we get this far we've not managed to match any refs


results: list[tuple()] = []


for tables in extract_pdf_tables(files):
    company_match = ref_accounts(config_list=config.companies, pdf_tables=tables)
    bank_name = company_match["name"]
    account_match = ref_accounts(config_list=company_match["accounts"], pdf_tables=tables)
    account_name = account_match["name"]
    account_type = account_match["account_type"]["key"]
    account_type_name = account_match["account_type"]["name"]

    #     except Exception:
    #         return None
    # except (KeyError, IndexError):
    #     return None

    # statements
    spec = account_match["statement"]
    sort_code = ref_specs(spec, tables, "sort_code")
    account_number = ref_specs(spec, tables, "account_number")
    card_number = ref_specs(spec, tables, "card_number")
    account_name = ref_specs(spec, tables, "account_name")
    opening_balance = float(debit_check(ref_specs(spec, tables, "opening_balance")))
    closing_balance = float(debit_check(ref_specs(spec, tables, "closing_balance")))
    payments_in = float(ref_specs(spec, tables, "payments_in"))
    payments_out = float(ref_specs(spec, tables, "payments_out"))

    results.append(
        (
            current_pdf,
            sort_code,
            account_number,
            card_number,
            account_name,
            opening_balance,
            closing_balance,
            payments_in,
            payments_out,
            balance_check(opening_balance, closing_balance, payments_in, payments_out, account_type),
        )
    )


print("matched: ")
for result in results:
    if result[-1]:
        print(result[0], ": ", result[5:])

print("unmatched: ")
for result in results:
    if not result[-1]:
        print(result[0], ": ", result[5:])

print(len(files), len(results))
# print("sort_code: ", sort_code)
# print("account: ", account_number)
# print("card: ", card_number)
# print("name: ", account_name)
# print("opening", opening_balance)
# print("closing", closing_balance)
# print("in", payments_in)
# print("out", payments_out)
# print()


# for b in config.banks_base:
#     bank_match = False
#     print(b["name"])
#     ref_results: list = []
#     for ref in b["refs"]:
#         ref = ref.replace(" ", "") if b["refs_strip"] else ref
#         text = tables[b["ref_table"]]["text_strip"] if b["refs_strip"] else tables[b["ref_table"]]["text"]
#         ref_results.append((ref, ref in text))
#     if len(ref_results) > 0:  # we've got some ref result
#         matches = sum(1 for result in ref_results if result[1])
#         if matches > 0 and (not b["refs_all"] or matches == len(ref_results)):
#             bank_match = True
#     if bank_match:
#         bank_name = b["name"]
#         break


# print(bank_name)
# def ref_cell_match(cell: str, ref: str, strip_spaces: bool = True) -> bool:
#     if strip_spaces:
#         cell = cell.replace(" ", "")
#         validator = ref.replace(" ", "")
#     return cell == validator

# for b in config.banks:
#     bank_valid = False
#     for ref in b["refs"]:
#         ref_valid = False
#         for row in tables[b["ref_table"]].data:
#             if ref_valid:
#                 break
#             for cell in row:
#                 valid = ref_cell_match(cell, ref, b["refs_strip"])
#         if ref_valid:
#             bank_valid = True
#         else:
#             break
#     if bank_valid:
#         bank_name = b["bank"]
#         for a in b["accounts"]:
#             account_valid = False
#             for v in a["validators"]:
#                 valid = False
#                 for row in tables[int(v["table"])].data:
#                     if valid:
#                         break
#                     for cell in row:
#                         valid = validator_cell_match(cell, v["validator"])
#                 if valid:
#                     account_valid = True
#                 else:
#                     break
#             if account_valid:
#                 account_name = a["account"]
#                 account_type = a["type"]

# print(bank_name)
# print(account_name)
# print(account_type)
