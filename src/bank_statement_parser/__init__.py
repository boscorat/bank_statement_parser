import re
from os import listdir

import camelot

import config

STATEMENT_DIRECTORY = "/home/boscorat/Downloads/Statements/"
# STATEMENT_DIRECTORY = "/home/boscorat/Downloads/Statements/cc/"

files = listdir(STATEMENT_DIRECTORY)  # List of PDF files in the statements directory
# Filter out non-PDF files using a list comprehension
files = [f"{STATEMENT_DIRECTORY}/{filename}" for filename in files if filename.endswith(".pdf")]


pdf_files: list = [
    "/home/boscorat/repos/bstec/statements/2022-12-08_Statement.pdf",  # CRD
    "/home/boscorat/Downloads/Statements/2025-05-08_Statement.pdf",  # CUR
    "/home/boscorat/Downloads/Statements/2025-02-28_Statement.pdf",  # SAV
]

current_pdf = ""


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


# pdf_file = "/home/boscorat/repos/bstec/statements/2022-12-08_Statement.pdf"  # CRD
# pdf_file = "/home/boscorat/Downloads/Statements/2025-05-08_Statement.pdf"      # CUR
# pdf_file = "/home/boscorat/Downloads/Statements/2025-02-28_Statement.pdf"      # SAV

# tables_extract = (camelot.read_pdf(pdf_file, pages="1-end", flavor="stream"), pdf_file)

# tables_crd = camelot.read_pdf(pdf_file_cc, pages="1-end", flavor="stream")
# tables_cur = camelot.read_pdf(pdf_file_ba, pages="1-end", flavor="stream")
# tables_sav = camelot.read_pdf(pdf_file_sa, pages="1-end", flavor="stream")
# tables = camelot.read_pdf(pdf_file, pages="1-end")

# for i, t in enumerate(tables):
#     t["table_number"] = i + 1


print()
bank_name: str = "<no bank name>"
account_name: str = "<no account name>"
account_type: str = "<bank account type>"


def ref_accounts(config_list: list, pdf_tables: list) -> dict | None:
    for cr in config_list:
        ref_results: list = []
        for ref in cr["refs"]:
            ref = ref.replace(" ", "") if cr["refs_strip"] else ref
            text = ""
            for table_text in pdf_tables:
                if table_text["page"] == cr["page"]:
                    text += table_text["text_strip"] if cr["refs_strip"] else table_text["text"]
            ref_results.append((ref, ref in text))
        if len(ref_results) > 0:  # we've got some ref result
            matches = sum(1 for result in ref_results if result[1])
            if matches > 0:
                if matches == len(ref_results) or not cr["refs_all"]:
                    return cr
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


for tables in extract_pdf_tables(files):
    bank_match = ref_accounts(config_list=config.banks, pdf_tables=tables)
    bank_name = bank_match["name"]
    account_match = ref_accounts(config_list=bank_match["accounts"], pdf_tables=tables)
    account_name = account_match["name"]
    account_type = account_match["type"]

    #     except Exception:
    #         return None
    # except (KeyError, IndexError):
    #     return None

    # statements
    spec = account_match["spec_statements"]
    sort_code = ref_specs(spec, tables, "sort_code")
    account_number = ref_specs(spec, tables, "account_number")
    card_number = ref_specs(spec, tables, "card_number")
    account_name = ref_specs(spec, tables, "account_name")
    opening_balance = ref_specs(spec, tables, "opening_balance")
    closing_balance = ref_specs(spec, tables, "closing_balance")
    payments_in = ref_specs(spec, tables, "payments_in")
    payments_out = ref_specs(spec, tables, "payments_out")

    print("sort_code: ", sort_code)
    print("account: ", account_number)
    print("card: ", card_number)
    print("name: ", account_name)
    print("opening", opening_balance)
    print("closing", closing_balance)
    print("in", payments_in)
    print("out", payments_out)
    print()

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
