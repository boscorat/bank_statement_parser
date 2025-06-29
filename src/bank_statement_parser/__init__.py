import camelot

import config

pdf_file_cc = "/home/boscorat/repos/bstec/statements/2022-12-08_Statement.pdf"
pdf_file_ba = "/home/boscorat/Downloads/Statements/2025-05-08_Statement.pdf"
pdf_file_sa = "/home/boscorat/Downloads/Statements/2025-02-28_Statement.pdf"


tables_cc = camelot.read_pdf(pdf_file_cc, pages="1-end", flavor="stream")
tables_ba = camelot.read_pdf(pdf_file_ba, pages="1-end", flavor="stream")
tables_sa = camelot.read_pdf(pdf_file_sa, pages="1-end", flavor="stream")
# tables = camelot.read_pdf(pdf_file, pages="1-end")
tables: list = [{"page": table.page, "data": table.data} for table in tables_sa]

# for i, t in enumerate(tables):
#     t["table_number"] = i + 1

for table in tables:
    table_text = ""
    for row in table["data"]:
        table_text += "|".join(row)
    table["text"] = table_text
    table["text_strip"] = table_text.replace(" ", "")

text0 = tables[0]["text"]

# print("Your Statement" in text0)

print()
bank_name: str = "<no bank name>"
account_name: str = "<no account name>"
account_type: str = "<bank account type>"


def ref_search(config_list: list, pdf_tables: list) -> dict | None:
    for cr in config_list:
        ref_results: list = []
        for ref in cr["refs"]:
            ref = ref.replace(" ", "") if cr["refs_strip"] else ref
            text = pdf_tables[cr["refs_table"]]["text_strip"] if cr["refs_strip"] else pdf_tables[b["refs_table"]]["text"]
            ref_results.append((ref, ref in text))
        if len(ref_results) > 0:  # we've got some ref result
            matches = sum(1 for result in ref_results if result[1])
            if matches > 0:
                if matches == len(ref_results) or not cr["refs_all"]:
                    return cr
                    break
    return None


bank_match = ref_search(config_list=config.banks, pdf_tables=tables)
bank_name = bank_match["name"]
account_match = ref_search(config_list=bank_match["accounts"], pdf_tables=tables)
account_name = account_match["name"]
account_type = account_match["type"]

print(bank_name, account_name, account_type)

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
