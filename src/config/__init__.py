import copy
import os
import pathlib

from tomllib import load

dir = pathlib.Path(__file__).parent
banks_config = load(open(os.path.join(dir, "banks.toml"), "rb"))
account_types_config = load(open(os.path.join(dir, "account_types.toml"), "rb"))
accounts_config = load(open(os.path.join(dir, "accounts.toml"), "rb"))

banks = [b for b in banks_config["banks"]]
account_types = [at for at in account_types_config["account_types"]]
accounts = [a for a in accounts_config["accounts"]]

for acct in accounts:  # merge account types into accounts
    acct["type"] = str([at["name"] for at in account_types if at["id"] == acct["id_type"]][0])

banks_base = copy.deepcopy(banks)  # deep copy of banks so we can still access it without the accounts if required

for bank in banks:  # merge accounts into banks
    bank["accounts"] = [acct for acct in accounts if acct["id"] in bank["accounts"]]


def bank_accounts(id_banks: int) -> list:
    bas = list(filter(lambda x: x["id"] == id_banks, banks_base))[0]
    return list(filter(lambda x: x["id"] in bas["accounts"], accounts))
