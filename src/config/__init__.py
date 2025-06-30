import copy
import os
import pathlib

from tomllib import load

dir = pathlib.Path(__file__).parent
banks_config = load(open(os.path.join(dir, "banks.toml"), "rb"))
account_types_config = load(open(os.path.join(dir, "account_types.toml"), "rb"))
accounts_config = load(open(os.path.join(dir, "accounts.toml"), "rb"))
spec_statements_config = load(open(os.path.join(dir, "spec_statements.toml"), "rb"))

banks = [b for b in banks_config["banks"]]
account_types = [at for at in account_types_config["account_types"]]
accounts = [a for a in accounts_config["accounts"]]
spec_statements = spec_statements_config


for acct in accounts:  # de-normalise - expand the accounts info
    acct["type"] = str(next(at["name"] for at in account_types if at["id"] == acct["id_type"]))
    acct["spec_statements"] = spec_statements[acct["spec_statement"]]

banks_base = copy.deepcopy(banks)  # deep copy of banks so we can still access it without the accounts if required

for bank in banks:  # merge accounts into banks
    bank["accounts"] = [acct for acct in accounts if acct["id"] in bank["accounts"]]


def bank_accounts(id_banks: int) -> list:
    acc_list = next(bb["accounts"] for bb in banks_base if bb["id"] == id_banks)
    return [ac for ac in accounts if ac["id"] in acc_list]
