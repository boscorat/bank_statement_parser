import os
import pathlib

from tomllib import load

dir = pathlib.Path(__file__).parent
_company = load(open(os.path.join(dir, "company.toml"), "rb"))
_account_type = load(open(os.path.join(dir, "account_type.toml"), "rb"))
_account = load(open(os.path.join(dir, "account.toml"), "rb"))
_statement = load(open(os.path.join(dir, "statement.toml"), "rb"))
_transaction = load(open(os.path.join(dir, "transaction.toml"), "rb"))

# Denormalise the config data...

# transactions into statements
for id, statement in _statement.items():
    statement["transaction"] = _transaction[statement["transaction"]]

# account types and statements into accounts
for id, account in _account.items():
    account["account_type"] = {"key": account["account_type"], "name": _account_type[account["account_type"]]["name"]}
    account["statement"] = _statement[account["statement"]]

# accounts into companies
for id, company in _company.items():
    company["accounts"] = {key: acct for key, acct in _account.items() if acct["company"] == id}

companies = _company


def company_accounts(company: str) -> list:
    return {key: acct for key, acct in _account.items() if acct["company"] == id}
