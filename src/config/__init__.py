import os
import pathlib

from dacite import from_dict
from tomllib import load

from .data_definitions import Account, AccountType, Company, StatementType

dir = pathlib.Path(__file__).parent
_companies = load(open(os.path.join(dir, "companies.toml"), "rb"))
_account_types = load(open(os.path.join(dir, "account_types.toml"), "rb"))
_accounts = load(open(os.path.join(dir, "accounts.toml"), "rb"))
_statement_types = load(open(os.path.join(dir, "statement_types.toml"), "rb"))

del dir, load, os, pathlib

key = None
for key in _companies.keys():
    _companies[key] = from_dict(data_class=Company, data=_companies[key])

for key in _account_types.keys():
    _account_types[key] = from_dict(data_class=AccountType, data=_account_types[key])

for key in _statement_types.keys():
    _statement_types[key] = from_dict(data_class=StatementType, data=_statement_types[key])

for key in _accounts.keys():
    _accounts[key] = from_dict(data_class=Account, data=_accounts[key])
del key

# account types, statements, and companies into accounts
for key, account in _accounts.items():
    _accounts[key].account_type = _account_types[account.account_type_key]
    _accounts[key].statement_type = _statement_types[account.statement_type_key]
    _accounts[key].company = _companies[account.company_key]

del key, account

config_accounts = _accounts
config_statement_types = _statement_types
config_companies = _companies


def config_company_accounts(company_key):
    return [acct for acct in config_accounts.values() if acct.company_key == company_key]


del _companies, _account_types, _accounts, _statement_types

__all__ = ["config_statement_types", "config_accounts", "config_companies"]
