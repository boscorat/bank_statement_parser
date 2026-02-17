import time
from copy import deepcopy
from datetime import datetime

import polars as pl
from dacite import from_dict
from pdfplumber.pdf import PDF
from tomllib import load

from bank_statement_parser.modules.data import (
    Account,
    AccountType,
    Company,
    StandardFields,
    StatementTable,
    StatementType,
)
from bank_statement_parser.modules.errors import ConfigFileError, StatementError
from bank_statement_parser.modules.paths import BASE_CONFIG, USER_CONFIG
from bank_statement_parser.modules.statement_functions import get_results

"""
WRITE SOME TESTS TO VALIDATE THE CONFIG FILES!!!!!
e.g. cell must be set for all non-transaction table fields
column must be set for all transaction fields
numeric fields should have a currency and none of the date or string specific fields
"""

__dir_base = BASE_CONFIG
__dir_user = USER_CONFIG

__config_dict = {
    "companies": {"dataclass": Company, "config": dict()},
    "account_types": {"dataclass": AccountType, "config": dict()},
    "accounts": {"dataclass": Account, "config": dict()},
    "statement_types": {"dataclass": StatementType, "config": dict()},
    "statement_tables": {"dataclass": StatementTable, "config": dict()},
    "standard_fields": {"dataclass": StandardFields, "config": dict()},
}

for key in __config_dict.keys():
    file = key + ".toml"
    try:
        with open(__dir_user.joinpath(file), "rb") as toml:
            __config_dict[key]["config"] = deepcopy(load(toml))
            toml.close()
    except FileNotFoundError:
        try:
            with open(__dir_base.joinpath(file), "rb") as toml:
                __config_dict[key]["config"] = deepcopy(load(toml))
                toml.close()
        except FileNotFoundError:
            try:
                raise ConfigFileError(file)
            except ConfigFileError as e:
                print(e)

for v in __config_dict.values():
    for k in v["config"].keys():
        v["config"][k] = from_dict(data_class=v["dataclass"], data=v["config"][k])

# Link statement table configurations to their corresponding statement type configs.
# For each statement type, if any header or line config references a statement_table_key,
# assign the corresponding StatementTable object from config_dict["statement_tables"]["config"]
# to the statement_table attribute of that config group.
for key, statement_type in __config_dict["statement_types"]["config"].items():
    if statement_type.header.configs:
        for id, config_group in enumerate(statement_type.header.configs):
            if config_group.statement_table_key:
                __config_dict["statement_types"]["config"][key].header.configs[id].statement_table = __config_dict["statement_tables"][
                    "config"
                ][config_group.statement_table_key]
    if statement_type.lines.configs:
        for id, config_group in enumerate(statement_type.lines.configs):
            if config_group.statement_table_key:
                __config_dict["statement_types"]["config"][key].lines.configs[id].statement_table = __config_dict["statement_tables"][
                    "config"
                ][config_group.statement_table_key]

# account types, statements, and companies into accounts
for key, account in __config_dict["accounts"]["config"].items():
    __config_dict["accounts"]["config"][key].account_type = __config_dict["account_types"]["config"][account.account_type_key]
    __config_dict["accounts"]["config"][key].statement_type = __config_dict["statement_types"]["config"][account.statement_type_key]
    __config_dict["accounts"]["config"][key].company = __config_dict["companies"]["config"][account.company_key]

config_accounts = __config_dict["accounts"]["config"]
config_statement_types = __config_dict["statement_types"]["config"]
config_companies = __config_dict["companies"]["config"]
config_standard_fields = __config_dict["standard_fields"]["config"]

config_accounts_df = pl.DataFrame(config_accounts).transpose(include_header=True, header_name="account", column_names=["config"])
config_statement_types_df = pl.DataFrame(config_statement_types).transpose(
    include_header=True, header_name="statement_type", column_names=["config"]
)
config_companies_df = pl.DataFrame(config_companies).transpose(include_header=True, header_name="company", column_names=["config"])


def config_company_accounts(company_key: str) -> list[Account]:
    return [acct for acct in config_accounts.values() if acct.company_key == company_key]


def pick_leaf(leaves: list[Account] | dict[str, Company], pdf: PDF, logs: pl.DataFrame, file_path: str) -> tuple:
    start = time.time()
    result: tuple | None = None
    if isinstance(leaves, dict):
        for key, leaf in leaves.items():
            config = leaf.config
            if not config:
                continue
            leaf_result = get_results(pdf, "pick", config, scope="success", logs=logs, file_path=file_path)
            if len(leaf_result) > 0:
                result = (leaf, key)
                break
    elif isinstance(leaves, list):
        for leaf in leaves:
            config = leaf.config
            leaf_result = get_results(pdf, "pick", config, scope="success", logs=logs, file_path=file_path)
            if len(leaf_result) > 0:
                result = (leaf, "")
                break
    else:
        raise TypeError("the pick_leaf() function requires leaves to be a dictionary or list")
    if not result:
        raise StatementError("the account cannot be identified from your statement")

    logs.vstack(
        pl.DataFrame([[file_path, "config", "pick_leaf", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"),
        in_place=True,
    )
    return result


def get_config_from_account(account_key: str, logs: pl.DataFrame, file_path: str) -> Account | None:
    start = time.time()
    config_account = config_accounts.get(account_key)
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file_path}")
    else:
        logs.vstack(
            pl.DataFrame(
                [[file_path, "config", "get_config_from_account", time.time() - start, 1, datetime.now(), ""]],
                schema=logs.schema,
                orient="row",
            ),
            in_place=True,
        )
        return config_account


def get_config_from_company(company_key: str, pdf: PDF, logs: pl.DataFrame, file_path: str) -> Account | None:
    start = time.time()
    company_accounts = None
    config_account = None
    try:
        company_accounts = config_company_accounts(company_key)
    except KeyError:
        print(f"{company_key} is not a valid company key")
    if company_accounts:
        try:
            config_account = pick_leaf(leaves=company_accounts, pdf=pdf, logs=logs, file_path=file_path)[0]
        except StatementError:
            config_account = None
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file_path}")
    logs.vstack(
        pl.DataFrame(
            [[file_path, "config", "get_config_from_company", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
        ),
        in_place=True,
    )
    return config_account


def get_config_from_statement(pdf: PDF, file_path: str, logs: pl.DataFrame) -> Account | None:
    start = time.time()
    company_leaf = pick_leaf(leaves=config_companies, pdf=pdf, logs=logs, file_path=file_path)
    if not company_leaf:
        raise StatementError(f"Unable to identify the company from the statement provided: {file_path}")
    company_key = company_leaf[1]
    config_account = None
    try:
        config_account = get_config_from_company(company_key, pdf, logs, file_path)
    except Exception as e:
        raise StatementError(f"Unable to identify the account from the statement provided: {file_path}") from e
    logs.vstack(
        pl.DataFrame(
            [[file_path, "config", "get_config_from_statement", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        ),
        in_place=True,
    )
    return config_account
