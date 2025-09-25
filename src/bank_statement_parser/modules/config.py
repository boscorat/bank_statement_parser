import os
import pathlib
from copy import deepcopy

from dacite import from_dict
from tomllib import load

from bank_statement_parser.modules.classes.data_definitions import Account, AccountType, Company, StatementTable, StatementType
from bank_statement_parser.modules.classes.errors import ConfigFileError, StatementError
from bank_statement_parser.modules.functions.statement_functions import extract_field_values

# if __name__ == "__main__":
#     from classes.data_definitions import Account, AccountType, Company, StatementTable, StatementType
#     from classes.errors import ConfigFileError
# else:
#     from .classes.data_definitions import Account, AccountType, Company, StatementTable, StatementType
#     from .classes.errors import ConfigFileError

__dir_base = os.path.join(pathlib.Path(__file__).parent.parent, "base_config")
__dir_user = os.path.join(pathlib.Path(__file__).parent.parent.parent, "user_config")

__config_dict = {
    "companies": {"dataclass": Company, "config": dict()},
    "account_types": {"dataclass": AccountType, "config": dict()},
    "accounts": {"dataclass": Account, "config": dict()},
    "statement_types": {"dataclass": StatementType, "config": dict()},
    "statement_tables": {"dataclass": StatementTable, "config": dict()},
}

for key in __config_dict.keys():
    file = key + ".toml"
    try:
        with open(os.path.join(__dir_user, file), "rb") as toml:
            __config_dict[key]["config"] = deepcopy(load(toml))
            toml.close()
    except FileNotFoundError:
        try:
            with open(os.path.join(__dir_base, file), "rb") as toml:
                __config_dict[key]["config"] = deepcopy(load(toml))
                toml.close()
        except FileNotFoundError:
            try:
                raise ConfigFileError(file)
            except ConfigFileError as e:
                print(e)
del key, file, toml

for v in __config_dict.values():
    for k in v["config"].keys():
        v["config"][k] = from_dict(data_class=v["dataclass"], data=v["config"][k])
del v, k

# Link statement table configurations to their corresponding statement type configs.
# For each statement type, if any header, page, or line config references a statement_table_key,
# assign the corresponding StatementTable object from config_dict["statement_tables"]["config"]
# to the statement_table attribute of that config group.
for key, statement_type in __config_dict["statement_types"]["config"].items():
    if statement_type.header.configs:
        for id, config_group in enumerate(statement_type.header.configs):
            if config_group.statement_table_key:
                __config_dict["statement_types"]["config"][key].header.configs[id].statement_table = __config_dict["statement_tables"][
                    "config"
                ][config_group.statement_table_key]
    if statement_type.pages.configs:
        for id, config_group in enumerate(statement_type.pages.configs):
            if config_group.statement_table_key:
                __config_dict["statement_types"]["config"][key].pages.configs[id].statement_table = __config_dict["statement_tables"][
                    "config"
                ][config_group.statement_table_key]
    if statement_type.lines.configs:
        for id, config_group in enumerate(statement_type.lines.configs):
            if config_group.statement_table_key:
                __config_dict["statement_types"]["config"][key].lines.configs[id].statement_table = __config_dict["statement_tables"][
                    "config"
                ][config_group.statement_table_key]
del key, statement_type, id, config_group

# account types, statements, and companies into accounts
for key, account in __config_dict["accounts"]["config"].items():
    __config_dict["accounts"]["config"][key].account_type = __config_dict["account_types"]["config"][account.account_type_key]
    __config_dict["accounts"]["config"][key].statement_type = __config_dict["statement_types"]["config"][account.statement_type_key]
    __config_dict["accounts"]["config"][key].company = __config_dict["companies"]["config"][account.company_key]
del key, account

config_accounts = __config_dict["accounts"]["config"]
config_statement_types = __config_dict["statement_types"]["config"]
config_companies = __config_dict["companies"]["config"]


def config_company_accounts(company_key):
    return [acct for acct in config_accounts.values() if acct.company_key == company_key]


def pick_leaf(leaves, statement) -> tuple[Account, str]:
    """
    Selects and returns the first matching leaf (account) from a collection of leaves (either a dictionary or list)
    based on the provided bank statement.

    The function iterates through the leaves, extracting field values using the leaf's configuration and the statement.
    If a successful extraction is found (i.e., at least one record with 'success' attribute set to True), it returns
    a tuple containing the matching leaf and its key (if leaves is a dictionary) or an empty string (if leaves is a list).

    Args:
        leaves (dict or list): A collection of leaf objects, either as a dictionary (keyed by account identifier)
            or a list. Each leaf must have a 'config' attribute.
        statement: The bank statement object to be used for extraction.

    Returns:
        tuple[Account, str]: A tuple containing the matched leaf (account) and its key (if from a dictionary)
            or an empty string (if from a list).

    Raises:
        TypeError: If 'leaves' is not a dictionary or list.
        StatementError: If no matching account can be identified from the statement.
    """
    result: tuple | None = None
    if isinstance(leaves, dict):
        for key, leaf in leaves.items():
            if hasattr(leaf, "config"):
                extracts = extract_field_values(config=leaf.config, pdf=statement)
                if extracts and (extract := extracts[0]):
                    if sum([1 for record in extract if getattr(record, "success", False)]):
                        result = (leaf, key)
                        break
    elif isinstance(leaves, list):
        for leaf in leaves:
            if hasattr(leaf, "config"):
                extracts = extract_field_values(config=leaf.config, pdf=statement)
                if extracts and (extract := extracts[0]):
                    if sum([1 for record in extract if getattr(record, "success", False)]):
                        result = (leaf, "")
                        break
    else:
        raise TypeError("the pick_leaf() function requires leaves to be a dictionary or list")
    if not result:
        raise StatementError("the account cannot be identified from your statement")
    return result


def get_config_from_statement(statement, file) -> Account:
    """
    Extracts the account configuration from a bank statement.

    This function attempts to identify the company associated with the provided statement,
    then retrieves the corresponding account configuration. If the company or account cannot
    be identified, a StatementError is raised.

    Args:
        statement: The bank statement data to be parsed.
        file: The file path or identifier of the statement for error reporting.

    Returns:
        Account: The configuration object for the identified account.

    Raises:
        StatementError: If the company or account cannot be identified from the statement.
    """
    company_leaf = pick_leaf(leaves=config_companies, statement=statement)
    if not company_leaf:
        raise StatementError(f"Unable to identify the company from the statement provided: {file}")
    company_key = company_leaf[1]
    config_account = None
    try:
        config_account = get_config_from_company(company_key, statement, file)
    except Exception as e:
        raise StatementError(f"Unable to identify the account from the statement provided: {file}") from e
    del company_leaf
    return config_account


def get_config_from_company(company_key: str, statement, file) -> Account:
    """
    Retrieves the account configuration for a given company based on the provided statement and file.

    Args:
        company_key (str): The key identifying the company.
        statement: The bank statement object used to identify the account.
        file: The file associated with the statement, used for error reporting.

    Returns:
        Account: The account configuration object identified from the statement.

    Raises:
        StatementError: If the company key is invalid or the account cannot be identified from the statement.
    """
    company_accounts = None
    config_account = None
    try:
        company_accounts = config_company_accounts(company_key)
    except KeyError:
        print(f"{company_key} is not a valid company key")
    if company_accounts:
        try:
            config_account = pick_leaf(leaves=company_accounts, statement=statement)[0]
        except StatementError:
            config_account = None
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file}")
    return config_account


def get_config_from_account(account_key: str, file) -> Account:
    """
    Retrieves the configuration for a given account key.

    Args:
        account_key (str): The key identifying the account in the configuration.
        file: The file object or path associated with the bank statement.

    Returns:
        Account: The configuration object corresponding to the account key.

    Raises:
        StatementError: If the account key is not found in the configuration.
    """
    config_account = config_accounts.get(account_key)
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file}")
    else:
        return config_account


del __dir_base, __dir_user, __config_dict, deepcopy, from_dict, load, os, pathlib
