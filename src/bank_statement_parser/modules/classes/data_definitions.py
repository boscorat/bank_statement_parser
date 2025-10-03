from dataclasses import dataclass
from typing import Optional


@dataclass
class Cell:
    row: Optional[int]
    col: int


@dataclass
class Field:
    field: str
    pattern: str
    cell: Optional[Cell]
    strip: Optional[list]
    vital: Optional[bool] = False
    type: Optional[str] = "str"


@dataclass
class Test:
    test_desc: str
    assertion: str


@dataclass
class Location:
    page_number: Optional[int]
    top_left: Optional[list[int]]
    bottom_right: Optional[list[int]]
    vertical_lines: Optional[list[int]]


@dataclass
class StatementBookend:
    start_fields: list[str]
    min_non_empty_start: int
    end_fields: list[str]
    min_non_empty_end: int


@dataclass
class StdDate:
    field: str
    format: str


@dataclass
class StdDescription:
    field: str
    max_length: int
    strip_chars_start: str
    strip_chars_end: str


@dataclass
class StdCreditDebit:
    field: str
    prefix: str
    suffix: str
    is_float: bool
    multiplier: float
    round_decimals: int


@dataclass
class MergeFields:
    fields: list[str]
    max_rows: int
    separator: str


@dataclass
class TransactionMods:
    transaction_bookends: StatementBookend
    std_date: StdDate
    std_description: StdDescription
    std_credit: StdCreditDebit
    std_debit: StdCreditDebit
    fill_forward_fields: Optional[list[str]]
    merge_fields: Optional[MergeFields]


@dataclass
class StatementTable:
    statement_table: str
    locations: list[Location]
    fields: list[Field]
    table_columns: Optional[int]
    table_rows: Optional[int]
    row_spacing: Optional[int]
    tests: Optional[list[Test]]
    delete_success_false: Optional[bool]
    delete_cast_success_false: Optional[bool]
    delete_rows_with_missing_vital_fields: Optional[bool]
    transaction_mods: Optional[TransactionMods]


@dataclass
class Config:
    config: Optional[str]
    statement_table_key: Optional[str]
    statement_table: Optional[StatementTable]
    location: Optional[Location]
    locations: Optional[list[Location]]
    field: Optional[Field]


@dataclass
class ConfigGroup:
    configs: Optional[list[Config]]


@dataclass
class StatementType:
    statement_type: str
    header: ConfigGroup
    pages: ConfigGroup
    lines: ConfigGroup


@dataclass
class AccountType:
    account_type: str


@dataclass
class Company:
    company: str
    config: Optional[Config]
    accounts: Optional[dict]


@dataclass
class Account:
    account: str
    company_key: str
    company: Optional[Company]
    account_type_key: str
    account_type: Optional[AccountType]
    statement_type_key: str
    statement_type: Optional[StatementType]
    config: Config
