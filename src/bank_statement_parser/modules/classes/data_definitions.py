from dataclasses import dataclass
from typing import Optional


@dataclass
class Cell:
    row: int
    col: int


@dataclass
class StringConfig:
    max_length: Optional[str]
    pattern: str = ".+"


@dataclass
class DateConfig:
    date_format: str = "%d %b %y"


@dataclass
class NumericModifier:
    prefix: Optional[str]
    suffix: Optional[str]
    multiplier: int = 1
    include_negative_values: bool = True
    include_positive_values: bool = True


@dataclass
class NumericConfig:
    strip_chars_start: Optional[str]
    strip_chars_end: Optional[str]
    numeric_modifier: Optional[NumericModifier]
    decimal_seperator: str = "."
    thousands_seperator: str = ","
    round_decimals: int = 2


@dataclass
class Field:
    field: str
    cell: Optional[Cell]
    column: Optional[int]
    vital: bool
    numeric_config: Optional[NumericConfig]
    string_config: Optional[StringConfig]
    date_config: Optional[DateConfig]


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
class StdField:
    field: Field


@dataclass
class StdDescription:
    field: Field


@dataclass
class StdCreditDebit:
    field: Field


@dataclass
class MergeFields:
    fields: list[str]
    max_rows: int
    separator: str


@dataclass
class TransactionSpec:
    transaction_bookends: StatementBookend
    std_date: StdField
    std_description: StdField
    std_credit: StdField
    std_debit: StdField
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
    transaction_spec: Optional[TransactionSpec]


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
