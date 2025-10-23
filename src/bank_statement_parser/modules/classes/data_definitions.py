from dataclasses import dataclass
from typing import Optional


@dataclass
class StdRefs:
    statement_type: str
    field: str
    format: Optional[str]
    multiplier: Optional[float] = 1
    exclude_positive_values: Optional[bool] = False
    exclude_negative_values: Optional[bool] = False


@dataclass
class StandardFields:
    section: str
    type: str
    std_refs: list[StdRefs]


@dataclass
class CurrencySpec:
    symbols: list[str]
    seperator_decimal: str
    seperators_thousands: list[str]
    round_decimals: int
    pattern: str


@dataclass
class Cell:
    row: int
    col: int


@dataclass
class NumericModifier:
    prefix: Optional[str]
    suffix: Optional[str]
    multiplier: float = 1
    exclude_negative_values: bool = False
    exclude_positive_values: bool = False


@dataclass
class Field:
    field: str
    cell: Optional[Cell]
    column: Optional[int]
    vital: bool
    type: str
    strip_characters_start: Optional[str] = None
    strip_characters_end: Optional[str] = None
    numeric_currency: Optional[str] = None
    numeric_modifier: Optional[NumericModifier] = None
    string_pattern: Optional[str] = None
    string_max_length: Optional[int] = None
    date_format: Optional[str] = None


@dataclass
class Test:
    test_desc: str
    assertion: str


@dataclass
class Location:
    page_number: Optional[int] = None
    top_left: Optional[list[int]] = None
    bottom_right: Optional[list[int]] = None
    vertical_lines: Optional[list[int]] = None
    allow_text_failover: Optional[bool] = False


@dataclass
class StatementBookend:
    start_fields: list[str]
    min_non_empty_start: int
    end_fields: list[str]
    min_non_empty_end: int


@dataclass
class MergeFields:
    fields: list[str]
    separator: str


@dataclass
class TransactionSpec:
    transaction_bookends: StatementBookend
    fill_forward_fields: Optional[list[str]]
    merge_fields: Optional[MergeFields]


@dataclass
class StatementTable:
    type: str
    statement_table: str
    header_text: Optional[str]
    remove_header: Optional[bool]
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
    config: str
    statement_table_key: Optional[str]
    statement_table: Optional[StatementTable]
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
    exclude_last_n_pages: int
    config: Config
