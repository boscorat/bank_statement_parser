from dataclasses import dataclass
from typing import Optional


@dataclass
class Cell:
    row: int
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
class Region:
    top_left: list[int]
    bottom_right: list[int]


@dataclass
class Config:
    config: Optional[str]
    page_number: Optional[int]
    regions: Optional[list[Region]]
    type: Optional[str]
    fields: Optional[list[Field]]
    row_spacing: Optional[int]
    header_rows: Optional[int]
    data_rows: Optional[int]
    tests: Optional[list[Test]]


@dataclass
class ConfigGroup:
    configs: Optional[list[Config]]


@dataclass
class StatementType:
    statement_type: str
    header: ConfigGroup
    page: ConfigGroup
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
