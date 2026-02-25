from collections import namedtuple
from dataclasses import dataclass
from typing import Optional


PdfResult = namedtuple(
    "PdfResult",
    [
        "batch_lines_stem",  # str | None — temp parquet stem for batch_lines
        "statement_heads_stem",  # str | None — temp parquet stem for statement_heads
        "statement_lines_stem",  # str | None — temp parquet stem for statement_lines
        "cab_stem",  # str | None — temp parquet stem for checks_and_balances
        "file_src",  # str | None — absolute path of the source PDF
        "file_dst",  # str | None — canonical rename target basename
        "error_cab",  # bool — True if checks & balances validation failed
        "error_config",  # bool — True if configuration or parsing failed
    ],
)
"""Named tuple returned by :func:`~bank_statement_parser.modules.statements.process_pdf_statement`
for each processed PDF.

Fields
------
batch_lines_stem:
    Filename stem of the temporary batch-lines parquet file, or ``None`` on failure.
statement_heads_stem:
    Filename stem of the temporary statement-heads parquet file, or ``None`` on failure.
statement_lines_stem:
    Filename stem of the temporary statement-lines parquet file, or ``None`` on failure.
cab_stem:
    Filename stem of the temporary checks-and-balances parquet file, or ``None`` on failure.
file_src:
    Absolute path string of the original PDF, or ``None`` on failure.
file_dst:
    Target basename (``{id_account}_{YYYYMMDD}.pdf``) for the project copy, or ``None`` if
    the statement did not produce a rename target.
error_cab:
    ``True`` if checks & balances validation failed.
error_config:
    ``True`` if a configuration or parsing failure occurred.
"""


@dataclass(frozen=True, slots=True)
class StdRefs:
    statement_type: str
    field: Optional[str]
    format: Optional[str]
    default: Optional[str]
    multiplier: Optional[float] = 1
    exclude_positive_values: Optional[bool] = False
    exclude_negative_values: Optional[bool] = False
    terminator: Optional[str] = None


@dataclass(frozen=True, slots=True)
class StandardFields:
    section: str
    type: str
    vital: bool
    std_refs: list[StdRefs]


@dataclass(frozen=True, slots=True)
class CurrencySpec:
    symbols: list[str]
    seperator_decimal: str
    seperators_thousands: list[str]
    round_decimals: int
    pattern: str


@dataclass(frozen=True, slots=True)
class Cell:
    row: int
    col: int


@dataclass(frozen=True, slots=True)
class NumericModifier:
    prefix: Optional[str]
    suffix: Optional[str]
    multiplier: float = 1
    exclude_negative_values: bool = False
    exclude_positive_values: bool = False


@dataclass(frozen=True, slots=True)
class FieldOffset:
    rows_offset: int
    cols_offset: int
    vital: bool
    type: str
    numeric_currency: Optional[str] = None
    numeric_modifier: Optional[NumericModifier] = None


@dataclass(frozen=True, slots=True)
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
    value_offset: Optional[FieldOffset] = None


@dataclass(frozen=True, slots=True)
class Test:
    test_desc: str
    assertion: str


@dataclass(frozen=True, slots=True)
class DynamicLineSpec:
    image_id: int
    image_location_tag: str


@dataclass(frozen=False, slots=True)
class Location:
    page_number: Optional[int] = None
    top_left: Optional[list[int]] = None
    bottom_right: Optional[list[int]] = None
    vertical_lines: Optional[list[int]] = None
    dynamic_last_vertical_line: Optional[DynamicLineSpec] = None
    allow_text_failover: Optional[bool] = False
    try_shift_down: Optional[int] = None


@dataclass(frozen=True, slots=True)
class FieldValidation:
    field: str
    pattern: str


@dataclass(frozen=True, slots=True)
class StatementBookend:
    start_fields: list[str]
    min_non_empty_start: int
    end_fields: list[str]
    min_non_empty_end: int
    extra_validation_start: Optional[FieldValidation]
    extra_validation_end: Optional[FieldValidation]
    sticky_fields: Optional[list[str]]


@dataclass(frozen=True, slots=True)
class MergeFields:
    fields: list[str]
    separator: str


@dataclass(frozen=True, slots=True)
class TransactionSpec:
    transaction_bookends: list[StatementBookend]
    fill_forward_fields: Optional[list[str]]
    merge_fields: Optional[MergeFields]


@dataclass(frozen=False, slots=True)
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


@dataclass(frozen=False, slots=True)
class Config:
    config: str
    statement_table_key: Optional[str]
    statement_table: Optional[StatementTable]
    locations: Optional[list[Location]]
    field: Optional[Field]


@dataclass(frozen=True, slots=True)
class ConfigGroup:
    configs: Optional[list[Config]]


@dataclass(frozen=True, slots=True)
class StatementType:
    statement_type: str
    header: ConfigGroup
    lines: ConfigGroup


@dataclass(frozen=True, slots=True)
class AccountType:
    account_type: str


@dataclass(frozen=True, slots=True)
class Company:
    company: str
    config: Optional[Config]
    accounts: Optional[dict]


@dataclass(frozen=False, slots=True)
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
