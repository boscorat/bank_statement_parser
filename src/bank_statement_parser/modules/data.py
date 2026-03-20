"""Dataclasses used throughout the bank_statement_parser pipeline.

Two distinct categories of dataclass live here:

**Pipeline result types** — returned by :func:`~bank_statement_parser.modules.statements.process_pdf_statement`
and consumed by downstream persistence, debug, and test code:

* :class:`StatementInfo` — statement-level metadata extracted from a successfully
  parsed PDF (account, date, balances, canonical rename target).
* :class:`ParquetFiles` — full :class:`~pathlib.Path` objects for the two
  statement-level temporary Parquet files (``statement_heads``, ``statement_lines``).
  Only populated on the ``SUCCESS`` path.
* :class:`Success` — groups ``StatementInfo`` + ``ParquetFiles`` for a
  fully-validated result.
* :class:`Review` — groups ``StatementInfo`` + ``ParquetFiles`` for a statement
  where extraction succeeded but checks-and-balances validation failed.  Carries
  the CAB error message and detail so the caller can decide whether to accept the
  data after human review.
* :class:`Failure` — carries the error message, error type, and optional detail
  for any failure mode where no usable statement data was produced.
* :class:`PdfResult` — top-level result returned by
  :func:`~bank_statement_parser.modules.statements.process_pdf_statement`.
  Always present; carries ``result`` (``"SUCCESS"`` / ``"REVIEW"`` / ``"FAILURE"``),
  ``outcome`` (the specific outcome label), ``batch_lines`` (always written),
  ``checks_and_balances`` (written for SUCCESS and REVIEW), and ``payload``
  (the concrete :class:`Success`, :class:`Review`, or :class:`Failure` instance).

**Configuration types** — loaded from TOML files by ``dacite`` into dataclasses.
Every field name here corresponds to a key in one of the project TOML files.

Annotation convention
---------------------
Each field comment is prefixed with one of:

* **[ACTIVE]** — the field is read and acted upon somewhere in the pipeline.
* **[STUB]**   — the field is declared (and may appear in TOML) but no pipeline
                 code currently reads it. These are reserved for future
                 implementation and should not be relied upon.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal, Optional


@dataclass(frozen=True, slots=True)
class StatementInfo:
    """Statement-level metadata extracted from a successfully validated PDF.

    Populated once per statement on the success path in
    :func:`~bank_statement_parser.modules.statements.process_pdf_statement`.
    All fields are non-optional; a ``StatementInfo`` is only constructed when
    extraction and checks-and-balances validation both pass.

    Attributes:
        id_statement: SHA512 hex digest of the first-page text, used as a
            unique, content-addressable identifier for the statement.
        id_account: Compound account identifier in the form
            ``{company_key}_{account_type_key}_{account_number}`` (spaces
            stripped from the account number).
        account: Human-readable account name from config
            (e.g. ``"Current Account"``).
        statement_date: Statement date extracted from the PDF header.
        payments_in: Total payments-in figure stated on the statement, as
            extracted by the checks-and-balances pass.
        payments_out: Total payments-out figure stated on the statement.
        opening_balance: Opening balance from the statement header.
        closing_balance: Closing balance from the statement header.
        filename_new: Canonical rename target basename for the source PDF in
            the form ``{id_account}_{YYYYMMDD}.pdf``.
    """

    id_statement: str
    id_account: str
    account: str
    statement_date: date
    payments_in: Decimal
    payments_out: Decimal
    opening_balance: Decimal
    closing_balance: Decimal
    filename_new: str


@dataclass(frozen=True, slots=True)
class ParquetFiles:
    """Paths to the statement-level temporary Parquet files written on the SUCCESS path.

    Each field holds the absolute :class:`~pathlib.Path` of a temporary file
    written by :func:`~bank_statement_parser.modules.statements.process_pdf_statement`,
    or ``None`` when the corresponding write was skipped due to a data error.

    ``batch_lines`` and ``checks_and_balances`` are written for more than just
    ``SUCCESS`` results and therefore live directly on :class:`PdfResult` rather
    than here.

    These paths are consumed by
    :func:`~bank_statement_parser.modules.parquet.update_parquet` and
    :func:`~bank_statement_parser.modules.database.update_db` to merge the
    temporary files into the permanent store, and by
    :func:`~bank_statement_parser.modules.statements.delete_temp_files` to
    clean them up afterwards.

    Attributes:
        statement_heads: Path to the temporary ``statement_heads`` Parquet file,
            or ``None`` if not written.
        statement_lines: Path to the temporary ``statement_lines`` Parquet file,
            or ``None`` if not written.
    """

    statement_heads: Path | None
    statement_lines: Path | None


@dataclass(frozen=True, slots=True)
class Success:
    """Payload for a fully-validated PDF result.

    Constructed only when both extraction and checks-and-balances validation
    succeed.  Groups the statement metadata and the temporary Parquet file
    paths into a single object carried by :class:`PdfResult`.

    Attributes:
        statement_info: Statement-level metadata (account, date, balances,
            rename target).
        parquet_files: Paths to the two statement-level temporary Parquet files
            (``statement_heads``, ``statement_lines``) written during processing.
    """

    statement_info: StatementInfo
    parquet_files: ParquetFiles


@dataclass(frozen=True, slots=True)
class Review:
    """Payload for a PDF where extraction succeeded but CAB validation failed.

    Constructed when statement data was fully extracted and written to Parquet
    but the checks-and-balances validation step found a discrepancy.  The
    caller must decide — after human review — whether to accept or discard the
    data.  The statement data is **not** automatically merged into the
    permanent store.

    Attributes:
        statement_info: Statement-level metadata (account, date, balances,
            rename target).  Populated even though CAB failed.
        parquet_files: Paths to the two statement-level temporary Parquet files
            (``statement_heads``, ``statement_lines``) written during processing.
        message: Primary human-readable CAB error message
            (e.g. ``"** Checks & Balances Failure ** ..."``).
        message_detail: Per-check delta lines produced by ``_cab_detail``.
            Empty string when not applicable.
    """

    statement_info: StatementInfo
    parquet_files: ParquetFiles
    message: str
    message_detail: str = ""


@dataclass(frozen=True, slots=True)
class Failure:
    """Payload for a PDF result where no usable statement data was produced.

    A single ``Failure`` type covers all non-recoverable failure modes; the
    specific mode is encoded in ``error_type`` rather than through subclasses,
    which avoids ``__slots__`` inheritance conflicts under ``frozen=True``.

    Note that CAB failures no longer use this class — they produce a
    :class:`Review` payload instead, because the statement data *was*
    extracted and *may* be usable after human review.

    Attributes:
        message: Primary human-readable error message accumulated during
            processing.
        error_type: Short string identifying the failure category:

            * ``"config"`` — a configuration or PDF-parsing failure occurred
              before or during extraction.
            * ``"data"``   — extraction passed but a Parquet schema/data-type
              error occurred during the write step.
            * ``"other"``  — an unexpected exception was caught by the
              last-resort guard in ``process_pdf_statement``.

        message_detail: Optional supplementary detail appended to ``message``.
            Empty string when not applicable.
    """

    message: str
    error_type: Literal["config", "data", "other"]
    message_detail: str = ""


@dataclass(frozen=True, slots=True)
class PdfResult:
    """Top-level result returned by :func:`~bank_statement_parser.modules.statements.process_pdf_statement`.

    Every call to ``process_pdf_statement`` returns exactly one ``PdfResult``
    regardless of whether processing succeeded or failed.  The ``result``
    sentinel provides fast top-level discrimination; ``outcome`` carries the
    specific outcome label; ``batch_lines`` and ``checks_and_balances`` carry
    the paths to always-written and conditionally-written audit files; and
    ``payload`` is the fully-typed result object.

    Typical usage
    -------------
    Check ``result`` first, then access the typed payload::

        pdf_result: PdfResult = process_pdf_statement(...)

        if pdf_result.result == "SUCCESS":
            info = pdf_result.payload.statement_info
            pq   = pdf_result.payload.parquet_files
            print(info.id_account, info.statement_date)
        elif pdf_result.result == "REVIEW":
            review = pdf_result.payload
            print("CAB failed:", review.message)
            # review.parquet_files holds the written statement data
        else:
            failure = pdf_result.payload
            print(failure.error_type, failure.message)

    For bulk processing via :class:`~bank_statement_parser.modules.statements.StatementBatch`,
    iterate ``batch.processed_pdfs`` which is typed as
    ``list[BaseException | PdfResult]``::

        for entry in batch.processed_pdfs:
            if isinstance(entry, BaseException):
                continue          # unhandled worker crash
            if entry.result == "FAILURE":
                print(entry.payload.message)

    Attributes:
        result: Top-level outcome sentinel — ``"SUCCESS"``, ``"REVIEW"``, or
            ``"FAILURE"``.  Use this for fast branching before inspecting
            ``payload``.
        outcome: Specific outcome label — one of:

            * ``"SUCCESS"``        — extraction and validation passed.
            * ``"REVIEW CAB"``     — extraction succeeded but CAB validation
              failed; data needs human review before acceptance.
            * ``"FAILURE CONFIG"`` — configuration / PDF-parsing failure.
            * ``"FAILURE DATA"``   — Parquet write failure during extraction.
            * ``"FAILURE OTHER"``  — unexpected exception (last-resort guard).

        batch_lines: Path to the temporary ``batch_lines`` Parquet file.
            Always written regardless of outcome.
        checks_and_balances: Path to the temporary ``checks_and_balances``
            Parquet file, or ``None`` if the CAB step did not run (i.e. a
            ``FAILURE CONFIG`` or ``FAILURE OTHER`` result where
            ``Statement.__init__`` never completed).
        payload: The concrete result object — a :class:`Success` on the
            success path, a :class:`Review` when CAB validation failed, or a
            :class:`Failure` on any non-recoverable failure path.
            Use ``isinstance(payload, Success)`` or check ``result`` before
            accessing type-specific attributes.

    See also
    --------
    :class:`Success`, :class:`Review`, :class:`Failure`, :class:`StatementInfo`, :class:`ParquetFiles`
    """

    result: Literal["SUCCESS", "REVIEW", "FAILURE"]
    outcome: Literal["SUCCESS", "REVIEW CAB", "FAILURE CONFIG", "FAILURE DATA", "FAILURE OTHER"]
    batch_lines: Path
    checks_and_balances: Path | None
    payload: "Success | Review | Failure"


@dataclass(frozen=True, slots=True)
class StdRefs:
    """Mapping rule that promotes a raw extracted field to a standard output column.

    One ``StdRefs`` entry is needed per ``statement_type`` for each ``StandardFields``
    entry.  At runtime ``get_standard_fields()`` filters the list to the entry whose
    ``statement_type`` matches the statement being processed, then applies the
    transformation described by the remaining fields.

    Defined in ``standard_fields.toml`` under each standard-field block's
    ``std_refs`` array.
    """

    statement_type: str
    # [ACTIVE] — Key used to select this rule; matched against the statement type string
    # of the PDF being processed (e.g. "HSBC UK Current Account").

    field: Optional[str]
    # [ACTIVE] — Name of the raw extracted column to promote.  Set to None (or omit)
    # when a literal default value should be used instead of a column value.

    format: Optional[str]
    # [ACTIVE] — strptime format string applied when StandardFields.type == "date"
    # (e.g. "%-d %B %Y").  Ignored for numeric and string types.

    default: Optional[str]
    # [ACTIVE] — Literal string value used as the output when ``field`` is None/absent.
    # Useful for injecting constant metadata (e.g. transaction_type = "CC").

    multiplier: Optional[float] = 1
    # [ACTIVE] — Scalar applied to the value after casting when
    # StandardFields.type == "numeric".  Use -1 to invert sign (e.g. to convert a
    # credit amount stored as positive into a negative figure).

    exclude_positive_values: Optional[bool] = False
    # [ACTIVE] — When True, any positive numeric value is replaced with 0 after
    # casting.  Used to isolate debit-side figures from a combined amount column.

    exclude_negative_values: Optional[bool] = False
    # [ACTIVE] — When True, any negative numeric value is replaced with 0 after
    # casting.  Used to isolate credit-side figures from a combined amount column.

    terminator: Optional[str] = None
    # [ACTIVE] — Regex pattern; when present the string value is truncated at the
    # first match position before being written to the standard column.  Useful for
    # stripping trailing boilerplate appended by merge_fields
    # (e.g. " | BALANCE CARRIED FORWARD").


@dataclass(frozen=True, slots=True)
class StandardFields:
    """Declaration of a single standard output column and how to derive it.

    Each entry maps one standard column (e.g. ``STD_PAYMENT_OUT``) to one or more
    ``StdRefs`` rules, one per supported statement type.

    Defined in ``standard_fields.toml``.
    """

    section: str
    # [ACTIVE] — Pipeline section this field belongs to: "header" (statement-level
    # metadata extracted once per statement) or "lines" (per-transaction data).
    # Used to dispatch the field to the correct extraction pass.

    type: str
    # [ACTIVE] — Data type of the standard column: "string", "numeric", or "date".
    # Controls casting, multiplier application, and date parsing in
    # get_standard_fields().

    vital: bool
    # [ACTIVE] — When True a ConfigError is raised if no matching StdRefs entry is
    # found for the current statement type, halting processing.  Set False for
    # optional fields that not all statement types provide.

    std_refs: list[StdRefs]
    # [ACTIVE] — One entry per supported statement type.  The correct entry is
    # selected at runtime by matching StdRefs.statement_type.


@dataclass(frozen=True, slots=True)
class CurrencySpec:
    """Currency formatting rules used to strip symbols and separators before numeric casting.

    Instances are defined in ``currency.py`` (not loaded from TOML) and keyed by
    currency code (e.g. "GBP").  A ``Field`` of ``type = "currency"`` inherits the
    spec from its account's ``Account.currency``.  A ``Field`` of ``type = "numeric"``
    may still override the spec via ``Field.currency_override``.
    """

    name: str
    # [ACTIVE] — Human-readable currency name (e.g. "British Pound Sterling").

    symbols: list[str]
    # [ACTIVE] — List of currency symbol strings to strip from the raw value before
    # casting (e.g. ["£", "$"]).  Replaced with empty string via str.replace_many().

    seperator_decimal: str
    # [STUB] — Intended decimal separator character (e.g. ".").  Declared but never
    # read by the pipeline; decimal handling is implicit after symbols and thousands
    # separators are stripped.

    seperators_thousands: list[str]
    # [ACTIVE] — List of thousands-separator strings to strip (e.g. [","]).
    # Replaced with empty string via str.replace_many() before casting.

    round_decimals: int
    # [STUB] — Intended rounding precision after casting.  Declared but never read
    # by the pipeline; no rounding is currently applied.

    pattern: str
    # [ACTIVE] — Regex pattern used to extract the numeric substring from the raw
    # cell text before symbol/separator stripping.  Passed to patmatch() via
    # build_pattern().


@dataclass(frozen=True, slots=True)
class Cell:
    """Zero-based row and column address of a cell within a PDF table.

    Used by ``Field.cell`` to address a specific cell in summary/detail tables
    (i.e. tables without a ``transaction_spec``).  Not used for transaction tables,
    which address fields by column index via ``Field.column``.

    Example TOML: ``cell = {row = 1, col = 2}``
    """

    row: int
    # [ACTIVE] — Zero-based row index within the extracted table.

    col: int
    # [ACTIVE] — Zero-based column index within the extracted table.


@dataclass(frozen=True, slots=True)
class NumericModifier:
    """Optional sign/multiplier transformation applied after numeric casting.

    Handles the common bank-statement pattern where negative values are represented
    with a surrounding character rather than a minus sign (e.g. "123.45 CR" or
    "(123.45)").

    Exactly one of ``prefix`` or ``suffix`` should be set per modifier; both being
    set is unsupported.  If neither is set the ``multiplier`` is still applied
    unconditionally.

    Example TOML: ``numeric_modifier = {suffix = "D", multiplier = -1.0}``
    """

    prefix: Optional[str]
    # [ACTIVE] — If the raw value starts with this string the prefix is stripped and
    # the multiplier applied.  Use for formats like "(123.45)" where "(" signals a
    # negative value.

    suffix: Optional[str]
    # [ACTIVE] — If the raw value ends with this string the suffix is stripped and
    # the multiplier applied.  Use for formats like "123.45 CR" or "123.45D".

    multiplier: float = 1
    # [ACTIVE] — Scalar applied to the cast value when the prefix/suffix matches, or
    # unconditionally if neither prefix nor suffix is set.  Typically -1 to invert
    # sign.

    exclude_negative_values: bool = False
    # [ACTIVE] — When True, any negative result after casting and multiplier
    # application is replaced with 0.  Useful for isolating one side of a combined
    # debit/credit column.

    exclude_positive_values: bool = False
    # [ACTIVE] — When True, any positive result after casting and multiplier
    # application is replaced with 0.  Useful for isolating one side of a combined
    # debit/credit column.


@dataclass(frozen=True, slots=True)
class FieldOffset:
    """Reads a field's value from an adjacent column rather than the field's own column.

    When set on a ``Field`` via ``value_offset``, the pipeline reads the raw text
    from ``Field.column + cols_offset`` and processes it with the type, currency, and
    modifier rules declared here — overriding those on the parent ``Field``.

    Useful when a value logically belongs to one column (e.g. a date) but the actual
    numeric amount sits one column to the right in the same row.

    Example TOML (currently commented-out reference):
    ``value_offset = {rows_offset = 0, cols_offset = 1, type = "currency", vital = false}``
    """

    rows_offset: int
    # [STUB] — Intended row offset for reading the value from a different row.
    # Declared and accepted in TOML but never read by the pipeline; only cols_offset
    # is currently consumed.  Always set to 0 in TOML examples.

    cols_offset: int
    # [ACTIVE] — Column offset applied to Field.column to locate the source cell
    # (e.g. 1 reads from the column immediately to the right).

    vital: bool
    # [ACTIVE] — Passed to the extraction pipeline for the offset field; when True
    # extraction failure is treated as a hard failure for that row.

    type: str
    # [ACTIVE] — Data type for the offset value: "string", "numeric", or "currency".
    # Overrides the parent Field.type for this value read.

    currency_override: str | None = None
    # [ACTIVE] — Explicit currency key (e.g. "GBP") for numeric stripping of the
    # offset value when type == "numeric".  Overrides the account-level currency.
    # When type == "currency" the account-level currency is used and this is ignored.

    numeric_modifier: Optional[NumericModifier] = None
    # [ACTIVE] — Sign/multiplier modifier for the offset value.  Overrides the
    # parent Field.numeric_modifier.


@dataclass(frozen=True, slots=True)
class Field:
    """Extraction specification for a single column or cell within a PDF table.

    Each entry in a ``StatementTable.fields`` list is one ``Field``.  For
    transaction tables set ``column``; for summary/detail tables set ``cell``.
    Exactly one of ``cell`` or ``column`` should be set for any given field.

    Defined in the ``fields`` array of each table entry in ``statement_tables.toml``,
    or inline in a ``Config`` entry in ``statement_types.toml``.

    Example TOML:
    ``{field = 'date', column = 0, vital = false, type = "string", string_pattern = '^[0-3][0-9]\\s?[A-Z][a-z]{2}'}``
    """

    field: str
    # [ACTIVE] — Output column name for this field (e.g. "date", "£_paid_out").
    # Used as the field identifier throughout the pipeline and in the output Parquet
    # files.

    cell: Optional[Cell]
    # [ACTIVE] — Row/column address for summary or detail table extraction.
    # Mutually exclusive with ``column``; set to None for transaction tables.

    column: int | None
    # [ACTIVE] — Zero-based column index for transaction table extraction.
    # Mutually exclusive with ``cell``; set to None for summary/detail tables.

    vital: bool
    # [ACTIVE] — When True, extraction failure for this field causes the row to be
    # flagged as a hard failure and excluded from output.  When False, failure is
    # recorded but the row is retained.

    type: str
    # [ACTIVE] — Data type: "string", "numeric", or "currency".
    #
    # * "string"   — raw text extraction; pattern matching and trimming applied.
    # * "numeric"  — numeric extraction with optional explicit currency stripping via
    #                ``currency_override``.
    # * "currency" — identical to "numeric" but inherits the CurrencySpec from the
    #                account's ``Account.currency`` rather than requiring an explicit
    #                ``currency_override`` on every field.  Use this for all monetary
    #                amount fields; reserve "numeric" for non-monetary numerics (e.g.
    #                APR, sort code).

    strip_characters_start: Optional[str] = None
    # [ACTIVE] — Characters to strip from the start of the raw string before pattern
    # matching (passed to Polars str.strip_chars_start()).  Useful for leading
    # currency symbols not covered by the account currency spec.

    strip_characters_end: Optional[str] = None
    # [ACTIVE] — Characters to strip from the end of the raw string before pattern
    # matching (passed to Polars str.strip_chars_end()).

    currency_override: str | None = None
    # [ACTIVE] — Explicit ISO 4217 currency key (e.g. "GBP") used when
    # ``type == "numeric"`` and currency stripping is needed but should differ from
    # the account-level ``Account.currency``.  Ignored when ``type == "currency"``
    # (which always uses the account-level currency).  Omit for non-monetary numeric
    # fields (e.g. APR, sort code) where no currency stripping is required.

    numeric_modifier: Optional[NumericModifier] = None
    # [ACTIVE] — Sign/multiplier transformation applied after numeric casting.
    # See NumericModifier.  Omit for straightforward positive numeric values.

    string_pattern: Optional[str] = None
    # [ACTIVE] — Regex pattern the extracted string must match.  Extraction is
    # marked as failed (success = False) if the value does not match.  Used to
    # validate field contents (e.g. date format) and to skip blank or irrelevant
    # rows.

    string_max_length: Optional[int] = None
    # [ACTIVE] — Maximum character length for string values; longer strings are
    # truncated via str.head().  Useful for capping free-text description fields.
    # Defaults to 999 if not set.

    date_format: Optional[str] = None
    # [STUB] — Intended strptime format for date parsing at the Field level.
    # Declared but never read by the pipeline; date format parsing is handled via
    # StdRefs.format in get_standard_fields() instead.

    value_offset: Optional["FieldOffset"] = None
    # [ACTIVE] — When set, reads the field's value from an adjacent column
    # (Field.column + FieldOffset.cols_offset) using the type and currency rules
    # defined in the FieldOffset rather than those on this Field.  The primary field
    # column is still extracted normally; the offset column value replaces it in the
    # output.  See FieldOffset.


@dataclass(frozen=True, slots=True)
class Test:
    """Declarative test assertion attached to a StatementTable.

    Note: the Test dataclass and StatementTable.tests are declared but **entirely
    unimplemented** — no pipeline code reads or evaluates them.  Reserved for a
    future automated config-validation pass.
    """

    test_desc: str
    # [STUB] — Human-readable description of the test assertion.

    assertion: str
    # [STUB] — The assertion expression to evaluate (format TBD).


@dataclass(frozen=True, slots=True)
class DynamicLineSpec:
    """Locates the position of the last vertical column divider from an embedded PDF image.

    Some statement layouts (e.g. HSBC credit card) have a rightmost column whose
    boundary is set by the position of a logo image rather than a fixed coordinate.
    This spec instructs ``get_table_from_region()`` to read that image's bounding box
    and use its x-coordinate as the final vertical line.

    Example TOML:
    ``dynamic_last_vertical_line = {image_id = 0, image_location_tag = "x1"}``
    """

    image_id: int
    # [ACTIVE] — Zero-based index into the list of images on the page, identifying
    # which image provides the boundary coordinate.

    image_location_tag: str
    # [ACTIVE] — Bounding-box attribute of the image to use as the x-coordinate
    # (e.g. "x0" for left edge, "x1" for right edge).


@dataclass(frozen=False, slots=True)
class Location:
    """Describes a rectangular region on a PDF page from which a table or text is extracted.

    Used in ``StatementTable.locations`` and ``Config.locations``.  When
    ``page_number`` is omitted the location is cloned for every page in the document
    (minus any ``exclude_last_n_pages`` tail pages), enabling a single location
    entry to cover multi-page transaction tables.

    All coordinate values are in PDF points (1/72 inch).

    Example TOML:
    ``{page_number = 1, top_left = [50, 120], bottom_right = [560, 400], vertical_lines = [50, 150, 150, 320]}``
    """

    page_number: Optional[int] = None
    # [ACTIVE] — 1-based page number.  When set the location is used only on that
    # page.  When None the location is cloned for every page (spawn_locations()).

    top_left: Optional[list[int]] = None
    # [ACTIVE] — [x, y] coordinates of the top-left corner of the crop rectangle.
    # Must be set together with bottom_right.  When both are None the full page is
    # used.

    bottom_right: Optional[list[int]] = None
    # [ACTIVE] — [x, y] coordinates of the bottom-right corner of the crop
    # rectangle.  Must be set together with top_left.

    vertical_lines: Optional[list[int]] = None
    # [ACTIVE] — Explicit x-coordinates of vertical column dividers supplied to
    # pdfplumber as explicit_vertical_lines.  Pairs of identical values create a
    # zero-width gap that forces a column boundary (e.g. [100, 100, 200, 200]).
    # When set, pdfplumber's automatic column detection is disabled for this region.

    dynamic_last_vertical_line: Optional[DynamicLineSpec] = None
    # [ACTIVE] — When set, the final value in vertical_lines is replaced at runtime
    # with an x-coordinate derived from a PDF image's bounding box.  See
    # DynamicLineSpec.  Used where the rightmost column boundary floats with a logo.

    allow_text_failover: Optional[bool] = False
    # [ACTIVE] — When True and the extracted table has the wrong number of columns,
    # the extraction is retried without vertical_lines, falling back to pdfplumber's
    # text-based column detection.  Useful as a safety net for pages where the
    # explicit dividers produce a malformed table.

    try_shift_down: Optional[int] = None
    # [ACTIVE] — Number of PDF points to shift the crop rectangle downward (applied
    # to both top_left[1] and bottom_right[1]) when the initial extraction returns an
    # empty region.  Handles statements where the table top boundary varies slightly
    # between pages.


@dataclass(frozen=True, slots=True)
class FieldValidation:
    """A field-name/regex-pattern pair used as a row filter or row qualification rule.

    Used in two contexts:

    * ``TransactionSpec.exclude_rows`` — rows where the named field's extracted value
      **matches** the pattern are dropped entirely before bookend detection.
    * ``TransactionBookend.extra_validation_start`` — rows where the named field's
      value does **not** match the pattern are excluded from being start-bookend
      candidates for that bookend.

    Example TOML:
    ``{field = 'details', pattern = 'STATEMENT[\\s]?CLOSING[\\s]?BALANCE'}``
    """

    field: str
    # [ACTIVE] — Name of the extracted field (output column name) whose value is
    # tested against the pattern.

    pattern: str
    # [ACTIVE] — Regex pattern tested via Polars str.contains().  For exclude_rows
    # a match causes exclusion; for extra_validation_start a non-match causes
    # exclusion.


@dataclass(frozen=True, slots=True)
class TransactionBookend:
    """Defines how the start and end of a single transaction are detected within a table.

    Transaction tables can contain multiple rows per transaction (e.g. a date/type row
    followed by one or more description rows followed by an amount row).  A bookend
    defines the field patterns that identify the first and last row of each
    transaction.  Multiple bookends can be declared in a ``TransactionSpec`` to handle
    different row shapes within the same table (e.g. normal transactions vs. a
    special interest charge line).

    The pipeline flags every table row as ``transaction_start``, ``transaction_end``,
    both, or neither.  Rows are then numbered by cumulative start count and collapsed
    to one output row per transaction (the end row) via ``process_transactions()``.

    Defined within ``transaction_bookends`` array in a ``[table_key.transaction_spec]``
    TOML block.

    Example TOML::

        {start_fields = ['payment_type', 'details'], min_non_empty_start = 2,
         end_fields = ['£_paid_out', '£_paid_in'], min_non_empty_end = 1}
    """

    start_fields: list[str]
    # [ACTIVE] — Field names that are checked to identify the first row of a
    # transaction.  A row qualifies as a start row when at least
    # min_non_empty_start of these fields extracted successfully (success = True).

    min_non_empty_start: int
    # [ACTIVE] — Minimum number of start_fields that must have extracted
    # successfully for a row to be flagged as transaction_start = True.

    end_fields: list[str]
    # [ACTIVE] — Field names checked to identify the last row of a transaction.
    # A row qualifies as an end row when at least min_non_empty_end of these
    # fields extracted successfully.

    min_non_empty_end: int
    # [ACTIVE] — Minimum number of end_fields that must have extracted
    # successfully for a row to be flagged as transaction_end = True.

    extra_validation_start: Optional[FieldValidation]
    # [ACTIVE] — When set, any row where the named field's value does NOT match the
    # pattern is excluded from being a start-bookend candidate for this bookend.
    # Rows excluded here may still be captured by another bookend in the list.
    # Useful for bookends that should only trigger on a specific row shape
    # (e.g. an interest charge line identified by its details text).

    extra_validation_end: Optional[FieldValidation]
    # [STUB] — Symmetric counterpart to extra_validation_start for end rows.
    # Declared but not yet implemented in the pipeline; no code currently reads
    # this field.  Reserved for future use.

    sticky_fields: Optional[list[str]]
    # [STUB] — Intended to forward-fill named fields from the start row of a
    # transaction down to its end row, scoped within a single transaction (as
    # opposed to fill_forward_fields which fills across transactions).  Declared
    # but not implemented; no pipeline code reads this field.


@dataclass(frozen=True, slots=True)
class MergeFields:
    """Specifies how multi-row text fields are collapsed into a single output value.

    When a transaction spans multiple table rows (e.g. a description split across
    two lines), ``merge_fields`` joins the per-row values into one string using
    ``separator`` as the delimiter.  The join is performed within each transaction
    number group via ``process_transactions()``.

    Example TOML: ``merge_fields = {fields = ['details'], separator = ' | '}``
    """

    fields: list[str]
    # [ACTIVE] — Names of the fields whose per-row values should be joined.

    separator: str
    # [ACTIVE] — Delimiter inserted between joined values (e.g. " | ").


@dataclass(frozen=True, slots=True)
class TransactionSpec:
    """Full specification for extracting transactions from a transaction-type table.

    Attached to a ``StatementTable`` via the ``transaction_spec`` sub-table in TOML.
    All fields are optional except ``transaction_bookends``.

    Example TOML block::

        [MY_TABLE_KEY.transaction_spec]
        exclude_rows = [{field = 'details', pattern = 'STATEMENT[\\s]?CLOSING[\\s]?BALANCE'}]
        fill_forward_fields = ['date', 'payment_type']
        merge_fields = {fields = ['details'], separator = ' | '}
        transaction_bookends = [
            {start_fields = ['payment_type', 'details'], min_non_empty_start = 2,
             end_fields = ['£_paid_out', '£_paid_in'], min_non_empty_end = 1},
        ]
    """

    transaction_bookends: list[TransactionBookend]
    # [ACTIVE] — One or more bookend definitions that identify transaction
    # boundaries.  Evaluated in order; a row matched by an earlier bookend is not
    # re-matched by a later one.  At least one bookend is required.

    fill_forward_fields: Optional[list[str]]
    # [ACTIVE] — Field names whose null values should be forward-filled across rows
    # within the same page after pivot.  Use for sparse columns where a value
    # (e.g. a date or payment type) appears only on the first row of a multi-row
    # block and needs propagating to the end row.

    merge_fields: Optional[MergeFields]
    # [ACTIVE] — When set, collapses multi-row text fields within each transaction
    # into a single joined string.  See MergeFields.

    exclude_rows: Optional[list[FieldValidation]]
    # [ACTIVE] — Rows where any rule's field value matches its pattern are removed
    # from the results before bookend detection runs.  Use to suppress known
    # non-transaction rows (e.g. a closing balance summary line) that would
    # otherwise interfere with transaction counting or checks & balances.
    # Each rule is a {field, pattern} pair; a row is excluded if any rule matches.


@dataclass(frozen=False, slots=True)
class StatementTable:
    """Full configuration for extracting one table from a PDF statement.

    Each key in ``statement_tables.toml`` defines one ``StatementTable``.  The key is
    referenced from ``statement_types.toml`` via ``Config.statement_table_key``.

    The ``type`` field determines which extraction path is used:

    * ``"transaction"`` — multi-row table with a ``transaction_spec``.
    * ``"summary"`` / ``"detail"`` — fixed-layout table addressed by cell coordinates.

    Example TOML::

        [MY_TABLE_KEY]
        type = "transaction"
        statement_table = 'Transactions'
        table_columns = 6
        locations = [{vertical_lines = [50, 150, 150, 320, 320, 400, 400, 480]}]
        fields = [
            {field = 'date',       column = 0, vital = false, type = "string", string_pattern = '^[0-3][0-9]'},
            {field = '£_paid_out', column = 3, vital = false, type = "currency"},
        ]
        [MY_TABLE_KEY.transaction_spec]
        transaction_bookends = [...]
    """

    type: str
    # [STUB] — Table type label: "transaction", "summary", or "detail".  Loaded from
    # TOML but not currently read by the pipeline; the extraction path is determined
    # by whether transaction_spec is present rather than this field.

    statement_table: str
    # [STUB] — Human-readable table label (e.g. "Transactions", "Account Summary").
    # Loaded from TOML for documentation purposes but not consumed by the pipeline.

    header_text: Optional[str]
    # [ACTIVE] — When set, the first table row whose text matches this string is
    # stripped before extraction.  Use when pdfplumber includes the column header
    # row in the extracted data.

    remove_header: Optional[bool]
    # [ACTIVE] — When True the first table row is unconditionally stripped.  Use
    # when the header row is always present but its text varies (making header_text
    # impractical).

    locations: list[Location]
    # [ACTIVE] — One or more Location entries describing where on the page to find
    # this table.  Locations without a page_number are cloned for every page.

    fields: list[Field]
    # [ACTIVE] — Ordered list of field extraction specs.  For transaction tables
    # each field must have a column; for summary/detail tables each field must have
    # a cell.

    table_columns: Optional[int]
    # [ACTIVE] — Expected minimum number of columns in the extracted table.  Passed
    # to pdfplumber as min_words_horizontal and used to validate column count after
    # extraction.  Also triggers allow_text_failover retry logic.

    table_rows: Optional[int]
    # [ACTIVE] — Expected minimum number of rows in the extracted table.  Passed to
    # pdfplumber as min_words_vertical.

    row_spacing: Optional[int]
    # [ACTIVE] — pdfplumber snap_y_tolerance in PDF points.  Rows whose top edges
    # fall within this distance of each other are merged into the same table row.
    # Increase if the statement uses tight line spacing that splits a single visual
    # row across multiple pdfplumber rows.

    tests: Optional[list[Test]]
    # [STUB] — Declarative post-extraction assertions.  Declared and accepted in
    # TOML but no pipeline code evaluates them.  Reserved for a future config
    # validation pass.

    delete_success_false: Optional[bool]
    # [STUB] — Intended to drop rows where any field extraction returned
    # success = False.  Declared and set in TOML (typically True) but no pipeline
    # code currently reads or acts on this flag.

    delete_cast_success_false: Optional[bool]
    # [STUB] — Intended to drop rows where numeric casting failed.  Declared and
    # set in TOML (typically True) but no pipeline code currently reads or acts on
    # this flag.

    delete_rows_with_missing_vital_fields: Optional[bool]
    # [STUB] — Intended to drop rows where any vital field is missing after
    # extraction.  Declared and set in TOML (typically True) but no pipeline code
    # currently reads or acts on this flag.  Note: vital-field hard-failure logic
    # exists in validate() but is separate from this flag.

    transaction_spec: Optional[TransactionSpec]
    # [ACTIVE] — When set, the table is processed as a transaction table using the
    # bookend-based multi-row extraction path.  Must be None for summary/detail
    # tables.


@dataclass(frozen=False, slots=True)
class Config:
    """A single extraction step: one table (or one standalone field) from one location.

    Configs are declared inline in ``statement_types.toml`` and may reference a
    ``StatementTable`` by key (via ``statement_table_key``) or carry their own
    ``locations`` and ``field`` for simple single-field extraction.

    At load time ``config.py`` resolves ``statement_table_key`` → ``statement_table``
    by joining against the loaded ``statement_tables.toml`` dictionary.
    """

    config: str
    # [ACTIVE] — Human-readable label for this extraction step (e.g. "Statement
    # Balances").  Written into the "config" column of the long-format results
    # DataFrame for traceability.

    statement_table_key: Optional[str]
    # [ACTIVE] — Key into statement_tables.toml that identifies the StatementTable
    # to use.  Resolved to statement_table at load time.  Set to None for inline
    # single-field configs.

    statement_table: Optional[StatementTable]
    # [ACTIVE] — Resolved at load time from statement_table_key.  The StatementTable
    # object used during extraction.  Not set directly in TOML.

    locations: Optional[list[Location]]
    # [ACTIVE] — Used only for inline single-field configs (where statement_table is
    # None).  Defines where on the page to find the field value.

    field: Optional[Field]
    # [ACTIVE] — Used only for inline single-field configs.  Defines the extraction
    # spec for the single value to read from the location.


@dataclass(frozen=True, slots=True)
class ConfigGroup:
    """An ordered list of Config extraction steps for one pipeline section.

    Used as ``StatementType.header`` and ``StatementType.lines``.

    Defined in ``statement_types.toml``.
    """

    configs: Optional[list[Config]]
    # [ACTIVE] — Ordered list of Config steps.  Executed in sequence during
    # extraction; results are stacked into the section's results DataFrame.


@dataclass(frozen=True, slots=True)
class StatementType:
    """Full extraction specification for one statement layout variant.

    Keyed in ``statement_types.toml``; referenced from ``accounts.toml`` via
    ``Account.statement_type_key``.  Groups all extraction steps for a given bank /
    account-type combination into a ``header`` group (run once per statement) and a
    ``lines`` group (run once per page for transaction data).
    """

    statement_type: str
    # [ACTIVE] — Human-readable label matching the value used in StdRefs.statement_type
    # (e.g. "HSBC UK Current Account").  Used to select the correct StdRefs mapping
    # when promoting raw fields to standard columns.

    header: ConfigGroup
    # [ACTIVE] — Config steps that extract statement-level metadata: dates, account
    # numbers, opening/closing balances, etc.

    lines: ConfigGroup
    # [ACTIVE] — Config steps that extract per-transaction data from the body of
    # each page.


@dataclass(frozen=True, slots=True)
class AccountType:
    """Simple lookup label for an account type category.

    Keyed in ``account_types.toml``; referenced from ``accounts.toml`` via
    ``Account.account_type_key``.
    """

    account_type: str
    # [ACTIVE] — Account type label (e.g. "CRD" for credit card, "CUR" for current
    # account).  Populated at load time but not subsequently consumed by the
    # pipeline; present for potential reporting or routing use.


@dataclass(frozen=True, slots=True)
class Company:
    """Configuration for a financial institution (bank/provider).

    Keyed in ``companies.toml``; linked into ``Account.company`` at load time.
    The optional ``config`` is used for statement-identification: a lightweight
    extraction pass run before full account matching to determine which bank issued
    the statement.
    """

    company: str
    # [ACTIVE] — Human-readable company name (e.g. "HSBC UK").  Used to populate
    # the STD_COMPANY standard field.

    config: Optional[Config]
    # [ACTIVE] — Extraction config used during the company-identification pass.
    # Extracts a discriminating field (e.g. a bank-specific header string) to
    # confirm the PDF belongs to this company before attempting account matching.

    accounts: Optional[dict]
    # [STUB] — Declared but never accessed by the pipeline after load.  Intended
    # as a lookup from account key to Account object but currently unused.


@dataclass(frozen=False, slots=True)
class Account:
    """Full runtime configuration for one bank account.

    Keyed in ``accounts.toml``.  At load time ``config.py`` resolves all ``*_key``
    references to their target objects.  This is the primary configuration object
    passed to the statement extraction pipeline.

    Example ``accounts.toml`` entry::

        [MY_ACCOUNT_KEY]
        account = "Current Account"
        company_key = "MY_BANK"
        account_type_key = "CUR"
        statement_type_key = "MY_BANK_CUR"
        exclude_last_n_pages = 1
        currency = "GBP"

        [MY_ACCOUNT_KEY.config]
        config = "Account Identification"
        locations = [{page_number = 1, top_left = [0, 0], bottom_right = [400, 80]}]
        field = {field = 'account_id_text', vital = true, type = "string",
                 string_pattern = 'Expected header text'}
    """

    account: str
    # [ACTIVE] — Human-readable account name (e.g. "Current Account").  Written to
    # the STD_ACCOUNT standard field in the output.

    company_key: str
    # [ACTIVE] — Key into companies.toml identifying the issuing bank.  Used to
    # build ID_ACCOUNT and to look up the Company object at load time.

    company: Optional[Company]
    # [ACTIVE] — Resolved at load time from company_key.  Provides the company name
    # and company-level identification config.

    account_type_key: str
    # [ACTIVE] — Key into account_types.toml (e.g. "CRD", "CUR", "SAV").  Used to
    # look up the AccountType object at load time.

    account_type: Optional[AccountType]
    # [STUB] — Resolved at load time from account_type_key.  The AccountType object
    # is populated but never subsequently read by any pipeline consumer.

    statement_type_key: str
    # [ACTIVE] — Key into statement_types.toml identifying the extraction layout for
    # this account's statements.  Used to look up the StatementType object at load
    # time.

    statement_type: Optional[StatementType]
    # [ACTIVE] — Resolved at load time from statement_type_key.  Provides the header
    # and lines ConfigGroups used during extraction.

    exclude_last_n_pages: int
    # [ACTIVE] — Number of trailing pages to skip when cloning per-page locations.
    # Set to 1 (or more) when the final page(s) contain terms & conditions or other
    # non-transaction content that would otherwise be passed to the extraction
    # pipeline.

    currency: str
    # [ACTIVE] — ISO 4217 currency code for all monetary fields on this account
    # (e.g. "GBP", "USD", "PHP").  Must be a key in ``currency_spec`` in
    # ``currency.py``; validated at config load time.  Used by the extraction
    # pipeline to resolve the CurrencySpec for fields of type "currency".

    config: Config
    # [ACTIVE] — Account-level identification config.  A lightweight extraction step
    # run to confirm a PDF belongs to this account before the full extraction pass.
    # Defined inline under ``[ACCOUNT_KEY.config]`` in accounts.toml.
