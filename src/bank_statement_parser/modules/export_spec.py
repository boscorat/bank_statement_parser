"""
Export spec engine — load TOML spec files and produce filtered, formatted exports.

A spec file declares the source table, column mapping, date/number formatting,
string sanitisation, and output options (format, split mode, polarity).  The
:func:`export_spec` function is the single public entry point.

Functions:
    export_spec: Load a TOML spec and write filtered, formatted export file(s).

Private helpers:
    _load_spec: Parse and validate a TOML spec file into an :class:`ExportSpec`.
    _build_frame: Query the database and apply account/date/statement filters.
    _apply_column_mapping: Rename, compute, and select output columns.
    _apply_date_format: Format date columns as strings per the spec.
    _sanitise_strings: Silently strip forbidden characters from string columns.
    _write_frames: Write one or more DataFrames to CSV or XLSX.
"""

import re
import sqlite3
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from xlsxwriter import Workbook

from bank_statement_parser.modules.errors import ConfigError, ProjectDatabaseMissing
from bank_statement_parser.modules.paths import ProjectPaths

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Supported computed column tokens
_COMPUTED_SIGNED_AMOUNT = "computed:signed_amount"

# All recognised computed tokens — used for validation
_KNOWN_COMPUTED: frozenset[str] = frozenset({_COMPUTED_SIGNED_AMOUNT})

# Columns in FlatTransaction that contain dates (need format conversion)
_DATE_COLUMNS: frozenset[str] = frozenset({"transaction_date", "statement_date"})

# Allowed source tables / views
_ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        "FlatTransaction",
        "FactTransaction",
        "FactBalance",
        "DimStatement",
        "DimAccount",
        "DimTime",
        "GapReport",
    }
)


# ---------------------------------------------------------------------------
# ExportSpec dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExportSpec:
    """Immutable representation of an export spec loaded from a TOML file.

    Attributes:
        description: Human-readable description of the spec.
        source_table: DB table or view to query (e.g. ``"FlatTransaction"``).
        format: Output format — ``"csv"`` or ``"xlsx"``.
        split_by_statement: When ``True``, produce one file per ``id_statement``.
        columns: Ordered mapping of export column name → source column name or
            computed token (e.g. ``"computed:signed_amount"``).
        date_format: :func:`strftime` format string for date columns
            (e.g. ``"%d/%m/%Y"``).
        float_precision: Decimal places for numeric output columns.
        invert_polarity: When ``True``, negate all computed monetary values.
        blank_zeros: When ``True``, replace ``0.0`` with ``null`` in numeric
            output columns before writing.
        strip_chars: Characters silently removed from every string field.
    """

    description: str
    source_table: str
    format: str
    split_by_statement: bool
    columns: dict[str, str]
    date_format: str
    float_precision: int
    invert_polarity: bool
    blank_zeros: bool
    strip_chars: str


# ---------------------------------------------------------------------------
# _load_spec
# ---------------------------------------------------------------------------


def _load_spec(spec_path: Path) -> ExportSpec:
    """Parse and validate a TOML spec file, returning an :class:`ExportSpec`.

    Args:
        spec_path: Path to the ``.toml`` spec file.

    Returns:
        A validated :class:`ExportSpec` instance.

    Raises:
        ConfigError: If the file cannot be read, is missing required keys,
            or contains invalid values.
    """
    if not spec_path.exists():
        raise ConfigError(f"Export spec file not found: {spec_path}")

    try:
        with open(spec_path, "rb") as fh:
            raw = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Export spec is not valid TOML ({spec_path}): {exc}") from exc

    # ---- required sections ----
    for section in ("meta", "export", "columns", "format", "sanitise"):
        if section not in raw:
            raise ConfigError(f"Export spec missing required section [{section}]: {spec_path}")

    meta = raw["meta"]
    export = raw["export"]
    columns = raw["columns"]
    fmt = raw["format"]
    sanitise = raw["sanitise"]

    # ---- [meta] ----
    description: str = _require_str(meta, "description", spec_path)
    source_table: str = _require_str(meta, "source_table", spec_path)
    if source_table not in _ALLOWED_TABLES:
        raise ConfigError(
            f"Export spec source_table {source_table!r} is not a recognised table/view (allowed: {sorted(_ALLOWED_TABLES)}): {spec_path}"
        )

    # ---- [export] ----
    fmt_value: str = _require_str(export, "format", spec_path)
    if fmt_value not in ("csv", "xlsx"):
        raise ConfigError(f"Export spec format must be 'csv' or 'xlsx', got {fmt_value!r}: {spec_path}")
    split_by_statement: bool = _require_bool(export, "split_by_statement", spec_path)

    # ---- [columns] ----
    if not isinstance(columns, dict) or not columns:
        raise ConfigError(f"Export spec [columns] must be a non-empty table: {spec_path}")
    for col_name, source in columns.items():
        if not isinstance(source, str):
            raise ConfigError(f"Export spec [columns] value for {col_name!r} must be a string: {spec_path}")
        if source.startswith("computed:") and source not in _KNOWN_COMPUTED:
            raise ConfigError(
                f"Export spec unknown computed token {source!r} for column {col_name!r} (known: {sorted(_KNOWN_COMPUTED)}): {spec_path}"
            )

    # ---- [format] ----
    date_format: str = _require_str(fmt, "date_format", spec_path)
    float_precision: int = _require_int(fmt, "float_precision", spec_path)
    invert_polarity: bool = _require_bool(fmt, "invert_polarity", spec_path)
    blank_zeros: bool = _require_bool(fmt, "blank_zeros", spec_path)

    # ---- [sanitise] ----
    strip_chars: str = _require_str(sanitise, "strip_chars", spec_path)

    return ExportSpec(
        description=description,
        source_table=source_table,
        format=fmt_value,
        split_by_statement=split_by_statement,
        columns=dict(columns),
        date_format=date_format,
        float_precision=float_precision,
        invert_polarity=invert_polarity,
        blank_zeros=blank_zeros,
        strip_chars=strip_chars,
    )


# ---------------------------------------------------------------------------
# _load_spec helpers
# ---------------------------------------------------------------------------


def _require_str(section: dict, key: str, spec_path: Path) -> str:
    """Return a string value from *section[key]*, raising ConfigError if absent or wrong type."""
    if key not in section:
        raise ConfigError(f"Export spec missing required key {key!r}: {spec_path}")
    value = section[key]
    if not isinstance(value, str):
        raise ConfigError(f"Export spec key {key!r} must be a string, got {type(value).__name__!r}: {spec_path}")
    return value


def _require_bool(section: dict, key: str, spec_path: Path) -> bool:
    """Return a bool value from *section[key]*, raising ConfigError if absent or wrong type."""
    if key not in section:
        raise ConfigError(f"Export spec missing required key {key!r}: {spec_path}")
    value = section[key]
    if not isinstance(value, bool):
        raise ConfigError(f"Export spec key {key!r} must be a boolean, got {type(value).__name__!r}: {spec_path}")
    return value


def _require_int(section: dict, key: str, spec_path: Path) -> int:
    """Return an int value from *section[key]*, raising ConfigError if absent or wrong type."""
    if key not in section:
        raise ConfigError(f"Export spec missing required key {key!r}: {spec_path}")
    value = section[key]
    # bool is a subclass of int in Python — reject it explicitly
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Export spec key {key!r} must be an integer, got {type(value).__name__!r}: {spec_path}")
    return value


# ---------------------------------------------------------------------------
# _build_frame
# ---------------------------------------------------------------------------

# Columns needed beyond those declared in a spec when split_by_statement is True.
# We always fetch id_statement so we can partition, then drop it from output.
_SPLIT_ANCHOR = "id_statement"


def _build_frame(
    db_path: Path,
    spec: ExportSpec,
    account_key: str,
    date_from: date | None,
    date_to: date | None,
    statement_key: str | None,
) -> pl.LazyFrame:
    """Query the database, apply filters, and return a raw :class:`pl.LazyFrame`.

    Filters are applied as parameterised SQL (no user-value interpolation).
    The returned frame contains all columns from *source_table* plus, when
    ``split_by_statement`` is ``True`` and the source is ``FlatTransaction``,
    an ``id_statement`` column joined from ``FactTransaction``.

    Args:
        db_path: Path to the SQLite project database.
        spec: The loaded :class:`ExportSpec`.
        account_key: Value to match against ``DimAccount.id_account``.
        date_from: Optional earliest transaction date (inclusive).
        date_to: Optional latest transaction date (inclusive).
        statement_key: Optional ``id_statement`` value to restrict rows.

    Returns:
        A :class:`pl.LazyFrame` of the filtered rows.
    """
    params: list = [account_key]
    where_clauses: list[str] = []

    # FlatTransaction is a view over FactTransaction + DimStatement + DimAccount.
    # Re-implement the same joins against the base tables so we can filter on
    # da.id_account without re-joining through the view (which would risk fan-out
    # if transaction_number is non-unique across accounts).
    if spec.source_table == "FlatTransaction":
        id_statement_col = ", ft2.id_statement" if spec.split_by_statement else ""
        base_query = (
            f"SELECT ft2.id_date AS transaction_date, ds.statement_date, ds.filename,"
            f" da.company, da.account_type, da.account_number, da.sortcode, da.account_holder,"
            f" ft2.transaction_number, ft2.transaction_credit_or_debit AS CD,"
            f" ft2.transaction_type AS type, ft2.transaction_desc,"
            f" SUBSTR(ft2.transaction_desc, 1, 25) AS short_desc,"
            f" ft2.value_in, ft2.value_out, ft2.value{id_statement_col}"
            f" FROM FactTransaction ft2"
            f" INNER JOIN DimStatement ds ON ft2.statement_id = ds.statement_id"
            f" INNER JOIN DimAccount da ON ft2.account_id = da.account_id"
        )
        where_clauses.append("da.id_account = ?")
    else:
        # For other tables that already carry id_account directly
        base_query = f"SELECT * FROM {spec.source_table}"  # noqa: S608
        where_clauses.append("id_account = ?")

    if date_from is not None:
        where_clauses.append("transaction_date >= ?")
        params.append(date_from.isoformat())
    if date_to is not None:
        where_clauses.append("transaction_date <= ?")
        params.append(date_to.isoformat())
    if statement_key is not None:
        if spec.source_table == "FlatTransaction":
            where_clauses.append("ft2.id_statement = ?")
        else:
            where_clauses.append("id_statement = ?")
        params.append(statement_key)

    query = base_query
    if where_clauses:
        query = f"{base_query} WHERE {' AND '.join(where_clauses)}"

    with sqlite3.connect(db_path) as conn:
        return pl.read_database(
            query,
            connection=conn,
            execute_options={"parameters": params},
            infer_schema_length=None,
        ).lazy()


# ---------------------------------------------------------------------------
# _apply_column_mapping
# ---------------------------------------------------------------------------


def _apply_column_mapping(df: pl.LazyFrame, spec: ExportSpec) -> pl.LazyFrame:
    """Rename columns, compute derived values, and select only output columns.

    Handles the ``computed:signed_amount`` token: produces ``value_in -
    value_out``, optionally negated when ``spec.invert_polarity`` is ``True``.

    Args:
        df: Input :class:`pl.LazyFrame` from the database query.
        spec: The loaded :class:`ExportSpec`.

    Returns:
        A :class:`pl.LazyFrame` containing only the mapped output columns,
        in the order declared in the spec.
    """
    expressions: list[pl.Expr] = []

    for export_name, source in spec.columns.items():
        if source == _COMPUTED_SIGNED_AMOUNT:
            amount_expr = pl.col("value_in") - pl.col("value_out")
            if spec.invert_polarity:
                amount_expr = -amount_expr
            expressions.append(amount_expr.alias(export_name))
        else:
            expressions.append(pl.col(source).alias(export_name))

    return df.select(expressions)


# ---------------------------------------------------------------------------
# _apply_date_format
# ---------------------------------------------------------------------------


def _apply_date_format(df: pl.LazyFrame, spec: ExportSpec) -> pl.LazyFrame:
    """Cast and format date columns as strings using ``spec.date_format``.

    Any output column whose *source* in the spec maps to a known date column
    (``transaction_date``, ``statement_date``) is cast to :class:`pl.Date`
    and then formatted as a string.

    Args:
        df: :class:`pl.LazyFrame` after column mapping has been applied.
        spec: The loaded :class:`ExportSpec`.

    Returns:
        The :class:`pl.LazyFrame` with date columns replaced by formatted strings.
    """
    # Identify output columns whose source was a date field
    date_output_cols: list[str] = [export_name for export_name, source in spec.columns.items() if source in _DATE_COLUMNS]
    if not date_output_cols:
        return df
    return df.with_columns([pl.col(col).cast(pl.Date).dt.strftime(spec.date_format).alias(col) for col in date_output_cols])


# ---------------------------------------------------------------------------
# _apply_blank_zeros
# ---------------------------------------------------------------------------


def _apply_blank_zeros(df: pl.LazyFrame, spec: ExportSpec) -> pl.LazyFrame:
    """Replace ``0.0`` with ``null`` in numeric output columns when ``spec.blank_zeros`` is set.

    Only columns with a numeric Polars dtype (Int* or Float*) are modified;
    string and date columns are left untouched.

    Args:
        df: :class:`pl.LazyFrame` after column mapping and date formatting.
        spec: The loaded :class:`ExportSpec`.

    Returns:
        The :class:`pl.LazyFrame` with zero values replaced by ``null`` in
        numeric columns.
    """
    if not spec.blank_zeros:
        return df
    # Inspect the actual schema so we only touch numeric-typed columns.
    # collect_schema() is a lazy, zero-cost schema inspection.
    schema = df.collect_schema()
    _numeric_types = (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    )
    exprs: list[pl.Expr] = []
    for col_name, dtype in schema.items():
        if isinstance(dtype, _numeric_types):
            exprs.append(pl.when(pl.col(col_name) == 0).then(None).otherwise(pl.col(col_name)).alias(col_name))
    if not exprs:
        return df
    return df.with_columns(exprs)


# ---------------------------------------------------------------------------
# _sanitise_strings
# ---------------------------------------------------------------------------


def _sanitise_strings(df: pl.LazyFrame, spec: ExportSpec) -> pl.LazyFrame:
    """Silently strip forbidden characters from all string columns.

    The characters in ``spec.strip_chars`` are compiled into a single regex
    character class and replaced with empty strings.

    Args:
        df: :class:`pl.LazyFrame` after all transformations.
        spec: The loaded :class:`ExportSpec`.

    Returns:
        The :class:`pl.LazyFrame` with special characters removed from string
        columns.
    """
    if not spec.strip_chars:
        return df
    # Build a regex character class from the raw strip_chars string.
    # re.escape each character individually so regex metacharacters are safe.
    escaped = "".join(re.escape(c) for c in spec.strip_chars)
    pattern = f"[{escaped}]"

    # Identify string-typed output columns (sources that are not numeric/date)
    # We do this by checking known non-string sources.
    _numeric_sources: frozenset[str] = frozenset(
        {
            "value_in",
            "value_out",
            "value",
            "opening_balance",
            "closing_balance",
            "movement",
            _COMPUTED_SIGNED_AMOUNT,
        }
    )
    _date_sources: frozenset[str] = _DATE_COLUMNS

    string_cols: list[str] = [
        export_name
        for export_name, source in spec.columns.items()
        if source not in _numeric_sources and source not in _date_sources and not source.startswith("computed:")
    ]
    if not string_cols:
        return df
    return df.with_columns([pl.col(col).cast(pl.String).str.replace_all(pattern, "").alias(col) for col in string_cols])


# ---------------------------------------------------------------------------
# _write_frames
# ---------------------------------------------------------------------------


def _write_frames(
    frames: list[tuple[str, pl.DataFrame]],
    output_dir: Path,
    spec: ExportSpec,
) -> list[Path]:
    """Write a list of ``(filename_stem, DataFrame)`` pairs to *output_dir*.

    Args:
        frames: List of ``(stem, DataFrame)`` pairs to write.
        output_dir: Directory to write output files into.
        spec: The loaded :class:`ExportSpec` (used for format and precision).

    Returns:
        List of :class:`Path` objects for every file written.
    """
    written: list[Path] = []

    if spec.format == "xlsx":
        for stem, df in frames:
            out_path = output_dir / f"{stem}.xlsx"
            with Workbook(str(out_path)) as wb:
                df.write_excel(
                    workbook=wb,
                    worksheet=stem[:31],  # Excel worksheet name limit
                    autofit=False,
                    table_name=re.sub(r"[^A-Za-z0-9_]", "_", stem),
                    table_style="Table Style Medium 4",
                    float_precision=spec.float_precision,
                )
            written.append(out_path)
    else:
        for stem, df in frames:
            out_path = output_dir / f"{stem}.csv"
            df.write_csv(
                file=out_path,
                separator=",",
                include_header=True,
                quote_style="non_numeric",
                float_precision=spec.float_precision,
                null_value="",
            )
            written.append(out_path)

    return written


# ---------------------------------------------------------------------------
# export_spec — public entry point
# ---------------------------------------------------------------------------


def export_spec(
    spec: Path,
    *,
    account_key: str,
    project_path: Path | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    statement_key: str | None = None,
    split_by_statement: bool | None = None,
    format: str | None = None,
    invert_polarity: bool | None = None,
) -> list[Path]:
    """Load a TOML export spec and write filtered, formatted export file(s).

    The spec file declares the source table, column mapping, date/number
    formatting, string sanitisation, and default output options.  Runtime
    arguments override the corresponding spec defaults when provided.

    Output is written to ``export/<spec_stem>/`` inside the project directory,
    created automatically if absent.  Files are named ``<account_key>.csv``
    (or ``.xlsx``) for single-file exports, or
    ``<account_key>_<id_statement>.csv`` when splitting by statement.

    Args:
        spec: Path to the ``.toml`` export spec file.
        account_key: ``DimAccount.id_account`` value to filter transactions by.
        project_path: Optional project root directory.  Falls back to the
            bundled default project when ``None``.
        date_from: Optional earliest transaction date (inclusive).
        date_to: Optional latest transaction date (inclusive).
        statement_key: Optional ``id_statement`` value; restricts output to a
            single statement's transactions.
        split_by_statement: Override the spec's ``split_by_statement`` flag.
            When ``True``, one file is produced per ``id_statement``.
        format: Override the spec's output format — ``"csv"`` or ``"xlsx"``.
        invert_polarity: Override the spec's ``invert_polarity`` flag.  When
            ``True``, the sign of all computed monetary values is negated.

    Returns:
        A list of :class:`Path` objects for every file written.

    Raises:
        ConfigError: If the spec file is missing, malformed, or contains
            invalid values.
        ProjectDatabaseMissing: If ``database/project.db`` does not exist.
    """
    loaded = _load_spec(spec)

    # Apply runtime overrides — rebuild with replaced fields via a plain dict
    # (ExportSpec is frozen, so we cannot mutate in place).
    overrides: dict = {}
    if split_by_statement is not None:
        overrides["split_by_statement"] = split_by_statement
    if format is not None:
        if format not in ("csv", "xlsx"):
            raise ConfigError(f"export_spec format override must be 'csv' or 'xlsx', got {format!r}")
        overrides["format"] = format
    if invert_polarity is not None:
        overrides["invert_polarity"] = invert_polarity

    if overrides:
        loaded = ExportSpec(
            description=loaded.description,
            source_table=loaded.source_table,
            format=overrides.get("format", loaded.format),
            split_by_statement=overrides.get("split_by_statement", loaded.split_by_statement),
            columns=loaded.columns,
            date_format=loaded.date_format,
            float_precision=loaded.float_precision,
            invert_polarity=overrides.get("invert_polarity", loaded.invert_polarity),
            blank_zeros=loaded.blank_zeros,
            strip_chars=loaded.strip_chars,
        )

    paths = ProjectPaths.resolve(project_path)
    if not paths.project_db.exists():
        raise ProjectDatabaseMissing(paths.project_db)

    # Query and transform
    lf = _build_frame(paths.project_db, loaded, account_key, date_from, date_to, statement_key)
    lf = _apply_column_mapping(lf, loaded)
    lf = _apply_date_format(lf, loaded)
    lf = _apply_blank_zeros(lf, loaded)
    lf = _sanitise_strings(lf, loaded)

    output_dir = paths.export_specs_output(spec.stem)
    paths.ensure_subdir_for_write(output_dir)

    if not loaded.split_by_statement:
        df = lf.collect()
        stem = account_key
        written = _write_frames([(stem, df)], output_dir, loaded)
    else:
        # Partition by id_statement — the column was added by _build_frame and
        # removed by _apply_column_mapping (it is not in spec.columns).
        # We need to re-fetch with split anchor before mapping strips it.
        lf_raw = _build_frame(paths.project_db, loaded, account_key, date_from, date_to, statement_key)
        df_raw = lf_raw.collect()

        # Determine unique statement keys
        if _SPLIT_ANCHOR not in df_raw.columns:
            # Fallback: write as single file if split column unavailable
            df_mapped = lf.collect()
            written = _write_frames([(account_key, df_mapped)], output_dir, loaded)
        else:
            statement_ids = df_raw[_SPLIT_ANCHOR].unique().sort().to_list()
            frames: list[tuple[str, pl.DataFrame]] = []
            for sid in statement_ids:
                partition = df_raw.filter(pl.col(_SPLIT_ANCHOR) == sid).lazy()
                partition = _apply_column_mapping(partition, loaded)
                partition = _apply_date_format(partition, loaded)
                partition = _apply_blank_zeros(partition, loaded)
                partition = _sanitise_strings(partition, loaded)
                stem = f"{account_key}_{sid}"
                frames.append((stem, partition.collect()))
            written = _write_frames(frames, output_dir, loaded)

    return written
