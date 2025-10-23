import time
from copy import deepcopy
from datetime import datetime
from uuid import uuid4

import polars as pl
import polars.selectors as cs
from pdfplumber.pdf import PDF

from bank_statement_parser.modules.classes.data_definitions import (
    Config,
    CurrencySpec,
    Field,
    Location,
    StatementTable,
    TransactionSpec,
)
from bank_statement_parser.modules.currency import currency_spec
from bank_statement_parser.modules.functions.pdf_functions import get_region, get_table_from_region

DEBUG = False


def spawn_locations(
    locations: list[Location], pdf: PDF, logs: pl.DataFrame, file_path: str, exclude_last_n_pages: int = 0
) -> list[Location]:
    """
    Generates a list of location objects, ensuring each location is associated with a page number in the statement.

    For each location in the input list:
    - If the location already has a page number, it is added to the result as-is.
    - If the location does not have a page number, it is duplicated for each page in the statement that does not already have a location assigned, and the page number is set accordingly.

    Args:
        locations (list): A list of location objects, each potentially with a 'page_number' attribute.
        statement (object): An object representing the statement, expected to have a 'pages' attribute (list).

    Returns:
        list: A list of location objects, each with an assigned page number.
    """
    start = time.time()
    location_page_numbers: list = []
    spawned_locations: list = []
    for location in locations:
        if location.page_number:
            location_page_numbers.append(location.page_number)
            spawned_locations.append(location)
    for location in locations:
        if not location.page_number:
            for page in range(len(pdf.pages) - exclude_last_n_pages):
                page_number = page + 1
                if page_number not in location_page_numbers:
                    spawned_location = deepcopy(location)
                    spawned_location.page_number = page_number
                    spawned_locations.append(spawned_location)

    log = pl.DataFrame(
        [[file_path, "statement_functions", "spawn_locations", time.time() - start, 1, datetime.now(), ""]],
        schema=logs.schema,
        orient="row",
    )
    logs.vstack(log, in_place=True)
    return spawned_locations


def strip(data: pl.LazyFrame, field: Field, logs: pl.DataFrame, file_path: str, spec: CurrencySpec | None = None) -> pl.LazyFrame:
    start = time.time()
    src = "raw"
    step = "strip"
    if DEBUG:
        print(f"{step}: {field.field}")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    data = data.with_columns(
        pl.col(f"value_{src}").fill_null("").alias(f"value_{step}"),
        pl.lit(False).alias(f"success_{step}"),
        pl.lit(f"{step} error").alias(f"error_{step}"),
    )
    if field.strip_characters_start:
        data = data.with_columns(
            pl.col(f"value_{step}").str.strip_chars_start(field.strip_characters_start).fill_null("").alias(f"value_{step}")
        )
    if field.strip_characters_end:
        data = data.with_columns(
            pl.col(f"value_{step}").str.strip_chars_end(field.strip_characters_end).fill_null("").alias(f"value_{step}")
        )

    if spec and field.type == "numeric":
        data = data.with_columns(
            pl.col(f"value_{step}")
            .str.replace_many(spec.symbols, [""])
            .str.replace_many(spec.seperators_thousands, [""])
            .str.replace_many([r"\s"], [""])
            .fill_null("")
            .alias(f"value_{step}")  # always remove any whitespace
        )

    data = data.with_columns(
        ((pl.col(f"value_{step}").is_not_null()) & (pl.col(f"value_{step}").str.len_bytes() > 0)).alias(f"success_{step}")
    ).with_columns(pl.when(pl.col(f"success_{step}")).then(None).otherwise(pl.col(f"error_{step}")).alias(f"error_{step}"))
    if DEBUG:
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())

    log = pl.DataFrame(
        [[file_path, "statement_functions", "strip", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return data


def build_pattern(
    string_pattern: str | None = None, spec_pattern: str | None = None, prefix: str | None = None, suffix: str | None = None
) -> str:
    pattern: str = r".+"
    if string_pattern is not None:
        pattern = string_pattern
        return pattern
    elif spec_pattern is not None:
        pattern = spec_pattern
        if prefix is None and suffix is None:
            return pattern
        if prefix is not None:
            pattern = str(pattern).replace("^", rf"^({prefix})?\s?")
        if suffix is not None:
            pattern = str(pattern).replace("$", rf"\s?({suffix})?$")
    return pattern


def patmatch(data: pl.LazyFrame, field: Field, logs: pl.DataFrame, file_path: str, spec: CurrencySpec | None = None) -> pl.LazyFrame:
    start = time.time()
    src = "strip"
    step = "pattern"
    pattern = build_pattern(
        string_pattern=field.string_pattern,
        spec_pattern=spec.pattern if spec else None,
        prefix=field.numeric_modifier.prefix if field.numeric_modifier else None,
        suffix=field.numeric_modifier.suffix if field.numeric_modifier else None,
    )
    if DEBUG:
        print(f"{step}: spec: {pattern}")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    data = data.with_columns(
        pl.lit("").alias(f"value_{step}"),
        pl.lit(False).alias(f"success_{step}"),
        pl.lit(f"{step} error").alias(f"error_{step}"),
    )
    data = data.with_columns(pl.col(f"value_{src}").str.extract(pattern, 0).fill_null("").alias(f"value_{step}"))
    data = data.with_columns(
        ((pl.col(f"value_{step}").is_not_null()) & (pl.col(f"value_{step}").str.len_bytes() > 0)).alias(f"success_{step}")
    ).with_columns(pl.when(pl.col(f"success_{step}")).then(None).otherwise(pl.col(f"error_{step}")).alias(f"error_{step}"))
    if DEBUG:
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    log = pl.DataFrame(
        [[file_path, "statement_functions", "patmatch", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return data


def cast(data: pl.LazyFrame, field: Field, logs: pl.DataFrame, file_path: str) -> pl.LazyFrame:
    start = time.time()
    src = "pattern"
    step = "cast"
    if DEBUG:
        print(f"{step}: {field.numeric_modifier}")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    data = data.with_columns(
        pl.lit("").alias(f"value_{step}"),
        pl.lit(False).alias(f"success_{step}"),
        pl.lit(f"{step} error").alias(f"error_{step}"),
    )
    if field.type == "numeric":
        if field.numeric_modifier:
            if field.numeric_modifier.prefix or field.numeric_modifier.suffix:
                if field.numeric_modifier.prefix:
                    data = data.with_columns(
                        pl.when(pl.col(f"value_{src}").str.starts_with(field.numeric_modifier.prefix))
                        .then(
                            pl.col(f"value_{src}")
                            .str.strip_chars_start(field.numeric_modifier.prefix)
                            .cast(float)
                            .mul(field.numeric_modifier.multiplier)
                        )
                        .otherwise(pl.col(f"value_{src}").cast(float, strict=False))
                        .alias(f"value_{step}")
                    )
                elif field.numeric_modifier.suffix:
                    data = data.with_columns(
                        pl.when(pl.col(f"value_{src}").str.ends_with(field.numeric_modifier.suffix))
                        .then(
                            pl.col(f"value_{src}")
                            .str.strip_chars_end(field.numeric_modifier.suffix)
                            .cast(float, strict=False)
                            .mul(field.numeric_modifier.multiplier)
                        )
                        .otherwise(pl.col(f"value_{src}").cast(float, strict=False))
                        .alias(f"value_{step}")
                    )
            else:
                data = data.with_columns(
                    pl.col(f"value_{src}").cast(float, strict=False).mul(field.numeric_modifier.multiplier).alias(f"value_{step}")
                )
            if field.numeric_modifier.exclude_negative_values:
                data = data.with_columns(pl.when(pl.col(f"value_{step}") < 0).then(pl.lit(0.00)).alias(f"value_{step}"))
            if field.numeric_modifier.exclude_positive_values:
                data = data.with_columns(pl.when(pl.col(f"value_{step}") > 0).then(pl.lit(0.00)).alias(f"value_{step}"))
        else:
            data = data.with_columns(pl.col(f"value_{src}").cast(float, strict=False).alias(f"value_{step}"))
    elif field.type == "string":
        data = data.with_columns(pl.col(f"value_{src}").cast(str, strict=False).alias(f"value_{step}"))

    # cast the value back as a string so we don't get mixed types in the column value_cast column
    data = data.with_columns(pl.col(f"value_{step}").cast(str).fill_null("").alias(f"value_{step}"))
    if field.type == "numeric":
        data = data.with_columns(pl.col(f"value_{step}").str.to_decimal(scale=4).cast(str).alias(f"value_{step}"))

    data = data.with_columns(
        ((pl.col(f"value_{step}").is_not_null()) & (pl.col(f"value_{step}").str.len_bytes() > 0)).alias(f"success_{step}")
    ).with_columns(pl.when(pl.col(f"success_{step}")).then(None).otherwise(pl.col(f"error_{step}")).alias(f"error_{step}"))
    if DEBUG:
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    log = pl.DataFrame(
        [[file_path, "statement_functions", "cast", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return data


def trim(data: pl.LazyFrame, field: Field, logs: pl.DataFrame, file_path: str) -> pl.LazyFrame:
    start = time.time()
    src = "cast"
    step = "trim"
    if DEBUG:
        print(f"{step}: {field.string_max_length}")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    data = data.with_columns(
        pl.lit("").alias(f"value_{step}"),
        pl.lit(False).alias(f"success_{step}"),
        pl.lit(f"{step} error").alias(f"error_{step}"),
    )
    string_max_length = field.string_max_length if field.string_max_length else 999
    if field.type == "string":
        data = data.with_columns(pl.col(f"value_{src}").str.head(string_max_length).fill_null("").alias(f"value_{step}"))
    else:
        data = data.with_columns(pl.col(f"value_{src}").fill_null("").alias(f"value_{step}"))
    data = data.with_columns(
        ((pl.col(f"value_{step}").is_not_null()) & (pl.col(f"value_{step}").str.len_bytes() > 0)).alias(f"success_{step}")
    ).with_columns(pl.when(pl.col(f"success_{step}")).then(None).otherwise(pl.col(f"error_{step}")).alias(f"error_{step}"))
    if DEBUG:
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    log = pl.DataFrame(
        [[file_path, "statement_functions", "trim", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return data


def validate(data: pl.LazyFrame, field: Field, logs: pl.DataFrame, file_path: str) -> pl.LazyFrame:
    start = time.time()
    src = "trim"
    if DEBUG:
        print("Validate")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())

    data = data.with_columns(
        value=pl.col(f"value_{src}"),
        success=pl.concat_list(cs.starts_with("success_")).list.all(),
        error=pl.concat_list(cs.starts_with("error_")).list.drop_nulls().list.first(),
    ).with_columns(hard_fail=~pl.col("success") & field.vital)
    if DEBUG:
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    log = pl.DataFrame(
        [[file_path, "statement_functions", "validate", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return data


def cleanup(data: pl.LazyFrame, logs: pl.DataFrame, file_path: str) -> pl.LazyFrame:
    # DEBUG = True
    start = time.time()
    if DEBUG:
        print("Validate")
        print("Before...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    # if not DEBUG:
    data = data.select("section", "location", "config", "row", "page", "field", "vital", "value", "success", "error", "hard_fail")
    # replace zero length values with None
    data = data.with_columns(
        value=pl.when(pl.col("value").str.len_bytes() == 0).then(pl.lit(None)).otherwise(pl.col("value")),
        # val_len=pl.col("value").str.len_bytes(),
    )
    log = pl.DataFrame(
        [[file_path, "statement_functions", "cleanup", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )

    if DEBUG:
        print("Validate")
        print("After...")
        with pl.Config(tbl_cols=-1, tbl_rows=-1):
            print(data.collect())
    logs.vstack(log, in_place=True)
    return data


def extract_fields(
    pdf: PDF,
    location: Location,
    statement_table: StatementTable | None,
    config_field: Field | None,
    config: str,
    section: str,
    location_id: int,
    logs: pl.DataFrame,
    file_path: str,
) -> pl.LazyFrame:
    start = time.time()
    results: pl.DataFrame = pl.DataFrame()
    result: pl.LazyFrame = pl.LazyFrame()
    region = get_region(location, pdf, logs, file_path)
    if not statement_table:
        if region and config_field:
            result = pl.LazyFrame(
                data=[
                    pl.Series("field", [config_field.field], dtype=pl.String),
                    pl.Series("vital", [config_field.vital], dtype=pl.Boolean),
                    pl.Series("value_raw", [region.extract_text()], dtype=pl.String),
                ]
            )
            result = result.select(
                pl.lit(section).alias("section"),
                pl.lit(location_id).alias("location"),
                pl.lit(config).alias("config"),
                pl.lit(0).alias("row"),
                pl.lit(location.page_number).alias("page"),
                pl.col("field"),
                pl.col("vital"),
                pl.col("value_raw"),
            )
            spec = None
            if config_field.type == "numeric":
                spec = currency_spec[config_field.numeric_currency]
            result = (
                result.pipe(strip, config_field, logs, file_path, spec)
                .pipe(patmatch, config_field, logs, file_path, spec)
                .pipe(cast, config_field, logs, file_path)
                .pipe(trim, config_field, logs, file_path)
                .pipe(validate, config_field, logs, file_path)
                .pipe(cleanup, logs, file_path)
            )
            try:
                results.vstack(result.collect(), in_place=True)
            except pl.exceptions.ColumnNotFoundError:
                log = pl.DataFrame(
                    [
                        [
                            file_path,
                            "statement_functions",
                            "extract_fields_exception",
                            time.time() - start,
                            1,
                            datetime.now(),
                            f"Column Not Found {config_field.field}",
                        ]
                    ],
                    schema=logs.schema,
                )
                logs.vstack(log, in_place=True)
                return results.lazy()

    else:  # if there is a statement table
        table = (
            get_table_from_region(
                region=region,
                table_rows=statement_table.table_rows,
                table_columns=statement_table.table_columns,
                row_spacing=statement_table.row_spacing,
                vertical_lines=location.vertical_lines,
                logs=logs,
                file_path=file_path,
                allow_text_failover=location.allow_text_failover,
                remove_header=statement_table.remove_header,
                header_text=statement_table.header_text,
            )
            if region
            else pl.LazyFrame()
        )
        table = table.collect().lazy()

        if not statement_table.transaction_spec:
            table_eager: pl.DataFrame = table.collect()
            for field in statement_table.fields:
                if field.cell is None:
                    continue
                result = pl.LazyFrame(
                    data=[
                        pl.Series("field", [field.field], dtype=pl.String),
                        pl.Series("vital", [field.vital], dtype=pl.Boolean),
                        pl.Series("value_raw", [table_eager.item(field.cell.row, field.cell.col)], dtype=pl.String),
                    ]
                )
                result = result.select(
                    pl.lit(section).alias("section"),
                    pl.lit(location_id).alias("location"),
                    pl.lit(config).alias("config"),
                    pl.lit(field.cell.row).alias("row"),
                    pl.lit(location.page_number).alias("page"),
                    pl.col("field"),
                    pl.col("vital"),
                    pl.col("value_raw"),
                )
                spec = None
                if field.type == "numeric":
                    spec = currency_spec[field.numeric_currency]
                result = (
                    result.pipe(strip, field, logs, file_path, spec)
                    .pipe(patmatch, field, logs, file_path, spec)
                    .pipe(cast, field, logs, file_path)
                    .pipe(trim, field, logs, file_path)
                    .pipe(validate, field, logs, file_path)
                    .pipe(cleanup, logs, file_path)
                )
                try:
                    results.vstack(result.collect(), in_place=True)
                except pl.exceptions.ColumnNotFoundError:
                    log = pl.DataFrame(
                        [
                            [
                                file_path,
                                "statement_functions",
                                "extract_fields_exception",
                                time.time() - start,
                                1,
                                datetime.now(),
                                f"Column Not Found {field.field}",
                            ]
                        ],
                        schema=logs.schema,
                    )
                    logs.vstack(log, in_place=True)
                    continue

        else:  # transaction records will be multi-line and have no row specification, but will have a column specification
            for field in statement_table.fields:
                if field.column is None:
                    continue
                result = table.select(
                    section=pl.lit(section),
                    location=pl.lit(location_id),
                    config=pl.lit(config),
                    page=pl.lit(location.page_number),
                    field=pl.lit(field.field),
                    vital=pl.lit(field.vital),
                    value_raw=pl.nth(field.column),
                ).with_row_index("row")
                spec = None
                if field.type == "numeric":
                    spec = currency_spec[field.numeric_currency]
                result = (
                    result.pipe(strip, field, logs, file_path, spec)
                    .pipe(patmatch, field, logs, file_path, spec)
                    .pipe(cast, field, logs, file_path)
                    .pipe(trim, field, logs, file_path)
                    .pipe(validate, field, logs, file_path)
                    .pipe(cleanup, logs, file_path)
                )
                # if field.field == "£_paid_out":
                #     with pl.Config(tbl_cols=-1, tbl_rows=-1):
                #         print(result.collect())
                try:
                    results.vstack(result.collect(), in_place=True)
                except pl.exceptions.ColumnNotFoundError:
                    log = pl.DataFrame(
                        [
                            [
                                file_path,
                                "statement_functions",
                                "extract_fields_exception",
                                time.time() - start,
                                1,
                                datetime.now(),
                                f"Column Not Found {field.field}",
                            ]
                        ],
                        schema=logs.schema,
                        orient="row",
                    )
                    logs.vstack(log, in_place=True)
                    continue
            # Transaction bookends
            bookends = statement_table.transaction_spec.transaction_bookends
            start_line = bookends.start_fields
            end_line = bookends.end_fields
            start_rows = (
                results.filter(pl.col("field").is_in(start_line))
                .group_by("row")
                .agg(pl.col("success").implode())
                .filter(pl.col("success").list.count_matches(True) >= bookends.min_non_empty_start)
                .select(pl.col("row"), transaction_start=pl.lit(True))
            )
            end_rows = (
                results.filter(pl.col("field").is_in(end_line))
                .group_by("row")
                .agg(pl.col("success").implode())
                .filter(pl.col("success").list.count_matches(True) >= bookends.min_non_empty_end)
                .select(pl.col("row"), transaction_end=pl.lit(True))
            )
            results = (
                results.join(start_rows, on="row", how="left", validate="m:1")
                .join(end_rows, on="row", how="left", validate="m:1")
                .with_columns(
                    transaction_start=pl.col("transaction_start").fill_null(False),
                    transaction_end=pl.col("transaction_end").fill_null(False),
                )
            )
            # with pl.Config(tbl_cols=-1, tbl_rows=-1):
            #     print(results)
    log = pl.DataFrame(
        [[file_path, "statement_functions", "extract_fields", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    )
    logs.vstack(log, in_place=True)
    return results.lazy()


def process_transactions(data: pl.DataFrame, transaction_spec: TransactionSpec, logs: pl.DataFrame, file_path: str) -> pl.DataFrame:
    start = time.time()
    data = (
        # data.filter(pl.col("success"))
        data.pivot(values="value", index=["page", "row", "transaction_start", "transaction_end"], on="field").sort("page", "row")
        # pivot the data
    )

    data = data.with_columns(transaction_number=pl.col("transaction_start").cum_sum()).filter(
        pl.col("transaction_number") > 0
    )  # number the transactions and remove rows before the 1st
    if fffs := transaction_spec.fill_forward_fields:  # fill forward if there are any fields in the spec
        for fff in fffs:
            data = data.with_columns(
                pl.col(fff).fill_null(strategy="forward").alias(fff),
            )
    if mfs := transaction_spec.merge_fields:
        for mf in mfs.fields:
            data = data.with_columns(
                pl.col(mf).str.join(delimiter=mfs.separator).over("transaction_number"),
            )
    data = data.filter(pl.col("transaction_end")).drop("transaction_start", "transaction_end")

    log = pl.DataFrame(
        [[file_path, "statement_functions", "process_transactions", time.time() - start, 1, datetime.now(), ""]],
        schema=logs.schema,
        orient="row",
    )
    logs.vstack(log, in_place=True)
    return data


def get_results(
    pdf: PDF, section: str, config: Config, logs: pl.DataFrame, file_path: str, scope: str = "success", exclude_last_n_pages: int = 0
) -> pl.DataFrame:  # scope can be all, success, fail, or hard_fail
    start = time.time()
    result: pl.LazyFrame = pl.LazyFrame()
    results: pl.DataFrame = pl.DataFrame()
    locations = config.statement_table.locations if config.statement_table else config.locations
    if locations:
        spawned_locations = spawn_locations(locations, pdf=pdf, logs=logs, file_path=file_path, exclude_last_n_pages=exclude_last_n_pages)
        for i, location in enumerate(spawned_locations):
            result = extract_fields(
                pdf,
                location,
                config.statement_table,
                config.field,
                config.config,
                section=section,
                location_id=i,
                logs=logs,
                file_path=file_path,
            )
            results.vstack(result.collect(), in_place=True)

    if statement_table := config.statement_table:
        # process transactions if there's a transaction spec
        if spec := statement_table.transaction_spec:
            results = results.pipe(process_transactions, logs=logs, file_path=file_path, transaction_spec=spec)
            log = pl.DataFrame(
                [[file_path, "statement_functions", "get_results_stmt_transaction", time.time() - start, 1, datetime.now(), ""]],
                schema=logs.schema,
                orient="row",
            )
            logs.vstack(log, in_place=True)
            return results

    if scope == "all":
        log = pl.DataFrame(
            [[file_path, "statement_functions", "get_results_all", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        )
        logs.vstack(log, in_place=True)
        return results
    elif scope == "success":
        log = pl.DataFrame(
            [[file_path, "statement_functions", "get_results_success", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        )
        logs.vstack(log, in_place=True)
        return results.filter(pl.col("success"))
    elif scope == "fail":
        log = pl.DataFrame(
            [[file_path, "statement_functions", "get_results_fail", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        )
        logs.vstack(log, in_place=True)
        return results.filter(~pl.col("success"))
    elif scope == "hard_fail":
        log = pl.DataFrame(
            [[file_path, "statement_functions", "get_results_hard_fail", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        )
        logs.vstack(log, in_place=True)
        return results.filter(pl.col("hard_fail"))
    else:
        log = pl.DataFrame(
            [[file_path, "statement_functions", "get_results_default", time.time() - start, 1, datetime.now(), ""]],
            schema=logs.schema,
            orient="row",
        )
        logs.vstack(log, in_place=True)
        return results


def get_standard_fields(
    data: pl.DataFrame,
    section: str,
    config_standard_fields: dict,
    statement_type: str,
    checks_and_balances: pl.DataFrame,
    logs: pl.DataFrame,
    file_path: str,
) -> pl.DataFrame:
    start = time.time()
    for std_field, std_config in config_standard_fields.items():
        if std_config.section == section:
            ref = [ref for ref in std_config.std_refs if ref.statement_type == statement_type][0]
            if ref:
                data = data.with_columns(pl.col(ref.field).alias(std_field))
                if std_config.type == "numeric":
                    # if we have credits and debits in the same column we might need to exclude positive or negative values
                    data = data.with_columns(
                        pl.when(ref.exclude_negative_values & (pl.col(std_field).cast(float) < 0.0000))
                        .then(pl.lit(0.0000))
                        .otherwise(pl.col(std_field))
                        .alias(std_field)
                    ).with_columns(
                        pl.when(ref.exclude_positive_values & (pl.col(std_field).cast(float) > 0.0000))
                        .then(pl.lit(0.0000))
                        .otherwise(pl.col(std_field))
                        .alias(std_field)
                    )
                    data = data.with_columns(
                        pl.col(std_field)
                        .fill_null(0.0000)
                        .cast(float)
                        .mul(ref.multiplier)
                        .cast(str)
                        .str.to_decimal(scale=4)
                        .alias(std_field)
                    )
                elif std_config.type == "date" and ref.format:
                    try:
                        data = data.with_columns(pl.col(std_field).str.to_date(format=ref.format))
                    except pl.exceptions.InvalidOperationError:  # failure to cast as a date
                        try:  # try removing the middle bit if the statement date is something like '9 July to 8 August 2025'
                            data = data.with_columns(
                                pl.col(std_field).str.split(by=" ").list.slice(-3).list.join(separator=" ").str.to_date(format=ref.format)
                            )
                        # except pl.exceptions.InvalidOperationError:  # still failed?
                        #     try:  # try removing the middle bit if the statement date is something like '9 December 2024 to 8 January 2025'
                        #         data = data.with_columns(
                        #             pl.col(std_field)
                        #             .str.split(by=" ")
                        #             .list.gather([0, 1, 6])
                        #             .list.join(separator=" ")
                        #             .str.to_date(format=ref.format)
                        #         )
                        except pl.exceptions.InvalidOperationError:  # still failed?
                            continue  # give up and return the date as it is
    if section == "pages":
        data = data.with_columns(STD_PAGE_NUMBER=pl.col("page"))
    if section == "lines":
        data = data.with_columns(STD_TRANSACTION_NUMBER=pl.col("transaction_number"), STD_PAGE_NUMBER=pl.col("page"))
        data = data.join(checks_and_balances.select(pl.col("STD_OPENING_BALANCE").alias("STD_RUNNING_BALANCE")), how="cross")
        data = data.with_columns(STD_MOVEMENT=pl.col("STD_PAYMENT_IN").sub("STD_PAYMENT_OUT")).with_columns(
            STD_RUNNING_BALANCE=pl.col("STD_RUNNING_BALANCE").add(pl.col("STD_MOVEMENT").cum_sum())
        )
    # Checks & Balances updates
    if section == "header":
        checks_and_balances.hstack(
            data.select("STD_CLOSING_BALANCE", "STD_OPENING_BALANCE", "STD_PAYMENTS_IN", "STD_PAYMENTS_OUT"), in_place=True
        )
        new_columns = checks_and_balances.with_columns(
            STD_STATEMENT_MOVEMENT=pl.col("STD_CLOSING_BALANCE").sub("STD_OPENING_BALANCE"),
            STD_BALANCE_OF_PAYMENTS=pl.col("STD_PAYMENTS_IN").sub("STD_PAYMENTS_OUT"),
        ).select(
            "STD_STATEMENT_MOVEMENT",
            "STD_BALANCE_OF_PAYMENTS",
        )
        checks_and_balances.hstack(new_columns, in_place=True)
    if section == "lines":
        checks_and_balances.hstack(data.select("STD_PAYMENT_IN", "STD_PAYMENT_OUT", "STD_MOVEMENT").sum(), in_place=True)
        checks_and_balances.hstack(data.select(pl.last("STD_RUNNING_BALANCE")), in_place=True)
    log = pl.DataFrame(
        [[file_path, "statement_functions", "get_standard_fields", time.time() - start, 1, datetime.now(), ""]],
        schema=logs.schema,
        orient="row",
    )
    logs.vstack(log, in_place=True)
    # add a GUID to each record
    data = data.with_columns(STD_GUID=pl.lit(f"{uuid4()}"))
    return data
