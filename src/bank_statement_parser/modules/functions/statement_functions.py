import re
from collections import namedtuple
from copy import deepcopy

import polars as pl

from bank_statement_parser.modules.classes.data_definitions import StatementBookend, StatementTable, TransactionMods
from bank_statement_parser.modules.classes.errors import StatementError
from bank_statement_parser.modules.functions.pdf_functions import page_crop, region_table

FieldResult = namedtuple("FieldResult", "config page_number field type field_value value success vital exception table_row id_row")
Result = namedtuple("Result", "file company account config fields tests")
ConfigResults = namedtuple("ConfigResults", "results_full results_clean results_transactions")


def spawn_locations(locations, statement):
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
    location_page_numbers: list = []
    spawned_locations: list = []
    for location in locations:
        if location.page_number:
            location_page_numbers.append(location.page_number)
            spawned_locations.append(location)
    for location in locations:
        if not location.page_number:
            for page in range(len(statement.pages)):
                page_number = page + 1
                if page_number not in location_page_numbers:
                    spawned_location = deepcopy(location)
                    spawned_location.page_number = page_number
                    spawned_locations.append(spawned_location)
    return spawned_locations


def field_strip(value: str, chars: list | None) -> str:
    """
    Removes all occurrences of specified characters from the input string.

    Args:
        value (str): The string to process.
        chars (list | None): A list of characters to remove from the string. If None or empty, the string is returned unchanged.

    Returns:
        str: The processed string with specified characters removed.
    """
    if chars:
        for char in chars:
            value = value.replace(char, "")
    return value


def field_validate(value: str | None, pattern: str) -> bool:
    """
    Validates whether the given value matches the specified regular expression pattern.

    Args:
        value (str | None): The string value to validate. If None, validation fails.
        pattern (str): The regular expression pattern to match against the value.

    Returns:
        bool: True if the value matches the pattern, False otherwise.
    """
    success = False
    if value:
        if re.search(pattern, value):
            success = True
    return success


def extract_text_field(statement, location, strip, pattern) -> tuple[bool, str, str]:
    """
    Extracts a text field from a specified region in a bank statement page.

    Args:
        statement: The bank statement object containing pages.
        location: An object specifying the page number and region coordinates (top_left, bottom_right).
        strip: Characters to strip from the extracted text, or None.
        pattern: A validation pattern (e.g., regex) to check the extracted text against.

    Returns:
        tuple[bool, str, str]: A tuple containing:
            - success (bool): True if extraction and validation succeeded, False otherwise.
            - field_value (str): The extracted and processed text value, or empty string if failed.
            - exception (str): An error message if extraction or validation failed, otherwise empty string.
    """
    success: bool = False
    exception: str = ""
    field_value: str = ""
    region = None
    if location.page_number:
        try:
            region = page_crop(statement.pages[location.page_number - 1], location.top_left, location.bottom_right)
        except IndexError:
            exception = "Page doesn't exist in Statement"
    else:
        exception = "No page number specified for the location"
    if region and not exception:
        field_value = region.extract_text()
    else:
        exception = "Failed to extract region"
    if field_value and not exception:
        field_value = field_strip(field_value, strip) if strip else field_value  # strip any specified characters
        if field_validate(field_value, pattern):
            success = True
        else:
            exception = "Failed to validate"

    return (success, field_value, exception)


def extract_table_fields(statement, location, statement_table) -> list[tuple]:
    """
    Extracts and validates field values from a specified table region within a bank statement.

    This function attempts to locate a table region on a given page of the statement using the provided
    location coordinates. It then extracts values for each field defined in the statement_table, applies
    optional stripping and validation, and returns a list of tuples containing the field, its extracted value,
    a success flag, and an exception message if applicable.

    Args:
        statement: The bank statement object containing pages and data.
        location: An object specifying the page number and coordinates (top_left, bottom_right) for the table region.
        statement_table: An object defining the table structure, including rows, columns, spacing, and fields to extract.

    Returns:
        list[tuple]: A list of tuples for each field in the table, where each tuple contains:
            - field: The field definition object.
            - field_value: The extracted and processed value (or None if extraction/validation failed).
            - success: A boolean indicating if extraction and validation were successful.
            - exception: A string describing any error or validation failure encountered.
            - row: The row index of the field in the table (if applicable).
    """
    success: bool = False
    exception: str = ""
    field_value: str | None = ""
    field_values: list[tuple] = []
    region = None
    table = None
    if location.page_number:
        try:
            region = page_crop(statement.pages[location.page_number - 1], location.top_left, location.bottom_right)
        except IndexError:
            exception = "Page doesn't exist in Statement"
        except ValueError:
            exception = "Specified location outside of the page boundaries"
    else:
        exception = "No page number specified for the location"
    if region and not exception:
        table = region_table(
            region=region,
            table_rows=statement_table.table_rows,
            table_columns=statement_table.table_columns,
            row_spacing=statement_table.row_spacing,
            vertical_lines=location.vertical_lines,
        )
    else:
        if not exception:
            exception = "Failed to extract region"
    if table and not exception:
        for id_row, row in enumerate(table):
            for field in statement_table.fields:
                success = False
                exception = ""
                field_value = None
                try:
                    if field.cell.row is not None and field.cell.row != id_row:
                        continue  # skip if the field is not on this row
                    field_value = table[id_row][field.cell.col]
                    if field_value and not exception:
                        field_value = (
                            field_strip(field_value, field.strip) if field.strip else field_value
                        )  # strip any specified characters
                        if field_validate(field_value, field.pattern):
                            success = True
                            exception = ""
                            field_values.append((field, field_value, success, exception, id_row))
                        else:
                            field_value = None
                            success = False
                            exception = f"{field_value} failed to validate against pattern {field.pattern}"
                            field_values.append((field, field_value, success, exception, id_row))
                except IndexError:
                    field_value = None
                    success = False
                    exception = f"cell[row={field.cell.row}, column={field.cell.col}] not found in specified table"
                    field_values.append((field, field_value, success, exception, id_row))
    else:
        if not exception:
            exception = "Failed to extract region"
    if not field_values:
        for field in statement_table.fields:
            field_values.append((field, None, False, exception, None))
    return field_values


def extract_field_values(config, pdf):
    """
    Extracts field values from a bank statement based on the provided configuration.

    This function processes the statement using the configuration object, which may specify
    table-based extraction, multiple locations, or a single location for field extraction.
    It returns a list of FieldResult objects containing the extracted field values and
    associated metadata, along with any tests performed.

    Args:
        config: An object containing extraction configuration, including field definitions,
                locations, and table extraction settings.
        pdf: The bank statement object to extract fields from.

    Returns:
        tuple:
            - field_results (list[FieldResult]): A list of FieldResult objects representing
              the extracted fields and their values.
            - tests (list or None): A list of tests performed during extraction, or None if
              no tests were executed.

    Raises:
        StatementError: If the configuration is incomplete or invalid.
    """
    field_results: list[FieldResult] = []
    field_value: str | None
    success: bool = False
    exception: str = ""
    tests: list | None = None
    if config.statement_table:
        spawned_locations = spawn_locations(config.statement_table.locations, pdf)
        for location in spawned_locations:
            table_fields = extract_table_fields(pdf, location, config.statement_table)
            for field_row in table_fields:
                field, field_value, success, exception, table_row = field_row
                field_results.append(
                    FieldResult(
                        config=config.config,
                        page_number=location.page_number,
                        field=field.field,
                        type=field.type,
                        field_value=field_value if field_value else "",
                        value=field_value if success else "",
                        success=success,
                        vital=field.vital,
                        exception=exception if exception else "",
                        table_row=table_row,
                        id_row=location.page_number * 1000 + (table_row if table_row is not None else 0),  # unique row id
                    )
                )

    elif config.locations:  # multiple locations
        spawned_locations = spawn_locations(config.locations, pdf)
        for location in spawned_locations:
            success, field_value, exception = extract_text_field(pdf, location, config.field.strip, config.field.pattern)
            field_results.append(
                FieldResult(
                    config=config.config,
                    page_number=location.page_number,
                    field=config.field.field,
                    type=config.field.type,
                    field_value=field_value if field_value else "",
                    value=field_value if success else "",
                    success=success,
                    vital=config.field.vital,
                    exception=exception if exception else "",
                    table_row=None,
                    id_row=location.page_number * 1000,  # unique row id
                )
            )

    else:
        try:
            if not getattr(config.location, "page_number", None):  # we assume the 1st page if no number set for a single config
                config.location.page_number = 1
            success, field_value, exception = extract_text_field(pdf, config.location, config.field.strip, config.field.pattern)
            field_results.append(
                FieldResult(
                    config=config.config,
                    page_number=config.location.page_number,
                    field=config.field.field,
                    type=config.field.type,
                    field_value=field_value if field_value else "",
                    value=field_value if success else "",
                    success=success,
                    vital=config.field.vital,
                    exception=exception if exception else "",
                    table_row=None,
                    id_row=config.location.page_number * 1000,  # row id
                )
            )
        except AttributeError:
            raise StatementError("Incomplete Configuration")

    return (field_results, tests)


def field_value_cast(value: str, type: str, success: bool) -> tuple[bool, str, object]:
    """
    Attempts to cast a string value to a specified type and returns the result along with success status and exception message.

    Args:
        value (str): The string value to be cast.
        type (str): The target type as a string (e.g., 'int', 'float', 'bool', etc.).

    Returns:
        tuple[bool, str, object]: A tuple containing:
            - success (bool): True if casting was successful, False otherwise.
            - exception (str): An error message if casting failed, empty string otherwise.
            - result (object): The casted value if successful, None otherwise.

    Notes:
        - The function attempts to cast using the provided type string, and if that fails, tries shortened versions of the type string.
        - Uses eval() for dynamic casting, which may have security implications if used with untrusted input.
    """
    cast_success: bool = False
    exception: str = ""
    result: object = None
    if success:  # only try to cast if the initial extraction was successful
        exp = f"{type}('{value}')"  # build the initial expression
        try:  # try to evaluate the cast
            result = eval(exp)
            cast_success = True
        except (ValueError, SyntaxError):
            exception = f"<CAST FAILURE: '{value}' is not of type {type}>"
        except NameError:  # if the formula fails to evaluate
            type_short = type[0:4]  # try shortening the type to catch bool's passed as 'boolean'
            exp = f"{type_short}('{value}')"
            try:
                result = eval(exp)
                cast_success = True
            except (ValueError, SyntaxError):
                exception = f"<CAST FAILURE: '{value}' is not of type {type}>"
            except NameError:
                type_shortest = type[0:3]
                exp = f"{type_shortest}('{value}')"
                try:
                    result = eval(exp)
                    cast_success = True
                except (ValueError, SyntaxError):
                    exception = f"<CAST FAILURE: '{value}' is not of type {type}>"
                except NameError:
                    exception = f"<CAST FAILURE: {type} is not a valid type>"
    return (cast_success, exception, result)


def get_field_results(result: Result) -> pl.DataFrame:
    """
    Processes a list of Result objects to extract and transform their fields into a Polars DataFrame.

    For each Result in the input list, this function stacks its fields into a DataFrame, then applies several transformations:
    - Attempts to cast field values to their specified types, storing the success and any exceptions.
    - Flags fields with unsuccessful casts as exceptions or warnings based on their 'vital' status.

    The resulting DataFrame is printed for inspection. If no fields are present, returns None; otherwise, returns the DataFrame.

    Args:
        results (list[Result]): A list of Result objects, each containing fields to be processed.

    Returns:
        pl.DataFrame | None: A DataFrame containing processed field data, or None if no fields are present.
    """
    result_df: pl.DataFrame = pl.DataFrame(strict=False)
    for field in result.fields:
        result_df = result_df.vstack(pl.DataFrame([field], strict=False))

    result_df = result_df.with_columns(
        cast_success=(
            pl.struct(["value", "type", "success"]).map_elements(
                lambda results: field_value_cast(value=results["value"], type=results["type"], success=results["success"])[0],
                return_dtype=pl.Boolean,
            )
        ),
        cast_exception=(
            pl.struct(["value", "type", "success"]).map_elements(
                lambda results: field_value_cast(value=results["value"], type=results["type"], success=results["success"])[1],
                return_dtype=pl.String,
            )
        ),
        flag_exception=(pl.col("success").not_() & pl.col("vital")),
        flag_warning=(pl.col("success").not_() & pl.col("vital").not_()),
    )
    return result_df


def remove_rows_with_missing_vital_fields(field_results: pl.DataFrame, vital_fields: list[str]) -> pl.DataFrame:
    """
    Removes rows from the DataFrame that contain missing vital fields.

    This function identifies rows in the provided DataFrame where any vital field (indicated by the 'vital' column)
    is missing (i.e., the 'value' column is null). It then filters out these rows, returning a new DataFrame
    that only includes rows with all vital fields present.

    Args:
        field_results (pl.DataFrame): A Polars DataFrame containing field results, including 'vital' and 'value' columns.

    Returns:
        pl.DataFrame: A new DataFrame with rows containing missing vital fields removed.
    """
    # Identify rows with missing vital fields
    row_vital_field_count = (
        field_results.filter(pl.col("field").is_in(vital_fields) & pl.col("success") & pl.col("cast_success"))
        .select(
            pl.col("page_number"),
            pl.col("table_row"),
            pl.col("field"),
            vital_count=1,
        )
        .unique()
    )
    all_pages_and_rows = field_results.select(pl.col("page_number"), pl.col("table_row")).unique()
    row_vital_fields = all_pages_and_rows.join(row_vital_field_count, on=["page_number", "table_row"], how="left").fill_null(0)

    rows_with_missing_vital = (
        row_vital_fields.group_by(["page_number", "table_row"])
        .agg(pl.col("vital_count").sum().alias("vital_count"))
        .filter(pl.col("vital_count") < len(vital_fields))
    )

    if rows_with_missing_vital.height > 0:
        # Filter out rows with missing vital fields
        filtered_results = field_results.join(
            rows_with_missing_vital.select(pl.col("page_number"), pl.col("table_row")),
            on=["page_number", "table_row"],
            how="anti",
        )
        return filtered_results
    else:
        return field_results


def flag_transaction_bookend(field_results: pl.DataFrame, transaction_bookends: StatementBookend) -> pl.DataFrame:
    """
    Flags the start of transactions in the DataFrame based on specified transaction start fields.

    This function identifies rows in the provided DataFrame where any of the specified transaction start fields
    are present and have successful extraction and casting. It then adds a new column 'transaction_start' to the
    DataFrame, marking these rows with a True value, while other rows are marked as False.

    Args:
        field_results (pl.DataFrame): A Polars DataFrame containing field results, including 'field', 'success', and 'cast_success' columns.
        transaction_start_fields (list[str]): A list of field names that indicate the start of a transaction.

    Returns:
        pl.DataFrame: A new DataFrame with a 'transaction_start' column added, indicating the start of transactions.
    """
    # Identify rows with missing vital fields
    start_rows = (
        field_results.filter(pl.col("field").is_in(transaction_bookends.start_fields) & pl.col("success") & pl.col("cast_success"))
        .select(
            pl.col("id_row"),
            pl.col("field"),
            start_count=1,
        )
        .unique()
        .group_by(["id_row"])
        .agg(pl.col("start_count").sum().alias("start_count"))
        .filter(pl.col("start_count") >= transaction_bookends.min_non_empty_start)
    )

    end_rows = (
        field_results.filter(pl.col("field").is_in(transaction_bookends.end_fields) & pl.col("success") & pl.col("cast_success"))
        .select(
            pl.col("id_row"),
            pl.col("field"),
            end_count=1,
        )
        .unique()
        .group_by(["id_row"])
        .agg(pl.col("end_count").sum().alias("end_count"))
        .filter(pl.col("end_count") >= transaction_bookends.min_non_empty_end)
    )

    # Flag rows as transaction starts based on the specified fields
    field_results = field_results.with_columns(
        transaction_start=pl.when(pl.col("id_row").is_in(start_rows["id_row"])).then(True).otherwise(False),
        transaction_end=pl.when(pl.col("id_row").is_in(end_rows["id_row"])).then(True).otherwise(False),
    )

    return field_results


def get_transactions(result_transactions: pl.DataFrame, mods: TransactionMods) -> pl.DataFrame:
    # remove rows before first transaction_start and after last transaction_end
    first_id_row = result_transactions.filter(pl.col("transaction_start")).select(pl.col("id_row")).min()[0, 0]
    last_id_row = result_transactions.filter(pl.col("transaction_end")).select(pl.col("id_row")).max()[0, 0]
    transactions = result_transactions.filter((pl.col("id_row") >= first_id_row) & (pl.col("id_row") <= last_id_row))

    if mods.fill_forward_fields:
        for field in mods.fill_forward_fields:
            if field in transactions.columns:
                transactions = transactions.with_columns(pl.col(field).fill_null(strategy="forward"))

    if mods.merge_fields:
        for field in mods.merge_fields.fields:
            for _ in range(
                mods.merge_fields.max_rows
            ):  # repeat multiple times to ensure all rows are merged - can handle up to 10 rows per transaction
                transactions = (
                    transactions.with_columns(
                        pl.when(
                            pl.col("transaction_end") & ~pl.col("transaction_start")
                        )  # if a row is a continuation of the previous transaction
                        .then(
                            pl.concat_str(
                                transactions.shift(1)[field], pl.col(field), separator=mods.merge_fields.separator, ignore_nulls=True
                            )
                        )  # merge with previous row
                        .otherwise(pl.col(field))  # else keep the same
                        .alias(f"{field}_merge"),  # new column with merged values
                        pl.when(
                            ~pl.col("transaction_end") & pl.col("transaction_start") & transactions.shift(-1)["transaction_end"]
                        )  # if a row is the start of a transaction, but not the end, and the next row is the end of a transaction we can delete this row
                        .then(True)
                        .otherwise(False)
                        .alias(f"{field}_delete_row"),
                    )
                    .drop(field)
                    .rename({f"{field}_merge": field})
                )
                transactions = transactions.filter(~pl.col(f"{field}_delete_row")).drop(
                    f"{field}_delete_row"
                )  # drop rows that have been merged into the following row

    transactions = transactions.filter(
        pl.col("transaction_end")
    )  # keep only rows where transaction ends, all transaction rows above have been merged into these rows

    # get standard fields
    # credit & debit columns first so these can be used in the movement column

    transactions = transactions.with_columns(
        # standard credit column
        std_credit=pl.when(
            pl.col(mods.std_credit.field).str.ends_with(mods.std_credit.suffix)
            & pl.col(mods.std_credit.field).str.starts_with(mods.std_credit.prefix)
            & (  # check if the field can be cast to float if is_float is True, or not if is_float is False
                (mods.std_credit.is_float & pl.col(mods.std_credit.field).cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean))
                | (
                    mods.std_credit.is_float
                    == False & ~pl.col(mods.std_credit.field).cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean)
                )
            )
        )
        .then(pl.col(mods.std_credit.field).str.strip_chars_end(mods.std_credit.suffix).str.strip_chars_start(mods.std_credit.prefix))
        .otherwise(0.00)
        .cast(pl.Float64)
        .mul(mods.std_credit.multiplier)
        .round(mods.std_credit.round_decimals),
        # standard debit column
        std_debit=pl.when(
            pl.col(mods.std_debit.field).str.ends_with(mods.std_debit.suffix)
            & pl.col(mods.std_debit.field).str.starts_with(mods.std_debit.prefix)
            & (  # check if the field can be cast to float if is_float is True, or not if is_float is False
                (mods.std_debit.is_float & pl.col(mods.std_debit.field).cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean))
                | (
                    mods.std_debit.is_float
                    == False & ~pl.col(mods.std_debit.field).cast(pl.Float64, strict=False).fill_null(False).cast(pl.Boolean)
                )
            )
        )
        .then(pl.col(mods.std_debit.field).str.strip_chars_end(mods.std_debit.suffix).str.strip_chars_start(mods.std_debit.prefix))
        .otherwise(0.00)
        .cast(pl.Float64)
        .mul(mods.std_debit.multiplier)
        .round(mods.std_debit.round_decimals),
    )
    # and then the other standard fields
    transactions = transactions.with_columns(
        std_date=pl.col(mods.std_date.field).str.to_date(format=mods.std_date.format, strict=True),
        std_description=pl.col(mods.std_description.field)
        .str.to_titlecase()
        .str.slice(0, mods.std_description.max_length)
        .str.strip_chars_start(mods.std_description.strip_chars_start)
        .str.strip_chars_end(mods.std_description.strip_chars_end),
        std_movement=(pl.col("std_credit") + pl.col("std_debit")).round(2),
    )

    return transactions


def get_field_values(configs, pdf, file, company, account) -> ConfigResults:
    """
    Extracts field values from a list of configuration objects for a given bank statement, file, company, and account.

    Args:
        configs (list): A list of configuration objects used to extract field values.
        pdf: The bank statement object to process.
        file: The file associated with the statement.
        company: The company associated with the statement.
        account: The account associated with the statement.

    Returns:
        ConfigResults: An object containing the full extraction results and field-specific results.
    """
    results_full: list = []
    results_clean: list[pl.DataFrame] = []
    results_transactions: list[pl.DataFrame] = []
    for config in configs:
        result_full: Result | None = None
        result_clean: pl.DataFrame = pl.DataFrame()
        result_transactions: pl.DataFrame = pl.DataFrame()
        mods: TransactionMods | None = None
        table: StatementTable | None = None
        # get the full results for the config
        extract = extract_field_values(config=config, pdf=pdf)
        fields, tests = extract
        result_full = Result(file, company, account, config.config, fields, tests)

        # now get the field results for the config, which are validated and cast
        result_clean = get_field_results(result_full)
        if table := config.statement_table:
            if table.delete_success_false:
                result_clean = result_clean.filter(pl.col("success"))
            if table.delete_cast_success_false:
                result_clean = result_clean.filter(pl.col("cast_success"))
            if table.delete_rows_with_missing_vital_fields:
                vital_fields = [field.field for field in table.fields if field.vital]
                result_clean = remove_rows_with_missing_vital_fields(result_clean, vital_fields)

            # extract and process the transactions
            if mods := table.transaction_mods:
                result_clean = flag_transaction_bookend(result_clean, mods.transaction_bookends)
                # transactions are now pivoted
                result_transactions = result_clean.pivot(
                    on="field",
                    values="value",
                    index=["config", "page_number", "table_row", "id_row", "transaction_start", "transaction_end"],
                )  # pivoted dataframe
                result_transactions = get_transactions(result_transactions, mods)
        if len(result_full.fields) > 0:
            results_full.append(result_full)
            results_clean.append(result_clean)
            results_transactions.append(result_transactions)
    return ConfigResults(results_full=results_full, results_clean=results_clean, results_transactions=results_transactions)
