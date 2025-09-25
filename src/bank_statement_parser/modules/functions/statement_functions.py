import re
from collections import namedtuple
from copy import deepcopy

from bank_statement_parser.modules.classes.errors import StatementError
from bank_statement_parser.modules.functions.pdf_functions import page_crop, region_table

FieldResult = namedtuple("FieldResult", "config page_number field type field_value value success vital exception")


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
        )
    else:
        if not exception:
            exception = "Failed to extract region"
    if table and not exception:
        for field in statement_table.fields:
            success = False
            try:
                field_value = table[field.cell.row][field.cell.col]
                if field_value and not exception:
                    field_value = field_strip(field_value, field.strip) if field.strip else field_value  # strip any specified characters
                    if field_validate(field_value, field.pattern):
                        success = True
                        exception = ""
                        field_values.append((field, field_value, success, exception))
                    else:
                        field_value = None
                        success = False
                        exception = f"{field_value} failed to validate against pattern {field.pattern}"
                        field_values.append((field, field_value, success, exception))
            except IndexError:
                field_value = None
                success = False
                exception = f"cell[row={field.cell.row}, column={field.cell.col}] not found in specified table"
                field_values.append((field, field_value, success, exception))
    else:
        if not exception:
            exception = "Failed to extract region"
    if not field_values:
        for field in statement_table.fields:
            field_values.append((field, None, False, exception))
    return field_values


def extract_field_values(config, statement):
    """
    Extracts field values from a bank statement based on the provided configuration.

    This function processes the statement using the configuration object, which may specify
    table-based extraction, multiple locations, or a single location for field extraction.
    It returns a list of FieldResult objects containing the extracted field values and
    associated metadata, along with any tests performed.

    Args:
        config: An object containing extraction configuration, including field definitions,
                locations, and table extraction settings.
        statement: The bank statement object to extract fields from.

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
        spawned_locations = spawn_locations(config.statement_table.locations, statement)
        for location in spawned_locations:
            table_fields = extract_table_fields(statement, location, config.statement_table)
            for field_row in table_fields:
                field, field_value, success, exception = field_row
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
                    )
                )

    elif config.locations:  # multiple locations
        spawned_locations = spawn_locations(config.locations, statement)
        for location in spawned_locations:
            success, field_value, exception = extract_text_field(statement, location, config.field.strip, config.field.pattern)
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
                )
            )

    else:
        try:
            if not getattr(config.location, "page_number", None):  # we assume the 1st page if no number set for a single config
                config.location.page_number = 1
            success, field_value, exception = extract_text_field(statement, config.location, config.field.strip, config.field.pattern)
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
                )
            )
        except AttributeError:
            raise StatementError("Incomplete Configuration")

    return (field_results, tests)
