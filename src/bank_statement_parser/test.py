import re
from collections import namedtuple
from copy import deepcopy

import polars as pl
from modules import StatementError, page_crop, pdf_open, region_search, region_table

import config as cf

Result = namedtuple("Result", "file company account config fields tests")
FieldResults = namedtuple("FieldResults", "field")
ConfigResults = namedtuple("ConfigResults", "results_full results_field")
FieldResult = namedtuple("FieldResult", "config page_number field type field_value value success vital exception")


def main():
    file = "/home/boscorat/Downloads/2025-07-12_Statement_Rewards_Credit_Card.pdf"  # CUR

    statement = pdf_open(file)

    account_key = None  # "HSBC_UK_CUR_ADV"
    account = ""
    company_key = ""
    company = ""

    if account_key:  # if we've got an account key it's straigtforward to get the config
        config_details = get_config_from_account(account_key, file)
        if not config_details:
            raise StatementError("An account key was provided but did not return the required config to process the statement")
    elif company_key:  # if we know the company we can identify
        config_details = get_config_from_company(company_key, statement, file)
        if not config_details:
            raise StatementError("A company key was provided but did not return the required config to process the statement")
    else:
        config_details = get_config_from_statement(statement, file)

    account = config_details.account
    if config_details.company:
        company = config_details.company.company

    if config_statement_type := config_details.statement_type:
        configs_header = config_statement_type.header.configs
        configs_page = config_statement_type.pages.configs
        configs_lines = config_statement_type.lines.configs

    if configs_header:
        header_results = get_field_values(configs_header, statement, file, company, account)
    # if configs_page:
    #     page_results = get_field_values(configs_page, statement, file, company, account)
    # if configs_lines:
    #     lines_results = get_field_values(configs_lines, statement, file, company, account)

    with pl.Config(tbl_cols=-1, tbl_rows=-1):
        print(header_results.results_field)
        # print(page_results.results_field)

    print()


def get_field_values(configs, statement, file, company, account) -> ConfigResults:
    results: list = []
    for config in configs:
        extract = extract_field_values(config=config, statement=statement)
        fields, tests = extract
        result = Result(file, company, account, config.config, fields, tests)
        results.append(result)
    field_results = get_field_results(results, file, company)
    return ConfigResults(results, field_results)


def field_value_cast(value: str, type: str) -> tuple[bool, str]:
    exp = f"{type}('{value}')"  # build the initial expression
    success: bool = False
    exception: str = ""
    result: str = ""
    try:  # try to evaluate the cast
        result = eval(exp)
    except ValueError:
        exception = f"<CAST FAILURE: '{value}' is not of type {type}>"  # if the conversion formula is valid, but the cast fails
    except NameError:  # if the formula fails to evaluate
        type_short = type[0:4]  # try shortening the type to catch bool's passed as 'boolean'
        exp = f"{type_short}('{value}')"  # rebuild the formula
        try:  # try to evaluate the shorter form
            result = eval(exp)
        except ValueError:
            exception = f"<CAST FAILURE: '{value}' is not of type {type}>"
        except NameError:  # still not working? try an even shorter form to catch int and str passed as integer and string
            type_shortest = type[0:3]
            exp = f"{type_shortest}('{value}')"
            try:
                result = eval(exp)
            except ValueError:
                exception = f"<CAST FAILURE: '{value}' is not of type {type}>"
            except NameError:
                exception = f"<CAST FAILURE: {type} is not a valid type>"
    success = True if result else False
    return (success, exception)


def get_field_results(results: list[Result], file, company) -> pl.DataFrame | None:
    # # test for exceptions and flatten results
    # field_results = None
    # field_names: list[str] = []
    # field_values: list[str | float | int] = []
    result_df: pl.DataFrame = pl.DataFrame(strict=False)
    for result in results:
        for field in result.fields:
            # field_result = pl.DataFrame([field])
            # result_df_v1 = pl.concat([field_result, result_df_v1], how="vertical_relaxed")
            result_df = result_df.vstack(pl.DataFrame([field], strict=False))

    # expr = (pl.col("b") / pl.col("a")).alias("b_div_a")
    result_df = result_df.with_columns(
        cast_success=(pl.struct(["value", "type"]).map_elements(lambda results: field_value_cast(results["value"], results["type"])[0])),
        cast_exception=(pl.struct(["value", "type"]).map_elements(lambda results: field_value_cast(results["value"], results["type"])[1])),
        flag_exception=(pl.col.success.not_() & pl.col.vital),
        flag_warning=(pl.col.success.not_() & pl.col.vital.not_()),
    )
    with pl.Config(tbl_cols=-1, tbl_rows=-1):
        print(result_df)
    # print()
    # for page, result in page_results:
    #     for result in results:
    #         for field in result.fields:
    #             if field.vital:
    #                 if field.exception:
    #                     raise StatementError(
    #                         f"EXCEPTION: {field.exception}\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
    #                     )
    #                 elif not field.success:
    #                     raise StatementError(
    #                         f"EXCEPTION: Vital Field Without Value!\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
    #                     )
    #             else:
    #                 if field.exception:
    #                     print(
    #                         f"WARNING: {field.exception}\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
    #                     )
    #                 elif not field.success:
    #                     print(
    #                         f"WARNING: non-vital field without value\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
    #                     )
    #             # field_names.append(field.

    #             if field.value:
    #                 try:
    #                     if field.type == "float":
    #                         field_values.append(float(field.value))
    #                     elif field.type == "int":
    #                         field_values.append(int(field.value))
    #                     else:
    #                         field_values.append(field.value)
    #                 except TypeError:
    #                     raise StatementError(
    #                         f"EXCEPTION: {field.field} should have the type of {field.type}!\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
    #                     )
    #             else:
    #                 del field_names[-1]  # delete the last field name if no value

    # # remove missing records
    # for id, val in enumerate(field_values):
    #     if val == "<missing>":
    #         del field_names[id]
    #         del field_values[id]

    # FieldResults = namedtuple("FieldResults", field_names)
    # field_results = FieldResults(*field_values)
    # if get_test_results(results, field_results, file, company):
    #     return field_results
    # else:
    #     return None

    return None if result_df.is_empty() else result_df


# def get_test_results(results, field_results, file, company) -> bool:
#     for result in results:
#         if result.tests:
#             for test in result.tests:
#                 try:
#                     formula = test.assertion.replace("{", "field_results.").replace("}", "").replace("=", "==")
#                     test_result = eval(formula)
#                     if not test_result:
#                         raise StatementError(
#                             f"EXCEPTION: '{test.test_desc}' failed!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
#                         )
#                 except AttributeError:
#                     raise StatementError(
#                         f"EXCEPTION: '{test.test_desc}' references an incorrect field!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
#                     )

#                 except SyntaxError:
#                     raise StatementError(
#                         f"EXCEPTION: '{test.test_desc}' contains a syntax error!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
#                     )
#     return True


def field_strip(value: str, chars: list | None) -> str:
    if chars:
        for char in chars:
            value = value.replace(char, "")
    return value


def field_validate(value: str | None, pattern: str) -> bool:
    success = False
    if value:
        if re.search(pattern, value):
            success = True
    return success


def extract_text_field(statement, location, strip, pattern) -> tuple[bool, str, str]:
    success: bool = False
    exception: str = ""
    field_value: str = ""
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


def spawn_locations(locations, statement):
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


def extract_field_values(config, statement):

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
            if not config.location.page_number:  # we assume the 1st page if no number set for a single config
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
        print()

    return (field_results, tests)


if __name__ == "__main__":
    main()

# my_pdf = utils.pdf_open(file)

# config = full

# for id, company in config.items():
#     page = my_pdf.pages[company.page]
#     region = utils.page_crop(page, top_left=company.region.top_left, bottom_right=company.region.bottom_right)
#     test = page.search("hsbc.co.uk", regex=True)
#     text = utils.page_text(region)
#     fields = []
#     for field in company.fields:
#         match = utils.region_search(region, pattern=field.pattern)
#         fields.append((field.field, match))
#     print()
# # with pdf_open(file) as pdf:
# #     cropped = pdf.pages[0].within_bbox((364, 64, 575, 150))
# #     text = cropped.extract_text()
# #     search = cropped.search("hsbc\.co\.uk", regex=True, groups=False, chars=False)
# #     table_extract = cropped.extract_table(
# #         table_settings={"vertical_strategy": "text", "horizontal_strategy": "text", "snap_y_tolerance": 1, "min_words_vertical": 2}
# #     )

# print()
# def main():t
#     for file in files:
#         pdf = get_pdf(file)
#         # tables = extract_tables(file=file, pages="1", fixed_area=.0, 0, 595, 841, fixed_columns = {"40, 50, 550, 570"}, column_tol = 9999)
#     print()

# def get_pdf(file: str):
#     # try:
#     pages: list = []
#     with suppress_stderr():
#         with pdf_open(file) as pdf:
#             cropped = pdf.pages[0].within_bbox((37, 370, 575, 415))
#             text = cropped.extract_text()
#             table_extract = cropped.extract_table(table_settings={"vertical_strategy": "text",
#     "horizontal_strategy": "text", "snap_y_tolerance": 3, "min_words_vertical": 2})
#             pages.append((table_extract))
#     # except Exception as e:
#     #     print(f"Error opening or processing PDF file '{file}': {e}")
#     return pages

# def extract_tables(file: str, pages: str, fixed_area: list[str] | None, fixed_columns: list[str] | None, column_tol: int) -> list[dict]:
#     global current_pdf
#     current_pdf = file
#     if fixed_area and fixed_columns:
#         raw = camelot.read_pdf(file, pages=pages, flavor="stream", table_areas=fixed_area, columns=fixed_columns)
#     elif fixed_area:
#         raw = camelot.read_pdf(file, pages=pages, flavor="stream", table_areas=fixed_area, column_tol=column_tol)
#     else:
#         raw = camelot.read_pdf(file, pages=pages, flavor="stream", column_tol=column_tol)
#     tables: list = [{"table_number": id, "page": table.page, "data": table.data} for id, table in enumerate(raw)]
#     for table in tables:
#         table_text = ""
#         for row in table.data:
#             table_text += "|".join(row)
#         table.text = table_text
#         table.text_strip = table_text.replace(" ", "")
#     current_pdf = file
#     return tables

# @contextmanager
# def suppress_stderr():
#     with open(os.devnull, "w") as devnull:
#         old_stderr = sys.stderr
#         sys.stderr = devnull
#         try:
#             yield
#         finally:
#             sys.stderr = old_stderr

# if __name__ == "__main__":
#     main()

# if __name__ == "__main__":
#     # if the file is run directly do some useful testing
#     ...
