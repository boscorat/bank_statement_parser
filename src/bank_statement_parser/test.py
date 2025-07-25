import re
from collections import namedtuple

from utils import page_crop, pdf_open, region_search, region_table

import config as cf


class StatementError(Exception):
    pass


Result = namedtuple("Result", "file company account config fields tests page_number_config")
FieldResults = namedtuple("FieldResults", "field")
ConfigResults = namedtuple("ConfigResults", "results_full results_field")
FieldResult = namedtuple("FieldResult", "page_number field type cell_value value success vital exception")


def main():
    file = "/home/boscorat/Downloads/2025-07-08_Statement_Advance_Account.pdf"  # CUR
    statement = pdf_open(file)

    account_key = None  # "HSBC_UK_CUR_ADV"
    account = ""
    company_key = "HSBC_UK"
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
        configs_page = config_statement_type.page.configs
        configs_lines = config_statement_type.lines.configs

    if configs_header:
        header_results = get_field_values(configs_header, statement, file, company, account)
    if configs_page:
        page_results = get_field_values(configs_page, statement, file, company, account)
    if configs_lines:
        lines_results = get_field_values(configs_lines, statement, file, company, account)

    print(header_results.results_field)
    print(page_results.results_field)


def get_field_values(configs, statement, file, company, account) -> ConfigResults:
    results: list = []
    for config in configs:
        extract = extract_field_values(config=config, statement=statement)
        fields, tests, page_number_config = extract
        result = Result(file, company, account, config.config, fields, tests, page_number_config)
        results.append(result)
    field_results = get_field_results(results, file, company)
    return ConfigResults(results, field_results)


def get_field_results(results: list[Result], file, company) -> tuple | None:
    # test for exceptions and flatten results
    field_results = None
    field_names: list[str] = []
    field_values: list[str | float | int] = []
    for result in results:
        for field in result.fields:
            if field.vital:
                if field.exception:
                    raise StatementError(
                        f"EXCEPTION: {field.exception}\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
                    )
                elif not field.success:
                    raise StatementError(
                        f"EXCEPTION: Vital Field Without Value!\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
                    )
            else:
                if field.exception:
                    print(
                        f"WARNING: {field.exception}\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
                    )
                elif not field.success:
                    print(
                        f"WARNING: non-vital field without value\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
                    )
            field_names.append(field.field + (f"_{str(field.page_number)}" if not result.page_number_config else ""))

            if field.value:
                try:
                    if field.type == "float":
                        field_values.append(float(field.value))
                    elif field.type == "int":
                        field_values.append(int(field.value))
                    else:
                        field_values.append(field.value)
                except TypeError:
                    raise StatementError(
                        f"EXCEPTION: {field.field} should have the type of {field.type}!\nfile: {file}\ncompany: {company}\nfield group: {result.config}\nfield: {field.field}"
                    )
            else:
                field_values.append("<missing>")

    FieldResults = namedtuple("FieldResults", field_names)
    field_results = FieldResults(*field_values)
    if get_test_results(results, field_results, file, company):
        return field_results
    else:
        return None


def get_test_results(results, field_results, file, company) -> bool:
    for result in results:
        if result.tests:
            for test in result.tests:
                try:
                    formula = test.assertion.replace("{", "field_results.").replace("}", "").replace("=", "==")
                    test_result = eval(formula)
                    if not test_result:
                        raise StatementError(
                            f"EXCEPTION: '{test.test_desc}' failed!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
                        )
                except AttributeError:
                    raise StatementError(
                        f"EXCEPTION: '{test.test_desc}' references an incorrect field!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
                    )

                except SyntaxError:
                    raise StatementError(
                        f"EXCEPTION: '{test.test_desc}' contains a syntax error!\nfile: {file}\ncompany: {company}\nfield group: {result.config}"
                    )
    return True


def get_config_from_statement(statement, file) -> cf.Account:
    company_leaf = pick_leaf(leaves=cf.config_companies, statement=statement)
    if not company_leaf:
        raise StatementError(f"Unable to identify the company from the statement provided: {file}")
    company_key = company_leaf[1]
    config_account = get_config_from_company(company_key, statement, file)
    del company_leaf
    return config_account


def get_config_from_company(company_key: str, statement, file) -> cf.Account:
    try:
        company_accounts = cf.config_company_accounts(company_key)
    except KeyError:
        print(f"{company_key} is not a valid company key")
    if company_accounts:
        config_account = pick_leaf(leaves=company_accounts, statement=statement)[0]
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file}")
    return config_account


def get_config_from_account(account_key: str, file) -> cf.Account:
    try:
        config_account = cf.config_accounts.get(account_key)
    except KeyError:
        print(f"{account_key} is not a valid account key")
    if not config_account:
        raise StatementError(f"Unable to identify the account from the statement provided: {file}")
    else:
        return config_account


def extract_field_values(config, statement):
    field_pages = []
    field_results: list[FieldResult] = []
    cell_value: str | None
    success: bool = False
    exception: str = ""
    if config.page_number:
        field_pages = [page for page in statement.pages if page.page_number == config.page_number]
    else:
        field_pages = [page for page in statement.pages]
    for page in field_pages:
        region = page_crop(page, config.region.top_left, config.region.bottom_right)
        for field in config.fields:
            cell_value = None
            success = False
            exception = ""
            if config.type == "search":
                if search := region_search(region, field.pattern):
                    cell_value = search
                    success = True
                else:
                    cell_value = None
                    success = False
                    exception = "" if success else f"{field.field}: {field.pattern} not found"
            elif config.type == "table":
                table = region_table(
                    region=region, header_rows=config.header_rows, data_rows=config.data_rows, row_spacing=config.row_spacing
                )
                try:
                    cell_value = table[field.cell.row][field.cell.col]
                except IndexError:
                    cell_value = None
                    success = False
                    exception = f"cell[row={field.cell.row}, column={field.cell.col}] not found in specified table"
                if cell_value:  # if we've got a cell value
                    # strip characters if required
                    if field.strip:
                        for char in field.strip:
                            cell_value = cell_value.replace(char, "")
                    # validate cell value
                    if search := re.search(field.pattern, cell_value):
                        success = True
                        cell_value = search.string
                    else:
                        success = False
                        exception = f"cell_value of {cell_value} does not match pattern {field.pattern}"
                else:
                    exception = exception if exception else f"cell[row={field.cell.row}, column={field.cell.col}] contains an empty string"
            else:
                cell_value = None
                success = False
                exception = "Unknown config type - should be 'search' or 'table'"

            field_results.append(
                FieldResult(
                    page_number=page.page_number,
                    field=field.field,
                    type=field.type,
                    cell_value=cell_value,
                    value=cell_value if success else None,
                    success=success,
                    vital=field.vital,
                    exception=exception,
                )
            )
    return (field_results, config.tests, config.page_number)


def pick_leaf(leaves, statement) -> tuple[cf.Account, str]:
    if type(leaves) is dict:
        for key, leaf in leaves.items():
            if extract := extract_field_values(config=leaf.config, statement=statement)[0]:
                if sum([1 for record in extract if record.success]):
                    result = (leaf, key)
                    break
    elif type(leaves) is list:
        for leaf in leaves:
            if extract := extract_field_values(config=leaf.config, statement=statement)[0]:
                if sum([1 for record in extract if record.success]):
                    result = (leaf, "")
                    break
    else:
        raise TypeError("the pick_leaf() function requires leaves to be a dictionary or list")
    if not result:
        raise StatementError("the account cannot be identified from your statement")
    return result


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
