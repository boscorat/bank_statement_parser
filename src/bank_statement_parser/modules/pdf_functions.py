import polars as pl
from pdfplumber import open
from pdfplumber.page import Page
from pdfplumber.pdf import PDF

from bank_statement_parser.modules.data import DynamicLineSpec, Location


def get_region(location: Location, pdf: PDF, logs: pl.DataFrame, file_path: str) -> Page | None:
    """Extract a cropped page region from a PDF based on location coordinates."""
    # start = time.time()
    if location.page_number:
        region = page_crop(pdf.pages[location.page_number - 1], location.top_left, location.bottom_right, logs, file_path)
    else:
        region = None
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "get_region", time.time() - start, 1, datetime.now(), ""]],
    #     schema=logs.schema,
    #     orient="row",
    # )
    # logs.vstack(log, in_place=True)
    return region


def pdf_open(file_path: str, logs: pl.DataFrame) -> PDF:
    """Open a PDF file and return the PDF object with performance logging."""
    # start = time.time()
    pdf = open(file_path)
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "pdf_open", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    # )
    # logs.vstack(log, in_place=True)
    return pdf


def pdf_close(pdf: PDF, logs: pl.DataFrame, file_path: str) -> bool:
    """Close a PDF file and log the operation duration."""
    # start = time.time()
    pdf.close()
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "pdf_close", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    # )
    # logs.vstack(log, in_place=True)
    return True


def page_crop(page: Page, top_left: list | None, bottom_right: list | None, logs: pl.DataFrame, file_path: str) -> Page:
    """Crop a PDF page to the specified bounding box coordinates, with smart defaults."""
    # start = time.time()
    if not top_left and not bottom_right:  # no need to crop if not specified
        return page
    elif not top_left:  # set top left to 0,0 if only bottom right specified
        top_left = [0, 0]
    elif not bottom_right:  # set bottom right to page width,height if only top left specified
        bottom_right = [page.width, page.height]
    else:
        page_cropped = page.within_bbox((top_left[0], top_left[1], bottom_right[0], bottom_right[1]))
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "page_crop", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    # )
    # logs.vstack(log, in_place=True)
    return page_cropped


def region_search(region: Page, pattern: str, logs: pl.DataFrame, file_path: str) -> str | None:
    """Search for a regex pattern within a PDF region and return the first match text."""
    # start = time.time()
    try:
        search_result = region.search(pattern, regex=True)[0]["text"]  # text of 1st result
    except IndexError:
        search_result = None
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "region_search", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    # )
    # logs.vstack(log, in_place=True)
    return search_result


def page_text(page: Page, logs: pl.DataFrame, file_path: str):
    """Extract all text content from a PDF page."""
    page_text = page.extract_text()
    # log = pl.DataFrame(
    #     [[file_path, "pdf_functions", "page_text", time.time() - start, 1, datetime.now(), ""]], schema=logs.schema, orient="row"
    # )
    # logs.vstack(log, in_place=True)
    return page_text


def get_table_from_region(
    region: Page,
    location: Location,
    pdf: PDF,
    logs: pl.DataFrame,
    file_path: str,
    table_rows: int | None = None,
    table_columns: int | None = None,
    row_spacing: int | None = None,
    vertical_lines: list | None = None,
    allow_text_failover: bool | None = None,
    remove_header: bool | None = None,
    header_text: str | None = None,
    dynamic_last_vertical_line: DynamicLineSpec | None = None,
    try_shift_down: int | None = None,
) -> pl.LazyFrame | None:
    """Extract a structured table from a PDF region using configurable extraction settings."""
    table = None
    tbl_settings: dict = {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "min_words_vertical": 2,
        "min_words_horizontal": 2,
        "snap_y_tolerance": 3,
    }
    if table_rows:
        tbl_settings["min_words_vertical"] = table_rows
    if table_columns:
        tbl_settings["min_words_horizontal"] = table_columns
    if row_spacing:
        tbl_settings["snap_y_tolerance"] = row_spacing
    if vertical_lines:
        tbl_settings["explicit_vertical_lines"] = vertical_lines
        tbl_settings["vertical_strategy"] = "explicit"
        tbl_settings["min_words_vertical"] = 1  # override if explicit vertical lines given
        tbl_settings["min_words_horizontal"] = 1  # override if explicit vertical lines given
    if vertical_lines and dynamic_last_vertical_line:  # if the last line may be driven by the position of an image we try to allign it
        try:  # not tied to this!  It could fail if a new image is added or removed
            current_final_line = vertical_lines[-1]  # the the current final vertical line
            dynamic_final_line = region.images[dynamic_last_vertical_line.image_id][
                dynamic_last_vertical_line.image_location_tag
            ]  # get the position of the image specified
            if abs(current_final_line - dynamic_final_line) <= 10:  # only allign them if they're fairly close
                vertical_lines[-1] = region.images[dynamic_last_vertical_line.image_id][
                    dynamic_last_vertical_line.image_location_tag
                ]  # replace the exisiting with the dynamic
            table = region.extract_table(table_settings=tbl_settings)
            if not table:  # if it hasn't returned a table we reset the final vertical line
                vertical_lines[-1] = current_final_line
            elif table_columns and len(table[0]) < table_columns:  # if the table doesn't have enough columns
                vertical_lines[-1] = current_final_line  # we reset the last line
                table = None  # and ditch the table
        except (IndexError, KeyError):
            pass  # any issues and we just smile, wave, and move on
    if not table:  # if we haven't already got a good looking table through the dynamic vertical lines we get it now
        table = region.extract_table(table_settings=tbl_settings)
    if not table and try_shift_down and location.top_left and location.bottom_right:
        try:
            location.top_left[1] = location.top_left[1] + try_shift_down
            location.bottom_right[1] = location.bottom_right[1] + try_shift_down
            region = get_region(location, pdf, logs, file_path)  # type: ignore
            table = region.extract_table(table_settings=tbl_settings)
        except IndexError:
            pass
    if table and table_columns and len(table[0]) < table_columns:  # if we haven't got enough columns..
        if allow_text_failover and vertical_lines:  # we can try failing over to text extraction
            vertical_lines = None
            return get_table_from_region(
                region,
                location,
                pdf,
                logs,
                file_path,
                table_rows,
                table_columns,
                row_spacing,
                vertical_lines,
                allow_text_failover,
                remove_header,
                header_text,
                dynamic_last_vertical_line,
                try_shift_down,
            )
        else:  # if that doens't work we've got adodgy table and we return None
            return None

    if table and remove_header and table[0]:
        if header_text:
            line_zero_text = str("".join(table[0])).lower().replace(" ", "")  # type: ignore
            if line_zero_text == header_text.lower().replace(" ", ""):
                table = table[1:]
        else:
            table = table[1:]

    column_names = ["col_" + str(i) for i in range(len(table[0]))] if table else []
    table = pl.LazyFrame(table[0:], schema=column_names, orient="row") if table else pl.LazyFrame()

    return table


if __name__ == "__main__":
    ...
    # # if the file is run directly do some useful testing
    # path = "/home/boscorat/Downloads/2025-07-08_Statement_Flexible_Saver.pdf"
    # pdf = pdf_open(path, logs=logs)
    # locations = [
    #     Location(vertical_lines=[50, 100, 100, 130, 130, 320, 320, 400, 400, 480, 480, 555]),
    # ]
    # spawned_locs = spawn_locations(locations, pdf, [2])
    # for loc in spawned_locs:
    #     region = get_region(location=loc, pdf=pdf, logs=logs, file_path=path)
    #     table = get_table_from_region(region=region, vertical_lines=loc.vertical_lines, logs=logs, file_path=path)
    #     with pl.Config(tbl_cols=-1, tbl_rows=-1):
    #         print(table.collect())
