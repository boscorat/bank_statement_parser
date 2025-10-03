from pdfplumber import open


def pdf_open(file: str):
    pdf = open(file)
    # pdfplumber.open does not need a context manager
    # with open(file) as opFile:
    #     pdf = opFile
    #     opFile.close
    #     opFile = None
    return pdf


def pdf_close(pdf):
    pdf.close()


def page_crop(page, top_left: list, bottom_right: list):
    if not top_left and not bottom_right:  # no need to crop if not specified
        return page
    elif not top_left:  # set top left to 0,0 if only bottom right specified
        top_left = [0, 0]
    elif not bottom_right:  # set bottom right to page width,height if only top left specified
        bottom_right = [page.width, page.height]
    else:
        page_cropped = page.within_bbox((top_left[0], top_left[1], bottom_right[0], bottom_right[1]))
    return page_cropped


def region_search(region, pattern):
    try:
        search_result = region.search(pattern, regex=True)[0]["text"]  # text of 1st result
    except IndexError:
        search_result = None
    return search_result


def page_text(page):
    page_text = page.extract_text()
    return page_text


def region_table(region, table_rows: int | None, table_columns: int | None, row_spacing: int | None, vertical_lines: list | None):
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

    table = region.extract_table(table_settings=tbl_settings)
    return table


if __name__ == "__main__":
    # if the file is run directly do some useful testing
    ...


# def page_table_largest(page, config: dict = {}):


# def page_table_all(page):
#     ...
