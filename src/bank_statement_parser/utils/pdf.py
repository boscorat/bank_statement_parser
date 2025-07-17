from pdfplumber import open


def pdf_open(file: str):
    pdf = open(file)
    return pdf


def page_crop(page, top_left: list, bottom_right: list):
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


def region_table(region, header_rows: int | None, data_rows: int | None, row_spacing: int | None):
    # if data_rows:
    #     min_words_vertical = 1  # data_rows  # + header_rows if header_rows else 0
    # else:
    #     min_words_vertical = 3
    snap_y_tolerance = row_spacing if row_spacing else 3
    table = region.extract_table(
        table_settings={
            "vertical_strategy": "text",
            "horizontal_strategy": "text",
            "snap_y_tolerance": snap_y_tolerance,
            "min_words_vertical": 2,
        }
    )
    return table


# def page_table_largest(page, config: dict = {}):


# def page_table_all(page):
#     ...
