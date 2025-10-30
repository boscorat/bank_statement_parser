import polars as pl

from bank_statement_parser.modules.classes import statements

# from bank_statement_parser.modules.classes.reports import GapReport


def main():
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_fmt_str_lengths(100)

    batch = statements.StatementBatch("/home/boscorat/Downloads/2025")

    # print(f"\n{batch.pdf_count} processed with {batch.errors} errors in {batch.duration}")
    # print("*" * 10)

    # print(GapReport().dataframe)


if __name__ == "__main__":
    main()
