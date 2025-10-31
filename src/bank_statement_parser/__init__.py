import polars as pl

import bank_statement_parser.modules.classes.database as db
import bank_statement_parser.modules.paths as pth
from bank_statement_parser.modules.classes import statements

# from bank_statement_parser.modules.classes.reports import GapReport


def main():
    # db.ChecksAndBalances().truncate()
    # db.BatchHeads().truncate()
    # db.BatchLines().truncate()
    # db.StatementHeads().truncate()
    # db.StatementLines().truncate()

    # statements.StatementBatch("/home/boscorat/Downloads/2025")
    statements.StatementBatch("/home/boscorat/Downloads/2025/success")
    # statements.StatementBatch("/home/boscorat/Downloads/2025/success/run1")
    # statements.StatementBatch("/home/boscorat/Downloads/2025/success/run2")

    # statements.StatementBatch("/home/boscorat/Downloads/2024")
    # statements.StatementBatch("/home/boscorat/Downloads/2023")
    # statements.StatementBatch("/home/boscorat/Downloads/2022")
    # statements.StatementBatch("/home/boscorat/Downloads/2021")
    # statements.StatementBatch("/home/boscorat/Downloads/2020")
    # statements.StatementBatch("/home/boscorat/Downloads/2019")
    # print(pl.read_parquet(pth.PATH_BATCH_HEADS))
    # print(pl.read_parquet(pth.PATH_STATEMENT_HEADS))
    # print(pl.read_parquet(pth.PATH_STATEMENT_LINES))

    # print(f"\n{batch.pdf_count} processed with {batch.errors} errors in {batch.duration}")
    # print("*" * 10)
    # print(pl.read_parquet(pth.PATH_BATCH_HEADS))
    # print(pl.read_parquet(pth.PATH_CAB))
    # print(GapReport().dataframe)


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(50)
    main()
