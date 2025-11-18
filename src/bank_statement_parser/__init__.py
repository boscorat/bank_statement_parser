from pathlib import Path

import polars as pl

# import bank_statement_parser.modules.classes.database as db
# import bank_statement_parser.modules.classes.reports as rp
# import bank_statement_parser.modules.paths as pt
from bank_statement_parser.modules.classes import statements

# from bank_statement_parser.modules.classes.reports import DimAccount


def main():
    # db.ChecksAndBalances().truncate()
    # db.BatchHeads().truncate()
    # db.BatchLines().truncate()
    # # db.StatementHeads().truncate()
    # # db.StatementLines().truncate()

    # bh = db.BatchHeads()
    # bh.delete_file()
    # bh.cleanup()
    # bh = None

    # db.BatchLines().delete_file()

    statements.StatementBatch(Path("C:\\Users\\Admin\\repos\\bsp\\stmts"), turbo=True, smart_rename=False)

    # statements.StatementBatch(Path("/home/boscorat/Downloads/2025"), turbo=True)
    # statements.StatementBatch(Path("/home/boscorat/Downloads/2025/success"), turbo=True)
    # statements.StatementBatch(Path("/home/boscorat/Downloads/2025/success/run1"), turbo=True)
    # statements.StatementBatch(Path("/home/boscorat/Downloads/2025/success/run2"), turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2024", turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2023", turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2022", turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2021", turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2020", turbo=True)
    # statements.StatementBatch("/home/boscorat/Downloads/2019", turbo=True)

    # async def wrapper():
    #     await asyncio.run(bob1(), debug=True)
    #     asyncio.run(bob2(), debug=True)
    # # asyncio.run(bob3(), debug=True)
    # asyncio.run(bob4(), debug=True)

    # print(pl.read_parquet(pth.STATEMENT_HEADS).with_columns(pl.col("ID_STATEMENT").str.len_chars()))
    # print(pl.read_parquet(pth.PATH_STATEMENT_LINES))

    # print(f"\n{batch.pdf_count} processed with {batch.errors} errors in {batch.duration}")
    # print("*" * 10)
    # last_batch = pl.read_parquet(pt.BATCH_HEADS).select(pl.col("ID_BATCH").last()).item()
    # print(pl.read_parquet(pt.BATCH_HEADS))
    # print(
    #     pl.read_parquet(pt.BATCH_LINES)
    #     .join(pl.read_parquet(pt.BATCH_HEADS), how="inner", on="ID_BATCH")
    #     .filter(pl.col("ID_BATCH") == pl.lit(last_batch))
    # )
    # print(DimAccount(ID_BATCH=last_batch).all.collect())

    # print(pl.read_parquet(pt.STATEMENT_HEADS))
    # print(pl.read_parquet(pt.STATEMENT_LINES))

    # db.BatchHeads().truncate()
    # db.BatchLines().truncate()
    # print(pl.read_parquet(pt.STATEMENT_HEADS))
    # print(pl.read_parquet(pt.STATEMENT_LINES))
    # print(pl.read_parquet(pt.CAB))
    # print(rp.GapReport().all)


if __name__ == "__main__":
    pl.Config.set_tbl_rows(500)
    pl.Config.set_tbl_cols(55)
    pl.Config.set_fmt_str_lengths(60)
    main()
