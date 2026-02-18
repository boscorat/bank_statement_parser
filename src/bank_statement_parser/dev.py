from pathlib import Path

import polars as pl

import bank_statement_parser.modules.paths as pt
import bank_statement_parser.modules.reports as rp
from bank_statement_parser.modules import statements


def main():
    # laptop
    batch = statements.StatementBatch(
        pdfs=[file for file in Path("/home/boscorat/Downloads/2024").iterdir() if file.is_file() and file.suffix == ".pdf"],
        turbo=True,
        smart_rename=False,
        config_path=Path("/home/boscorat/repos/bank_statement_parser/src/bank_statement_parser/base_config"),
    )
    print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")
    batch.update_parquet()
    print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")
    # print(
    #     f"pdfs: {batch.pdf_count}, queued: {batch.queued_count}, processed: {batch.processed_count}, parquet: {batch.parquet_written_count}, db: {batch.db_written_count}"
    # )
    # #windows
    # statements.StatementBatch(Path("C:\\Users\\Admin\\repos\\bsp\\stmts"), turbo=True, smart_rename=False)

    # last_batch = pl.read_parquet(pt.BATCH_HEADS).select(pl.col("ID_BATCH").last()).item()
    print(pl.read_parquet(pt.BATCH_HEADS))
    # print(
    #     pl.read_parquet(pt.BATCH_LINES)
    #     .join(pl.read_parquet(pt.BATCH_HEADS), how="semi", on="ID_BATCH")
    #     .filter(pl.col("ID_BATCH") == pl.lit(last_batch))
    # )
    # print(rp.DimAccount(ID_BATCH=last_batch).all.collect())
    # export_excel(pt.EXCEL.joinpath("test_simple.xlsx"), type="simple", ID_BATCH=last_batch)
    # export_excel(pt.EXCEL.joinpath("test.xlsx"), type="full", ID_BATCH=last_batch)
    # export_csv(pt.CSV.joinpath("test_simple.csv"), type="simple", ID_BATCH=last_batch)
    # export_db_mssql(type="full")
    # export_csv(folder=pt.CSV, type="full", ID_BATCH=last_batch)

    # print(pl.read_parquet(pt.STATEMENT_HEADS))


# print(pl.read_parquet(pt.STATEMENT_LINES))

# db.BatchHeads().truncate()
# db.BatchLines().truncate()
# print(pl.read_parquet(pt.STATEMENT_HEADS))
# print(pl.read_parquet(pt.STATEMENT_LINES))
# print(pl.read_parquet(pt.CAB))
# print(rp.GapReport().all)


main()
