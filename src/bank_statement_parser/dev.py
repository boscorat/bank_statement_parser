from pathlib import Path

from bank_statement_parser.modules import statements


def main():
    # laptop
    batch = statements.StatementBatch(
        pdfs=[
            file
            for file in Path("/home/boscorat/repos/bank_statement_parser/tests/pdfs/bad").iterdir()
            if file.is_file() and file.suffix == ".pdf"
        ],
        turbo=False,
        # project_path=Path("/home/boscorat/Projects/Telford"),
    )
    print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")
    batch.update_parquet()
    # batch.update_db(db_path=Path("/home/boscorat/repos/bank_statement_parser/src/bank_statement_parser/project/database/project.db"))
    batch.update_db()
    batch.copy_statements_to_project()
    batch.delete_temp_files()
    print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")

    # #windows
    # statements.StatementBatch(Path("C:\\Users\\Admin\\repos\\bsp\\stmts"), turbo=True, smart_rename=False)

    # last_batch = pl.read_parquet(pt.BATCH_HEADS).select(pl.col("ID_BATCH").last()).item()
    # rpt_statement = DimStatement(folder=Path("~/Projects/Jason/parquet")).all.collect()
    # print(rpt_statement)
    # print(pl.read_parquet(pt.BATCH_HEADS))
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


# main()
