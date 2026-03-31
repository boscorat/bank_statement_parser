from pathlib import Path

# from bank_statement_parser import get_exchange_rates
# import bank_statement_parser as bsp
from bank_statement_parser.modules import statements


def main():
    project_path = Path("/home/boscorat/Projects/bsp_project")
    # bsp.anonymise_pdf(Path("/home/boscorat/Projects/tsb_spend_and_save_example_1.pdf"))
    # laptop
    # folder = Path("/Users/boscorat/Library/CloudStorage/OneDrive-Personal/OpenStan/Statements/HSBC/2024")
    folder = Path("/home/boscorat/Downloads/2025")
    # folder = Path("/home/boscorat/repos/bank_statement_parser/tests/pdfs/bad")
    include_subdirs = True  # set True to also include one level of subdirectories

    pdfs = [f for f in folder.iterdir() if f.is_file() and f.suffix == ".pdf"]
    if include_subdirs:
        for subdir in folder.iterdir():
            if subdir.is_dir():
                for subsubdir in subdir.iterdir():
                    if subsubdir.is_dir():
                        pdfs.extend(f for f in subsubdir.iterdir() if f.is_file() and f.suffix == ".pdf")
                    else:
                        pdfs.extend(f for f in subdir.iterdir() if f.is_file() and f.suffix == ".pdf")

    batch = statements.StatementBatch(
        pdfs=pdfs,
        turbo=True,
        # project_path=Path("/Users/boscorat/Projects/bsp_project"),
        project_path=Path(project_path),
    )
    print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")
    # batch.debug()

    batch.update_data()
    # get_exchange_rates(project_path=project_path)
    # batch.copy_statements_to_project()
    # batch.delete_temp_files()
    batch.export(filetype="all", filename_timestamp=True, type="multi")  # writes Excel, CSV, and JSON
    batch.export(filetype="reporting")  # writes Excel, CSV, and JSON
    # print(f"total: {batch.duration_secs}, process: {batch.process_secs}, parquet: {batch.parquet_secs}, db: {batch.db_secs}")
    # if batch.errors:
    #     written = batch.debug()
    #     print(f"debug: {written} file(s) written to project/log/debug/")

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


if __name__ == "__main__":
    main()
