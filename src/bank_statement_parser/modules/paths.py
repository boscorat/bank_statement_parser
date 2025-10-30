from pathlib import Path

# root
PATH_SRC = Path(__file__).parent.parent.parent
# level 1
PATH_BSP = PATH_SRC.joinpath("bank_statement_parser")
PATH_EXPORTS = PATH_SRC.joinpath("exports")
PATH_USER_CONFIG = PATH_SRC.joinpath("user_config")
# level 2
# exports
PATH_CSV = PATH_EXPORTS.joinpath("csv")
PATH_EXCEL = PATH_EXPORTS.joinpath("excel")
PATH_JSON = PATH_EXPORTS.joinpath("json")
PATH_LOGS = PATH_EXPORTS.joinpath("logs")
PATH_PARQUET = PATH_EXPORTS.joinpath("parquet")
# bsp
PATH_BASE_CONFIG = PATH_BSP.joinpath("base_config")
PATH_MODULES = PATH_BSP.joinpath("modules")
PATH_TESTS = PATH_BSP.joinpath("tests")
# level 3
# modules
PATH_CLASSES = PATH_MODULES.joinpath("classes")
PATH_FUNCTIONS = PATH_MODULES.joinpath("functions")
# files
# parquet
PATH_BATCH_HEADS = PATH_PARQUET.joinpath("batch_heads.parquet")
PATH_BATCH_LINES = PATH_PARQUET.joinpath("batch_lines.parquet")
PATH_INVOICE_HEADS = PATH_PARQUET.joinpath("invoice_heads.parquet")
PATH_INVOICE_LINES = PATH_PARQUET.joinpath("invoice_lines.parquet")

PATH_DIM_STATEMENT = PATH_PARQUET.joinpath("DIM_Statement.parquet")
PATH_FACT_TRANSACTION = PATH_PARQUET.joinpath("FACT_Transaction.parquet")
PATH_FACT_BALANCE_DAILY = PATH_PARQUET.joinpath("DIM_Balance_Daily.parquet")
PATH_FACT_BALANCE_MONTHLY = PATH_PARQUET.joinpath("DIM_Balance_Monthly.parquet")
PATH_FACT_BALANCE_YEARLY = PATH_PARQUET.joinpath("DIM_Balance_Yearly.parquet")
PATH_DIM_BATCH = PATH_PARQUET.joinpath("DIM_Batch.parquet")
PATH_FACT_BATCH = PATH_PARQUET.joinpath("FACT_Batch.parquet")
# logs
PATH_CAB = PATH_LOGS.joinpath("CAB.parquet")
PATH_LOG_ERROR = PATH_LOGS.joinpath("error.parquet")
PATH_LOG_PERF = PATH_LOGS.joinpath("perf.parquet")
