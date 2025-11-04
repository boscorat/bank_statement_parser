from pathlib import Path

# root
SRC = Path(__file__).parent.parent.parent
# level 1
BSP = SRC.joinpath("bank_statement_parser")
EXPORTS = SRC.joinpath("exports")
USER_CONFIG = SRC.joinpath("user_config")
# level 2
# exports
CSV = EXPORTS.joinpath("csv")
EXCEL = EXPORTS.joinpath("excel")
JSON = EXPORTS.joinpath("json")
LOGS = EXPORTS.joinpath("logs")
PARQUET = EXPORTS.joinpath("parquet")
# bsp
BASE_CONFIG = BSP.joinpath("base_config")
MODULES = BSP.joinpath("modules")
TESTS = BSP.joinpath("tests")
# level 3
# modules
CLASSES = MODULES.joinpath("classes")
FUNCTIONS = MODULES.joinpath("functions")
# files
# parquet
CAB = PARQUET.joinpath("checks_and_balances.parquet")
BATCH_HEADS = PARQUET.joinpath("batch_heads.parquet")
BATCH_LINES = PARQUET.joinpath("batch_lines.parquet")
STATEMENT_HEADS = PARQUET.joinpath("statement_heads.parquet")
STATEMENT_LINES = PARQUET.joinpath("statement_lines.parquet")


def CAB_TEMP(id: int):
    return PARQUET.joinpath(f"checks_and_balances_{str(id)}.parquet")


def BATCH_LINES_TEMP(id: int):
    return PARQUET.joinpath(f"batch_lines_{str(id)}.parquet")


def STATEMENT_HEADS_TEMP(id: int):
    return PARQUET.joinpath(f"statement_heads_{str(id)}.parquet")


def STATEMENT_LINES_TEMP(id: int):
    return PARQUET.joinpath(f"statement_lines_{str(id)}.parquet")


# logs
LOG_ERROR = LOGS.joinpath("error.parquet")
LOG_PERF = LOGS.joinpath("perf.parquet")


# dynamic
