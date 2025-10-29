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
PATH_DIM_STATEMENT = PATH_PARQUET.joinpath("DIM_Statement.parquet")
PATH_FACT_TRANSACTION = PATH_PARQUET.joinpath("FACT_Statement.parquet")

# # CONSTANTS
# NUMBERS_GBP = r"^[£]?[\s]*[-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.)?\d+$"
# NUMBERS_USD = r"^[$]?[\s]*[-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.)?\d+$"
# NUMBERS_EURO = r"^[-]?\d{1,3}(?:\.\d{3})*(?:,\d+)?$|^\d+(?:,)?\d+[EUR]?$"
# STRIP_GBP_USD = ["$", "£", " ", ",", "\n"]
# STRIP_EURO = ["EUR", " ", ",", "\n"]

# PATHS
