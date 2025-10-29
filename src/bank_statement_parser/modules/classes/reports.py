from pathlib import Path

dir_exports = (Path(__file__).parent.parent.parent.parent).joinpath("exports")
dir_parquet = dir_exports.joinpath("parquet")
