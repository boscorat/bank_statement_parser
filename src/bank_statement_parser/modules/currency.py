from bank_statement_parser.modules.classes.data import CurrencySpec

currency_dict: dict = {
    "GBP": {
        "symbols": ["GBP", "£"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "USD": {
        "symbols": ["USD", "$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "EUR": {
        "symbols": ["EUR", "EURO", "EUROS"],
        "seperator_decimal": ",",
        "seperators_thousands": [".", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[,][\d]{2}$",
    },
}

currency_spec = {k: CurrencySpec(**v) for k, v in currency_dict.items()}
