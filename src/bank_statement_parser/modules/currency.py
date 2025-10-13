from bank_statement_parser.modules.classes.data_definitions import CurrencySpec

currency_dict: dict = {
    "GBP": {
        "symbols": ["GBP", "£"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[£]?[\s]*[-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.)?\d+[ ]?[GBP]?$",
    },
    "USD": {
        "symbols": ["USD", "$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[$]?[\s]*[-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.)?\d+[ ]?[USD]?$",
    },
    "EUR": {
        "symbols": ["EUR", "EURO", "EUROS"],
        "seperator_decimal": ",",
        "seperators_thousands": [".", " "],
        "round_decimals": 2,
        "pattern": r"^[\s]*[-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.)?\d+[ ]?[EUR[O[S]?]?]?$",
    },
}

currency_spec = {k: CurrencySpec(**v) for k, v in currency_dict.items()}
