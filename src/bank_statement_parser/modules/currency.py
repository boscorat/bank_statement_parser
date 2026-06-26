# This file is part of bank_statement_parser.
#
# Copyright (c) 2026 Jason Farrar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from bank_statement_parser.modules.data import CurrencySpec

currency_dict: dict = {
    "GBP": {
        "name": "British Pound Sterling",
        "symbols": ["GBP", "£"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "USD": {
        "name": "United States Dollar",
        "symbols": ["USD", "$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "EUR": {
        "name": "Euro",
        "symbols": ["EUR", "EURO", "EUROS", "€"],
        "seperator_decimal": ",",
        "seperators_thousands": [".", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[,][\d]{2}$",
    },
    "PHP": {
        "name": "Philippine Peso",
        "symbols": ["PHP", "₱"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "JPY": {
        "name": "Japanese Yen",
        "symbols": ["JPY", "¥", "円"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 0,
        "pattern": r"^[\d]+$",
    },
    "CHF": {
        "name": "Swiss Franc",
        "symbols": ["CHF", "Fr", "SFr"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " ", "'"],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "AUD": {
        "name": "Australian Dollar",
        "symbols": ["AUD", "A$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "CAD": {
        "name": "Canadian Dollar",
        "symbols": ["CAD", "C$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "SGD": {
        "name": "Singapore Dollar",
        "symbols": ["SGD", "S$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "HKD": {
        "name": "Hong Kong Dollar",
        "symbols": ["HKD", "HK$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "AED": {
        "name": "UAE Dirham",
        "symbols": ["AED", "د.إ"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "SAR": {
        "name": "Saudi Riyal",
        "symbols": ["SAR", "﷼", "SR"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "CNY": {
        "name": "Chinese Yuan Renminbi",
        "symbols": ["CNY", "RMB", "元", "¥"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "NZD": {
        "name": "New Zealand Dollar",
        "symbols": ["NZD", "NZ$"],
        "seperator_decimal": ".",
        "seperators_thousands": [",", " "],
        "round_decimals": 2,
        "pattern": r"^[\d]+[.][\d]{2}$",
    },
    "IDR": {
        "name": "Indonesian Rupiah",
        "symbols": ["IDR", "Rp"],
        "seperator_decimal": ",",
        "seperators_thousands": [".", " "],
        "round_decimals": 0,
        "pattern": r"^[\d]+$",
    },
}

currency_spec = {k: CurrencySpec(**v) for k, v in currency_dict.items()}
