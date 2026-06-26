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

"""
Public interface for the bank_statement_parser.data sub-package.

Exposes:
    build_datamart   -- rebuild the SQLite star-schema mart tables
    create_db        -- create (or recreate) the raw SQLite database
    Housekeeping     -- orphan-detection and cascaded-delete helper
"""

from bank_statement_parser.data.build_datamart import build_datamart
from bank_statement_parser.data.create_project_db import main as create_db
from bank_statement_parser.data.housekeeping import Housekeeping

__all__ = [
    "build_datamart",
    "create_db",
    "Housekeeping",
]
