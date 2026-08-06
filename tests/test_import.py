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
test_import — smoke test verifying the package is importable.

Catches SyntaxErrors, circular imports, and missing dependencies that would
prevent ``import bank_statement_parser`` from succeeding.
"""


def test_package_importable() -> None:
    """The top-level package must be importable without errors."""
    import bank_statement_parser  # noqa: F401
