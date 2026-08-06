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
test_import — syntax and importability smoke tests.

Validates that:
- Every ``.py`` file in the package parses without SyntaxErrors (via
  ``ast.parse``), catching indentation bugs and other parse failures
  regardless of whether optional dependencies are installed.
- The top-level package is importable at runtime.
"""

import ast
from pathlib import Path

import pytest

_PKG_DIR = Path(__file__).resolve().parent.parent / "src" / "bank_statement_parser"


class TestSyntax:
    """Verify all source files are syntactically valid Python."""

    @pytest.mark.parametrize(
        "py_file",
        sorted(_PKG_DIR.rglob("*.py")),
        ids=lambda p: str(p.relative_to(_PKG_DIR)),
    )
    def test_module_parses(self, py_file: Path) -> None:
        """Each .py file must parse without SyntaxErrors."""
        ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))


def test_package_importable() -> None:
    """The top-level package must be importable without errors."""
    import bank_statement_parser  # noqa: F401
