"""
test_docs — documentation freshness and accuracy tests.

Validates that:
- Generated documentation files are up-to-date with the source code (running
  each generator and comparing its output to the committed ``.md`` file).
- Every symbol in ``__all__`` appears in the Python API reference page.
- Every CLI subcommand and option appears in the CLI reference page.

These tests do NOT require any project fixtures or PDF files.
"""

import ast
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = _REPO_ROOT / "scripts"
_SRC = _REPO_ROOT / "src" / "bank_statement_parser"
_DOCS = _REPO_ROOT / "docs"

# Ensure scripts/ is importable
sys.path.insert(0, str(_SCRIPTS))
import generate_docs  # noqa: E402


# ===================================================================
# Freshness tests — generated output matches committed files
# ===================================================================


class TestDocsFreshness:
    """Verify that committed .md files match what generate_docs.py produces."""

    @pytest.mark.parametrize(
        "output_path, generator",
        generate_docs._PAGES,
        ids=[p.stem for p, _ in generate_docs._PAGES],
    )
    def test_generated_page_is_up_to_date(self, output_path: Path, generator: object) -> None:
        """Each committed doc page must match its generator's current output."""
        assert output_path.exists(), f"Missing generated file: {output_path.relative_to(_REPO_ROOT)}"
        committed = output_path.read_text(encoding="utf-8")
        generated = generator()  # type: ignore[operator]
        assert committed == generated, f"{output_path.relative_to(_REPO_ROOT)} is stale — re-run: python scripts/generate_docs.py"

    def test_all_pages_have_do_not_edit_header(self) -> None:
        """Every generated page must start with the DO NOT EDIT comment."""
        for output_path, _ in generate_docs._PAGES:
            assert output_path.exists(), f"Missing: {output_path.relative_to(_REPO_ROOT)}"
            first_line = output_path.read_text(encoding="utf-8").split("\n", 1)[0]
            assert "DO NOT EDIT" in first_line, f"{output_path.relative_to(_REPO_ROOT)} missing DO NOT EDIT header"


# ===================================================================
# API coverage — every __all__ symbol appears in python-api.md
# ===================================================================


class TestApiCoverage:
    """Verify that the Python API reference documents every public symbol."""

    @staticmethod
    def _parse_all_symbols() -> list[str]:
        """Extract the list of names from __all__ in __init__.py."""
        source = (_SRC / "__init__.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "__all__":
                        if isinstance(node.value, ast.List):
                            return [
                                elt.value  # type: ignore[union-attr]
                                for elt in node.value.elts
                                if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                            ]
        return []

    def test_every_all_symbol_in_api_docs(self) -> None:
        """Every symbol listed in __all__ must appear in python-api.md."""
        api_md = (_DOCS / "reference" / "python-api.md").read_text(encoding="utf-8")
        symbols = self._parse_all_symbols()
        assert symbols, "__all__ is empty — this should never happen"

        missing = [s for s in symbols if s not in api_md]
        assert not missing, f"Symbols in __all__ but missing from python-api.md: {missing}"


# ===================================================================
# CLI coverage — every subcommand and option appears in cli.md
# ===================================================================


class TestCliCoverage:
    """Verify that the CLI reference documents every subcommand and option."""

    @staticmethod
    def _parse_subcommand_names() -> list[str]:
        """Extract subcommand names from add_parser() calls in cli.py."""
        source = (_SRC / "cli.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        names: list[str] = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_parser"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                names.append(node.args[0].value)
        return names

    @staticmethod
    def _parse_argument_flags() -> list[str]:
        """Extract all argument flag names (e.g. '--project', '--pdfs') from cli.py."""
        source = (_SRC / "cli.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        flags: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument" and node.args:
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        flags.append(arg.value)
        return flags

    def test_every_subcommand_in_cli_docs(self) -> None:
        """Every subcommand from add_parser() must appear in cli.md."""
        cli_md = (_DOCS / "reference" / "cli.md").read_text(encoding="utf-8")
        subcommands = self._parse_subcommand_names()
        assert subcommands, "No subcommands found in cli.py — parsing error?"

        missing = [s for s in subcommands if s not in cli_md]
        assert not missing, f"Subcommands in cli.py but missing from cli.md: {missing}"

    def test_every_option_flag_in_cli_docs(self) -> None:
        """Every --flag from add_argument() must appear in cli.md."""
        cli_md = (_DOCS / "reference" / "cli.md").read_text(encoding="utf-8")
        flags = self._parse_argument_flags()
        assert flags, "No argument flags found in cli.py — parsing error?"

        # Only check flags that start with -- (skip positional args like "target")
        option_flags = [f for f in flags if f.startswith("--")]
        missing = [f for f in option_flags if f not in cli_md]
        assert not missing, f"Option flags in cli.py but missing from cli.md: {missing}"
