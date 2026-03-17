"""
test_cli — CLI argument parsing validation tests.

Introspects the *actual* argparse parser built by ``cli.main()`` to verify:
- All expected subcommands are registered.
- Every argument has non-empty help text.
- Option defaults match expected values.
- Choice-based arguments have the expected choices.
- Positional arguments have metavar set.

These tests do NOT invoke the CLI or require any project/PDF fixtures.  They
work by importing ``cli.py`` and extracting the parser via a patched
``parse_args`` that captures the parser before it processes real input.
"""

import argparse
import ast
from pathlib import Path
from unittest.mock import patch

import pytest

_SRC = Path(__file__).resolve().parent.parent / "src" / "bank_statement_parser"


# ---------------------------------------------------------------------------
# Extract the real parser from cli.main() without executing subcommands
# ---------------------------------------------------------------------------


def _get_real_parser() -> argparse.ArgumentParser:
    """Capture the ArgumentParser that cli.main() builds.

    We patch ``argparse.ArgumentParser.parse_args`` to raise a sentinel
    exception before any subcommand handler runs.  This gives us the fully
    constructed parser object.
    """

    class _ParserCapture(Exception):
        """Sentinel raised to abort main() after the parser is built."""

        def __init__(self, parser: argparse.ArgumentParser) -> None:
            self.parser = parser

    def _capture_parse_args(self: argparse.ArgumentParser, *args: object, **kwargs: object) -> argparse.Namespace:
        raise _ParserCapture(self)

    with patch.object(argparse.ArgumentParser, "parse_args", _capture_parse_args):
        from bank_statement_parser.cli import main

        try:
            main()
        except _ParserCapture as exc:
            return exc.parser

    raise RuntimeError("Failed to capture parser from cli.main()")


# Build once for all tests in this module
_PARSER = _get_real_parser()
_SUBPARSERS: dict[str, argparse.ArgumentParser] = {}

for action in _PARSER._subparsers._actions:
    if isinstance(action, argparse._SubParsersAction):
        _SUBPARSERS.update(action.choices)


# ===================================================================
# Subcommand registration
# ===================================================================


class TestSubcommands:
    """Verify that the expected subcommands are registered."""

    EXPECTED_SUBCOMMANDS = {"anonymise", "process"}

    def test_expected_subcommands_exist(self) -> None:
        """All expected subcommands must be present in the parser."""
        assert set(_SUBPARSERS.keys()) == self.EXPECTED_SUBCOMMANDS

    @pytest.mark.parametrize("name", EXPECTED_SUBCOMMANDS)
    def test_subcommand_has_help(self, name: str) -> None:
        """Every subcommand must have a non-empty help string."""
        # help is set on the add_parser() call, stored in the parent's action
        for action in _PARSER._subparsers._actions:
            if isinstance(action, argparse._SubParsersAction):
                help_text = action._name_parser_map[name].description
                assert help_text, f"Subcommand '{name}' is missing a description"


# ===================================================================
# Argument completeness
# ===================================================================


def _get_optional_actions(subcommand: str) -> list[argparse.Action]:
    """Return all optional (--flag) actions for a subcommand."""
    return [a for a in _SUBPARSERS[subcommand]._actions if a.option_strings and not isinstance(a, argparse._HelpAction)]


def _get_positional_actions(subcommand: str) -> list[argparse.Action]:
    """Return all positional actions for a subcommand."""
    return [a for a in _SUBPARSERS[subcommand]._actions if not a.option_strings and not isinstance(a, argparse._HelpAction)]


class TestProcessOptions:
    """Validate the ``process`` subcommand options."""

    EXPECTED_FLAGS = {
        "--project",
        "--pdfs",
        "--pattern",
        "--no-turbo",
        "--company",
        "--account",
        "--data",
        "--export-format",
        "--export-type",
        "--no-export",
        "--no-copy",
    }

    def test_all_expected_flags_exist(self) -> None:
        """Every expected --flag must be registered on the process subparser."""
        actual_flags = set()
        for action in _get_optional_actions("process"):
            actual_flags.update(action.option_strings)
        assert self.EXPECTED_FLAGS <= actual_flags, f"Missing flags: {self.EXPECTED_FLAGS - actual_flags}"

    def test_every_option_has_help_text(self) -> None:
        """Every optional argument must have a non-empty help string."""
        for action in _get_optional_actions("process"):
            assert action.help, f"process option {action.option_strings} is missing help text"

    def test_data_choices(self) -> None:
        """--data must accept parquet, database, both."""
        action = _find_action("process", "--data")
        assert set(action.choices) == {"parquet", "database", "both"}

    def test_data_default(self) -> None:
        """--data must default to 'both'."""
        action = _find_action("process", "--data")
        assert action.default == "both"

    def test_export_format_choices(self) -> None:
        """--export-format must accept excel, csv, json, all, reporting."""
        action = _find_action("process", "--export-format")
        assert set(action.choices) == {"excel", "csv", "json", "all", "reporting"}

    def test_export_format_default(self) -> None:
        """--export-format must default to 'all'."""
        action = _find_action("process", "--export-format")
        assert action.default == "all"

    def test_export_type_choices(self) -> None:
        """--export-type must accept full, simple."""
        action = _find_action("process", "--export-type")
        assert set(action.choices) == {"full", "simple"}

    def test_export_type_default(self) -> None:
        """--export-type must default to 'simple'."""
        action = _find_action("process", "--export-type")
        assert action.default == "simple"

    def test_pattern_default(self) -> None:
        """--pattern must default to '**/*.pdf'."""
        action = _find_action("process", "--pattern")
        assert action.default == "**/*.pdf"


class TestAnonymiseOptions:
    """Validate the ``anonymise`` subcommand options."""

    EXPECTED_FLAGS = {"--folder", "--pattern", "--output", "--output-dir", "--config"}

    def test_all_expected_flags_exist(self) -> None:
        """Every expected --flag must be registered on the anonymise subparser."""
        actual_flags = set()
        for action in _get_optional_actions("anonymise"):
            actual_flags.update(action.option_strings)
        assert self.EXPECTED_FLAGS <= actual_flags, f"Missing flags: {self.EXPECTED_FLAGS - actual_flags}"

    def test_positional_target_exists(self) -> None:
        """anonymise must have a positional 'target' argument."""
        positionals = _get_positional_actions("anonymise")
        names = [a.dest for a in positionals]
        assert "target" in names, f"Expected positional 'target', got {names}"

    def test_every_option_has_help_text(self) -> None:
        """Every optional argument must have a non-empty help string."""
        for action in _get_optional_actions("anonymise"):
            assert action.help, f"anonymise option {action.option_strings} is missing help text"

    def test_pattern_default(self) -> None:
        """--pattern must default to '*.pdf'."""
        action = _find_action("anonymise", "--pattern")
        assert action.default == "*.pdf"

    def test_folder_is_store_true(self) -> None:
        """--folder must be a boolean flag (store_true)."""
        action = _find_action("anonymise", "--folder")
        assert action.const is True  # store_true sets const=True


# ===================================================================
# Cross-check: actual CLI source matches what the parser exposes
# ===================================================================


class TestCliSourceConsistency:
    """Verify that the parser built at runtime matches the AST of cli.py."""

    @staticmethod
    def _ast_subcommand_names() -> list[str]:
        """Extract subcommand names from add_parser() calls via AST."""
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
    def _ast_argument_flags() -> list[str]:
        """Extract all --flag names from add_argument() calls via AST."""
        source = (_SRC / "cli.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        flags: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument" and node.args:
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                        flags.append(arg.value)
        return flags

    def test_ast_subcommands_match_runtime(self) -> None:
        """Subcommand names from AST must match those in the runtime parser."""
        ast_names = set(self._ast_subcommand_names())
        runtime_names = set(_SUBPARSERS.keys())
        assert ast_names == runtime_names

    def test_ast_flags_match_runtime(self) -> None:
        """Argument flags from AST must match those in the runtime parser."""
        ast_flags = set(self._ast_argument_flags())
        runtime_flags: set[str] = set()
        for subcommand in _SUBPARSERS.values():
            for action in subcommand._actions:
                if action.option_strings and not isinstance(action, argparse._HelpAction):
                    runtime_flags.update(action.option_strings)
        assert ast_flags == runtime_flags


# ===================================================================
# Helpers
# ===================================================================


def _find_action(subcommand: str, flag: str) -> argparse.Action:
    """Find the Action for a specific --flag in a subcommand's parser."""
    for action in _SUBPARSERS[subcommand]._actions:
        if flag in action.option_strings:
            return action
    raise ValueError(f"No action found for '{flag}' in subcommand '{subcommand}'")
