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

"""Tests for the expression language (lexer, parser, evaluator, validation).

Run with:
    pytest tests/test_expression_evaluator.py -v
"""

import warnings
from datetime import date
from typing import Any

import polars as pl
import pytest

from bank_statement_parser.modules.errors import ConfigError
from bank_statement_parser.modules.expression_evaluator import (
    ArrayIndexNode,
    ASTNode,
    BinaryOpNode,
    CaseNode,
    ColumnRefNode,
    ExpressionEvaluator,
    ExpressionLexer,
    ExpressionParser,
    FunctionCallNode,
    LiteralNode,
    TokenType,
    UnaryOpNode,
    collect_expression_column_refs,
    validate_expression,
)

FLOAT_TOL = 0.005


def _eval(expr_str: str, df: pl.DataFrame, schema: dict[str, Any] | None = None) -> pl.Series:
    """Helper: parse expr_str, evaluate against df, return first result column."""
    _schema = schema if schema is not None else dict(df.schema)
    tokens = ExpressionLexer.tokenize(expr_str)
    ast = ExpressionParser(tokens).parse()
    ev = ExpressionEvaluator(_schema)
    polars_expr = ev.evaluate(ast)
    return df.with_columns(polars_expr.alias("_result"))["_result"]


# ===========================================================================
# TestExpressionLexer
# ===========================================================================


class TestExpressionLexer:
    """Unit tests for ExpressionLexer.tokenize()."""

    def test_simple_identifier_tokenised(self) -> None:
        """A bare identifier produces one IDENTIFIER token followed by EOF."""
        tokens = ExpressionLexer.tokenize("my_col")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "my_col"
        assert tokens[-1].type == TokenType.EOF

    def test_keyword_case_insensitive(self) -> None:
        """Keywords are recognised regardless of case."""
        for kw in ("case", "CASE", "Case", "cAsE"):
            tokens = ExpressionLexer.tokenize(kw)
            assert tokens[0].type == TokenType.CASE, f"failed for {kw}"

    def test_all_keywords_recognised(self) -> None:
        """All reserved keywords produce their own token type."""
        pairs = [
            ("WHEN", TokenType.WHEN),
            ("THEN", TokenType.THEN),
            ("ELSE", TokenType.ELSE),
            ("END", TokenType.END),
            ("IN", TokenType.IN),
            ("AND", TokenType.AND),
            ("OR", TokenType.OR),
            ("NOT", TokenType.NOT),
            ("NULL", TokenType.NULL),
            ("AS", TokenType.AS),
        ]
        for kw, expected in pairs:
            tokens = ExpressionLexer.tokenize(kw)
            assert tokens[0].type == expected, f"keyword {kw!r} expected {expected}"

    def test_string_literal(self) -> None:
        """Single-quoted string literal is tokenised with its value."""
        tokens = ExpressionLexer.tokenize("'hello world'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "hello world"

    def test_string_escape_single_quote(self) -> None:
        """Escaped single quote inside a string literal."""
        tokens = ExpressionLexer.tokenize(r"'it\'s fine'")
        assert tokens[0].value == "it's fine"

    def test_string_escape_backslash(self) -> None:
        """Escaped backslash inside a string literal."""
        tokens = ExpressionLexer.tokenize(r"'back\\slash'")
        assert tokens[0].value == "back\\slash"

    def test_string_escape_newline(self) -> None:
        """\\n escape produces a newline character."""
        tokens = ExpressionLexer.tokenize(r"'line1\nline2'")
        assert tokens[0].value == "line1\nline2"

    def test_string_escape_tab(self) -> None:
        """\\t escape produces a tab character."""
        tokens = ExpressionLexer.tokenize(r"'col1\tcol2'")
        assert tokens[0].value == "col1\tcol2"

    def test_integer_literal(self) -> None:
        """Integer literal token."""
        tokens = ExpressionLexer.tokenize("42")
        assert tokens[0].type == TokenType.NUMBER
        assert tokens[0].value == "42"

    def test_float_literal(self) -> None:
        """Float literal token."""
        tokens = ExpressionLexer.tokenize("3.14")
        assert tokens[0].type == TokenType.NUMBER
        assert tokens[0].value == "3.14"

    def test_operators_tokenised(self) -> None:
        """All single and double-char operators are tokenised correctly."""
        cases = [
            ("+", TokenType.PLUS),
            ("-", TokenType.MINUS),
            ("*", TokenType.STAR),
            ("/", TokenType.SLASH),
            ("=", TokenType.EQ),
            ("<>", TokenType.NEQ),
            ("<", TokenType.LT),
            (">", TokenType.GT),
            ("<=", TokenType.LTE),
            (">=", TokenType.GTE),
        ]
        for src, expected in cases:
            tokens = ExpressionLexer.tokenize(src)
            assert tokens[0].type == expected, f"{src!r} → {tokens[0].type}"

    def test_punctuation_tokenised(self) -> None:
        """Parentheses, brackets, and comma are tokenised."""
        tokens = ExpressionLexer.tokenize("([,])")
        types = [t.type for t in tokens[:-1]]  # exclude EOF
        assert types == [TokenType.LPAREN, TokenType.LBRACKET, TokenType.COMMA, TokenType.RBRACKET, TokenType.RPAREN]

    def test_whitespace_skipped(self) -> None:
        """Whitespace between tokens is ignored."""
        tokens = ExpressionLexer.tokenize("  a  +  b  ")
        assert [t.type for t in tokens] == [TokenType.IDENTIFIER, TokenType.PLUS, TokenType.IDENTIFIER, TokenType.EOF]

    def test_unclosed_string_raises(self) -> None:
        """An unclosed string literal raises ConfigError."""
        with pytest.raises(ConfigError, match="unclosed string"):
            ExpressionLexer.tokenize("'hello")

    def test_invalid_escape_raises(self) -> None:
        """An invalid escape sequence raises ConfigError with suggestion."""
        with pytest.raises(ConfigError, match="invalid escape"):
            ExpressionLexer.tokenize(r"'\q'")

    def test_unknown_character_raises(self) -> None:
        """An unknown character raises ConfigError with position."""
        with pytest.raises(ConfigError, match="unexpected character"):
            ExpressionLexer.tokenize("a @ b")

    def test_malformed_number_exponent_raises(self) -> None:
        """A number with an exponent but no digits raises ConfigError."""
        with pytest.raises(ConfigError, match="malformed number"):
            ExpressionLexer.tokenize("1e")

    def test_position_reported_in_token(self) -> None:
        """Token carries the correct source position."""
        tokens = ExpressionLexer.tokenize("  foo")
        assert tokens[0].pos == 2

    def test_eof_always_last(self) -> None:
        """EOF is always the last token."""
        for expr in ("", "a", "CONCAT(a, b)", "1 + 2"):
            tokens = ExpressionLexer.tokenize(expr)
            assert tokens[-1].type == TokenType.EOF


# ===========================================================================
# TestExpressionParser
# ===========================================================================


class TestExpressionParser:
    """Unit tests for ExpressionParser.parse()."""

    def _parse(self, expr: str) -> ASTNode:
        return ExpressionParser(ExpressionLexer.tokenize(expr)).parse()

    def test_literal_string(self) -> None:
        """String literal parses to LiteralNode."""
        node = self._parse("'hello'")
        assert isinstance(node, LiteralNode)
        assert node.value == "hello"

    def test_literal_integer(self) -> None:
        """Integer literal parses to LiteralNode with int value."""
        node = self._parse("42")
        assert isinstance(node, LiteralNode)
        assert node.value == 42
        assert isinstance(node.value, int)

    def test_literal_float(self) -> None:
        """Float literal parses to LiteralNode with float value."""
        node = self._parse("3.14")
        assert isinstance(node, LiteralNode)
        assert isinstance(node.value, float)

    def test_literal_null(self) -> None:
        """NULL keyword parses to LiteralNode(None)."""
        node = self._parse("NULL")
        assert isinstance(node, LiteralNode)
        assert node.value is None

    def test_column_ref(self) -> None:
        """Unknown identifier parses to ColumnRefNode."""
        node = self._parse("my_column")
        assert isinstance(node, ColumnRefNode)
        assert node.name == "my_column"

    def test_binary_plus(self) -> None:
        """Addition parses to BinaryOpNode."""
        node = self._parse("a + b")
        assert isinstance(node, BinaryOpNode)
        assert node.op == "+"

    def test_binary_comparison(self) -> None:
        """Comparison parses to BinaryOpNode with correct op."""
        for op in ("=", "<>", "<", ">", "<=", ">="):
            node = self._parse(f"a {op} b")
            assert isinstance(node, BinaryOpNode)
            assert node.op == op

    def test_in_operator(self) -> None:
        """IN operator parses to BinaryOpNode(op='IN') with LiteralNode list."""
        node = self._parse("month IN ('Jan', 'Feb', 'Mar')")
        assert isinstance(node, BinaryOpNode)
        assert node.op == "IN"
        assert isinstance(node.right, LiteralNode)
        assert node.right.value == ["Jan", "Feb", "Mar"]

    def test_precedence_mul_before_add(self) -> None:
        """Multiplication binds tighter than addition."""
        node = self._parse("a + b * c")
        assert isinstance(node, BinaryOpNode)
        assert node.op == "+"
        assert isinstance(node.right, BinaryOpNode)
        assert node.right.op == "*"

    def test_precedence_paren_overrides(self) -> None:
        """Parentheses override default precedence."""
        node = self._parse("(a + b) * c")
        assert isinstance(node, BinaryOpNode)
        assert node.op == "*"
        assert isinstance(node.left, BinaryOpNode)
        assert node.left.op == "+"

    def test_unary_minus(self) -> None:
        """Unary minus on a column reference parses to UnaryOpNode."""
        node = self._parse("-col")
        assert isinstance(node, UnaryOpNode)
        assert node.op == "-"

    def test_not_operator(self) -> None:
        """NOT keyword parses to UnaryOpNode."""
        node = self._parse("NOT a")
        assert isinstance(node, UnaryOpNode)
        assert node.op == "NOT"

    def test_and_or_precedence(self) -> None:
        """AND binds tighter than OR."""
        node = self._parse("a OR b AND c")
        assert isinstance(node, BinaryOpNode)
        assert node.op == "OR"
        assert isinstance(node.right, BinaryOpNode)
        assert node.right.op == "AND"

    def test_function_call(self) -> None:
        """Function call parses to FunctionCallNode."""
        node = self._parse("UPPER(name)")
        assert isinstance(node, FunctionCallNode)
        assert node.name == "UPPER"
        assert len(node.args) == 1

    def test_function_call_variadic(self) -> None:
        """CONCAT with multiple args parses correctly."""
        node = self._parse("CONCAT(a, b, c)")
        assert isinstance(node, FunctionCallNode)
        assert node.name == "CONCAT"
        assert len(node.args) == 3

    def test_cast_as_syntax(self) -> None:
        """CAST(expr AS type) is parsed with AS keyword."""
        node = self._parse("CAST(x AS INT)")
        assert isinstance(node, FunctionCallNode)
        assert node.name == "CAST"
        assert len(node.args) == 2
        assert isinstance(node.args[1], LiteralNode)
        assert node.args[1].value == "INT"

    def test_case_searched_form(self) -> None:
        """Searched CASE form parses to CaseNode with no subject."""
        node = self._parse("CASE WHEN a = 1 THEN 'x' ELSE 'y' END")
        assert isinstance(node, CaseNode)
        assert node.subject is None
        assert len(node.when_clauses) == 1
        assert node.else_node is not None

    def test_case_simple_form(self) -> None:
        """Simple CASE form parses to CaseNode with a subject."""
        node = self._parse("CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END")
        assert isinstance(node, CaseNode)
        assert node.subject is not None
        assert len(node.when_clauses) == 2
        assert node.else_node is None

    def test_array_index(self) -> None:
        """Array index postfix operator parses to ArrayIndexNode."""
        node = self._parse("SPLIT(col, '-')[0]")
        assert isinstance(node, ArrayIndexNode)
        assert node.index == 0
        assert isinstance(node.array_expr, FunctionCallNode)

    def test_array_index_negative(self) -> None:
        """Negative array index is parsed correctly."""
        node = self._parse("SPLIT(col, '-')[-1]")
        assert isinstance(node, ArrayIndexNode)
        assert node.index == -1

    def test_unexpected_token_raises(self) -> None:
        """Unexpected token raises ConfigError."""
        with pytest.raises(ConfigError):
            ExpressionParser(ExpressionLexer.tokenize("1 + + 2")).parse()

    def test_unmatched_paren_raises(self) -> None:
        """Unmatched opening parenthesis raises ConfigError."""
        with pytest.raises(ConfigError):
            ExpressionParser(ExpressionLexer.tokenize("(1 + 2")).parse()

    def test_wrong_arg_count_raises(self) -> None:
        """Wrong number of arguments raises ConfigError."""
        with pytest.raises(ConfigError, match="expects"):
            ExpressionParser(ExpressionLexer.tokenize("UPPER(a, b)")).parse()

    def test_case_no_when_raises(self) -> None:
        """CASE with no WHEN clause raises ConfigError."""
        with pytest.raises(ConfigError):
            ExpressionParser(ExpressionLexer.tokenize("CASE ELSE 'x' END")).parse()

    def test_in_list_non_literal_raises(self) -> None:
        """IN list with a non-literal value raises ConfigError."""
        with pytest.raises(ConfigError, match="IN list"):
            ExpressionParser(ExpressionLexer.tokenize("x IN (a, b)")).parse()

    def test_all_registry_functions_parseable(self) -> None:
        """Every function in the registry can be called with its minimum args."""
        # Build a minimal valid call for each function
        minimal: dict[str, str] = {
            "CONCAT": "CONCAT('a')",
            "SUBSTRING": "SUBSTRING(col, 0)",
            "UPPER": "UPPER(col)",
            "LOWER": "LOWER(col)",
            "TRIM": "TRIM(col)",
            "LTRIM": "LTRIM(col)",
            "RTRIM": "RTRIM(col)",
            "LENGTH": "LENGTH(col)",
            "REPLACE": "REPLACE(col, 'a', 'b')",
            "SPLIT": "SPLIT(col, '-')",
            "YEAR": "YEAR(col)",
            "MONTH": "MONTH(col)",
            "DAY": "DAY(col)",
            "MONTH_NAME": "MONTH_NAME(col)",
            "CAST": "CAST(col AS STRING)",
            "COALESCE": "COALESCE(col)",
        }
        for name, expr in minimal.items():
            node = ExpressionParser(ExpressionLexer.tokenize(expr)).parse()
            assert isinstance(node, FunctionCallNode), f"{name}: expected FunctionCallNode"
            assert node.name == name


# ===========================================================================
# TestStringFunctions
# ===========================================================================


class TestStringFunctions:
    """Tests for string function evaluation."""

    def test_upper(self) -> None:
        """UPPER converts all characters to uppercase."""
        df = pl.DataFrame({"name": ["hello", "World"]})
        result = _eval("UPPER(name)", df)
        assert result.to_list() == ["HELLO", "WORLD"]

    def test_lower(self) -> None:
        """LOWER converts all characters to lowercase."""
        df = pl.DataFrame({"name": ["HELLO", "World"]})
        result = _eval("LOWER(name)", df)
        assert result.to_list() == ["hello", "world"]

    def test_trim_whitespace(self) -> None:
        """TRIM removes leading and trailing whitespace."""
        df = pl.DataFrame({"s": ["  hello  ", " world "]})
        result = _eval("TRIM(s)", df)
        assert result.to_list() == ["hello", "world"]

    def test_ltrim(self) -> None:
        """LTRIM removes leading whitespace only."""
        df = pl.DataFrame({"s": ["  hello  "]})
        result = _eval("LTRIM(s)", df)
        assert result[0] == "hello  "

    def test_rtrim(self) -> None:
        """RTRIM removes trailing whitespace only."""
        df = pl.DataFrame({"s": ["  hello  "]})
        result = _eval("RTRIM(s)", df)
        assert result[0] == "  hello"

    def test_trim_specific_chars(self) -> None:
        """TRIM with explicit characters strips those chars from both ends."""
        df = pl.DataFrame({"s": ["***hello***"]})
        result = _eval("TRIM(s, '*')", df)
        assert result[0] == "hello"

    def test_concat_two_strings(self) -> None:
        """CONCAT joins two string columns."""
        df = pl.DataFrame({"a": ["foo"], "b": ["bar"]})
        result = _eval("CONCAT(a, b)", df)
        assert result[0] == "foobar"

    def test_concat_with_separator(self) -> None:
        """CONCAT with literal separator between columns."""
        df = pl.DataFrame({"a": ["foo"], "b": ["bar"]})
        result = _eval("CONCAT(a, ' ', b)", df)
        assert result[0] == "foo bar"

    def test_concat_null_safe(self) -> None:
        """CONCAT treats NULL values as empty strings."""
        df = pl.DataFrame({"a": ["foo", None], "b": ["bar", "baz"]})
        result = _eval("CONCAT(a, b)", df)
        assert result[0] == "foobar"
        assert result[1] == "baz"

    def test_substring_start_only(self) -> None:
        """SUBSTRING with only start index (0-based)."""
        df = pl.DataFrame({"s": ["hello world"]})
        result = _eval("SUBSTRING(s, 6)", df)
        assert result[0] == "world"

    def test_substring_start_and_length(self) -> None:
        """SUBSTRING with start and length."""
        df = pl.DataFrame({"s": ["hello world"]})
        result = _eval("SUBSTRING(s, 0, 5)", df)
        assert result[0] == "hello"

    def test_length(self) -> None:
        """LENGTH returns character count."""
        df = pl.DataFrame({"s": ["hello", ""]})
        result = _eval("LENGTH(s)", df)
        assert result.to_list() == [5, 0]

    def test_replace(self) -> None:
        """REPLACE substitutes all occurrences of a substring."""
        df = pl.DataFrame({"s": ["foo bar foo"]})
        result = _eval("REPLACE(s, 'foo', 'baz')", df)
        assert result[0] == "baz bar baz"

    def test_replace_no_match(self) -> None:
        """REPLACE returns original string when pattern not found."""
        df = pl.DataFrame({"s": ["hello"]})
        result = _eval("REPLACE(s, 'xyz', 'abc')", df)
        assert result[0] == "hello"

    def test_split_returns_list(self) -> None:
        """SPLIT returns a list column."""
        df = pl.DataFrame({"s": ["a-b-c"]})
        tokens = ExpressionLexer.tokenize("SPLIT(s, '-')")
        ast = ExpressionParser(tokens).parse()
        ev = ExpressionEvaluator(dict(df.schema))
        result_df = df.with_columns(ev.evaluate(ast).alias("r"))
        assert result_df["r"][0].to_list() == ["a", "b", "c"]

    def test_split_index_first(self) -> None:
        """SPLIT(col, '-')[0] returns the first element."""
        df = pl.DataFrame({"s": ["a-b-c"]})
        result = _eval("SPLIT(s, '-')[0]", df)
        assert result[0] == "a"

    def test_split_index_last_negative(self) -> None:
        """SPLIT(col, '-')[-1] returns the last element."""
        df = pl.DataFrame({"s": ["a-b-c"]})
        result = _eval("SPLIT(s, '-')[-1]", df)
        assert result[0] == "c"

    def test_split_index_out_of_bounds_null(self) -> None:
        """SPLIT with out-of-bounds index returns NULL."""
        df = pl.DataFrame({"s": ["a-b"]})
        result = _eval("SPLIT(s, '-')[10]", df)
        assert result[0] is None

    def test_upper_trim_chained(self) -> None:
        """Chained UPPER(TRIM(col)) works correctly."""
        df = pl.DataFrame({"s": ["  hello  "]})
        result = _eval("UPPER(TRIM(s))", df)
        assert result[0] == "HELLO"


# ===========================================================================
# TestDateFunctions
# ===========================================================================


class TestDateFunctions:
    """Tests for date extraction functions."""

    def test_year_from_date_column(self) -> None:
        """YEAR extracts year from a Date-typed column."""
        df = pl.DataFrame({"d": [date(2026, 3, 15)]}).with_columns(pl.col("d").cast(pl.Date))
        result = _eval("YEAR(d)", df, {"d": pl.Date})
        assert result[0] == 2026

    def test_month_from_date_column(self) -> None:
        """MONTH extracts month from a Date-typed column."""
        df = pl.DataFrame({"d": [date(2026, 3, 15)]}).with_columns(pl.col("d").cast(pl.Date))
        result = _eval("MONTH(d)", df, {"d": pl.Date})
        assert result[0] == 3

    def test_day_from_date_column(self) -> None:
        """DAY extracts day from a Date-typed column."""
        df = pl.DataFrame({"d": [date(2026, 3, 15)]}).with_columns(pl.col("d").cast(pl.Date))
        result = _eval("DAY(d)", df, {"d": pl.Date})
        assert result[0] == 15

    def test_year_from_iso_string(self) -> None:
        """YEAR extracts year from an ISO date string column."""
        df = pl.DataFrame({"d": ["2026-03-15"]})
        result = _eval("YEAR(d)", df)
        assert result[0] == 2026

    def test_month_from_iso_string(self) -> None:
        """MONTH extracts month from an ISO date string column."""
        df = pl.DataFrame({"d": ["2026-03-15"]})
        result = _eval("MONTH(d)", df)
        assert result[0] == 3

    def test_month_name_from_date_column(self) -> None:
        """MONTH_NAME returns title-case abbreviation from a Date column."""
        df = pl.DataFrame({"d": [date(2026, 1, 1), date(2026, 12, 1)]}).with_columns(pl.col("d").cast(pl.Date))
        result = _eval("MONTH_NAME(d)", df, {"d": pl.Date})
        assert result.to_list() == ["Jan", "Dec"]

    def test_month_name_from_string_abbreviation(self) -> None:
        """MONTH_NAME normalises abbreviations of any case to title-case."""
        df = pl.DataFrame({"m": ["jan", "JAN", "Jan", "FEB", "feb"]})
        result = _eval("MONTH_NAME(m)", df)
        assert result.to_list() == ["Jan", "Jan", "Jan", "Feb", "Feb"]

    def test_month_name_from_full_name(self) -> None:
        """MONTH_NAME accepts full month names and returns abbreviation."""
        df = pl.DataFrame({"m": ["January", "december", "MARCH"]})
        result = _eval("MONTH_NAME(m)", df)
        assert result.to_list() == ["Jan", "Dec", "Mar"]

    def test_month_name_from_integer_string(self) -> None:
        """MONTH_NAME accepts month number as string."""
        df = pl.DataFrame({"m": ["1", "12", "9", "09"]})
        result = _eval("MONTH_NAME(m)", df)
        assert result.to_list() == ["Jan", "Dec", "Sep", "Sep"]

    def test_month_name_invalid_returns_null(self) -> None:
        """MONTH_NAME returns NULL for unrecognised input."""
        df = pl.DataFrame({"m": ["Xyz", "0", "13"]})
        result = _eval("MONTH_NAME(m)", df)
        assert result[0] is None

    def test_month_name_all_months(self) -> None:
        """MONTH_NAME returns correct abbreviation for all 12 months."""
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        expected = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        df = pl.DataFrame({"m": months})
        result = _eval("MONTH_NAME(m)", df)
        assert result.to_list() == expected

    def test_cast_to_string(self) -> None:
        """CAST(x AS STRING) converts integer to string."""
        df = pl.DataFrame({"n": [2026]})
        result = _eval("CAST(n AS STRING)", df)
        assert result[0] == "2026"

    def test_cast_to_int(self) -> None:
        """CAST(x AS INT) converts string to integer."""
        df = pl.DataFrame({"s": ["42"]})
        result = _eval("CAST(s AS INT)", df)
        assert result[0] == 42

    def test_cast_to_float(self) -> None:
        """CAST(x AS FLOAT) converts string to float."""
        df = pl.DataFrame({"s": ["3.14"]})
        result = _eval("CAST(s AS FLOAT)", df)
        assert abs(result[0] - 3.14) < FLOAT_TOL

    def test_cast_unknown_type_raises(self) -> None:
        """CAST with an unrecognised type name raises ConfigError at eval time."""
        df = pl.DataFrame({"n": [1]})
        tokens = ExpressionLexer.tokenize("CAST(n AS BLOB)")
        ast = ExpressionParser(tokens).parse()
        ev = ExpressionEvaluator(dict(df.schema))
        with pytest.raises(ConfigError, match="unrecognised CAST type"):
            ev.evaluate(ast)

    def test_coalesce_first_non_null(self) -> None:
        """COALESCE returns the first non-NULL expression."""
        df = pl.DataFrame({"a": [None, "x"], "b": ["y", "z"]})
        result = _eval("COALESCE(a, b)", df)
        assert result.to_list() == ["y", "x"]


# ===========================================================================
# TestTypeCoercion
# ===========================================================================


class TestTypeCoercion:
    """Tests for arithmetic type coercion retry logic."""

    def test_int_plus_int(self) -> None:
        """Integer arithmetic works natively without coercion."""
        df = pl.DataFrame({"a": [3], "b": [4]})
        result = _eval("a + b", df)
        assert result[0] == 7

    def test_float_arithmetic(self) -> None:
        """Float arithmetic works natively."""
        df = pl.DataFrame({"a": [3.5], "b": [1.5]})
        result = _eval("a * b", df)
        assert abs(result[0] - 5.25) < FLOAT_TOL

    def test_literal_arithmetic(self) -> None:
        """Arithmetic on two literal integers."""
        df = pl.DataFrame({"dummy": [0]})
        result = _eval("10 + 5", df)
        assert result[0] == 15

    def test_year_minus_literal(self) -> None:
        """YEAR(date) - 1 produces an integer result."""
        df = pl.DataFrame({"d": [date(2026, 1, 1)]}).with_columns(pl.col("d").cast(pl.Date))
        result = _eval("YEAR(d) - 1", df, {"d": pl.Date})
        assert result[0] == 2025

    def test_comparison_equal(self) -> None:
        """Equality comparison returns boolean series."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = _eval("a = 2", df)
        assert result.to_list() == [False, True, False]

    def test_comparison_not_equal(self) -> None:
        """Not-equal comparison."""
        df = pl.DataFrame({"a": [1, 2]})
        result = _eval("a <> 1", df)
        assert result.to_list() == [False, True]

    def test_comparison_less_than(self) -> None:
        """Less-than comparison."""
        df = pl.DataFrame({"a": [1, 3]})
        result = _eval("a < 2", df)
        assert result.to_list() == [True, False]

    def test_comparison_greater_than_or_equal(self) -> None:
        """Greater-than-or-equal comparison."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = _eval("a >= 2", df)
        assert result.to_list() == [False, True, True]

    def test_in_operator_matches(self) -> None:
        """IN operator returns True for matching values."""
        df = pl.DataFrame({"m": ["Jan", "Mar", "Dec", "Jul"]})
        result = _eval("m IN ('Jan', 'Dec')", df)
        assert result.to_list() == [True, False, True, False]

    def test_and_logical(self) -> None:
        """AND combines two boolean conditions."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [3, 2, 1]})
        result = _eval("a < 3 AND b > 1", df)
        assert result.to_list() == [True, True, False]

    def test_or_logical(self) -> None:
        """OR combines two boolean conditions."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = _eval("a = 1 OR a = 3", df)
        assert result.to_list() == [True, False, True]

    def test_not_logical(self) -> None:
        """NOT negates a boolean expression."""
        df = pl.DataFrame({"a": [True, False]})
        result = _eval("NOT a", df)
        assert result.to_list() == [False, True]

    def test_unary_minus_literal(self) -> None:
        """Unary minus on a literal."""
        df = pl.DataFrame({"dummy": [0]})
        result = _eval("-5", df)
        assert result[0] == -5

    def test_unary_minus_column(self) -> None:
        """Unary minus negates a numeric column."""
        df = pl.DataFrame({"n": [3, -2]})
        result = _eval("-n", df)
        assert result.to_list() == [-3, 2]


# ===========================================================================
# TestNullHandling
# ===========================================================================


class TestNullHandling:
    """Tests for null handling strategies."""

    def test_return_null_strategy(self) -> None:
        """return_null (default) passes NULLs through."""
        df = pl.DataFrame({"a": [None, "hello"]})
        tokens = ExpressionLexer.tokenize("UPPER(a)")
        ast = ExpressionParser(tokens).parse()
        ev = ExpressionEvaluator(dict(df.schema), null_strategy="return_null")
        result = df.with_columns(ev.evaluate(ast).alias("r"))["r"]
        assert result[0] is None
        assert result[1] == "HELLO"

    def test_default_value_strategy(self) -> None:
        """default_value strategy replaces NULLs with the configured fallback."""
        df = pl.DataFrame({"a": [None, "hello"]})
        tokens = ExpressionLexer.tokenize("UPPER(a)")
        ast = ExpressionParser(tokens).parse()
        ev = ExpressionEvaluator(
            dict(df.schema),
            null_strategy="default_value",
            null_default_value="UNKNOWN",
        )
        result = df.with_columns(ev.evaluate(ast).alias("r"))["r"]
        assert result[0] == "UNKNOWN"
        assert result[1] == "HELLO"

    def test_null_literal(self) -> None:
        """NULL literal evaluates to a NULL expression."""
        df = pl.DataFrame({"a": ["x"]})
        result = _eval("NULL", df)
        assert result[0] is None

    def test_coalesce_null_fallback(self) -> None:
        """COALESCE with NULL first argument falls back to second."""
        df = pl.DataFrame({"a": [None], "b": ["fallback"]})
        result = _eval("COALESCE(a, b)", df)
        assert result[0] == "fallback"

    def test_coalesce_all_null(self) -> None:
        """COALESCE with all NULL arguments returns NULL."""
        df = pl.DataFrame({"a": [None], "b": [None]})
        result = _eval("COALESCE(a, b)", df)
        assert result[0] is None

    def test_default_value_in_concat(self) -> None:
        """default_value fills NULL produced by CONCAT with a NULL input column."""
        df = pl.DataFrame({"a": [None], "b": ["world"]})
        tokens = ExpressionLexer.tokenize("CONCAT(a, ' ', b)")
        ast = ExpressionParser(tokens).parse()
        # CONCAT is null-safe by itself; adding default_value as extra safety
        ev = ExpressionEvaluator(
            dict(df.schema),
            null_strategy="default_value",
            null_default_value="MISSING",
        )
        result = df.with_columns(ev.evaluate(ast).alias("r"))["r"]
        # CONCAT fills null inputs with "", so result is " world" not MISSING
        assert result[0] == " world"


# ===========================================================================
# TestExpressionValidation
# ===========================================================================


class TestExpressionValidation:
    """Tests for validate_expression() and collect_expression_column_refs()."""

    def test_valid_expression_passes(self) -> None:
        """A syntactically correct expression passes without error."""
        validate_expression("UPPER(TRIM(name))")  # should not raise

    def test_invalid_syntax_raises(self) -> None:
        """A syntactically invalid expression raises ConfigError."""
        with pytest.raises(ConfigError, match="Invalid expression"):
            validate_expression("CONCAT(a, )")

    def test_unclosed_paren_raises(self) -> None:
        """An unclosed parenthesis raises ConfigError."""
        with pytest.raises(ConfigError):
            validate_expression("CONCAT(a, b")

    def test_column_ref_validation_passes(self) -> None:
        """Column references in available_columns passes validation."""
        validate_expression("UPPER(name)", available_columns=["name", "age"])

    def test_column_ref_validation_fails(self) -> None:
        """Column references not in available_columns raises ConfigError."""
        with pytest.raises(ConfigError, match="column reference"):
            validate_expression("UPPER(missing_col)", available_columns=["name"])

    def test_full_expression_in_error_message(self) -> None:
        """The full expression text appears in the error message."""
        expr = "CONCAT(a, )"
        with pytest.raises(ConfigError, match=re.escape(expr)):
            validate_expression(expr)

    def test_collect_column_refs_simple(self) -> None:
        """collect_expression_column_refs finds column references."""
        refs = collect_expression_column_refs("UPPER(TRIM(name))")
        assert "name" in refs

    def test_collect_column_refs_case_expr(self) -> None:
        """collect_expression_column_refs finds refs inside CASE expressions."""
        expr = "CASE WHEN month = 1 THEN year ELSE other END"
        refs = collect_expression_column_refs(expr)
        assert "month" in refs
        assert "year" in refs
        assert "other" in refs

    def test_collect_column_refs_excludes_literals(self) -> None:
        """collect_expression_column_refs does not include string/number literals."""
        refs = collect_expression_column_refs("CONCAT('prefix', col)")
        assert "prefix" not in refs
        assert "col" in refs


# ===========================================================================
# TestStdRefsValidation
# ===========================================================================


class TestStdRefsValidation:
    """Tests for StdRefs.__post_init__ validation."""

    def test_expression_field_mutually_exclusive(self) -> None:
        """expression and field cannot both be set."""
        from bank_statement_parser.modules.data import StdRefs

        with pytest.raises(ConfigError, match="mutually exclusive"):
            StdRefs(
                statement_type="Test",
                field="col",
                concat_fields=None,
                format=None,
                default=None,
                expression="UPPER(col)",
            )

    def test_default_value_requires_null_default_value(self) -> None:
        """null_strategy='default_value' without null_default_value raises ConfigError."""
        from bank_statement_parser.modules.data import StdRefs

        with pytest.raises(ConfigError, match="null_default_value"):
            StdRefs(
                statement_type="Test",
                field=None,
                concat_fields=None,
                format=None,
                default=None,
                expression="UPPER(col)",
                null_strategy="default_value",
            )

    def test_concat_fields_emits_deprecation_warning(self) -> None:
        """Using concat_fields emits a DeprecationWarning."""
        from bank_statement_parser.modules.data import StdRefs

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            StdRefs(
                statement_type="Test",
                field=None,
                concat_fields=["a", "b"],
                format=None,
                default=None,
            )
        assert any(issubclass(warning.category, DeprecationWarning) for warning in w)

    def test_valid_expression_passes(self) -> None:
        """A valid expression entry creates a StdRefs without error."""
        from bank_statement_parser.modules.data import StdRefs

        ref = StdRefs(
            statement_type="Test",
            field=None,
            concat_fields=None,
            format=None,
            default=None,
            expression="UPPER(TRIM(description))",
        )
        assert ref.expression is not None

    def test_valid_default_value_passes(self) -> None:
        """null_strategy='default_value' with null_default_value set is valid."""
        from bank_statement_parser.modules.data import StdRefs

        ref = StdRefs(
            statement_type="Test",
            field=None,
            concat_fields=None,
            format=None,
            default=None,
            expression="UPPER(col)",
            null_strategy="default_value",
            null_default_value="UNKNOWN",
        )
        assert ref.null_default_value == "UNKNOWN"


# ===========================================================================
# TestGetStandardFieldsExpression (integration)
# ===========================================================================


class TestGetStandardFieldsExpression:
    """Integration tests for expression evaluation inside get_standard_fields()."""

    def _make_std_fields(self, std_field: str, section: str, stmt_type: str, **ref_kwargs: Any) -> dict:
        """Build a minimal config_standard_fields dict for testing."""
        from bank_statement_parser.modules.data import StandardFields, StdRefs

        # Extract format separately since it's a positional StdRefs field
        fmt = ref_kwargs.pop("format", None)
        ref = StdRefs(
            statement_type=stmt_type,
            field=None,
            concat_fields=None,
            format=fmt,
            default=None,
            **ref_kwargs,
        )
        return {std_field: StandardFields(section=section, type="string", vital=False, std_refs=[ref])}

    def _lines_df(self, base: pl.DataFrame) -> pl.DataFrame:
        """Add required lines-section columns including numeric STD fields."""
        return base.with_columns(
            pl.lit(1).alias("transaction_number"),
            pl.lit(1).alias("page"),
            pl.lit(0).cast(pl.Decimal(scale=4)).alias("STD_TRANSACTION_PAYMENTS_IN"),
            pl.lit(0).cast(pl.Decimal(scale=4)).alias("STD_TRANSACTION_PAYMENTS_OUT"),
        )

    def _checks_and_balances(self) -> pl.DataFrame:
        """Minimal checks_and_balances DataFrame required for the lines section."""
        return pl.DataFrame(
            {
                "STD_OPENING_BALANCE": pl.Series([0.0], dtype=pl.Float64),
            }
        )

    def _pages_df(self, base: pl.DataFrame) -> pl.DataFrame:
        """Add required pages-section column."""
        return base.with_columns(pl.lit(1).alias("page"))

    def test_expression_applied_to_lines_data(self) -> None:
        """Expression is evaluated and result stored in the target column."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"description": ["  hello  ", " WORLD "]}))
        std_fields = self._make_std_fields(
            "STD_DESC",
            "pages",
            "TestBank",
            expression="UPPER(TRIM(description))",
        )
        result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_DESC"].to_list() == ["HELLO", "WORLD"]

    def test_expression_concat_columns(self) -> None:
        """CONCAT expression joins multiple columns."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"day": ["20"], "month": ["Dec"], "year": ["2026"]}))
        std_fields = self._make_std_fields(
            "STD_DATE_STR",
            "pages",
            "TestBank",
            expression="CONCAT(day, ' ', month, ' ', year)",
        )
        result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_DATE_STR"][0] == "20 Dec 2026"

    def test_expression_with_header_scalars(self) -> None:
        """Lines expression can reference header scalar values via header_scalars."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"day": ["20"], "month": ["Dec"]}))
        std_fields = self._make_std_fields(
            "STD_DATE_STR",
            "pages",
            "TestBank",
            expression="CONCAT(day, ' ', month, ' ', CAST(STD_STATEMENT_YEAR AS STRING))",
        )
        result = get_standard_fields(
            df,
            "pages",
            std_fields,
            "TestBank",
            pl.DataFrame(),
            header_scalars={"STD_STATEMENT_YEAR": 2026},
        )
        assert result["STD_DATE_STR"][0] == "20 Dec 2026"

    def test_expression_null_default_value_strategy(self) -> None:
        """null_strategy='default_value' replaces NULLs in output."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"col": [None, "hello"]}))
        std_fields = self._make_std_fields(
            "STD_COL",
            "pages",
            "TestBank",
            expression="UPPER(col)",
            null_strategy="default_value",
            null_default_value="MISSING",
        )
        result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_COL"][0] == "MISSING"
        assert result["STD_COL"][1] == "HELLO"

    def test_expression_skip_row_strategy(self) -> None:
        """null_strategy='skip_row' removes rows where expression result is NULL."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"col": [None, "hello", None, "world"]}))
        std_fields = self._make_std_fields(
            "STD_COL",
            "pages",
            "TestBank",
            expression="UPPER(col)",
            null_strategy="skip_row",
        )
        result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_COL"].drop_nulls().to_list() == ["HELLO", "WORLD"]

    def test_statement_type_no_match_vital_false_null(self) -> None:
        """When statement_type has no match and vital=False, column is set to None."""
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        df = self._pages_df(pl.DataFrame({"col": ["x"]}))
        std_fields = self._make_std_fields(
            "STD_COL",
            "pages",
            "OtherBank",
            expression="UPPER(col)",
        )
        result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_COL"][0] is None

    def test_concat_fields_still_works_with_deprecation(self) -> None:
        """concat_fields still works (backward compat) but emits DeprecationWarning."""
        from bank_statement_parser.modules.data import StandardFields, StdRefs
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            ref = StdRefs(
                statement_type="TestBank",
                field=None,
                concat_fields=["a", "b"],
                format=None,
                default=None,
            )
        df = self._pages_df(pl.DataFrame({"a": ["foo"], "b": ["bar"]}))
        std_fields = {"STD_COL": StandardFields(section="pages", type="string", vital=False, std_refs=[ref])}
        with warnings.catch_warnings(record=True) as w2:
            warnings.simplefilter("always")
            result = get_standard_fields(df, "pages", std_fields, "TestBank", pl.DataFrame())
        assert result["STD_COL"][0] == "foobar"
        assert any(issubclass(warning.category, DeprecationWarning) for warning in w2)

    def test_santander_date_year_inference(self) -> None:
        """Santander date year inference: SPLIT extracts month from single 'date' field.

        Production expression mirrors standard_fields.toml exactly:
        - 'date' column contains 'DD MMM' or 'D MMM' (e.g. '20 Dec', '5 Nov')
        - STD_STATEMENT_DATE injected as a header scalar (Date type)
        - Month-boundary logic: Q4 transaction on Q1 statement → year - 1
        - format = '%d %b %Y' (parses the constructed string back to a Date;
          single-digit days are NOT zero-padded by CONCAT — the format string
          is used for strptime parsing, not output formatting)
        """
        from bank_statement_parser.modules.statement_functions import get_standard_fields

        # Production expression from standard_fields.toml
        expr = (
            "CONCAT(date,' ',"
            "CASE WHEN MONTH(STD_STATEMENT_DATE) <= 3"
            " AND MONTH_NAME(SPLIT(date, ' ')[1]) IN ('Oct', 'Nov', 'Dec')"
            " THEN CAST(YEAR(STD_STATEMENT_DATE) - 1 AS String)"
            " ELSE CAST(YEAR(STD_STATEMENT_DATE) AS String)"
            " END)"
        )

        # Six cases covering all boundary conditions:
        #   row 0: Q4 txn (Dec) on Q1 statement (Jan) → prior year
        #   row 1: Q4 txn (Nov) on Q1 statement (Feb) → prior year
        #   row 2: Q4 txn (Oct) on Q1 statement (Mar) → prior year
        #   row 3: non-Q4 txn (Sep) on Q1 statement (Jan) → same year
        #   row 4: Q4 txn (Dec) on Q4 statement (Dec, month > 3) → same year
        #   row 5: non-Q4 txn (Mar) on Q2 statement (Apr, month > 3) → same year
        base_df = pl.DataFrame(
            {
                "date": ["20 Dec", "5 Nov", "15 Oct", "20 Sep", "15 Dec", "1 Mar"],
                "STD_STATEMENT_DATE": [
                    date(2026, 1, 15),
                    date(2026, 2, 1),
                    date(2026, 3, 10),
                    date(2026, 1, 15),
                    date(2025, 12, 20),
                    date(2026, 4, 10),
                ],
            }
        ).with_columns(pl.col("STD_STATEMENT_DATE").cast(pl.Date))
        df = self._lines_df(base_df)
        std_fields = self._make_std_fields(
            "STD_TRANSACTION_DATE",
            "lines",
            "Santander UK Current Account",
            expression=expr,
            format="%d %b %Y",
            null_strategy="default_value",
            null_default_value="1 Jan 2000",
        )
        result = get_standard_fields(
            df,
            "lines",
            std_fields,
            "Santander UK Current Account",
            self._checks_and_balances(),
        )
        dates = result["STD_TRANSACTION_DATE"].to_list()
        # Q4 txn on Q1 statement → year decremented
        assert dates[0] == "20 Dec 2025", f"row 0: {dates[0]}"
        assert dates[1] == "5 Nov 2025", f"row 1: {dates[1]}"
        assert dates[2] == "15 Oct 2025", f"row 2: {dates[2]}"
        # non-Q4 txn on Q1 statement → same year
        assert dates[3] == "20 Sep 2026", f"row 3: {dates[3]}"
        # Q4 txn on Q4 statement (month > 3) → same year
        assert dates[4] == "15 Dec 2025", f"row 4: {dates[4]}"
        # non-Q4 txn on Q2 statement (month > 3) → same year
        assert dates[5] == "1 Mar 2026", f"row 5: {dates[5]}"


# ---------------------------------------------------------------------------
# allow running with python directly
# ---------------------------------------------------------------------------
import re
