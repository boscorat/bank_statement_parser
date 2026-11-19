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

"""Expression language for config-driven standard field transformations.

Implements a recursive-descent parser that compiles TOML expression strings
into Polars expressions (``pl.Expr``).  Expressions are evaluated at the
standard-fields mapping stage via ``get_standard_fields()``; parsing overhead
is per-statement invocation, not per-row.

Supported constructs
--------------------
- **String functions**: CONCAT, SUBSTRING, UPPER, LOWER, TRIM, LTRIM, RTRIM,
  LENGTH, REPLACE, SPLIT (with array indexing)
- **Date functions**: YEAR, MONTH, DAY, MONTH_NAME (auto-detect format)
- **Conversion**: CAST, COALESCE
- **Operators**: arithmetic (+, -, *, /), comparison (=, <>, <, >, <=, >=),
  logical (AND, OR, NOT), set membership (IN)
- **CASE / WHEN / THEN / ELSE / END** searched and simple forms
- **NULL literal**

Design constraints (from AGENTS.md)
-------------------------------------
- No ``from __future__ import annotations`` — not in the allowed list of 5 files.
- No ``ABC``, ``Protocol``, or ``@abstractmethod``.
- No ``eval()`` or dynamic imports.
- ``__slots__`` on every class; dataclasses use ``slots=True``.
- Errors raised as ``ConfigError`` (from ``errors.py``).
- Logging via ``get_logger(__name__)``.
- f-strings only.
"""

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Literal

import polars as pl

from bank_statement_parser.modules.errors import ConfigError
from bank_statement_parser.modules.logging_config import get_logger

_logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Token types
# ---------------------------------------------------------------------------


class TokenType(Enum):
    """All token categories produced by the lexer."""

    # Literals
    STRING = auto()
    NUMBER = auto()
    NULL = auto()

    # Identifiers / keywords
    IDENTIFIER = auto()
    CASE = auto()
    WHEN = auto()
    THEN = auto()
    ELSE = auto()
    END = auto()
    IN = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    AS = auto()

    # Operators
    PLUS = auto()
    MINUS = auto()
    STAR = auto()
    SLASH = auto()

    # Comparison
    EQ = auto()  # =
    NEQ = auto()  # <>
    LT = auto()  # <
    GT = auto()  # >
    LTE = auto()  # <=
    GTE = auto()  # >=

    # Punctuation
    LPAREN = auto()  # (
    RPAREN = auto()  # )
    LBRACKET = auto()  # [
    RBRACKET = auto()  # ]
    COMMA = auto()  # ,

    # Sentinel
    EOF = auto()


_KEYWORDS: dict[str, TokenType] = {
    "CASE": TokenType.CASE,
    "WHEN": TokenType.WHEN,
    "THEN": TokenType.THEN,
    "ELSE": TokenType.ELSE,
    "END": TokenType.END,
    "IN": TokenType.IN,
    "AND": TokenType.AND,
    "OR": TokenType.OR,
    "NOT": TokenType.NOT,
    "NULL": TokenType.NULL,
    "AS": TokenType.AS,
}

# ---------------------------------------------------------------------------
# Token dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Token:
    """A single lexical token with its value and source position.

    Args:
        type: The ``TokenType`` category.
        value: The raw string value from the source expression.
        pos: Zero-based character offset in the source expression.
    """

    type: TokenType
    value: str
    pos: int


# ---------------------------------------------------------------------------
# Lexer
# ---------------------------------------------------------------------------


class ExpressionLexer:
    """Tokenise an expression string into a list of ``Token`` objects.

    All public functionality is exposed via the ``tokenize`` static method.
    No instance state is required.
    """

    __slots__ = ()

    @staticmethod
    def tokenize(expr: str) -> list[Token]:
        """Convert an expression string to a flat token list.

        Args:
            expr: The raw expression string from a TOML ``std_refs`` entry.

        Returns:
            List of ``Token`` objects ending with an ``EOF`` token.

        Raises:
            ConfigError: On any lexical error (unknown char, unclosed string,
                invalid escape sequence, malformed number).
        """
        tokens: list[Token] = []
        pos = 0
        length = len(expr)

        while pos < length:
            ch = expr[pos]

            # Whitespace
            if ch in " \t\n\r":
                pos += 1
                continue

            # Single-quoted string literal
            if ch == "'":
                tok, pos = ExpressionLexer._read_string(expr, pos)
                tokens.append(tok)
                continue

            # Numeric literal — digits only; unary minus is handled by the parser
            if ch.isdigit():
                tok, pos = ExpressionLexer._read_number(expr, pos)
                tokens.append(tok)
                continue

            # Identifier or keyword
            if ch.isalpha() or ch == "_":
                tok, pos = ExpressionLexer._read_identifier(expr, pos)
                tokens.append(tok)
                continue

            # Two-character operators first
            two = expr[pos : pos + 2]
            if two == "<>":
                tokens.append(Token(TokenType.NEQ, two, pos))
                pos += 2
                continue
            if two == "<=":
                tokens.append(Token(TokenType.LTE, two, pos))
                pos += 2
                continue
            if two == ">=":
                tokens.append(Token(TokenType.GTE, two, pos))
                pos += 2
                continue

            # Single-character operators and punctuation
            _single: dict[str, TokenType] = {
                "+": TokenType.PLUS,
                "-": TokenType.MINUS,
                "*": TokenType.STAR,
                "/": TokenType.SLASH,
                "=": TokenType.EQ,
                "<": TokenType.LT,
                ">": TokenType.GT,
                "(": TokenType.LPAREN,
                ")": TokenType.RPAREN,
                "[": TokenType.LBRACKET,
                "]": TokenType.RBRACKET,
                ",": TokenType.COMMA,
            }
            if ch in _single:
                tokens.append(Token(_single[ch], ch, pos))
                pos += 1
                continue

            raise ConfigError(f"Expression lexer: unexpected character {ch!r} at position {pos} in expression: {expr!r}")

        tokens.append(Token(TokenType.EOF, "", pos))
        return tokens

    @staticmethod
    def _read_string(expr: str, start: int) -> tuple[Token, int]:
        """Read a single-quoted string literal with escape sequences.

        Args:
            expr: Full expression source.
            start: Position of the opening quote character.

        Returns:
            Tuple of (Token, new_position_after_closing_quote).

        Raises:
            ConfigError: If the string is unclosed or contains an invalid escape.
        """
        pos = start + 1  # skip opening quote
        buf: list[str] = []
        length = len(expr)
        while pos < length:
            ch = expr[pos]
            if ch == "\\":
                if pos + 1 >= length:
                    raise ConfigError(f"Expression lexer: unterminated escape sequence at position {pos} in expression: {expr!r}")
                nxt = expr[pos + 1]
                if nxt == "'":
                    buf.append("'")
                elif nxt == "\\":
                    buf.append("\\")
                elif nxt == "n":
                    buf.append("\n")
                elif nxt == "t":
                    buf.append("\t")
                else:
                    raise ConfigError(
                        f"Expression lexer: invalid escape sequence '\\{nxt}' at position {pos} "
                        f"in expression: {expr!r}. "
                        f"Supported escapes: \\' \\\\ \\n \\t"
                    )
                pos += 2
            elif ch == "'":
                return Token(TokenType.STRING, "".join(buf), start), pos + 1
            else:
                buf.append(ch)
                pos += 1
        raise ConfigError(f"Expression lexer: unclosed string literal starting at position {start} in expression: {expr!r}")

    @staticmethod
    def _read_number(expr: str, start: int) -> tuple[Token, int]:
        """Read an integer or floating-point number literal.

        Called only when the character at ``start`` is a digit; unary minus
        is handled by the parser so negative-number literals are not consumed
        here.

        Args:
            expr: Full expression source.
            start: Position of the first digit.

        Returns:
            Tuple of (Token, new_position_after_number).

        Raises:
            ConfigError: If the number is malformed (e.g. ``1e`` with no exponent).
        """
        pos = start
        while pos < len(expr) and expr[pos].isdigit():
            pos += 1
        if pos < len(expr) and expr[pos] == ".":
            pos += 1
            if pos >= len(expr) or not expr[pos].isdigit():
                raise ConfigError(
                    f"Expression lexer: malformed number at position {start} (expected digit after decimal point) in expression: {expr!r}"
                )
            while pos < len(expr) and expr[pos].isdigit():
                pos += 1
        # Optional exponent
        if pos < len(expr) and expr[pos] in "eE":
            pos += 1
            if pos < len(expr) and expr[pos] in "+-":
                pos += 1
            if pos >= len(expr) or not expr[pos].isdigit():
                raise ConfigError(
                    f"Expression lexer: malformed number at position {start} (expected digit in exponent) in expression: {expr!r}"
                )
            while pos < len(expr) and expr[pos].isdigit():
                pos += 1
        raw = expr[start:pos]
        return Token(TokenType.NUMBER, raw, start), pos

    @staticmethod
    def _read_identifier(expr: str, start: int) -> tuple[Token, int]:
        """Read an identifier or keyword (case-insensitive keywords).

        Args:
            expr: Full expression source.
            start: Position of the first character.

        Returns:
            Tuple of (Token with IDENTIFIER or keyword type, new_position).
        """
        pos = start
        while pos < len(expr) and (expr[pos].isalnum() or expr[pos] == "_"):
            pos += 1
        raw = expr[start:pos]
        ttype = _KEYWORDS.get(raw.upper(), TokenType.IDENTIFIER)
        return Token(ttype, raw, start), pos


# ---------------------------------------------------------------------------
# AST node dataclasses (plain frozen dataclasses — no ABC)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ASTNode:
    """Base marker for all AST node types.  No abstract methods — evaluator
    dispatches via ``isinstance``."""


@dataclass(frozen=True, slots=True)
class LiteralNode(ASTNode):
    """A constant value: string, number, NULL, or list (for IN operator).

    Args:
        value: Python value — ``str``, ``int``, ``float``, ``None``, or
            ``list`` (for the right-hand side of an IN expression).
    """

    value: str | int | float | None | list  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class ColumnRefNode(ASTNode):
    """Reference to a DataFrame column by name.

    Args:
        name: Column name exactly as it appears in the expression.
    """

    name: str


@dataclass(frozen=True, slots=True)
class BinaryOpNode(ASTNode):
    """A binary operation: arithmetic, comparison, logical, or IN.

    Args:
        op: Operator string — one of ``+``, ``-``, ``*``, ``/``, ``=``,
            ``<>``, ``<``, ``>``, ``<=``, ``>=``, ``AND``, ``OR``, ``IN``.
        left: Left operand node.
        right: Right operand node.
    """

    op: str
    left: ASTNode
    right: ASTNode


@dataclass(frozen=True, slots=True)
class UnaryOpNode(ASTNode):
    """A unary operation.

    Args:
        op: Operator string — ``-`` (negation) or ``NOT``.
        operand: The operand node.
    """

    op: str
    operand: ASTNode


@dataclass(frozen=True, slots=True)
class FunctionCallNode(ASTNode):
    """A function call with zero or more argument nodes.

    Args:
        name: Upper-cased function name (e.g. ``CONCAT``).
        args: Tuple of argument AST nodes (immutable for frozen dataclass).
    """

    name: str
    args: tuple  # tuple[ASTNode, ...]  — quoted to avoid forward-ref issues


@dataclass(frozen=True, slots=True)
class WhenClause(ASTNode):
    """A single WHEN … THEN … clause inside a CASE expression.

    Args:
        condition: The WHEN condition node.
        result: The THEN result node.
    """

    condition: ASTNode
    result: ASTNode


@dataclass(frozen=True, slots=True)
class CaseNode(ASTNode):
    """A CASE expression (searched or simple form).

    Args:
        subject: For simple-form CASE the subject expression; ``None`` for
            searched form.
        when_clauses: Tuple of ``WhenClause`` nodes.
        else_node: The ELSE branch node, or ``None`` if omitted.
    """

    subject: ASTNode | None
    when_clauses: tuple  # tuple[WhenClause, ...]
    else_node: ASTNode | None


@dataclass(frozen=True, slots=True)
class ArrayIndexNode(ASTNode):
    """Array indexing: ``expr[index]`` (e.g. ``SPLIT(col, '-')[0]``).

    Args:
        array_expr: The expression that evaluates to an array (e.g.
            ``FunctionCallNode(name='SPLIT', ...))``).
        index: Integer index.  Negative indices are Python-style (``-1``
            means last element).
    """

    array_expr: ASTNode
    index: int


# ---------------------------------------------------------------------------
# Function spec registry (module-level constant)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FunctionSpec:
    """Metadata for a single built-in function.

    Args:
        name: Upper-cased function name.
        min_args: Minimum number of arguments.
        max_args: Maximum number of arguments; ``-1`` means variadic.
        returns: Descriptive return type string (for error messages only).
    """

    name: str
    min_args: int
    max_args: int  # -1 = variadic
    returns: str


_FUNCTION_REGISTRY: dict[str, FunctionSpec] = {
    "CONCAT": FunctionSpec("CONCAT", 1, -1, "STRING"),
    "SUBSTRING": FunctionSpec("SUBSTRING", 2, 3, "STRING"),
    "UPPER": FunctionSpec("UPPER", 1, 1, "STRING"),
    "LOWER": FunctionSpec("LOWER", 1, 1, "STRING"),
    "TRIM": FunctionSpec("TRIM", 1, 2, "STRING"),
    "LTRIM": FunctionSpec("LTRIM", 1, 2, "STRING"),
    "RTRIM": FunctionSpec("RTRIM", 1, 2, "STRING"),
    "LENGTH": FunctionSpec("LENGTH", 1, 1, "INT"),
    "REPLACE": FunctionSpec("REPLACE", 3, 3, "STRING"),
    "SPLIT": FunctionSpec("SPLIT", 2, 2, "ARRAY[STRING]"),
    "YEAR": FunctionSpec("YEAR", 1, 1, "INT"),
    "MONTH": FunctionSpec("MONTH", 1, 1, "INT"),
    "DAY": FunctionSpec("DAY", 1, 1, "INT"),
    "MONTH_NAME": FunctionSpec("MONTH_NAME", 1, 1, "STRING"),
    "CAST": FunctionSpec("CAST", 2, 2, "specified_type"),
    "COALESCE": FunctionSpec("COALESCE", 1, -1, "type_of_first_non_null"),
}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class ExpressionParser:
    """Recursive-descent parser converting a token list into an AST.

    Precedence levels (lowest → highest):
        1. OR
        2. AND
        3. NOT
        4. = <> < > <= >= IN
        5. + -
        6. * /
        7. unary minus
        8. [index] (postfix array indexing)
        9. primary (literals, column refs, function calls, parens, CASE)

    Args:
        tokens: Token list produced by ``ExpressionLexer.tokenize()``.
    """

    __slots__ = ("_pos", "_tokens")

    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    # -- helpers -------------------------------------------------------------

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, ttype: TokenType) -> Token:
        tok = self._peek()
        if tok.type != ttype:
            raise ConfigError(f"Expression parser: expected {ttype.name} but got {tok.type.name} ({tok.value!r}) at position {tok.pos}")
        return self._advance()

    def _match(self, *ttypes: TokenType) -> bool:
        return self._peek().type in ttypes

    # -- public entry point --------------------------------------------------

    def parse(self) -> ASTNode:
        """Parse the full expression and return the root AST node.

        Returns:
            Root ``ASTNode`` of the parsed expression tree.

        Raises:
            ConfigError: On any syntax error.
        """
        node = self._parse_or()
        if self._peek().type != TokenType.EOF:
            tok = self._peek()
            raise ConfigError(
                f"Expression parser: unexpected token {tok.type.name} ({tok.value!r}) at position {tok.pos} — expected end of expression"
            )
        return node

    # -- precedence levels ---------------------------------------------------

    def _parse_or(self) -> ASTNode:
        node = self._parse_and()
        while self._match(TokenType.OR):
            self._advance()
            right = self._parse_and()
            node = BinaryOpNode("OR", node, right)
        return node

    def _parse_and(self) -> ASTNode:
        node = self._parse_not()
        while self._match(TokenType.AND):
            self._advance()
            right = self._parse_not()
            node = BinaryOpNode("AND", node, right)
        return node

    def _parse_not(self) -> ASTNode:
        if self._match(TokenType.NOT):
            self._advance()
            operand = self._parse_not()
            return UnaryOpNode("NOT", operand)
        return self._parse_comparison()

    def _parse_comparison(self) -> ASTNode:
        node = self._parse_addition()
        _cmp_types = (
            TokenType.EQ,
            TokenType.NEQ,
            TokenType.LT,
            TokenType.GT,
            TokenType.LTE,
            TokenType.GTE,
            TokenType.IN,
        )
        if self._match(*_cmp_types):
            tok = self._advance()
            op = tok.value.upper() if tok.type == TokenType.IN else tok.value
            if tok.type == TokenType.IN:
                right = self._parse_in_list()
            else:
                right = self._parse_addition()
            node = BinaryOpNode(op, node, right)
        return node

    def _parse_in_list(self) -> LiteralNode:
        """Parse the ``(val1, val2, ...)`` list on the right of IN."""
        self._expect(TokenType.LPAREN)
        values: list[Any] = []
        while not self._match(TokenType.RPAREN):
            tok = self._peek()
            if tok.type == TokenType.STRING:
                self._advance()
                values.append(tok.value)
            elif tok.type == TokenType.NUMBER:
                self._advance()
                values.append(_parse_number(tok.value))
            elif tok.type == TokenType.NULL:
                self._advance()
                values.append(None)
            else:
                raise ConfigError(f"Expression parser: IN list may only contain literal values, got {tok.type.name} at position {tok.pos}")
            if self._match(TokenType.COMMA):
                self._advance()
        self._expect(TokenType.RPAREN)
        return LiteralNode(values)

    def _parse_addition(self) -> ASTNode:
        node = self._parse_multiplication()
        while self._match(TokenType.PLUS, TokenType.MINUS):
            tok = self._advance()
            right = self._parse_multiplication()
            node = BinaryOpNode(tok.value, node, right)
        return node

    def _parse_multiplication(self) -> ASTNode:
        node = self._parse_unary()
        while self._match(TokenType.STAR, TokenType.SLASH):
            tok = self._advance()
            right = self._parse_unary()
            node = BinaryOpNode(tok.value, node, right)
        return node

    def _parse_unary(self) -> ASTNode:
        if self._match(TokenType.MINUS):
            self._advance()
            operand = self._parse_unary()
            return UnaryOpNode("-", operand)
        return self._parse_postfix()

    def _parse_postfix(self) -> ASTNode:
        node = self._parse_primary()
        while self._match(TokenType.LBRACKET):
            self._advance()
            tok = self._peek()
            if tok.type == TokenType.MINUS:
                self._advance()
                idx_tok = self._expect(TokenType.NUMBER)
                index = -int(idx_tok.value)
            elif tok.type == TokenType.NUMBER:
                idx_tok = self._advance()
                index = int(idx_tok.value)
            else:
                raise ConfigError(f"Expression parser: array index must be an integer literal, got {tok.type.name} at position {tok.pos}")
            self._expect(TokenType.RBRACKET)
            node = ArrayIndexNode(node, index)
        return node

    def _parse_primary(self) -> ASTNode:
        tok = self._peek()

        # NULL literal
        if tok.type == TokenType.NULL:
            self._advance()
            return LiteralNode(None)

        # String literal
        if tok.type == TokenType.STRING:
            self._advance()
            return LiteralNode(tok.value)

        # Number literal
        if tok.type == TokenType.NUMBER:
            self._advance()
            return LiteralNode(_parse_number(tok.value))

        # Parenthesised expression
        if tok.type == TokenType.LPAREN:
            self._advance()
            node = self._parse_or()
            self._expect(TokenType.RPAREN)
            return node

        # CASE expression
        if tok.type == TokenType.CASE:
            return self._parse_case()

        # Identifier: either a function call or a column reference
        if tok.type == TokenType.IDENTIFIER:
            self._advance()
            name_upper = tok.value.upper()
            if self._match(TokenType.LPAREN) and name_upper in _FUNCTION_REGISTRY:
                return self._parse_function_call(name_upper, tok.pos)
            # Column reference (preserve original case)
            return ColumnRefNode(tok.value)

        raise ConfigError(
            f"Expression parser: unexpected token {tok.type.name} ({tok.value!r}) "
            f"at position {tok.pos} — expected a value, column name, or function call"
        )

    def _parse_function_call(self, name: str, name_pos: int) -> FunctionCallNode:
        """Parse a function call ``NAME(arg, arg, ...)``.

        For ``CAST``, the second argument is a type keyword parsed as a
        ``LiteralNode(str)``.

        Args:
            name: Upper-cased function name (already validated against registry).
            name_pos: Source position of the function name token (for errors).

        Returns:
            ``FunctionCallNode`` with parsed argument nodes.

        Raises:
            ConfigError: On incorrect argument count.
        """
        spec = _FUNCTION_REGISTRY[name]
        self._expect(TokenType.LPAREN)
        args: list[ASTNode] = []

        if name == "CAST":
            # CAST(expr AS type)
            value_node = self._parse_or()
            self._expect(TokenType.AS)
            type_tok = self._peek()
            if type_tok.type not in (TokenType.IDENTIFIER, TokenType.STRING):
                raise ConfigError(f"Expression parser: CAST expects a type name as second argument at position {type_tok.pos}")
            self._advance()
            args = [value_node, LiteralNode(type_tok.value.upper())]
        else:
            if not self._match(TokenType.RPAREN):
                args.append(self._parse_or())
                while self._match(TokenType.COMMA):
                    self._advance()
                    args.append(self._parse_or())

        self._expect(TokenType.RPAREN)

        # Validate arg count
        n = len(args)
        if n < spec.min_args or (spec.max_args != -1 and n > spec.max_args):
            expected = (
                f"{spec.min_args}+"
                if spec.max_args == -1
                else f"{spec.min_args}"
                if spec.min_args == spec.max_args
                else f"{spec.min_args}–{spec.max_args}"
            )
            raise ConfigError(f"Expression parser: function {name} expects {expected} argument(s) but got {n} at position {name_pos}")

        return FunctionCallNode(name, tuple(args))

    def _parse_case(self) -> CaseNode:
        """Parse a CASE … END expression (searched or simple form).

        Returns:
            ``CaseNode`` with when_clauses and optional else_node.

        Raises:
            ConfigError: On malformed CASE syntax.
        """
        self._expect(TokenType.CASE)

        # Determine form: if next token is WHEN it's searched form; otherwise simple.
        subject: ASTNode | None = None
        if not self._match(TokenType.WHEN):
            subject = self._parse_or()

        when_clauses: list[WhenClause] = []
        while self._match(TokenType.WHEN):
            self._advance()
            condition = self._parse_or()
            self._expect(TokenType.THEN)
            result = self._parse_or()
            when_clauses.append(WhenClause(condition, result))

        if not when_clauses:
            tok = self._peek()
            raise ConfigError(f"Expression parser: CASE expression requires at least one WHEN clause at position {tok.pos}")

        else_node: ASTNode | None = None
        if self._match(TokenType.ELSE):
            self._advance()
            else_node = self._parse_or()

        self._expect(TokenType.END)
        return CaseNode(subject, tuple(when_clauses), else_node)


# ---------------------------------------------------------------------------
# Helper: parse a number token value to int or float
# ---------------------------------------------------------------------------


def _parse_number(raw: str) -> int | float:
    """Convert a raw number string to int or float.

    Args:
        raw: String from a NUMBER token (e.g. ``'42'``, ``'3.14'``).

    Returns:
        ``int`` if the value has no decimal point or exponent, else ``float``.
    """
    if "." in raw or "e" in raw.lower():
        return float(raw)
    return int(raw)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

# Polars Utf8 dtype alias for compatibility across Polars versions
_STR_DTYPE = pl.Utf8


class ExpressionEvaluator:
    """Compile an AST into a Polars expression (``pl.Expr``).

    The resulting ``pl.Expr`` is suitable for use with
    ``DataFrame.with_columns()``.  No Python-level row iteration is performed;
    all operations run inside Polars' query engine.

    Args:
        schema: Mapping of column name → Polars dtype from the DataFrame that
            will evaluate the expression.  Used to determine how date columns
            should be accessed.
        null_strategy: How to handle NULL results from the expression.
            ``'return_null'`` (default) passes NULLs through; ``'skip_row'``
            marks rows for post-loop filtering (see ``get_standard_fields``);
            ``'default_value'`` replaces NULLs with ``null_default_value``.
        null_default_value: String literal to substitute when
            ``null_strategy='default_value'`` and the expression result is NULL.
    """

    __slots__ = ("_null_default_value", "_null_strategy", "_schema")

    def __init__(
        self,
        schema: dict[str, Any],
        null_strategy: Literal["return_null", "skip_row", "default_value"] = "return_null",
        null_default_value: str | None = None,
    ) -> None:
        self._schema = schema
        self._null_strategy = null_strategy
        self._null_default_value = null_default_value

    def evaluate(self, node: ASTNode) -> pl.Expr:
        """Walk the AST and return a Polars expression.

        Args:
            node: Root AST node returned by ``ExpressionParser.parse()``.

        Returns:
            A ``pl.Expr`` ready for ``DataFrame.with_columns()``.
        """
        expr = self._eval(node)
        if self._null_strategy == "default_value" and self._null_default_value is not None:
            expr = expr.fill_null(pl.lit(self._null_default_value))
        return expr

    # -- dispatch ------------------------------------------------------------

    def _eval(self, node: ASTNode) -> pl.Expr:
        if isinstance(node, LiteralNode):
            return self._eval_literal(node)
        if isinstance(node, ColumnRefNode):
            return self._eval_column_ref(node)
        if isinstance(node, BinaryOpNode):
            return self._eval_binary_op(node)
        if isinstance(node, UnaryOpNode):
            return self._eval_unary_op(node)
        if isinstance(node, FunctionCallNode):
            return self._eval_function_call(node)
        if isinstance(node, CaseNode):
            return self._eval_case(node)
        if isinstance(node, ArrayIndexNode):
            return self._eval_array_index(node)
        raise ConfigError(f"Expression evaluator: unknown AST node type {type(node).__name__}")

    # -- literals and column refs --------------------------------------------

    def _eval_literal(self, node: LiteralNode) -> pl.Expr:
        if node.value is None:
            return pl.lit(None)
        if isinstance(node.value, list):
            # list literal used only as the right operand of IN
            return pl.lit(node.value)
        return pl.lit(node.value)

    def _eval_column_ref(self, node: ColumnRefNode) -> pl.Expr:
        return pl.col(node.name)

    # -- binary operators ----------------------------------------------------

    def _eval_binary_op(self, node: BinaryOpNode) -> pl.Expr:
        op = node.op

        if op == "OR":
            return self._eval(node.left) | self._eval(node.right)
        if op == "AND":
            return self._eval(node.left) & self._eval(node.right)
        if op == "IN":
            left_expr = self._eval(node.left)
            # right must be a LiteralNode holding a list
            right_node = node.right
            if not isinstance(right_node, LiteralNode) or not isinstance(right_node.value, list):
                raise ConfigError("Expression evaluator: right operand of IN must be a literal list")
            return left_expr.is_in(right_node.value)

        left_expr = self._eval(node.left)
        right_expr = self._eval(node.right)

        # Arithmetic operators: apply auto-coercion on type mismatch
        if op == "+":
            return self._coerce_arithmetic(left_expr, right_expr, op)
        if op == "-":
            return self._coerce_arithmetic(left_expr, right_expr, op)
        if op == "*":
            return self._coerce_arithmetic(left_expr, right_expr, op)
        if op == "/":
            return self._coerce_arithmetic(left_expr, right_expr, op)

        # Comparison operators: no implicit coercion — caller must CAST explicitly
        if op == "=":
            return left_expr == right_expr
        if op == "<>":
            return left_expr != right_expr
        if op == "<":
            return left_expr < right_expr
        if op == ">":
            return left_expr > right_expr
        if op == "<=":
            return left_expr <= right_expr
        if op == ">=":
            return left_expr >= right_expr

        raise ConfigError(f"Expression evaluator: unknown binary operator {op!r}")

    def _coerce_arithmetic(self, left: pl.Expr, right: pl.Expr, op: str) -> pl.Expr:
        """Apply arithmetic operator with STRING→FLOAT→INT coercion retry.

        Polars raises ``InvalidOperationError`` when types are incompatible.
        On failure the evaluator retries by casting any string operand to
        Float64, then Int64, before giving up and returning NULL.

        Args:
            left: Left Polars expression.
            right: Right Polars expression.
            op: One of ``+``, ``-``, ``*``, ``/``.

        Returns:
            Polars expression for the arithmetic operation.
        """

        def _apply(l: pl.Expr, r: pl.Expr, o: str) -> pl.Expr:
            if o == "+":
                return l + r
            if o == "-":
                return l - r
            if o == "*":
                return l * r
            return l / r

        # First attempt: native
        try:
            return _apply(left, right, op)
        except (pl.exceptions.InvalidOperationError, pl.exceptions.SchemaError):
            pass

        # Retry: cast string operands to Float64
        try:
            left_f = left.cast(pl.Float64)
            right_f = right.cast(pl.Float64)
            _logger.info(f"Expression type coercion: retried arithmetic '{op}' by casting operands to Float64")
            return _apply(left_f, right_f, op)
        except (pl.exceptions.InvalidOperationError, pl.exceptions.SchemaError):
            pass

        # Retry: cast to Int64
        try:
            left_i = left.cast(pl.Int64)
            right_i = right.cast(pl.Int64)
            _logger.info(f"Expression type coercion: retried arithmetic '{op}' by casting operands to Int64")
            return _apply(left_i, right_i, op)
        except (pl.exceptions.InvalidOperationError, pl.exceptions.SchemaError):
            pass

        _logger.info(f"Expression type coercion: arithmetic '{op}' failed all coercion attempts; returning NULL")
        return pl.lit(None)

    # -- unary operators -----------------------------------------------------

    def _eval_unary_op(self, node: UnaryOpNode) -> pl.Expr:
        operand = self._eval(node.operand)
        if node.op == "-":
            return operand.neg()
        if node.op == "NOT":
            return ~operand
        raise ConfigError(f"Expression evaluator: unknown unary operator {node.op!r}")

    # -- CASE ----------------------------------------------------------------

    def _eval_case(self, node: CaseNode) -> pl.Expr:
        """Compile a CASE expression to a chain of ``pl.when().then()`` calls.

        Args:
            node: ``CaseNode`` from the parser.

        Returns:
            Polars expression implementing the CASE logic.
        """
        subject = self._eval(node.subject) if node.subject is not None else None
        else_expr: pl.Expr = pl.lit(None)
        if node.else_node is not None:
            else_expr = self._eval(node.else_node)

        # Build from last WHEN backwards so we can chain .when().then()
        # Polars: pl.when(cond).then(result).when(cond2).then(result2).otherwise(else)
        # We need to accumulate a chain.  Polars' when().then() returns a ChainedWhen
        # which supports further .when() calls.

        chain = None
        for clause in node.when_clauses:
            if subject is not None:
                # Simple form: CASE subject WHEN value THEN result
                condition = subject == self._eval(clause.condition)
            else:
                condition = self._eval(clause.condition)
            result = self._eval(clause.result)
            if chain is None:
                chain = pl.when(condition).then(result)
            else:
                chain = chain.when(condition).then(result)  # type: ignore[union-attr]

        if chain is None:
            return else_expr
        return chain.otherwise(else_expr)

    # -- array indexing ------------------------------------------------------

    def _eval_array_index(self, node: ArrayIndexNode) -> pl.Expr:
        """Evaluate ``array_expr[index]`` — retrieves element from a list column.

        Args:
            node: ``ArrayIndexNode`` with the array expression and integer index.

        Returns:
            Polars expression that extracts the indexed element, returning NULL
            on out-of-bounds access.
        """
        arr_expr = self._eval(node.array_expr)
        idx = node.index
        if idx < 0:
            # Polars list.get supports negative indexing natively
            return arr_expr.list.get(idx, null_on_oob=True)
        return arr_expr.list.get(idx, null_on_oob=True)

    # -- function dispatch ---------------------------------------------------

    def _eval_function_call(self, node: FunctionCallNode) -> pl.Expr:
        """Dispatch to the appropriate function evaluator.

        Args:
            node: ``FunctionCallNode`` with upper-cased function name.

        Returns:
            Polars expression for the function call.

        Raises:
            ConfigError: If the function name is unrecognised.
        """
        fn = node.name
        args = node.args

        # String functions
        if fn == "CONCAT":
            return self._eval_concat(args)
        if fn == "SUBSTRING":
            return self._eval_substring(args)
        if fn == "UPPER":
            return self._eval(args[0]).str.to_uppercase()
        if fn == "LOWER":
            return self._eval(args[0]).str.to_lowercase()
        if fn == "TRIM":
            return self._eval_trim(args, "both")
        if fn == "LTRIM":
            return self._eval_trim(args, "left")
        if fn == "RTRIM":
            return self._eval_trim(args, "right")
        if fn == "LENGTH":
            return self._eval(args[0]).str.len_chars()
        if fn == "REPLACE":
            return self._eval_replace(args)
        if fn == "SPLIT":
            return self._eval_split(args)

        # Date functions
        if fn == "YEAR":
            return self._eval_date_part(args[0], "year")
        if fn == "MONTH":
            return self._eval_date_part(args[0], "month")
        if fn == "DAY":
            return self._eval_date_part(args[0], "day")
        if fn == "MONTH_NAME":
            return self._eval_month_name(args[0])

        # Conversion functions
        if fn == "CAST":
            return self._eval_cast(args)
        if fn == "COALESCE":
            return self._eval_coalesce(args)

        raise ConfigError(f"Expression evaluator: unknown function {fn!r}")

    # -- string function implementations -------------------------------------

    def _eval_concat(self, args: tuple) -> pl.Expr:
        """Concatenate all arguments as strings, casting each to Utf8 first.

        NULL values in any argument are treated as empty strings so that a
        NULL in one field does not null out the whole concatenation.

        Args:
            args: One or more ASTNode arguments.

        Returns:
            Polars string concatenation expression.
        """
        parts = [self._eval(a).cast(pl.Utf8).fill_null("") for a in args]
        return pl.concat_str(parts, ignore_nulls=True)

    def _eval_substring(self, args: tuple) -> pl.Expr:
        """Extract a substring using 0-based indexing (Polars convention).

        Args:
            args: (source_expr, start[, length]) — start is 0-based.

        Returns:
            Polars ``str.slice`` expression.
        """
        source = self._eval(args[0])
        start_node = args[1]
        # start must be a literal integer
        if not isinstance(start_node, LiteralNode) or not isinstance(start_node.value, int):
            raise ConfigError("Expression evaluator: SUBSTRING start argument must be an integer literal")
        start: int = start_node.value
        if len(args) == 3:
            length_node = args[2]
            if not isinstance(length_node, LiteralNode) or not isinstance(length_node.value, int):
                raise ConfigError("Expression evaluator: SUBSTRING length argument must be an integer literal")
            length: int = length_node.value
            return source.str.slice(start, length)
        return source.str.slice(start)

    def _eval_trim(self, args: tuple, side: str) -> pl.Expr:
        """Trim characters from a string.

        Args:
            args: (source_expr[, chars_expr]).
            side: One of ``'both'``, ``'left'``, ``'right'``.

        Returns:
            Polars trim expression.
        """
        source = self._eval(args[0])
        chars: str | None = None
        if len(args) == 2:
            chars_node = args[1]
            if isinstance(chars_node, LiteralNode) and isinstance(chars_node.value, str):
                chars = chars_node.value
        if side == "both":
            return source.str.strip_chars(chars)
        if side == "left":
            return source.str.strip_chars_start(chars)
        return source.str.strip_chars_end(chars)

    def _eval_replace(self, args: tuple) -> pl.Expr:
        """Replace all occurrences of a substring.

        Args:
            args: (source_expr, pattern_expr, replacement_expr).

        Returns:
            Polars ``str.replace_all`` expression (literal match, not regex).
        """
        source = self._eval(args[0])
        pattern_node = args[1]
        replacement_node = args[2]
        if isinstance(pattern_node, LiteralNode) and isinstance(pattern_node.value, str):
            pattern = pattern_node.value
        else:
            raise ConfigError("Expression evaluator: REPLACE pattern must be a string literal")
        if isinstance(replacement_node, LiteralNode) and isinstance(replacement_node.value, str):
            replacement = replacement_node.value
        else:
            raise ConfigError("Expression evaluator: REPLACE replacement must be a string literal")
        return source.str.replace_all(re.escape(pattern), replacement, literal=True)

    def _eval_split(self, args: tuple) -> pl.Expr:
        """Split a string by a delimiter, returning a list column.

        Args:
            args: (source_expr, delimiter_expr).

        Returns:
            Polars ``str.split`` expression yielding a list column.
        """
        source = self._eval(args[0])
        delim_node = args[1]
        if not isinstance(delim_node, LiteralNode) or not isinstance(delim_node.value, str):
            raise ConfigError("Expression evaluator: SPLIT delimiter must be a string literal")
        return source.str.split(delim_node.value)

    # -- date function implementations ---------------------------------------

    def _eval_date_part(self, arg_node: ASTNode, part: str) -> pl.Expr:
        """Extract a date component (year, month, day) from various input types.

        Accepts: Date column, ISO date string column, or integer column.

        Args:
            arg_node: The argument AST node.
            part: One of ``'year'``, ``'month'``, ``'day'``.

        Returns:
            Polars integer expression for the date component.
        """
        expr = self._eval(arg_node)
        col_name = arg_node.name if isinstance(arg_node, ColumnRefNode) else None
        col_dtype = self._schema.get(col_name) if col_name else None

        if col_dtype is not None and col_dtype == pl.Date:
            return getattr(expr.dt, part)()

        # Try parsing as ISO date string; fall back to cast
        date_expr = expr.str.to_date(format="%Y-%m-%d", strict=False)
        return getattr(date_expr.dt, part)()

    def _eval_month_name(self, arg_node: ASTNode) -> pl.Expr:
        """Return the 3-letter title-case month abbreviation (Jan, Feb, ..., Dec).

        Auto-detects input format: Date column, integer 1-12, integer string,
        abbreviation (any case), or full name (any case).

        Args:
            arg_node: The argument AST node.

        Returns:
            Polars Utf8 expression with 3-letter title-case abbreviation.
        """
        _MONTH_MAP: dict[str, str] = {
            "1": "Jan",
            "01": "Jan",
            "january": "Jan",
            "jan": "Jan",
            "2": "Feb",
            "02": "Feb",
            "february": "Feb",
            "feb": "Feb",
            "3": "Mar",
            "03": "Mar",
            "march": "Mar",
            "mar": "Mar",
            "4": "Apr",
            "04": "Apr",
            "april": "Apr",
            "apr": "Apr",
            "5": "May",
            "05": "May",
            "may": "May",
            "6": "Jun",
            "06": "Jun",
            "june": "Jun",
            "jun": "Jun",
            "7": "Jul",
            "07": "Jul",
            "july": "Jul",
            "jul": "Jul",
            "8": "Aug",
            "08": "Aug",
            "august": "Aug",
            "aug": "Aug",
            "9": "Sep",
            "09": "Sep",
            "september": "Sep",
            "sep": "Sep",
            "10": "Oct",
            "october": "Oct",
            "oct": "Oct",
            "11": "Nov",
            "november": "Nov",
            "nov": "Nov",
            "12": "Dec",
            "december": "Dec",
            "dec": "Dec",
        }
        expr = self._eval(arg_node)
        col_name = arg_node.name if isinstance(arg_node, ColumnRefNode) else None
        col_dtype = self._schema.get(col_name) if col_name else None

        if col_dtype is not None and col_dtype == pl.Date:
            # Extract month integer then map to abbreviation
            month_int_expr = expr.dt.month()
            return month_int_expr.cast(pl.Utf8).replace_strict(_MONTH_MAP, default=None)

        if col_dtype is not None and col_dtype in (pl.Int32, pl.Int64, pl.UInt32, pl.UInt64):
            return expr.cast(pl.Utf8).replace_strict(_MONTH_MAP, default=None)

        # String column: normalise to lowercase for map lookup, return title-case
        return expr.str.to_lowercase().replace_strict(_MONTH_MAP, default=None)

    # -- conversion function implementations ---------------------------------

    def _eval_cast(self, args: tuple) -> pl.Expr:
        """Cast a value to a specified type.

        The type name is the second argument as a string literal:
        ``STRING`` / ``TEXT``, ``INT`` / ``INTEGER``, ``FLOAT``,
        ``DATE``.

        Args:
            args: (value_node, type_literal_node).

        Returns:
            Polars cast expression.

        Raises:
            ConfigError: If the type name is unrecognised.
        """
        value_expr = self._eval(args[0])
        type_node = args[1]
        if not isinstance(type_node, LiteralNode) or not isinstance(type_node.value, str):
            raise ConfigError("Expression evaluator: CAST second argument must be a type name string")
        type_name = type_node.value.upper()
        _type_map: dict[str, type[pl.DataType]] = {
            "STRING": pl.Utf8,
            "TEXT": pl.Utf8,
            "INT": pl.Int64,
            "INTEGER": pl.Int64,
            "FLOAT": pl.Float64,
            "DATE": pl.Date,
        }
        if type_name not in _type_map:
            raise ConfigError(f"Expression evaluator: unrecognised CAST type {type_name!r}. Supported: {', '.join(_type_map)}")
        return value_expr.cast(_type_map[type_name])

    def _eval_coalesce(self, args: tuple) -> pl.Expr:
        """Return the first non-NULL value from the argument list.

        Args:
            args: One or more ASTNode arguments.

        Returns:
            Polars ``coalesce`` expression.
        """
        exprs = [self._eval(a) for a in args]
        return pl.coalesce(exprs)


# ---------------------------------------------------------------------------
# Public validation function
# ---------------------------------------------------------------------------


def validate_expression(expr: str, available_columns: list[str] | None = None) -> None:
    """Validate an expression string at config load time.

    Performs lexical and syntactic checks.  If ``available_columns`` is
    provided, also validates that all column references exist in the list.

    Args:
        expr: The raw expression string from a ``std_refs`` TOML entry.
        available_columns: Optional list of column names available at runtime.
            When ``None``, semantic (column-reference) validation is skipped.

    Raises:
        ConfigError: On any lexical, syntactic, or semantic error, with the
            full expression included in the error message.
    """
    try:
        tokens = ExpressionLexer.tokenize(expr)
        ast = ExpressionParser(tokens).parse()
    except ConfigError as exc:
        raise ConfigError(f"Invalid expression {expr!r}: {exc}") from exc

    if available_columns is not None:
        refs = _collect_column_refs(ast)
        available = frozenset(available_columns)
        missing = [r for r in refs if r not in available]
        if missing:
            raise ConfigError(
                f"Invalid expression {expr!r}: column reference(s) not found in "
                f"available columns: {missing}. Available: {sorted(available)}"
            )


def _collect_column_refs(node: ASTNode) -> list[str]:
    """Recursively collect all column reference names from an AST.

    Args:
        node: Root or sub-node to traverse.

    Returns:
        List of column name strings (may contain duplicates).
    """
    if isinstance(node, ColumnRefNode):
        return [node.name]
    if isinstance(node, LiteralNode):
        return []
    if isinstance(node, BinaryOpNode):
        return _collect_column_refs(node.left) + _collect_column_refs(node.right)
    if isinstance(node, UnaryOpNode):
        return _collect_column_refs(node.operand)
    if isinstance(node, FunctionCallNode):
        result: list[str] = []
        for arg in node.args:
            result.extend(_collect_column_refs(arg))
        return result
    if isinstance(node, CaseNode):
        result = []
        if node.subject is not None:
            result.extend(_collect_column_refs(node.subject))
        for clause in node.when_clauses:
            result.extend(_collect_column_refs(clause.condition))
            result.extend(_collect_column_refs(clause.result))
        if node.else_node is not None:
            result.extend(_collect_column_refs(node.else_node))
        return result
    if isinstance(node, ArrayIndexNode):
        return _collect_column_refs(node.array_expr)
    if isinstance(node, WhenClause):
        return _collect_column_refs(node.condition) + _collect_column_refs(node.result)
    return []


def collect_expression_column_refs(expr: str) -> frozenset[str]:
    """Return all unique column names referenced in an expression string.

    Parses the expression but does not raise on unknown columns.  Used by
    ``get_standard_fields`` to decide which header scalars to inject before
    evaluating a lines expression.

    Args:
        expr: Raw expression string.

    Returns:
        Frozenset of unique column name strings.

    Raises:
        ConfigError: If the expression fails to parse.
    """
    tokens = ExpressionLexer.tokenize(expr)
    ast = ExpressionParser(tokens).parse()
    return frozenset(_collect_column_refs(ast))
