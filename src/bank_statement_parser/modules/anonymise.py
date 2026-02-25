"""
anonymise — PDF anonymisation utility.

Replaces personally identifiable information (names, addresses, account numbers,
sort codes, IBAN numbers, card numbers) in bank statement PDFs with structurally
equivalent dummy values, producing an anonymised copy suitable for attaching to
support tickets or committing to a test suite.

Substitutions are driven by a TOML config file (``anonymise.toml``) with two
sections:

``[global_replacements]``
    Applied on every page of the document, across the entire page area.
    Use for names, name fragments, account numbers, and any value that is
    uniquely personal and cannot collide with merchant names.

``[address_replacements]``
    Applied on **page 1 only**, within a bounding-box that covers the personal
    address block in the top-left of HSBC statement pages.  Use for address
    lines, city names, and postcodes that also appear as merchant or location
    names inside transaction descriptions.

Entries within each section are applied in declaration order.  Place longer,
more-specific strings before shorter fragments to avoid partial-match collisions
(e.g. list the full ``"Jason Telford Marland Farrar"`` before ``"Farrar"``).

Optionally, transaction descriptions can be scrambled by passing
``scramble_descriptions=True`` to :func:`anonymise_pdf` or
:func:`anonymise_folder`.  Every letter in a description span is replaced with
a different random letter of the same case; non-alpha characters (spaces,
punctuation, digits) are left unchanged.  The substitution table is generated
once per document so the mapping is internally consistent within a file.

Scrambling is restricted to spans that lie within **transaction blocks** —
logical lines that contain a transaction date (``dd Mmm yy`` or the
split-date form ``dd Mmm`` / ``yy`` used by HSBC credit card statements on
their last pages) or a recognised structural anchor (such as
``BALANCE BROUGHT FORWARD`` on continuation pages) act as anchors, and any
immediately following continuation lines (multi-line descriptions) are
included until a purely numeric line (balance / summary row) breaks the
block.  Statement headers, account summaries, statement dates, totals, page
numbers, and balance rows are therefore never scrambled.

Within an eligible transaction line, spans that are themselves classified as
dates (full ``dd Mmm yy`` or partial ``dd Mmm``), payment type codes (one of
the exact known HSBC codes: ``BP``, ``)))``, ``VIS``, ``DD``, ``TFR``,
``SO``, ``CR``, ``DR``, ``ATM``, ``CC``, ``OBP``), numeric values, or
structural balance markers (``BALANCE BROUGHT FORWARD`` /
``BALANCE CARRIED FORWARD``) are **not** scrambled.  No per-statement
configuration is required.  Short merchant-name abbreviations such as
``"OVO"``, ``"SJP"``, or ``"GYM"`` are **not** payment type codes and are
therefore scrambled.

Implementation note
-------------------
PDF I/O is handled by two libraries:

* **pdfplumber** (read-only) — used to extract per-character bounding boxes and
  font metadata from each page via ``page.chars``.  This provides the
  precise coordinates and font information needed to locate target strings and
  size replacement text.

* **pikepdf** — used to parse and rewrite each page's content stream via
  ``pikepdf.parse_content_stream`` / ``pikepdf.unparse_content_stream``.  Text
  operators whose decoded string matches a target are replaced in-stream so the
  original bytes are physically absent from the output PDF, not merely covered
  by a visual overlay.

Coordinate systems
------------------
pdfplumber reports coordinates with the origin at the **top-left** of the page.
pikepdf / PDF spec uses the origin at the **bottom-left**.  All bounding-box
clip tests (``_ADDRESS_CLIP``) are expressed in pdfplumber (top-left) space and
compared against pdfplumber character data directly, so no coordinate flip is
needed for clipping.  The content-stream rewriting stage operates entirely in
PDF space and never uses clip coordinates.

Font encoding
-------------
HSBC statements use Base-14 fonts with Latin-1 / WinAnsiEncoding.  Every
character that can appear in a bank statement name, address, or description maps
1:1 to a single byte in that encoding, which means a replacement string of the
same length can be encoded as a PDF string literal with a straightforward
``latin-1`` encode.  For the small number of characters outside Latin-1 a
best-effort ASCII transliteration is applied; any remaining unmappable
characters are dropped.

Public API
----------
    anonymise_pdf(input_path, output_path=None, config_path=None, scramble_descriptions=False) -> Path
    anonymise_folder(folder_path, pattern="*.pdf", output_dir=None, config_path=None, scramble_descriptions=False) -> list[Path]

Example
-------
    from pathlib import Path
    from bank_statement_parser.modules.anonymise import anonymise_pdf, anonymise_folder

    # Single file — writes anonymised_<stem>.pdf alongside the original
    out = anonymise_pdf(Path("statement.pdf"))

    # With description scrambling enabled
    out = anonymise_pdf(Path("statement.pdf"), scramble_descriptions=True)

    # All PDFs in a folder, with scrambling
    outputs = anonymise_folder(Path("tests/pdfs"), scramble_descriptions=True)
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pikepdf
import pdfplumber
import tomllib

from bank_statement_parser.modules.paths import BASE_CONFIG

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default config path — alongside the other project TOML files.
_DEFAULT_CONFIG_PATH: Path = BASE_CONFIG / "anonymise.toml"

# Bounding-box used for address_replacements: covers the personal address
# block in the top-left corner of page 1 across all supported HSBC statement
# layouts.  Coordinates are in pdfplumber space (origin top-left), points.
#   x0=0, y0=130, x1=320, y1=250
_ADDRESS_CLIP: tuple[float, float, float, float] = (0.0, 130.0, 320.0, 250.0)

# Suffix prepended to the input file stem to form the output file name.
_ANONYMISED_PREFIX: str = "anonymised_"

# The 14 standard PDF base fonts.  Any font name NOT in this set is an embedded
# proprietary font whose name cannot be reused for content-stream replacement
# text — it will be mapped to a standard substitute via _FONT_FALLBACK_MAP.
_BASE14_FONTS: frozenset[str] = frozenset(
    {
        "Courier",
        "Courier-Bold",
        "Courier-Oblique",
        "Courier-BoldOblique",
        "Helvetica",
        "Helvetica-Bold",
        "Helvetica-Oblique",
        "Helvetica-BoldOblique",
        "Times-Roman",
        "Times-Bold",
        "Times-Italic",
        "Times-BoldItalic",
        "Symbol",
        "ZapfDingbats",
    }
)

# Map proprietary font name fragments to the closest standard substitute.
# Matched case-insensitively against the full font name.
_FONT_FALLBACK_MAP: tuple[tuple[str, str], ...] = (
    ("bold", "Times-Bold"),
    ("italic", "Times-Italic"),
    ("light", "Times-Roman"),
    ("medium", "Times-Roman"),
    ("regular", "Times-Roman"),
)

# ---------------------------------------------------------------------------
# Description-scrambling constants
# ---------------------------------------------------------------------------

# Compiled patterns used to classify text spans before deciding whether to
# scramble them.  A span matching ANY of these patterns is left unchanged.

# Transaction date: "dd Mmm yy"  (e.g. "23 Jan 25").  Matches a single token
# when the PDF renders the whole date as one span.
_DATE_RE: re.Pattern[str] = re.compile(r"^\d{1,2}\s[A-Z][a-z]{2}\s\d{2}$")

# Partial date — day + month only, no year (e.g. "03 Jan").  HSBC credit card
# statements render the year as a separate span on the last page and on
# year-boundary rows.  These spans must be detected as transaction anchors and
# protected from scrambling even though they do not match _DATE_RE.
_DATE_DAY_MONTH_RE: re.Pattern[str] = re.compile(r"^\d{1,2}\s[A-Z][a-z]{2}$")

# Compact date — "dd MmmYY" with no space between month and two-digit year
# (e.g. "11 Dec21").  HSBC credit card statements use this format for the
# received/transaction date columns on pages 2+.  May appear as a single token
# or as a two-token pair ("11" + "Dec21") depending on how pdfplumber splits the
# character runs.
_DATE_COMPACT_RE: re.Pattern[str] = re.compile(r"^\d{1,2}\s[A-Z][a-z]{2}\d{2}$")

# Payment type code: one of the known HSBC transaction type codes or the
# three closing parentheses artefact that HSBC uses as a type code.
# The list is exhaustive — short uppercase words that are NOT in this set
# (e.g. "OVO", "SJP", "GYM") are merchant names and must be scrambled.
_PAYMENT_TYPE_RE: re.Pattern[str] = re.compile(r"^(?:BP|\)\)\)|VIS|DD|TFR|SO|CR|DR|ATM|CC|OBP)$")

# Compound token produced when pdfplumber's extract_words merges a payment-type
# code directly with the following description word, with no whitespace between
# them.  The most common case is ")))GREGGS" where ")))" is the HSBC type code
# artefact.  Group 1 captures the payment-type prefix; group 2 captures the
# description text that must be scrambled.
_COMPOUND_TYPE_DESC_RE: re.Pattern[str] = re.compile(r"^(BP|\)\)\)|VIS|DD|TFR|SO|CR|DR|ATM|CC|OBP)([A-Za-z].*)$")

# Numeric value / polarity suffix: any string that consists only of digits,
# currency symbols, thousands separators, decimal points, whitespace, and
# optional trailing polarity letters (D, CR).  This covers amounts like
# "1,234.56", "1,234.56D", "1,234.56 CR", and isolated suffixes "D" / "CR".
_NUMERIC_RE: re.Pattern[str] = re.compile(r"^[\d£$€\s,\.\-]+(?:CR|D)?$|^CR$|^D$")

# Reference number: starts with a digit, contains only digits and hyphens,
# at least 5 characters total.  Covers bare numeric refs ("35314369001") and
# hyphenated payment refs such as "353-12477661" (Amazon Prime subscription
# reference rendered on a continuation line alongside the transaction amount).
# Must not cause the continuation block to be terminated even though every
# other token on the line may be numeric.
# The leading-digit anchor prevents bare negative numbers like "-1.99" from
# matching.
_REF_NUMBER_RE: re.Pattern[str] = re.compile(r"^\d[\d\-]{4,}$")

# Standalone month name in title-case (e.g. "Jan", "Feb").  Produced when
# extract_words splits a "dd Mmm yy" date into three separate tokens; the
# middle token is the month name.  Must not be scrambled.
_MONTH_NAME_RE: re.Pattern[str] = re.compile(r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$")

# Compact month+year token (e.g. "Dec21", "Jan22").  Produced when HSBC CRD
# statements render the date as "dd MmmYY" and extract_words emits the
# month+year portion as a single token.  Must not be scrambled.
_MONTH_COMPACT_RE: re.Pattern[str] = re.compile(r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\d{2}$")

# Structural balance markers used as transaction block boundaries on
# continuation pages (pages 2+ of multi-page current account statements).
# On continuation pages these lines appear WITHOUT a date, so they would
# otherwise be missed by the date-only anchor logic in _is_transaction_line.
# The same labels also appear WITH a date on page 1 (already caught by
# _DATE_RE), but matching them here is harmless — the line is still eligible.
#
# These labels must ALSO be excluded from scrambling (they are load-bearing
# structural markers required by the statement parser).
#
# Two forms are matched:
#   - Space-separated ("BALANCE BROUGHT FORWARD") — when the PDF renders the
#     label as three separate character runs that extract_words keeps apart.
#   - Merged ("BALANCEBROUGHTFORWARD") — when extract_words collapses
#     consecutive characters into a single token with no intervening space.
_BALANCE_FORWARD_RE: re.Pattern[str] = re.compile(
    r"^BALANCE\s+(?:BROUGHT|CARRIED)\s+FORWARD$"
    r"|^BALANCE(?:BROUGHT|CARRIED)FORWARD$"
)

# Matches only the CARRIED variant — used to close the transaction block after
# the last transaction row on a page.  BALANCE BROUGHT FORWARD opens a block
# on continuation pages; BALANCE CARRIED FORWARD explicitly ends it.
_BALANCE_CARRIED_FORWARD_RE: re.Pattern[str] = re.compile(r"^BALANCE\s+CARRIED\s+FORWARD$|^BALANCECARRIEDFORWARD$")

# Ordered tuple of compiled patterns whose presence on a line qualifies that
# line as a **transaction block anchor** in addition to the date-based test.
# Extend this tuple to support other bank / statement layouts without
# modifying _is_transaction_line itself.
_TRANSACTION_ANCHOR_PATTERNS: tuple[re.Pattern[str], ...] = (_BALANCE_FORWARD_RE,)

# Patterns that match the **full reconstructed text of a logical line** and
# mark every token on that line as protected from scrambling.
#
# This catches structural labels whose individual tokens (e.g. "BALANCE",
# "BROUGHT", "FORWARD") would otherwise pass _classify_span_as_description
# because the token on its own is not recognisable as a structural marker.
#
# Each pattern is matched case-insensitively against the space-joined token
# texts for a line.  Add further patterns here to protect additional
# load-bearing labels in other statement layouts.
_PROTECTED_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Current-account structural balance markers (all token-split variants).
    re.compile(r"^BALANCE\s+(?:BROUGHT|CARRIED)\s+FORWARD", re.IGNORECASE),
    # Credit-card statement section header.
    re.compile(r"^Summary\s+Of\s+Interest\s+On\s+This\s+Statement$", re.IGNORECASE),
)

# Exact phrase strings (concatenated, no spaces) that HSBC PDFs render as a
# run of individual single-character ``Tj`` operators interleaved with ``Tm``
# positioning operators.  The content-stream rewriter uses this set to detect
# such runs and skip applying replacements to those characters, preventing
# structural labels from being scrambled even when the scramble map contains
# entries for individual letters.
#
# The strings here must match the raw concatenated text exactly as it appears
# in the PDF (i.e. with no spaces, matching the pdfplumber merged token form).
_PROTECTED_CHARRUN_PHRASES: frozenset[str] = frozenset(
    {
        "BALANCEBROUGHTFORWARD",
        "BALANCECARRIEDFORWARD",
        "SummaryOfInterestOnThisStatement",
    }
)

# Tolerance (in PDF points) added to each side of the derived transaction-block
# y-range when guarding scramble replacements in the content stream.  Absorbs
# small floating-point rounding differences between pdfplumber's coordinate
# extraction and the raw ``Tm`` values stored in the PDF content stream.
_SCRAMBLE_Y_TOLERANCE: float = 2.0

# The 26 lowercase and uppercase ASCII letters as tuples, used to build the
# per-document scramble mapping.
_LOWER_LETTERS: tuple[str, ...] = tuple("abcdefghijklmnopqrstuvwxyz")
_UPPER_LETTERS: tuple[str, ...] = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


# ---------------------------------------------------------------------------
# Internal dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Redaction:
    """A single search-and-replace operation to apply to a PDF page.

    Args:
        search: The exact text string to locate.
        replacement: The text to render in place of the located string.
        clip: Optional bounding-box restricting the search area, expressed as
            ``(x0, y0, x1, y1)`` in pdfplumber coordinates (origin top-left).
            ``None`` means search the entire page.
        page_indices: Optional set of 0-based page indices to restrict
            processing to.  ``None`` means apply to all pages.
    """

    search: str
    replacement: str
    clip: tuple[float, float, float, float] | None = field(default=None)
    page_indices: frozenset[int] | None = field(default=None)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_redactions(config_path: Path) -> list[_Redaction]:
    """Parse ``anonymise.toml`` and return an ordered list of :class:`_Redaction` objects.

    Args:
        config_path: Path to the TOML anonymisation config file.

    Returns:
        Ordered list of redactions.  Global replacements precede address
        replacements; within each group, declaration order is preserved.

    Raises:
        FileNotFoundError: If *config_path* does not exist.
        KeyError: If the TOML file contains no recognised sections.
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"anonymise.toml not found at {config_path}.\nCopy anonymise_example.toml to anonymise.toml and fill in your real details."
        )

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    redactions: list[_Redaction] = []

    for search, replacement in data.get("global_replacements", {}).items():
        redactions.append(_Redaction(search=search, replacement=replacement))

    for search, replacement in data.get("address_replacements", {}).items():
        redactions.append(
            _Redaction(
                search=search,
                replacement=replacement,
                clip=_ADDRESS_CLIP,
                page_indices=frozenset({0}),
            )
        )

    return redactions


def _anonymise_filename(stem: str, redactions: list[_Redaction]) -> str:
    """Apply global redaction search/replace pairs to a filename stem.

    Only :class:`_Redaction` entries without a ``clip`` restriction are
    considered — address-scoped replacements are page-1/bounding-box specific
    and are unlikely to appear verbatim in a filename.  Replacements are
    applied in declaration order so that longer, more-specific entries (which
    are listed first in the TOML) take precedence over shorter fragments.

    Args:
        stem: The filename stem (i.e. the filename without its extension or
            directory component) to sanitise.
        redactions: Ordered list of :class:`_Redaction` objects as returned by
            :func:`_load_redactions`.

    Returns:
        The stem with all matching sensitive substrings replaced by their
        configured substitutes.
    """
    result = stem
    for redaction in redactions:
        if redaction.clip is None:  # global replacements only
            result = result.replace(redaction.search, redaction.replacement)
    return result


# ---------------------------------------------------------------------------
# Core redaction engine — pdfplumber location + pikepdf content-stream rewrite
# ---------------------------------------------------------------------------

# Fallback font used when no overlapping char can be found.
_FALLBACK_FONT = "Times-Roman"
_FALLBACK_FONTSIZE = 9.0


def _normalise_fontname(fontname: str) -> str:
    """Map an arbitrary font name to a Base-14 embeddable name.

    If *fontname* is already one of the 14 standard PDF base fonts it is
    returned unchanged.  Otherwise the name is matched (case-insensitively)
    against :data:`_FONT_FALLBACK_MAP` to pick the closest substitute, falling
    back to :data:`_FALLBACK_FONT` when no keyword matches.

    Args:
        fontname: Font name as reported by ``page.chars``.

    Returns:
        A standard PDF base-14 font name.
    """
    if fontname in _BASE14_FONTS:
        return fontname
    lower = fontname.lower()
    for keyword, substitute in _FONT_FALLBACK_MAP:
        if keyword in lower:
            return substitute
    return _FALLBACK_FONT


def _font_at_bbox(
    chars: list[dict],
    bbox: tuple[float, float, float, float],
) -> tuple[str, float]:
    """Return the font name and size of the char that best covers *bbox*.

    Iterates the pdfplumber char list for a page and returns the
    ``(fontname, fontsize)`` of the character whose bounding box has the
    greatest overlap area with *bbox*.  Falls back to
    :data:`_FALLBACK_FONT` / :data:`_FALLBACK_FONTSIZE` when no char
    intersects.

    Args:
        chars: List of pdfplumber char dicts (``page.chars``).  Each dict
            must have keys ``"x0"``, ``"top"``, ``"x1"``, ``"bottom"``,
            ``"fontname"``, and ``"size"``.
        bbox: ``(x0, y0, x1, y1)`` bounding box in pdfplumber coordinates
            (origin top-left).

    Returns:
        Tuple of ``(normalised_fontname, fontsize)``.
    """
    bx0, by0, bx1, by1 = bbox
    best_font = _FALLBACK_FONT
    best_size = _FALLBACK_FONTSIZE
    best_area = 0.0

    for ch in chars:
        cx0 = ch["x0"]
        cy0 = ch["top"]
        cx1 = ch["x1"]
        cy1 = ch["bottom"]
        # Intersection
        ix0 = max(bx0, cx0)
        iy0 = max(by0, cy0)
        ix1 = min(bx1, cx1)
        iy1 = min(by1, cy1)
        if ix1 <= ix0 or iy1 <= iy0:
            continue
        area = (ix1 - ix0) * (iy1 - iy0)
        if area > best_area:
            best_area = area
            best_font = _normalise_fontname(ch.get("fontname", _FALLBACK_FONT))
            best_size = float(ch.get("size", _FALLBACK_FONTSIZE))

    return best_font, best_size


def _char_bbox(ch: dict) -> tuple[float, float, float, float]:
    """Extract the bounding box of a pdfplumber char dict.

    Args:
        ch: A pdfplumber char dict with keys ``"x0"``, ``"top"``,
            ``"x1"``, ``"bottom"``.

    Returns:
        ``(x0, y0, x1, y1)`` bounding box in pdfplumber coordinates.
    """
    return (ch["x0"], ch["top"], ch["x1"], ch["bottom"])


def _bbox_within_clip(bbox: tuple[float, float, float, float], clip: tuple[float, float, float, float]) -> bool:
    """Return ``True`` if *bbox* overlaps *clip*.

    Both arguments are in pdfplumber coordinates (origin top-left).

    Args:
        bbox: ``(x0, y0, x1, y1)`` of the candidate region.
        clip: ``(x0, y0, x1, y1)`` clip rectangle.

    Returns:
        ``True`` if the rectangles overlap (non-empty intersection).
    """
    bx0, by0, bx1, by1 = bbox
    cx0, cy0, cx1, cy1 = clip
    return bx0 < cx1 and bx1 > cx0 and by0 < cy1 and by1 > cy0


def _encode_pdf_string(text: str) -> bytes:
    """Encode a Python string as a PDF literal string in Latin-1.

    Characters outside Latin-1 are transliterated to their closest ASCII
    equivalent where possible; any remaining unmappable characters are dropped.
    The returned bytes are ready to be used as the content of a ``pikepdf.String``.

    Args:
        text: The Python string to encode.

    Returns:
        Bytes suitable for constructing a ``pikepdf.String``.
    """
    try:
        return text.encode("latin-1")
    except UnicodeEncodeError:
        # Best-effort: encode each character individually, dropping failures.
        result = bytearray()
        for ch in text:
            try:
                result.extend(ch.encode("latin-1"))
            except UnicodeEncodeError:
                pass
        return bytes(result)


# ---------------------------------------------------------------------------
# Content-stream text search helpers
# ---------------------------------------------------------------------------


def _find_hits_on_plumber_page(
    plumber_page: Any,
    search: str,
    clip: tuple[float, float, float, float] | None,
) -> list[tuple[float, float, float, float]]:
    """Locate all non-overlapping occurrences of *search* on *plumber_page*.

    Uses the pdfplumber character list to find every run of consecutive
    characters whose concatenated text equals *search*, then returns the
    bounding box of each match as a ``(x0, y0, x1, y1)`` tuple in pdfplumber
    coordinates (origin top-left).  When *clip* is provided only matches whose
    bounding box overlaps the clip rectangle are returned.

    Characters are grouped into words by sorting on ``(top, x0)`` and merging
    consecutive characters whose ``top`` values are within 3 points of each
    other (the same snap tolerance used elsewhere in this module).  This mirrors
    how pdfplumber builds word spans.

    Args:
        plumber_page: The pdfplumber page to search.
        search: The literal string to find.
        clip: Optional ``(x0, y0, x1, y1)`` region to restrict results.

    Returns:
        List of ``(x0, y0, x1, y1)`` bounding boxes for each match, in
        top-to-bottom, left-to-right order.
    """
    chars = plumber_page.chars
    if not chars or not search:
        return []

    # Sort characters top-to-bottom, left-to-right.
    sorted_chars = sorted(chars, key=lambda c: (round(c["top"], 1), c["x0"]))

    n = len(search)
    hits: list[tuple[float, float, float, float]] = []
    i = 0
    while i <= len(sorted_chars) - n:
        # Try to match *search* starting at position i.
        candidate = sorted_chars[i : i + n]
        # Quick text check.
        if "".join(c["text"] for c in candidate) == search:
            # Verify all chars are on the same logical line (top within 3 pts).
            tops = [c["top"] for c in candidate]
            if max(tops) - min(tops) <= 3.0:
                bbox = (
                    min(c["x0"] for c in candidate),
                    min(c["top"] for c in candidate),
                    max(c["x1"] for c in candidate),
                    max(c["bottom"] for c in candidate),
                )
                if clip is None or _bbox_within_clip(bbox, clip):
                    hits.append(bbox)
                i += n  # advance past this match
                continue
        i += 1

    return hits


# ---------------------------------------------------------------------------
# Content-stream rewriting helpers
# ---------------------------------------------------------------------------


def _decode_pdf_operand(obj: pikepdf.Object) -> str:
    """Decode a pikepdf string operand to a Python str, best-effort.

    Attempts Latin-1 first (the encoding used by HSBC Base-14 fonts), then
    UTF-16-BE (PDF standard for strings starting with a BOM), then falls back
    to UTF-8 with replacement.

    Args:
        obj: A ``pikepdf.String`` or ``pikepdf.Object`` from a content stream
             operand list.

    Returns:
        Python str of the decoded text.
    """
    raw: bytes = bytes(obj)
    # PDF strings starting with BOM are UTF-16-BE.
    if raw.startswith(b"\xfe\xff"):
        return raw[2:].decode("utf-16-be", errors="replace")
    try:
        return raw.decode("latin-1")
    except Exception:
        return raw.decode("utf-8", errors="replace")


def _rewrite_page_content_stream(
    pike_page: pikepdf.Page,
    replacements: list[tuple[str, str]],
    pike_doc: pikepdf.Pdf,
    *,
    scramble_pairs: list[tuple[str, str]] | None = None,
    scramble_y_range: tuple[float, float] | None = None,
) -> bool:
    """Physically replace text strings in *pike_page*'s content stream.

    Parses the page content stream with ``pikepdf.parse_content_stream``,
    iterates every operator, and for ``Tj`` and ``TJ`` text-showing operators
    replaces any string operand whose decoded text contains a target substring
    with the replacement text re-encoded for the same font.

    The replacement is done at the operand level: each string in a ``TJ`` array
    is inspected independently, and a string that contains the search text has
    the matching portion replaced while the rest of the string is left intact.
    For ``Tj`` the single string operand is replaced in full when it matches.

    *replacements* (redaction pairs) are applied unconditionally to every
    ``Tj``/``TJ`` on the page.  *scramble_pairs* are applied only when the
    current text-matrix y-position (tracked from ``Tm`` operands) falls within
    *scramble_y_range*.  This prevents collateral corruption of structural
    labels that sit outside the transaction block.

    Before applying replacements a **pre-pass** scans for runs of single-character
    ``Tj`` operators interleaved only with ``Tm`` (text-matrix) positioning
    operators that together spell a phrase in :data:`_PROTECTED_CHARRUN_PHRASES`.
    HSBC PDFs render structural labels such as ``BALANCEBROUGHTFORWARD`` as one
    ``Tj`` per letter.  Any ``Tj`` instruction index that forms part of such a
    run is marked frozen and skipped during the replacement pass, ensuring the
    individual characters of the protected phrase are never altered.

    The modified stream is serialised back with ``pikepdf.unparse_content_stream``
    and written to ``pike_page.obj["/Contents"]``.

    Args:
        pike_page: The pikepdf page to modify in place.
        replacements: List of ``(search, replacement)`` redaction pairs to apply
            unconditionally, in order.  Longer/more-specific strings should come
            first.
        pike_doc: The owning :class:`pikepdf.Pdf` document, used to create
            the replacement content stream object.
        scramble_pairs: Optional list of ``(original, scrambled)`` pairs for
            description scrambling.  Applied only when the current content-stream
            y-position is within *scramble_y_range*.
        scramble_y_range: Optional ``(y_min, y_max)`` tuple in PDF content-stream
            coordinates (origin at page bottom).  When provided, scramble pairs
            are only applied to ``Tj``/``TJ`` instructions whose ``Tm`` y-value
            is within this range.  When ``None``, scramble pairs are applied
            unconditionally.

    Returns:
        ``True`` if at least one replacement was made; ``False`` otherwise.
    """
    if not replacements and not scramble_pairs:
        return False

    try:
        instructions = list(pikepdf.parse_content_stream(pike_page))
    except Exception:
        return False

    # ------------------------------------------------------------------
    # Pre-pass: identify Tj instruction indices that are part of a
    # single-character-per-Tj run spelling a protected phrase.
    # Such runs are interleaved with Tm operators; we accumulate pending
    # (index, char) pairs and flush when a non-Tj/non-Tm operator is seen
    # or when the accumulated string matches (or can no longer match) any
    # protected phrase.
    # ------------------------------------------------------------------
    frozen_indices: set[int] = set()

    # Build a prefix-lookup: map each possible prefix of a protected phrase
    # to the full phrase(s) it belongs to, for fast incremental matching.
    # We just need to know: given accumulated chars so far, is there still
    # at least one protected phrase that starts with them?
    def _any_phrase_starts_with(prefix: str) -> bool:
        return any(p.startswith(prefix) for p in _PROTECTED_CHARRUN_PHRASES)

    pending: list[tuple[int, str]] = []  # (instruction_index, char)

    def _flush_pending(is_complete: bool) -> None:
        """Commit or discard the current pending run."""
        if not pending:
            return
        accumulated = "".join(ch for _, ch in pending)
        if is_complete and accumulated in _PROTECTED_CHARRUN_PHRASES:
            for idx, _ in pending:
                frozen_indices.add(idx)
        pending.clear()

    for idx, (operands, operator) in enumerate(instructions):
        op_name = str(operator)
        if op_name == "Tm":
            # Positioning operator — allowed between chars of a run; skip.
            continue
        if op_name == "Tj" and operands and isinstance(operands[0], pikepdf.String):
            ch = _decode_pdf_operand(operands[0])
            if len(ch) == 1:
                # Potentially part of a char-by-char run.
                candidate = "".join(c for _, c in pending) + ch
                if _any_phrase_starts_with(candidate):
                    pending.append((idx, ch))
                    if candidate in _PROTECTED_CHARRUN_PHRASES:
                        # Complete match — freeze and start fresh.
                        _flush_pending(is_complete=True)
                    continue
                else:
                    # This char breaks the current run — flush without freezing,
                    # then start a fresh run with this char if it can begin a phrase.
                    _flush_pending(is_complete=False)
                    if _any_phrase_starts_with(ch):
                        pending.append((idx, ch))
                        continue
            else:
                # Multi-char Tj — cannot be part of a char-by-char run.
                _flush_pending(is_complete=False)
        else:
            # Any other operator breaks the run.
            _flush_pending(is_complete=False)

    _flush_pending(is_complete=False)

    # ------------------------------------------------------------------
    # Main pass: apply replacements, skipping frozen indices.
    # Redaction pairs (replacements) are applied unconditionally.
    # Scramble pairs are gated by the current text-matrix y-position
    # derived from Tm operators: only applied when current_y falls
    # within scramble_y_range (when that range is provided).
    # ------------------------------------------------------------------
    changed = False
    new_instructions: list[tuple[list[pikepdf.Object], pikepdf.Operator]] = []
    current_y: float = 0.0  # tracks Tm f operand (y position, PDF coords)

    _effective_scramble_pairs: list[tuple[str, str]] = scramble_pairs if scramble_pairs else []

    for idx, (operands, operator) in enumerate(instructions):
        op_name = str(operator)

        # Track the current y-position from Tm (text matrix) operators.
        # Tm has 6 operands: a b c d e f — where f is the y translation.
        if op_name == "Tm" and len(operands) >= 6:
            try:
                current_y = float(operands[5])
            except (ValueError, TypeError):
                pass

        if op_name == "Tj" and operands and idx not in frozen_indices:
            obj = operands[0]
            if isinstance(obj, pikepdf.String):
                decoded = _decode_pdf_operand(obj)
                new_text = decoded
                for search, replacement in replacements:
                    new_text = new_text.replace(search, replacement)
                # Apply scramble pairs only when y is within the transaction block.
                if _effective_scramble_pairs:
                    if scramble_y_range is None or (scramble_y_range[0] <= current_y <= scramble_y_range[1]):
                        for search, replacement in _effective_scramble_pairs:
                            new_text = new_text.replace(search, replacement)
                if new_text != decoded:
                    operands = [pikepdf.String(_encode_pdf_string(new_text))]
                    changed = True

        elif op_name == "TJ" and operands:
            arr = operands[0]
            if isinstance(arr, pikepdf.Array):
                apply_scramble = _effective_scramble_pairs and (
                    scramble_y_range is None or (scramble_y_range[0] <= current_y <= scramble_y_range[1])
                )
                new_arr_items: list[pikepdf.Object] = []
                for item in list(arr):  # type: ignore[arg-type]
                    if isinstance(item, pikepdf.String):
                        decoded = _decode_pdf_operand(item)
                        new_text = decoded
                        for search, replacement in replacements:
                            new_text = new_text.replace(search, replacement)
                        if apply_scramble:
                            for search, replacement in _effective_scramble_pairs:
                                new_text = new_text.replace(search, replacement)
                        if new_text != decoded:
                            new_arr_items.append(pikepdf.String(_encode_pdf_string(new_text)))
                            changed = True
                        else:
                            new_arr_items.append(item)
                    else:
                        new_arr_items.append(item)
                operands = [pikepdf.Array(new_arr_items)]  # type: ignore[assignment]

        new_instructions.append((operands, operator))  # type: ignore[arg-type]

    if changed:
        new_stream = pikepdf.unparse_content_stream(new_instructions)
        pike_page.obj["/Contents"] = pike_doc.make_stream(new_stream)

    return changed


# ---------------------------------------------------------------------------
# Description scrambling
# ---------------------------------------------------------------------------


def _make_scramble_map() -> dict[int, int]:
    """Build a randomised character-translation table for letter scrambling.

    Each lowercase letter is mapped to a different randomly-chosen lowercase
    letter; each uppercase letter is mapped to a different randomly-chosen
    uppercase letter.  The table is generated once per :func:`anonymise_pdf`
    call so every letter maps consistently within a single document.

    Returns:
        A translation table suitable for use with :meth:`str.translate`.
    """
    lower_shuffled = list(_LOWER_LETTERS)
    # Keep shuffling until no letter maps to itself.
    while True:
        random.shuffle(lower_shuffled)
        if all(orig != shuf for orig, shuf in zip(_LOWER_LETTERS, lower_shuffled)):
            break

    upper_shuffled = list(_UPPER_LETTERS)
    while True:
        random.shuffle(upper_shuffled)
        if all(orig != shuf for orig, shuf in zip(_UPPER_LETTERS, upper_shuffled)):
            break

    mapping: dict[int, int] = {}
    for orig, shuf in zip(_LOWER_LETTERS, lower_shuffled):
        mapping[ord(orig)] = ord(shuf)
    for orig, shuf in zip(_UPPER_LETTERS, upper_shuffled):
        mapping[ord(orig)] = ord(shuf)
    return mapping


def _classify_span_as_description(text: str) -> bool:
    """Return ``True`` if a span's text should be scrambled as a description.

    A span is classified as a *description* (and therefore scrambled) when it
    does **not** match any of the following patterns:

    - A transaction date (``dd Mmm yy``, e.g. ``"23 Jan 25"``)
    - A partial date — day + month only (``dd Mmm``, e.g. ``"03 Jan"``).
      HSBC credit card statements split the year onto a separate span on the
      last page and at year boundaries; these partial spans are dates and must
      not be scrambled.
    - A payment type code — one of the exact known HSBC codes (``BP``,
      ``)))``, ``VIS``, ``DD``, ``TFR``, ``SO``, ``CR``, ``DR``, ``ATM``,
      ``CC``, ``OBP``).  Short uppercase merchant abbreviations that are not
      in this list (e.g. ``"OVO"``, ``"SJP"``, ``"GYM"``) are **not**
      excluded and will be scrambled.
    - A numeric value or polarity suffix (digits, currency symbols, ``D``/``CR``)
    - A structural balance marker (``BALANCE BROUGHT FORWARD`` /
      ``BALANCE CARRIED FORWARD``) — these are load-bearing labels required
      by the statement parser and must never be scrambled.

    The stripped text is used for matching so that incidental surrounding
    whitespace does not prevent a correct classification.

    Args:
        text: The raw text content of a single PDF span.

    Returns:
        ``True`` if the span should be scrambled; ``False`` otherwise.
    """
    stripped = text.strip()
    if not stripped:
        return False
    # Single-character tokens (e.g. a name initial like "A") are never
    # meaningful description content on their own.  More critically, adding
    # a single-letter pair such as ("A", "H") to the scramble list causes
    # every occurrence of that letter across the entire page content stream to
    # be replaced — corrupting structural labels like "Account", "Advance",
    # "Arranged" that appear well outside the transaction block.
    if len(stripped) < 2:
        return False
    if _DATE_RE.match(stripped):
        return False
    if _DATE_COMPACT_RE.match(stripped):
        return False
    if _DATE_DAY_MONTH_RE.match(stripped):
        return False
    if _MONTH_NAME_RE.match(stripped):
        return False
    if _MONTH_COMPACT_RE.match(stripped):
        return False
    if _PAYMENT_TYPE_RE.match(stripped):
        return False
    if _NUMERIC_RE.match(stripped):
        return False
    if _BALANCE_FORWARD_RE.match(stripped):
        return False
    return True


def _is_protected_line(line_spans: list[dict]) -> bool:
    """Return ``True`` if every token on *line_spans* must be left unscrambled.

    Reconstructs the full line text by space-joining each span's text and
    checks it against :data:`_PROTECTED_LINE_PATTERNS`.  A match means the
    entire line is a load-bearing structural label (e.g. ``BALANCE BROUGHT
    FORWARD``, ``Summary Of Interest On This Statement``) that must not be
    altered even when the individual tokens would otherwise pass
    :func:`_classify_span_as_description`.

    Args:
        line_spans: List of span dicts (all on the same logical line) as
            produced by :func:`_group_spans_into_lines`.

    Returns:
        ``True`` if the line matches a protected pattern; ``False`` otherwise.
    """
    line_text = " ".join(span.get("text", "").strip() for span in line_spans).strip()
    return any(pat.match(line_text) for pat in _PROTECTED_LINE_PATTERNS)


def _group_spans_into_lines(spans: list[dict], snap_tolerance: float = 3.0) -> list[list[dict]]:
    """Group a flat list of spans into logical lines by vertical proximity.

    Two spans are considered to be on the same line when the difference between
    their y-axis midpoints (``(top + bottom) / 2``) is within *snap_tolerance*
    points.  This mirrors the ``snap_y_tolerance`` approach used by pdfplumber
    when building table rows.

    Spans are first sorted by y-midpoint then by x0 so that lines are ordered
    top-to-bottom and spans within each line are ordered left-to-right.

    Args:
        spans: Flat list of span dicts.  Each dict must have ``"top"``,
            ``"bottom"``, and ``"x0"`` keys (pdfplumber char format).
        snap_tolerance: Maximum point difference between y-midpoints for two
            spans to be considered on the same line.  Defaults to ``3.0``.

    Returns:
        Ordered list of lines, where each line is itself a list of span dicts
        sorted left-to-right.  Lines are ordered top-to-bottom.
    """
    if not spans:
        return []

    def _y_mid(span: dict) -> float:
        return (span["top"] + span["bottom"]) / 2.0

    sorted_spans = sorted(spans, key=lambda s: (_y_mid(s), s["x0"]))

    lines: list[list[dict]] = []
    current_line: list[dict] = [sorted_spans[0]]
    current_y = _y_mid(sorted_spans[0])

    for span in sorted_spans[1:]:
        y = _y_mid(span)
        if abs(y - current_y) <= snap_tolerance:
            current_line.append(span)
        else:
            lines.append(current_line)
            current_line = [span]
            current_y = y

    lines.append(current_line)
    return lines


def _is_transaction_line(line_spans: list[dict]) -> bool:
    """Return ``True`` if *line_spans* looks like the opening line of a transaction row.

    A line is classified as a transaction anchor when it contains **at least
    one span that matches the date pattern** (``_DATE_RE``, e.g. ``"23 Jan 25"``)
    **or** a partial date span (``_DATE_DAY_MONTH_RE``, e.g. ``"03 Jan"`` — used
    by HSBC credit card statements on their last page and at year-boundary rows
    where the year is rendered as a separate span) **or** when any span matches
    one of the patterns in :data:`_TRANSACTION_ANCHOR_PATTERNS`.

    The date test is the primary anchor and covers all first-page transaction
    rows across all supported statement layouts.  The partial-date test catches
    the year-split rendering used on HSBC CRD last pages.  The additional anchor
    patterns handle structural lines that open a transaction block without a date
    — for example, ``BALANCE BROUGHT FORWARD`` on continuation pages (pages 2+
    of multi-page HSBC current account statements), where that label appears on
    the first transaction-table line without any date span.

    Date spans are also detected across **consecutive tokens** on the same line
    because ``extract_words`` splits ``"09 Dec 19"`` into three separate tokens
    (``"09"``, ``"Dec"``, ``"19"``) rather than keeping them as a single span.
    Pairs ``"dd Mmm"`` and triplets ``"dd Mmm yy"`` are therefore reconstructed
    by joining adjacent token texts with a single space before matching.

    A payment-type code alone (``_PAYMENT_TYPE_RE``) is intentionally **not**
    sufficient to anchor a transaction block.  Short uppercase sequences such as
    ``APR``, ``MR``, ``AER``, ``NO``, ``ON`` appear in non-transaction contexts
    and would cause entire summary and boilerplate sections to be incorrectly
    swept into the scramble region.  (Note: these examples are not valid
    transaction type codes — only the exact known HSBC codes are recognised.)

    Args:
        line_spans: List of span dicts (all on the same logical line) as
            produced by :func:`_group_spans_into_lines`.

    Returns:
        ``True`` if the line contains at least one transaction-anchor span.
    """
    texts = [span.get("text", "").strip() for span in line_spans]

    for i, text in enumerate(texts):
        # Single-token matches.
        if _DATE_RE.match(text):
            return True
        if _DATE_DAY_MONTH_RE.match(text):
            return True
        if _DATE_COMPACT_RE.match(text):
            return True
        for pattern in _TRANSACTION_ANCHOR_PATTERNS:
            if pattern.match(text):
                return True

        # Multi-token date detection: "dd Mmm" across two consecutive tokens.
        if i + 1 < len(texts):
            next_text = texts[i + 1]
            pair = f"{text} {next_text}"
            if _DATE_DAY_MONTH_RE.match(pair):
                # Guard 1: if the token after the pair is a 4-digit year this is
                # boilerplate prose ("...on 07 Feb 2022"), not a transaction date.
                if i + 2 < len(texts) and re.match(r"^\d{4}$", texts[i + 2]):
                    pass  # skip — not a transaction anchor
                # Guard 2: boilerplate dates appear far to the right of the page
                # (e.g. "...paid by 07 Feb 2022").  Transaction date columns are
                # always in the leftmost region (x0 < 200 pts on all HSBC layouts).
                elif line_spans[i].get("x0", 0.0) > 200.0:
                    pass  # skip — not a transaction anchor
                # Guard 3: a "dd Mmm" pair followed immediately by the word "to"
                # is a statement period date range ("11 May to 10 June 2019"),
                # not a transaction date.
                elif i + 2 < len(texts) and texts[i + 2].lower() == "to":
                    pass  # skip — not a transaction anchor
                else:
                    return True

            # "dd MmmYY" compact date across two tokens: digit + MmmYY.
            compact_pair = f"{text} {next_text}"
            if _DATE_COMPACT_RE.match(compact_pair):
                return True

            # "dd Mmm yy" across three consecutive tokens.
            if i + 2 < len(texts):
                triplet = f"{text} {next_text} {texts[i + 2]}"
                if _DATE_RE.match(triplet):
                    return True

    return False


def _find_transaction_line_indices(lines: list[list[dict]]) -> frozenset[int]:
    """Identify the indices of lines that belong to a transaction block.

    A **transaction anchor line** is one where :func:`_is_transaction_line`
    returns ``True``.  A **continuation line** is any line that immediately
    follows a transaction anchor or another continuation line, provided it does
    not consist solely of numeric content (which would indicate a standalone
    balance, total, or summary row that happens to follow the last transaction).

    A continuation line is considered to have ended the transaction block when
    every non-whitespace span on the line matches ``_NUMERIC_RE`` — i.e. the
    line is pure numbers/amounts with no descriptive text at all.

    Args:
        lines: Ordered list of lines as returned by :func:`_group_spans_into_lines`.

    Returns:
        Frozen set of line indices (0-based) that should be treated as part of
        a transaction block and therefore eligible for description scrambling.
    """
    eligible: set[int] = set()
    in_transaction = False

    for i, line_spans in enumerate(lines):
        if _is_transaction_line(line_spans):
            eligible.add(i)
            in_transaction = True
            # BALANCE CARRIED FORWARD is the explicit end-of-block marker.
            # Include it in the eligible set (so it is processed by the
            # protected-line check) but close the transaction block immediately
            # so that the boilerplate text below the transaction table is never
            # swept in as continuation lines.
            texts = [span.get("text", "").strip() for span in line_spans]
            if any(_BALANCE_CARRIED_FORWARD_RE.match(t) for t in texts):
                in_transaction = False
        elif in_transaction:
            # Check whether this continuation line is purely numeric (balance /
            # summary row) — if so, it terminates the transaction block.
            # Exception: a line containing a long bare-digit reference number
            # (≥5 digits, no decimal/comma — e.g. "35314369001") is a merchant
            # or account reference and must NOT terminate the block even though
            # every other token on the line may be numeric.
            non_empty = [span.get("text", "").strip() for span in line_spans if span.get("text", "").strip()]
            has_ref_number = any(_REF_NUMBER_RE.match(t) for t in non_empty)
            if non_empty and not has_ref_number and all(_NUMERIC_RE.match(t) for t in non_empty):
                in_transaction = False
            else:
                eligible.add(i)
                # in_transaction remains True for the next line

    return frozenset(eligible)


def _build_scramble_replacements(
    plumber_page: Any,
    scramble_map: dict[int, int],
) -> tuple[list[tuple[str, str]], tuple[float, float] | None]:
    """Build a list of (original, scrambled) string pairs for description spans on *plumber_page*.

    Uses pdfplumber character data to group characters into logical lines,
    identify transaction-block lines, then collect all eligible description
    spans and produce scrambled replacements.  Only spans where at least one
    letter changes are included.  Spans on lines that match
    :data:`_PROTECTED_LINE_PATTERNS` (e.g. ``BALANCE BROUGHT FORWARD``,
    ``Summary Of Interest On This Statement``) are excluded even when their
    individual tokens would otherwise pass :func:`_classify_span_as_description`.

    In addition to the replacement pairs, this function derives the **PDF-space
    y-range** that encloses all eligible transaction-block lines.  pdfplumber
    uses a top-down coordinate system (``top`` = distance from page top), while
    PDF content streams use a bottom-up system (``y`` = distance from page
    bottom).  The returned range is expressed in PDF content-stream coordinates
    so it can be used directly as a guard in
    :func:`_rewrite_page_content_stream`:

    ``pdf_y_max = page_height - min(eligible_span.top)``
    ``pdf_y_min = page_height - max(eligible_span.bottom)``

    A small tolerance of ``_SCRAMBLE_Y_TOLERANCE`` points is added on each side
    to absorb floating-point rounding differences between pdfplumber's
    extraction and the raw ``Tm`` values in the content stream.

    Args:
        plumber_page: The pdfplumber page to inspect.
        scramble_map: Translation table produced by :func:`_make_scramble_map`.

    Returns:
        A 2-tuple ``(pairs, y_range)`` where *pairs* is an ordered list of
        ``(original_text, scrambled_text)`` pairs (longest first) and
        *y_range* is a ``(y_min, y_max)`` tuple in PDF content-stream
        coordinates, or ``None`` when there are no eligible lines.
    """
    chars = plumber_page.chars
    if not chars:
        return [], None

    # Build word-level spans from pdfplumber chars by grouping into words.
    # pdfplumber's `words` property does this nicely.
    words = plumber_page.extract_words(keep_blank_chars=False, use_text_flow=False, extra_attrs=["fontname", "size"])
    if not words:
        return [], None

    # Convert words to a span-like format compatible with _group_spans_into_lines.
    word_spans: list[dict] = []
    for w in words:
        word_spans.append(
            {
                "text": w["text"],
                "x0": w["x0"],
                "top": w["top"],
                "x1": w["x1"],
                "bottom": w["bottom"],
                "fontname": w.get("fontname", _FALLBACK_FONT),
                "size": w.get("size", _FALLBACK_FONTSIZE),
            }
        )

    lines = _group_spans_into_lines(word_spans)
    eligible_line_indices = _find_transaction_line_indices(lines)

    # Build lookup: span id → line index.
    # Also identify lines that are fully protected structural labels —
    # every token on such a line must be skipped even if it individually
    # passes _classify_span_as_description (e.g. "BALANCE", "BROUGHT",
    # "FORWARD", "Summary", "Of", "Interest" etc.).
    span_to_line: dict[int, int] = {}
    protected_line_indices: frozenset[int] = frozenset(i for i, line_spans in enumerate(lines) if _is_protected_line(line_spans))
    for line_idx, line_spans in enumerate(lines):
        for span in line_spans:
            span_to_line[id(span)] = line_idx

    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()

    for span in word_spans:
        line_idx = span_to_line.get(id(span), -1)
        if line_idx not in eligible_line_indices:
            continue
        if line_idx in protected_line_indices:
            continue
        text: str = span.get("text", "")

        # Handle compound tokens where pdfplumber's extract_words has merged a
        # payment-type code directly with the following description word (no
        # space between them), e.g. ")))GREGGS" or "DDTOWERWOOD".  In the PDF
        # content stream these are stored as two separate string operands, so
        # the scramble pair must target just the description portion (group 2).
        compound_match = _COMPOUND_TYPE_DESC_RE.match(text)
        if compound_match:
            desc_part = compound_match.group(2)
            if desc_part not in seen:
                scrambled_part = desc_part.translate(scramble_map)
                if scrambled_part != desc_part:
                    pairs.append((desc_part, scrambled_part))
                    seen.add(desc_part)
            continue

        if not _classify_span_as_description(text):
            continue
        if text in seen:
            continue
        scrambled = text.translate(scramble_map)
        if scrambled == text:
            continue
        pairs.append((text, scrambled))
        seen.add(text)

    # Sort longest first to avoid shorter fragments matching inside longer ones.
    pairs.sort(key=lambda p: len(p[0]), reverse=True)

    # Derive the transaction block y-range in PDF content-stream coordinates
    # (origin at bottom of page) from the eligible pdfplumber spans (origin at
    # top of page).  This range is passed to _rewrite_page_content_stream so
    # that scramble replacements are only applied to Tj/TJ instructions whose
    # current text-matrix y-position falls within the transaction zone,
    # preventing collateral corruption of structural labels above the block.
    page_height: float = plumber_page.height
    eligible_spans = [span for i in eligible_line_indices for span in lines[i]]
    if eligible_spans:
        min_top = min(s["top"] for s in eligible_spans)
        max_bottom = max(s["bottom"] for s in eligible_spans)
        raw_y_max = page_height - min_top
        raw_y_min = page_height - max_bottom
        scramble_y_range: tuple[float, float] | None = (
            raw_y_min - _SCRAMBLE_Y_TOLERANCE,
            raw_y_max + _SCRAMBLE_Y_TOLERANCE,
        )
    else:
        scramble_y_range = None

    return pairs, scramble_y_range


# ---------------------------------------------------------------------------
# Page-level orchestration
# ---------------------------------------------------------------------------


def _process_page(
    plumber_page: Any,
    pike_page: pikepdf.Page,
    redactions: list[_Redaction],
    page_index: int,
    scramble_map: dict[int, int] | None,
    pike_doc: pikepdf.Pdf,
) -> int:
    """Apply redactions and optional scrambling to a single page.

    Phase 1 — Locate (pdfplumber): find all bounding boxes for each redaction
    target string on this page.

    Phase 2 — Rewrite (pikepdf): build a list of ``(search, replacement)``
    pairs covering all applicable redactions (and scramble pairs if requested),
    then call :func:`_rewrite_page_content_stream` once to physically remove the
    original text from the content stream and write the replacement strings.

    Redactions are applied in declaration order (longer/more-specific first as
    configured in the TOML).  Within the content stream they are passed as an
    ordered list so the most-specific replacement runs first.

    Args:
        plumber_page: The pdfplumber page used for text location.
        pike_page: The corresponding pikepdf page used for content-stream rewriting.
        redactions: Full list of :class:`_Redaction` objects (filtered by
            ``page_indices`` inside this function).
        page_index: 0-based index of this page (used to filter ``page_indices``).
        scramble_map: Translation table from :func:`_make_scramble_map`, or
            ``None`` if scrambling is disabled.
        pike_doc: The owning :class:`pikepdf.Pdf` document, forwarded to
            :func:`_rewrite_page_content_stream` for stream creation.

    Returns:
        Number of distinct replacement strings applied on this page (redaction
        hits + scramble hits).
    """
    # Build redaction replacement pairs for this page.
    redaction_pairs: list[tuple[str, str]] = []
    for red in redactions:
        if red.page_indices is not None and page_index not in red.page_indices:
            continue
        hits = _find_hits_on_plumber_page(plumber_page, red.search, red.clip)
        if hits:
            redaction_pairs.append((red.search, red.replacement))

    # Build scramble replacement pairs for this page.
    scramble_pairs: list[tuple[str, str]] = []
    scramble_y_range: tuple[float, float] | None = None
    if scramble_map is not None:
        scramble_pairs, scramble_y_range = _build_scramble_replacements(plumber_page, scramble_map)

    if not redaction_pairs and not scramble_pairs:
        return 0

    # Sort each list longest-first independently, then pass separately so
    # _rewrite_page_content_stream can apply the y-range guard only to
    # scramble pairs (redaction pairs must always apply unconditionally).
    redaction_pairs.sort(key=lambda p: len(p[0]), reverse=True)
    scramble_pairs.sort(key=lambda p: len(p[0]), reverse=True)

    _rewrite_page_content_stream(pike_page, redaction_pairs, pike_doc, scramble_pairs=scramble_pairs, scramble_y_range=scramble_y_range)
    return len(redaction_pairs) + len(scramble_pairs)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def anonymise_pdf(
    input_path: Path,
    output_path: Path | None = None,
    config_path: Path | None = None,
    scramble_descriptions: bool = True,
) -> Path:
    """Anonymise a single PDF, writing the result to a new file.

    Loads substitution rules from ``anonymise.toml`` (or *config_path*),
    applies them to every page of *input_path*, and saves the result.  The
    original file is never modified.

    Text removal is performed at the content-stream level via pikepdf:
    matching string operands in ``Tj`` and ``TJ`` operators are replaced
    in-stream, so the original bytes are physically absent from the output PDF
    rather than merely covered by a visual overlay.

    Optionally, every transaction description span on every page is scrambled:
    each letter is replaced with a different random letter of the same case,
    preserving length, non-alpha characters, font name, and font size.  Dates,
    payment type codes (1-3 all-uppercase chars), and numeric values are not
    scrambled.  A single scramble table is generated per document call so the
    mapping is internally consistent within the file.

    Args:
        input_path: Path to the source PDF to anonymise.
        output_path: Destination path for the anonymised PDF.  When ``None``,
            the output filename is derived from *input_path*: each
            ``global_replacements`` entry from the TOML config is applied to
            the stem to remove sensitive substrings, and the result is prefixed
            with ``anonymised_`` and written in the same directory as
            *input_path*.
        config_path: Path to the TOML config file.  When ``None``, uses the
            default project config at
            ``src/bank_statement_parser/project/config/anonymise.toml``.
        scramble_descriptions: When ``True``, scramble the letters of every
            transaction description span across all pages after applying the
            TOML-driven redactions.  Defaults to ``True``.

    Returns:
        Path to the anonymised output PDF.

    Raises:
        FileNotFoundError: If *input_path* or *config_path* does not exist.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_path}")

    resolved_config = Path(config_path) if config_path is not None else _DEFAULT_CONFIG_PATH
    redactions = _load_redactions(resolved_config)

    if output_path is None:
        clean_stem = _anonymise_filename(input_path.stem, redactions)
        output_path = input_path.with_stem(f"{_ANONYMISED_PREFIX}{clean_stem}")
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    scramble_map: dict[int, int] | None = _make_scramble_map() if scramble_descriptions else None
    total_hits = 0

    with pdfplumber.open(str(input_path)) as plumber_doc:
        pike_doc = pikepdf.open(str(input_path))
        try:
            for page_index, (plumber_page, pike_page) in enumerate(zip(plumber_doc.pages, pike_doc.pages)):
                total_hits += _process_page(plumber_page, pike_page, redactions, page_index, scramble_map, pike_doc)

            pike_doc.save(str(output_path), compress_streams=True)
        finally:
            pike_doc.close()

    print(f"Anonymised: {input_path.name} → {output_path.name} ({total_hits} replacement(s))")
    return output_path


def anonymise_folder(
    folder_path: Path,
    pattern: str = "*.pdf",
    output_dir: Path | None = None,
    config_path: Path | None = None,
    scramble_descriptions: bool = True,
) -> list[Path]:
    """Anonymise all PDFs matching *pattern* in *folder_path*.

    Skips any PDF whose stem already starts with ``anonymised_`` to avoid
    re-processing previously anonymised files.

    Args:
        folder_path: Directory to search for PDFs.
        pattern: Glob pattern used to find PDFs within *folder_path*.
            Defaults to ``"*.pdf"``.
        output_dir: Directory to write anonymised PDFs into.  When ``None``,
            each output file is written alongside its source.  In both cases
            the output filename is derived by applying all ``global_replacements``
            from the TOML config to the source stem to remove sensitive
            substrings, then prepending ``anonymised_``.
        config_path: Path to the TOML config file.  When ``None``, uses the
            default project config.
        scramble_descriptions: When ``True``, scramble transaction description
            letters in every processed PDF.  Passed through to
            :func:`anonymise_pdf`.  Defaults to ``True``.

    Returns:
        List of paths to the anonymised output PDFs, in the order processed.

    Raises:
        FileNotFoundError: If *folder_path* does not exist.
        FileNotFoundError: If the config file cannot be found.
    """
    folder_path = Path(folder_path)
    if not folder_path.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    resolved_config = Path(config_path) if config_path is not None else _DEFAULT_CONFIG_PATH
    # Validate config exists once before iterating, so we fail fast.
    redactions = _load_redactions(resolved_config)

    pdfs = sorted(p for p in folder_path.glob(pattern) if not p.stem.startswith(_ANONYMISED_PREFIX))

    if not pdfs:
        print(f"No PDFs matching '{pattern}' found in {folder_path}")
        return []

    outputs: list[Path] = []
    for pdf in pdfs:
        if output_dir is not None:
            clean_stem = _anonymise_filename(pdf.stem, redactions)
            out: Path | None = Path(output_dir) / f"{_ANONYMISED_PREFIX}{clean_stem}{pdf.suffix}"
        else:
            out = None  # anonymise_pdf will default to alongside the source
        outputs.append(anonymise_pdf(pdf, output_path=out, config_path=resolved_config, scramble_descriptions=scramble_descriptions))

    return outputs
