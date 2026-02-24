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
dates (full ``dd Mmm yy`` or partial ``dd Mmm``), payment type codes (1-3
all-uppercase chars), numeric values, or structural balance markers
(``BALANCE BROUGHT FORWARD`` / ``BALANCE CARRIED FORWARD``) are **not**
scrambled.  No per-statement configuration is required.

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

import fitz  # PyMuPDF
import tomllib

from bank_statement_parser.modules.paths import BASE_CONFIG

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default config path — alongside the other project TOML files.
_DEFAULT_CONFIG_PATH: Path = BASE_CONFIG / "anonymise.toml"

# Bounding-box used for address_replacements: covers the personal address
# block in the top-left corner of page 1 across all supported HSBC statement
# layouts.  Coordinates are in PDF user-space points (origin top-left in fitz).
#   x0=0, y0=130, x1=320, y1=250
_ADDRESS_CLIP: fitz.Rect = fitz.Rect(0, 130, 320, 250)

# Suffix appended to the input file stem to form the output file name.
_ANONYMISED_PREFIX: str = "anonymised_"

# The 14 standard PDF base fonts that PyMuPDF can embed without a font file.
# Any font name returned by get_text() that is NOT in this set is an embedded
# proprietary font that cannot be used for add_redact_annot replacement text.
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

# Transaction date: "dd Mmm yy"  (e.g. "23 Jan 25")
_DATE_RE: re.Pattern[str] = re.compile(r"^\d{1,2}\s[A-Z][a-z]{2}\s\d{2}$")

# Partial date — day + month only, no year (e.g. "03 Jan").  HSBC credit card
# statements render the year as a separate span on the last page and on
# year-boundary rows.  These spans must be detected as transaction anchors and
# protected from scrambling even though they do not match _DATE_RE.
_DATE_DAY_MONTH_RE: re.Pattern[str] = re.compile(r"^\d{1,2}\s[A-Z][a-z]{2}$")

# Payment type code: 1–3 all-uppercase alphanumeric characters, no lowercase
# (e.g. "VIS", "DD", "SO", "BP", "ATM", "TFR").  Three closing parentheses
# are a known HSBC artefact also used as a type code.
_PAYMENT_TYPE_RE: re.Pattern[str] = re.compile(r"^[A-Z0-9]{1,3}$|^\)\)\)$")

# Numeric value / polarity suffix: any string that consists only of digits,
# currency symbols, thousands separators, decimal points, whitespace, and
# optional trailing polarity letters (D, CR).  This covers amounts like
# "1,234.56", "1,234.56D", "1,234.56 CR", and isolated suffixes "D" / "CR".
_NUMERIC_RE: re.Pattern[str] = re.compile(r"^[\d£$€\s,\.\-]+(?:CR|D)?$|^CR$|^D$")

# Structural balance markers used as transaction block boundaries on
# continuation pages (pages 2+ of multi-page current account statements).
# On continuation pages these lines appear WITHOUT a date, so they would
# otherwise be missed by the date-only anchor logic in _is_transaction_line.
# The same labels also appear WITH a date on page 1 (already caught by
# _DATE_RE), but matching them here is harmless — the line is still eligible.
#
# These labels must ALSO be excluded from scrambling (they are load-bearing
# structural markers required by the statement parser).
_BALANCE_FORWARD_RE: re.Pattern[str] = re.compile(r"^BALANCE\s+(?:BROUGHT|CARRIED)\s+FORWARD$")

# Ordered tuple of compiled patterns whose presence on a line qualifies that
# line as a **transaction block anchor** in addition to the date-based test.
# Extend this tuple to support other bank / statement layouts without
# modifying _is_transaction_line itself.
_TRANSACTION_ANCHOR_PATTERNS: tuple[re.Pattern[str], ...] = (_BALANCE_FORWARD_RE,)

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
        clip: Optional bounding-box restricting the search area.  ``None``
            means search the entire page.
        page_indices: Optional set of 0-based page indices to restrict
            processing to.  ``None`` means apply to all pages.
    """

    search: str
    replacement: str
    clip: fitz.Rect | None = field(default=None)
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
# Core redaction engine
# ---------------------------------------------------------------------------

# Fallback font used when no overlapping span can be found.
_FALLBACK_FONT = "Times-Roman"
_FALLBACK_FONTSIZE = 9.0


def _normalise_fontname(fontname: str) -> str:
    """Map an arbitrary font name to a PyMuPDF-embeddable base-14 name.

    If *fontname* is already one of the 14 standard PDF base fonts it is
    returned unchanged.  Otherwise the name is matched (case-insensitively)
    against :data:`_FONT_FALLBACK_MAP` to pick the closest substitute, falling
    back to :data:`_FALLBACK_FONT` when no keyword matches.

    Args:
        fontname: Font name as reported by ``page.get_text()``.

    Returns:
        A font name that PyMuPDF can embed without an external font file.
    """
    if fontname in _BASE14_FONTS:
        return fontname
    lower = fontname.lower()
    for keyword, substitute in _FONT_FALLBACK_MAP:
        if keyword in lower:
            return substitute
    return _FALLBACK_FONT


def _font_at_rect(page_spans: list[dict], rect: fitz.Rect) -> tuple[str, float]:
    """Return the font name and size of the span that best covers *rect*.

    Iterates pre-extracted span data for the page and returns the (fontname,
    fontsize) of the span whose bounding box has the greatest overlap area with
    *rect*.  Falls back to :data:`_FALLBACK_FONT` / :data:`_FALLBACK_FONTSIZE`
    when no span intersects.

    Args:
        page_spans: List of span dicts as produced by ``page.get_text('dict')``.
            Each dict must have keys ``"font"``, ``"size"``, and ``"bbox"``.
        rect: The hit rectangle returned by ``page.search_for()``.

    Returns:
        Tuple of ``(fontname, fontsize)``.
    """
    best_font = _FALLBACK_FONT
    best_size = _FALLBACK_FONTSIZE
    best_area = 0.0

    for span in page_spans:
        span_rect = fitz.Rect(span["bbox"])
        intersection = span_rect & rect  # type: ignore[operator]
        if intersection.is_empty:
            continue
        area = intersection.width * intersection.height
        if area > best_area:
            best_area = area
            best_font = _normalise_fontname(span["font"])
            best_size = span["size"]

    return best_font, best_size


def _extract_spans(page: fitz.Page) -> list[dict]:
    """Extract all text spans from *page* as a flat list.

    Args:
        page: PyMuPDF page to extract from.

    Returns:
        Flat list of span dicts, each with at minimum ``"font"``, ``"size"``,
        and ``"bbox"`` keys.
    """
    spans: list[dict] = []
    raw = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
    for block in raw.get("blocks", []):  # type: ignore[union-attr]
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                spans.append(span)
    return spans


def _apply_redactions_to_page(page: fitz.Page, redactions: list[_Redaction], page_index: int) -> int:
    """Apply all applicable redactions to a single PDF page.

    Uses PyMuPDF's redaction-annotation pipeline:
    1. :meth:`fitz.Page.search_for` locates each hit rectangle.
    2. :meth:`fitz.Page.add_redact_annot` marks the area for removal and
       specifies replacement text, using the font and size of the original span.
    3. :meth:`fitz.Page.apply_redactions` burns all annotations into the page,
       stripping the original text content and rendering the replacement.

    Each redaction entry is applied and immediately burned in before searching
    for the next.  This prevents shorter-fragment replacements from matching
    inside text that was already replaced by an earlier entry in the list —
    which would produce stacked/duplicate glyph artefacts when pdfplumber
    re-extracts the text.

    The font name and size are sampled from the original span that most overlaps
    each hit rectangle, so replacement text is rendered at the same size and in
    the same typeface as the text it replaces.

    Args:
        page: The PyMuPDF page object to modify in place.
        redactions: Full list of redactions (filtered by ``page_indices`` here).
        page_index: 0-based index of this page (used to filter ``page_indices``).

    Returns:
        Number of individual text hits replaced on this page.
    """
    hits_total = 0

    for red in redactions:
        # Skip if this redaction is restricted to specific pages.
        if red.page_indices is not None and page_index not in red.page_indices:
            continue

        hits = page.search_for(red.search, clip=red.clip)
        if not hits:
            continue

        # Sample span info before marking any annotations so that the original
        # text is still present in the page's text layer.
        spans = _extract_spans(page)

        for rect in hits:
            fontname, fontsize = _font_at_rect(spans, rect)
            page.add_redact_annot(
                quad=rect,
                text=red.replacement,
                fontname=fontname,
                fontsize=fontsize,
                align=fitz.TEXT_ALIGN_LEFT,
            )
            hits_total += 1

        # Burn in immediately so subsequent search_for calls cannot match
        # inside already-replaced text (avoids stacked-glyph artefacts).
        page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

    return hits_total


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
    - A payment type code (1-3 all-uppercase alphanumeric chars, e.g. ``"VIS"``)
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
    if _DATE_RE.match(stripped):
        return False
    if _DATE_DAY_MONTH_RE.match(stripped):
        return False
    if _PAYMENT_TYPE_RE.match(stripped):
        return False
    if _NUMERIC_RE.match(stripped):
        return False
    if _BALANCE_FORWARD_RE.match(stripped):
        return False
    return True


def _group_spans_into_lines(spans: list[dict], snap_tolerance: float = 3.0) -> list[list[dict]]:
    """Group a flat list of spans into logical lines by vertical proximity.

    Two spans are considered to be on the same line when the difference between
    their y-axis midpoints (``(bbox[1] + bbox[3]) / 2``) is within
    *snap_tolerance* points.  This mirrors the ``snap_y_tolerance`` approach
    used by pdfplumber when building table rows.

    Spans are first sorted by y-midpoint then by x0 so that lines are ordered
    top-to-bottom and spans within each line are ordered left-to-right.

    Args:
        spans: Flat list of span dicts as returned by :func:`_extract_spans`.
            Each dict must have a ``"bbox"`` key containing a 4-tuple
            ``(x0, y0, x1, y1)``.
        snap_tolerance: Maximum point difference between y-midpoints for two
            spans to be considered on the same line.  Defaults to ``3.0``.

    Returns:
        Ordered list of lines, where each line is itself a list of span dicts
        sorted left-to-right.  Lines are ordered top-to-bottom.
    """
    if not spans:
        return []

    def _y_mid(span: dict) -> float:
        bbox = span["bbox"]
        return (bbox[1] + bbox[3]) / 2.0

    sorted_spans = sorted(spans, key=lambda s: (_y_mid(s), s["bbox"][0]))

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

    A payment-type code alone (``_PAYMENT_TYPE_RE``) is intentionally **not**
    sufficient to anchor a transaction block.  Short uppercase sequences such as
    ``APR``, ``MR``, ``AER``, ``NO``, ``ON`` appear in non-transaction contexts
    and would cause entire summary and boilerplate sections to be incorrectly
    swept into the scramble region.

    Args:
        line_spans: List of span dicts (all on the same logical line) as
            produced by :func:`_group_spans_into_lines`.

    Returns:
        ``True`` if the line contains at least one transaction-anchor span.
    """
    for span in line_spans:
        text = span.get("text", "").strip()
        if _DATE_RE.match(text):
            return True
        if _DATE_DAY_MONTH_RE.match(text):
            return True
        for pattern in _TRANSACTION_ANCHOR_PATTERNS:
            if pattern.match(text):
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
        elif in_transaction:
            # Check whether this continuation line is purely numeric (balance /
            # summary row) — if so, it terminates the transaction block.
            non_empty = [span.get("text", "").strip() for span in line_spans if span.get("text", "").strip()]
            if non_empty and all(_NUMERIC_RE.match(t) for t in non_empty):
                in_transaction = False
            else:
                eligible.add(i)
                # in_transaction remains True for the next line

    return frozenset(eligible)


def _scramble_description_spans(page: fitz.Page, scramble_map: dict[int, int]) -> int:
    """Scramble the letters of description spans within transaction blocks on *page*.

    Spans are only scrambled when they satisfy **both** of the following
    conditions:

    1. The span's logical line belongs to a transaction block as determined by
       :func:`_find_transaction_line_indices`.  A transaction block begins on
       any line that contains a date or payment-type code marker, and extends
       through consecutive continuation lines until a purely-numeric line
       (balance / summary row) or a new non-transaction line is encountered.
       This restricts scrambling to the transaction table body and prevents
       statement headers, account summaries, statement dates, totals, page
       numbers, and balance rows from being affected.

    2. :func:`_classify_span_as_description` returns ``True`` for the span's
       text — i.e. the span is not itself a date, payment type code, or numeric
       value.

    The span text is translated through *scramble_map* (letters only; non-alpha
    characters are preserved), then written back over the original using a
    redaction annotation that matches the original font name and size.

    A single :meth:`fitz.Page.apply_redactions` call burns all annotations for
    this page at the end of the function, which is efficient and avoids the
    stacked-glyph artefacts that would arise from per-span apply calls.

    Args:
        page: The PyMuPDF page object to modify in place.
        scramble_map: Translation table produced by :func:`_make_scramble_map`.

    Returns:
        Number of spans scrambled on this page.
    """
    spans = _extract_spans(page)
    hits = 0

    # Group spans into logical lines and identify which lines belong to a
    # transaction block (anchor line + continuation lines).
    lines = _group_spans_into_lines(spans)
    eligible_line_indices = _find_transaction_line_indices(lines)

    # Build a lookup from span identity to its line index so we can gate each
    # span against the eligible set in O(1).
    span_to_line: dict[int, int] = {}
    for line_idx, line_spans in enumerate(lines):
        for span in line_spans:
            span_to_line[id(span)] = line_idx

    # Build a list of rects for *protected* spans — spans whose text matches a
    # structural anchor pattern (e.g. "BALANCE BROUGHT FORWARD").  These must
    # not be scrambled AND their bboxes must not be intersected by any scramble
    # redact_annot, because PyMuPDF's apply_redactions erases the full redact
    # rect from the page content layer — an overlapping annotation would silently
    # clip part of the protected span's rendered glyphs.
    protected_rects: list[fitz.Rect] = [
        fitz.Rect(span["bbox"]) for span in spans if any(pat.match(span.get("text", "").strip()) for pat in _TRANSACTION_ANCHOR_PATTERNS)
    ]

    for span in spans:
        # Gate 1: span must be on a transaction-block line.
        if span_to_line.get(id(span), -1) not in eligible_line_indices:
            continue

        text: str = span.get("text", "")

        # Gate 2: span must be classified as a description (not date/type/number).
        if not _classify_span_as_description(text):
            continue

        scrambled = text.translate(scramble_map)

        # Skip if translation produced no change (e.g. span has no letters).
        if scrambled == text:
            continue

        rect = fitz.Rect(span["bbox"])

        # Gate 3: skip if this span's bbox overlaps any protected structural
        # span.  apply_redactions erases the entire redact rect from the page
        # content, so an overlap would corrupt the protected text even though
        # we do not intend to replace it.
        if any(not (rect & pr).is_empty for pr in protected_rects):  # type: ignore[operator]
            continue

        fontname, fontsize = _font_at_rect(spans, rect)
        page.add_redact_annot(
            quad=rect,
            text=scrambled,
            fontname=fontname,
            fontsize=fontsize,
            align=fitz.TEXT_ALIGN_LEFT,
        )
        hits += 1

    if hits:
        page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)

    return hits


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
            TOML-driven redactions.  Defaults to ``False``.

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

    doc: fitz.Document = fitz.open(str(input_path))
    total_hits = 0

    for page_index, page in enumerate(doc):
        total_hits += _apply_redactions_to_page(page, redactions, page_index)

    if scramble_descriptions:
        scramble_map = _make_scramble_map()
        for page in doc:  # type: ignore[union-attr]
            total_hits += _scramble_description_spans(page, scramble_map)

    doc.save(str(output_path), garbage=4, deflate=True)
    doc.close()

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
            :func:`anonymise_pdf`.  Defaults to ``False``.

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
