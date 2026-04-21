"""
_anonymise_shared — shared constants, patterns, and engine functions for PDF anonymisation.

This internal module is imported by :mod:`anonymise` (exclusion-based
full-scramble anonymisation).  It contains:

* Font-handling constants and helpers
* Compiled regex patterns for date, payment-type, numeric, and structural token
  classification
* The per-document scramble-map builder (:func:`_make_scramble_map`)
* The pikepdf content-stream rewriter (:func:`_rewrite_page_content_stream`)
* pdfplumber span-grouping and transaction-block detection helpers
* The description-scramble pair builder (:func:`_build_scramble_replacements`)

None of the symbols here form part of the public API.  Import from
:mod:`anonymise` instead.
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass, field
from typing import Any

import pikepdf
import pdfplumber  # noqa: F401 — imported for type-hint context in callers

# ---------------------------------------------------------------------------
# ToUnicode CMap parsing
# ---------------------------------------------------------------------------


def _parse_tounicode_cmap(stream_bytes: bytes) -> dict[int, str]:
    """Parse a PDF ToUnicode CMap stream into a glyph-byte → Unicode-char mapping.

    Handles the ``beginbfchar`` / ``endbfchar`` sections found in standard
    ToUnicode CMaps.  Only single-byte glyph codes (``<XX>``) mapping to a
    single Unicode code point (``<YYYY>``) are extracted; multi-byte codes and
    range sections (``beginbfrange``) are ignored.

    Args:
        stream_bytes: Raw bytes of the ToUnicode CMap stream.

    Returns:
        Dict mapping each glyph byte value (0–255) to the Unicode character it
        represents.  Entries where the Unicode code point is U+0000 (unmapped)
        are omitted.
    """
    text = stream_bytes.decode("latin-1", errors="replace")
    result: dict[int, str] = {}
    for m in re.finditer(r"<([0-9a-fA-F]{2})>\s*<([0-9a-fA-F]{4})>", text):
        glyph_byte = int(m.group(1), 16)
        unicode_cp = int(m.group(2), 16)
        if unicode_cp != 0:
            result[glyph_byte] = chr(unicode_cp)
    return result


def _build_font_cmap_reverse(pike_page: pikepdf.Page) -> dict[str, dict[str, int]]:
    """Build per-font reverse ToUnicode mappings for *pike_page*.

    For each font resource on the page that has a ``/ToUnicode`` stream,
    parses the CMap and inverts it to produce a ``unicode_char → glyph_byte``
    dict.  This allows a caller to encode a known Unicode word back into the
    raw byte sequence that the PDF stores internally — which is necessary when
    the font uses a custom ``/Differences`` encoding rather than Latin-1.

    Args:
        pike_page: The pikepdf page whose ``/Resources/Font`` dict is inspected.

    Returns:
        Dict mapping each PDF font resource name (e.g. ``"/F1"``) to a
        ``{unicode_char: glyph_byte}`` reverse-mapping dict.  Only fonts with
        parseable ``/ToUnicode`` streams appear in the returned dict.
    """
    result: dict[str, dict[str, int]] = {}
    try:
        resources = pike_page.obj.get("/Resources", pikepdf.Dictionary())
        if resources is None:
            return result
        font_dict = resources.get("/Font", pikepdf.Dictionary())
        if font_dict is None:
            return result
    except Exception:
        return result

    for fname in font_dict.keys():
        try:
            f = font_dict[fname]
            to_uni_obj = f.get("/ToUnicode")
            if to_uni_obj is None:
                continue
            stream_data = bytes(to_uni_obj.read_bytes())
            forward: dict[int, str] = _parse_tounicode_cmap(stream_data)
            if not forward:
                continue
            # Invert: unicode_char → glyph_byte (first occurrence wins)
            reverse: dict[str, int] = {}
            for glyph_byte, uni_char in forward.items():
                if uni_char not in reverse:
                    reverse[uni_char] = glyph_byte
            result[str(fname)] = reverse
        except Exception:
            continue

    return result


# ---------------------------------------------------------------------------
# Output filename prefix
# ---------------------------------------------------------------------------

# Prefix prepended to the input file stem by anonymise.py to form the output
# file name.
_ANONYMISED_PREFIX: str = "anonymised_"

# ---------------------------------------------------------------------------
# Font-handling constants
# ---------------------------------------------------------------------------

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

# Fallback font/size used when no overlapping char can be found.
_FALLBACK_FONT: str = "Times-Roman"
_FALLBACK_FONTSIZE: float = 9.0

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
_PAYMENT_TYPE_RE: re.Pattern[str] = re.compile(r"^(?:BP|\)\)\)|VIS|DD|TFR|SO|CR|DR|ATM|CC|OBP)$")

# Compound token produced when pdfplumber's extract_words merges a payment-type
# code directly with the following description word, with no whitespace between
# them.  Group 1 captures the payment-type prefix; group 2 captures the
# description text that must be scrambled.
_COMPOUND_TYPE_DESC_RE: re.Pattern[str] = re.compile(r"^(BP|\)\)\)|VIS|DD|TFR|SO|CR|DR|ATM|CC|OBP)([A-Za-z].*)$")

# Numeric value / polarity suffix: any string that consists only of digits,
# currency symbols, thousands separators, decimal points, whitespace, and
# optional trailing polarity letters (D, CR).
_NUMERIC_RE: re.Pattern[str] = re.compile(r"^[\d£$€\s,\.\-]+(?:CR|D)?$|^CR$|^D$")

# Reference number: starts with a digit, contains only digits and hyphens,
# at least 5 characters total.
_REF_NUMBER_RE: re.Pattern[str] = re.compile(r"^\d[\d\-]{4,}$")

# Standalone month name in title-case (e.g. "Jan", "Feb").
_MONTH_NAME_RE: re.Pattern[str] = re.compile(r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$")

# Compact month+year token (e.g. "Dec21", "Jan22").
_MONTH_COMPACT_RE: re.Pattern[str] = re.compile(r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\d{2}$")

# Structural balance markers used as transaction block boundaries on
# continuation pages.
_BALANCE_FORWARD_RE: re.Pattern[str] = re.compile(
    r"^BALANCE\s+(?:BROUGHT|CARRIED)\s+FORWARD$"
    r"|^BALANCE(?:BROUGHT|CARRIED)FORWARD$"
)

# Matches only the CARRIED variant — used to close the transaction block.
_BALANCE_CARRIED_FORWARD_RE: re.Pattern[str] = re.compile(r"^BALANCE\s+CARRIED\s+FORWARD$|^BALANCECARRIEDFORWARD$")

# Ordered tuple of compiled patterns whose presence on a line qualifies that
# line as a transaction block anchor in addition to the date-based test.
_TRANSACTION_ANCHOR_PATTERNS: tuple[re.Pattern[str], ...] = (_BALANCE_FORWARD_RE,)

# Patterns that match the full reconstructed text of a logical line and
# mark every token on that line as protected from scrambling.
_PROTECTED_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^BALANCE\s+(?:BROUGHT|CARRIED)\s+FORWARD", re.IGNORECASE),
    re.compile(r"^Summary\s+Of\s+Interest\s+On\s+This\s+Statement$", re.IGNORECASE),
)

# Exact phrase strings (concatenated, no spaces) that HSBC PDFs render as a
# run of individual single-character Tj operators.
_PROTECTED_CHARRUN_PHRASES: frozenset[str] = frozenset(
    {
        "BALANCEBROUGHTFORWARD",
        "BALANCECARRIEDFORWARD",
        "SummaryOfInterestOnThisStatement",
    }
)

# Tolerance (in PDF points) added to each side of the derived transaction-block
# y-range when guarding scramble replacements in the content stream.
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
# Font helpers
# ---------------------------------------------------------------------------


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

    Args:
        chars: List of pdfplumber char dicts (``page.chars``).
        bbox: ``(x0, y0, x1, y1)`` bounding box in pdfplumber coordinates.

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

    Args:
        bbox: ``(x0, y0, x1, y1)`` of the candidate region.
        clip: ``(x0, y0, x1, y1)`` clip rectangle.

    Returns:
        ``True`` if the rectangles overlap (non-empty intersection).
    """
    bx0, by0, bx1, by1 = bbox
    cx0, cy0, cx1, cy1 = clip
    return bx0 < cx1 and bx1 > cx0 and by0 < cy1 and by1 > cy0


# ---------------------------------------------------------------------------
# PDF string encoding
# ---------------------------------------------------------------------------


def _encode_pdf_string(text: str) -> bytes:
    """Encode a Python string as a PDF literal string in Latin-1.

    Characters outside Latin-1 are transliterated to their closest ASCII
    equivalent where possible; any remaining unmappable characters are dropped.

    Args:
        text: The Python string to encode.

    Returns:
        Bytes suitable for constructing a ``pikepdf.String``.
    """
    try:
        return text.encode("latin-1")
    except UnicodeEncodeError:
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

    Args:
        plumber_page: The pdfplumber page to search.
        search: The literal string to find.
        clip: Optional ``(x0, y0, x1, y1)`` region to restrict results.

    Returns:
        List of ``(x0, y0, x1, y1)`` bounding boxes for each match.
    """
    chars = plumber_page.chars
    if not chars or not search:
        return []

    sorted_chars = sorted(chars, key=lambda c: (round(c["top"], 1), c["x0"]))

    n = len(search)
    hits: list[tuple[float, float, float, float]] = []
    i = 0
    while i <= len(sorted_chars) - n:
        candidate = sorted_chars[i : i + n]
        if "".join(c["text"] for c in candidate) == search:
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
                i += n
                continue
        i += 1

    return hits


# ---------------------------------------------------------------------------
# Content-stream rewriting
# ---------------------------------------------------------------------------


def _decode_pdf_operand(obj: pikepdf.Object) -> str:
    """Decode a pikepdf string operand to a Python str, best-effort.

    Args:
        obj: A ``pikepdf.String`` or ``pikepdf.Object`` from a content stream.

    Returns:
        Python str of the decoded text.
    """
    raw: bytes = bytes(obj)
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
    font_byte_tables: dict[str, bytes] | None = None,
    scramble_bytes_pairs: list[tuple[bytes, bytes]] | None = None,
) -> bool:
    """Physically replace text strings in *pike_page*'s content stream.

    Parses the page content stream, iterates every operator, and for ``Tj``
    and ``TJ`` text-showing operators replaces any string operand whose decoded
    text contains a target substring with the replacement text re-encoded for
    the same font.

    *replacements* (redaction pairs) are applied unconditionally to every
    ``Tj``/``TJ`` on the page.  *scramble_pairs* are applied only when the
    current text-matrix y-position falls within *scramble_y_range* (when
    provided).  When *scramble_y_range* is ``None``, scramble pairs are applied
    unconditionally to every ``Tj``/``TJ`` on the page.

    When *scramble_bytes_pairs* is provided, it is used **instead of**
    *scramble_pairs* for scrambling.  Each pair is ``(original_raw_bytes,
    scrambled_raw_bytes)``; replacement is done via :meth:`bytes.replace`
    directly on the raw content-stream string before any decoding.  This
    handles PDFs that use custom ``/Differences`` encodings or embedded fonts
    where the raw ``Tj`` bytes are glyph IDs rather than Latin-1 character
    codes.  *scramble_pairs* is ignored when *scramble_bytes_pairs* is
    provided and non-empty.

    When *font_byte_tables* is provided (a dict of font resource name →
    256-byte translate table produced by :func:`_build_font_byte_scramble_tables`),
    byte-level scramble is applied per font via :meth:`bytes.translate`.
    This is a blunt-instrument fallback; prefer *scramble_bytes_pairs* for
    exclusion-aware scrambling.

    A pre-pass scans for runs of single-character ``Tj`` operators interleaved
    with ``Tm`` positioning operators that together spell a phrase in
    :data:`_PROTECTED_CHARRUN_PHRASES`.  Matching ``Tj`` indices are frozen
    and skipped during the replacement pass.

    Args:
        pike_page: The pikepdf page to modify in place.
        replacements: List of ``(search, replacement)`` redaction pairs applied
            unconditionally, in order.
        pike_doc: The owning :class:`pikepdf.Pdf` document.
        scramble_pairs: Optional list of ``(original, scrambled)`` Unicode string
            pairs for description scrambling.  Used only when *scramble_bytes_pairs*
            is not provided.
        scramble_y_range: Optional ``(y_min, y_max)`` in PDF content-stream
            coordinates (origin at page bottom).  ``None`` means apply scramble
            to every instruction unconditionally.
        font_byte_tables: Optional dict mapping PDF font resource names (e.g.
            ``"/F1"``) to 256-byte translation tables.  When supplied, byte-level
            scramble is used per-font; ignored when *scramble_bytes_pairs* is set.
        scramble_bytes_pairs: Optional list of ``(original_raw_bytes,
            scrambled_raw_bytes)`` pairs applied via :meth:`bytes.replace`.
            When non-empty, takes precedence over both *scramble_pairs* and
            *font_byte_tables*.

    Returns:
        ``True`` if at least one replacement was made; ``False`` otherwise.
    """
    if not replacements and not scramble_pairs and not font_byte_tables and not scramble_bytes_pairs:
        return False

    try:
        instructions = list(pikepdf.parse_content_stream(pike_page))
    except Exception:
        return False

    # ------------------------------------------------------------------
    # Pre-pass: identify Tj instruction indices that are part of a
    # single-character-per-Tj run spelling a protected phrase.
    # ------------------------------------------------------------------
    frozen_indices: set[int] = set()

    def _any_phrase_starts_with(prefix: str) -> bool:
        return any(p.startswith(prefix) for p in _PROTECTED_CHARRUN_PHRASES)

    pending: list[tuple[int, str]] = []

    def _flush_pending(is_complete: bool) -> None:
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
            continue
        if op_name == "Tj" and operands and isinstance(operands[0], pikepdf.String):
            ch = _decode_pdf_operand(operands[0])
            if len(ch) == 1:
                candidate = "".join(c for _, c in pending) + ch
                if _any_phrase_starts_with(candidate):
                    pending.append((idx, ch))
                    if candidate in _PROTECTED_CHARRUN_PHRASES:
                        _flush_pending(is_complete=True)
                    continue
                else:
                    _flush_pending(is_complete=False)
                    if _any_phrase_starts_with(ch):
                        pending.append((idx, ch))
                        continue
            else:
                _flush_pending(is_complete=False)
        else:
            _flush_pending(is_complete=False)

    _flush_pending(is_complete=False)

    # ------------------------------------------------------------------
    # Main pass: apply replacements, skipping frozen indices.
    # ------------------------------------------------------------------
    changed = False
    new_instructions: list[tuple[list[pikepdf.Object], pikepdf.Operator]] = []
    current_y: float = 0.0
    current_font: str = ""  # tracks the active font resource name (e.g. "/F1")

    _effective_scramble_pairs: list[tuple[str, str]] = scramble_pairs if scramble_pairs else []
    _effective_bytes_pairs: list[tuple[bytes, bytes]] = scramble_bytes_pairs if scramble_bytes_pairs else []
    _use_byte_tables: bool = bool(font_byte_tables)
    _use_bytes_pairs: bool = bool(_effective_bytes_pairs)

    def _apply_byte_scramble(raw: bytes, font_name: str) -> bytes:
        """Apply byte-level table scramble for *font_name* to *raw*, if a table exists."""
        if not _use_byte_tables or font_byte_tables is None:
            return raw
        table = font_byte_tables.get(font_name)
        if table is None:
            return raw
        return raw.translate(table)

    def _apply_string_scramble(text: str) -> str:
        """Apply string-level scramble pairs to *text*."""
        for search, replacement in _effective_scramble_pairs:
            text = text.replace(search, replacement)
        return text

    def _apply_bytes_scramble(raw: bytes) -> bytes:
        """Return the scrambled replacement if *raw* exactly matches a pair, else *raw* unchanged."""
        for search_b, replacement_b in _effective_bytes_pairs:
            if raw == search_b:
                return replacement_b
        return raw

    for idx, (operands, operator) in enumerate(instructions):
        op_name = str(operator)

        if op_name == "Tf" and len(operands) >= 1:
            # Track the current font resource name for byte-table lookup.
            try:
                current_font = str(operands[0])
            except Exception:
                current_font = ""

        if op_name == "Tm" and len(operands) >= 6:
            try:
                current_y = float(operands[5])
            except ValueError, TypeError:
                pass

        if op_name == "Tj" and operands and idx not in frozen_indices:
            obj = operands[0]
            if isinstance(obj, pikepdf.String):
                raw_original = bytes(obj)
                within_range = scramble_y_range is None or (scramble_y_range[0] <= current_y <= scramble_y_range[1])

                if within_range and _use_bytes_pairs:
                    # Bytes-pair scramble: operate directly on raw bytes — no decode needed.
                    new_raw = _apply_bytes_scramble(raw_original)
                    if new_raw != raw_original:
                        operands = [pikepdf.String(new_raw)]
                        changed = True
                        new_instructions.append((operands, operator))  # type: ignore[arg-type]
                        continue
                    # Fall through to apply redaction replacements (string level) if any.

                # Apply redaction replacements (string level, Latin-1 decode)
                decoded = _decode_pdf_operand(obj)
                new_text = decoded
                for search, replacement in replacements:
                    new_text = new_text.replace(search, replacement)

                # Scrambling fallbacks: byte-table per font, then string pairs
                if within_range and not _use_bytes_pairs:
                    if _use_byte_tables and font_byte_tables is not None and current_font in font_byte_tables:
                        new_raw2 = _apply_byte_scramble(raw_original, current_font)
                        if new_raw2 != raw_original:
                            operands = [pikepdf.String(new_raw2)]
                            changed = True
                            new_instructions.append((operands, operator))  # type: ignore[arg-type]
                            continue
                    elif _effective_scramble_pairs:
                        new_text = _apply_string_scramble(new_text)

                if new_text != decoded:
                    operands = [pikepdf.String(_encode_pdf_string(new_text))]
                    changed = True

        elif op_name == "TJ" and operands:
            arr = operands[0]
            if isinstance(arr, pikepdf.Array):
                within_range = scramble_y_range is None or (scramble_y_range[0] <= current_y <= scramble_y_range[1])
                apply_bytes = _use_bytes_pairs and within_range
                apply_byte_table = _use_byte_tables and font_byte_tables is not None and current_font in (font_byte_tables or {})
                apply_str = bool(_effective_scramble_pairs)
                new_arr_items: list[pikepdf.Object] = []
                for item in list(arr):  # type: ignore[arg-type]
                    if isinstance(item, pikepdf.String):
                        raw_item = bytes(item)
                        if apply_bytes:
                            new_raw_item = _apply_bytes_scramble(raw_item)
                            if new_raw_item != raw_item:
                                new_arr_items.append(pikepdf.String(new_raw_item))
                                changed = True
                                continue
                        decoded = _decode_pdf_operand(item)
                        new_text = decoded
                        for search, replacement in replacements:
                            new_text = new_text.replace(search, replacement)
                        if within_range and not apply_bytes:
                            if apply_byte_table:
                                new_raw_tbl = _apply_byte_scramble(raw_item, current_font)
                                if new_raw_tbl != raw_item:
                                    new_arr_items.append(pikepdf.String(new_raw_tbl))
                                    changed = True
                                    continue
                            elif apply_str:
                                new_text = _apply_string_scramble(new_text)
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
    uppercase letter.  Generated once per document call for internal consistency.

    Returns:
        A translation table suitable for use with :meth:`str.translate`.
    """
    lower_shuffled = list(_LOWER_LETTERS)
    _rng = secrets.SystemRandom()
    while True:
        _rng.shuffle(lower_shuffled)
        if all(orig != shuf for orig, shuf in zip(_LOWER_LETTERS, lower_shuffled)):
            break

    upper_shuffled = list(_UPPER_LETTERS)
    while True:
        _rng.shuffle(upper_shuffled)
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

    A span is classified as a description (and therefore scrambled) when it
    does not match any of the following patterns:

    - A transaction date (``dd Mmm yy``, partial ``dd Mmm``, compact ``dd MmmYY``)
    - A standalone or compact month name token
    - A payment type code (exact known codes only)
    - A numeric value or polarity suffix
    - A structural balance marker

    Single-character tokens are never scrambled (adding a single-letter pair
    to the scramble list would corrupt structural labels across the page).

    Args:
        text: The raw text content of a single PDF span.

    Returns:
        ``True`` if the span should be scrambled; ``False`` otherwise.
    """
    stripped = text.strip()
    if not stripped:
        return False
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

    Checks the reconstructed line text against :data:`_PROTECTED_LINE_PATTERNS`.

    Args:
        line_spans: List of span dicts (all on the same logical line).

    Returns:
        ``True`` if the line matches a protected pattern; ``False`` otherwise.
    """
    line_text = " ".join(span.get("text", "").strip() for span in line_spans).strip()
    return any(pat.match(line_text) for pat in _PROTECTED_LINE_PATTERNS)


def _group_spans_into_lines(spans: list[dict], snap_tolerance: float = 3.0) -> list[list[dict]]:
    """Group a flat list of spans into logical lines by vertical proximity.

    Two spans are considered to be on the same line when the difference between
    their y-axis midpoints is within *snap_tolerance* points.

    Args:
        spans: Flat list of span dicts with ``"top"``, ``"bottom"``, ``"x0"``.
        snap_tolerance: Maximum point difference between y-midpoints.

    Returns:
        Ordered list of lines (each line is a list of span dicts, left-to-right).
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

    A line is classified as a transaction anchor when it contains at least one
    span matching a date pattern or one of the :data:`_TRANSACTION_ANCHOR_PATTERNS`.
    Date spans are also detected across consecutive tokens (``extract_words``
    can split ``"09 Dec 19"`` into three separate tokens).

    Args:
        line_spans: List of span dicts on the same logical line.

    Returns:
        ``True`` if the line contains at least one transaction-anchor span.
    """
    texts = [span.get("text", "").strip() for span in line_spans]

    for i, text in enumerate(texts):
        if _DATE_RE.match(text):
            return True
        if _DATE_DAY_MONTH_RE.match(text):
            return True
        if _DATE_COMPACT_RE.match(text):
            return True
        for pattern in _TRANSACTION_ANCHOR_PATTERNS:
            if pattern.match(text):
                return True

        if i + 1 < len(texts):
            next_text = texts[i + 1]
            pair = f"{text} {next_text}"
            if _DATE_DAY_MONTH_RE.match(pair):
                if i + 2 < len(texts) and re.match(r"^\d{4}$", texts[i + 2]):
                    pass
                elif line_spans[i].get("x0", 0.0) > 200.0:
                    pass
                elif i + 2 < len(texts) and texts[i + 2].lower() == "to":
                    pass
                else:
                    return True

            compact_pair = f"{text} {next_text}"
            if _DATE_COMPACT_RE.match(compact_pair):
                return True

            if i + 2 < len(texts):
                triplet = f"{text} {next_text} {texts[i + 2]}"
                if _DATE_RE.match(triplet):
                    return True

    return False


def _find_transaction_line_indices(lines: list[list[dict]]) -> frozenset[int]:
    """Identify the indices of lines that belong to a transaction block.

    A transaction anchor line is one where :func:`_is_transaction_line` returns
    ``True``.  A continuation line is any line that immediately follows an
    anchor or continuation, unless it consists solely of numeric content.

    Args:
        lines: Ordered list of lines as returned by :func:`_group_spans_into_lines`.

    Returns:
        Frozen set of 0-based line indices eligible for description scrambling.
    """
    eligible: set[int] = set()
    in_transaction = False

    for i, line_spans in enumerate(lines):
        if _is_transaction_line(line_spans):
            eligible.add(i)
            in_transaction = True
            texts = [span.get("text", "").strip() for span in line_spans]
            if any(_BALANCE_CARRIED_FORWARD_RE.match(t) for t in texts):
                in_transaction = False
        elif in_transaction:
            non_empty = [span.get("text", "").strip() for span in line_spans if span.get("text", "").strip()]
            has_ref_number = any(_REF_NUMBER_RE.match(t) for t in non_empty)
            if non_empty and not has_ref_number and all(_NUMERIC_RE.match(t) for t in non_empty):
                in_transaction = False
            else:
                eligible.add(i)

    return frozenset(eligible)


def _build_scramble_replacements(
    plumber_page: Any,
    scramble_map: dict[int, int],
) -> tuple[list[tuple[str, str]], tuple[float, float] | None]:
    """Build a list of (original, scrambled) string pairs for description spans on *plumber_page*.

    Uses pdfplumber character data to group characters into logical lines,
    identify transaction-block lines, then collect all eligible description
    spans.  Only spans where at least one letter changes are included.

    Also derives the PDF-space y-range that encloses all eligible lines for use
    as a guard in :func:`_rewrite_page_content_stream`.

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

    words = plumber_page.extract_words(keep_blank_chars=False, use_text_flow=False, extra_attrs=["fontname", "size"])
    if not words:
        return [], None

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

    pairs.sort(key=lambda p: len(p[0]), reverse=True)

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
