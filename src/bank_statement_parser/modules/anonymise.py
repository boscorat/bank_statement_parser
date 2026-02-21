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

Public API
----------
    anonymise_pdf(input_path, output_path=None, config_path=None) -> Path
    anonymise_folder(folder_path, pattern="*.pdf", output_dir=None, config_path=None) -> list[Path]

Example
-------
    from pathlib import Path
    from bank_statement_parser.modules.anonymise import anonymise_pdf, anonymise_folder

    # Single file — writes <stem>_anonymised.pdf alongside the original
    out = anonymise_pdf(Path("statement.pdf"))

    # All PDFs in a folder
    outputs = anonymise_folder(Path("tests/pdfs"))
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF

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
_ANONYMISED_SUFFIX: str = "_anonymised"

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
# Public API
# ---------------------------------------------------------------------------


def anonymise_pdf(
    input_path: Path,
    output_path: Path | None = None,
    config_path: Path | None = None,
) -> Path:
    """Anonymise a single PDF, writing the result to a new file.

    Loads substitution rules from ``anonymise.toml`` (or *config_path*),
    applies them to every page of *input_path*, and saves the result.  The
    original file is never modified.

    Args:
        input_path: Path to the source PDF to anonymise.
        output_path: Destination path for the anonymised PDF.  When ``None``,
            the output is written to ``<input_stem>_anonymised.pdf`` in the
            same directory as *input_path*.
        config_path: Path to the TOML config file.  When ``None``, uses the
            default project config at
            ``src/bank_statement_parser/project/config/anonymise.toml``.

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
        output_path = input_path.with_stem(f"{input_path.stem}{_ANONYMISED_SUFFIX}")
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    doc: fitz.Document = fitz.open(str(input_path))
    total_hits = 0

    for page_index, page in enumerate(doc):
        total_hits += _apply_redactions_to_page(page, redactions, page_index)

    doc.save(str(output_path), garbage=4, deflate=True)
    doc.close()

    print(f"Anonymised: {input_path.name} → {output_path.name} ({total_hits} replacement(s))")
    return output_path


def anonymise_folder(
    folder_path: Path,
    pattern: str = "*.pdf",
    output_dir: Path | None = None,
    config_path: Path | None = None,
) -> list[Path]:
    """Anonymise all PDFs matching *pattern* in *folder_path*.

    Skips any PDF whose stem already ends with ``_anonymised`` to avoid
    re-processing previously anonymised files.

    Args:
        folder_path: Directory to search for PDFs.
        pattern: Glob pattern used to find PDFs within *folder_path*.
            Defaults to ``"*.pdf"``.
        output_dir: Directory to write anonymised PDFs into.  When ``None``,
            each output file is written alongside its source (same directory,
            ``<stem>_anonymised.pdf``).
        config_path: Path to the TOML config file.  When ``None``, uses the
            default project config.

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

    pdfs = sorted(p for p in folder_path.glob(pattern) if not p.stem.endswith(_ANONYMISED_SUFFIX))

    if not pdfs:
        print(f"No PDFs matching '{pattern}' found in {folder_path}")
        return []

    outputs: list[Path] = []
    for pdf in pdfs:
        if output_dir is not None:
            out = Path(output_dir) / f"{pdf.stem}{_ANONYMISED_SUFFIX}{pdf.suffix}"
        else:
            out = None  # anonymise_pdf will default to alongside the source
        outputs.append(anonymise_pdf(pdf, output_path=out, config_path=resolved_config))

    return outputs
