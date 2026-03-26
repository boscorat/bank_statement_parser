"""
anonymise — exclusion-based full-scramble PDF anonymisation utility.

Unlike a traditional inclusion-based approach (where you specify what to redact),
this module starts from a **completely scrambled PDF** — every *letter* on
every page is replaced with a different random letter of the same case —
and then uses ``anonymise.toml`` to specify what to keep unchanged and
what numbers to scramble.

Scrambling rules
----------------
* Only **letters** (a-z, A-Z) are scrambled by default — digits, punctuation,
  symbols and whitespace are left exactly as they appear in the PDF.
* Numbers listed under ``[numbers_to_scramble]`` are an opt-in exception:
  any Tj fragment whose decoded text *contains* a listed number has its
  digit characters scrambled (replaced by random different digits); all
  non-digit characters in that fragment are left unchanged.
* Text listed under ``[words_to_not_scramble]`` (exact, case-insensitive)
  is never scrambled, regardless of its content.

Config file (``anonymise.toml``)
----------------------------------
``[numbers_to_scramble]``
    ``values = [...]`` — list of number strings (e.g. ``"11-22-33"``,
    ``"12345678"``) whose digit characters should be scrambled wherever
    those strings appear as a substring of a Tj fragment's decoded text.
    Matching is substring-based and case-insensitive.  Only the digit
    characters inside the matching fragment are replaced; hyphens, spaces
    and other separators are preserved.

``[words_to_not_scramble]``
    ``exclude = [...]`` — list of words or phrases (e.g. month names,
    transaction type codes, date conjunctions) that must appear unchanged
    in the output.  Matching is case-insensitive and ignores all whitespace
    (so multi-token phrases rendered as separate Tj calls are detected via
    a sliding-window accumulator).  Pre-populate with English month names,
    ``"from"``, ``"to"``, and any bank-specific transaction type codes.

``[filename_replacements]``
    Key/value pairs applied to the output file stem before prepending the
    ``anonymised_`` prefix.  Same format as the old ``[global_replacements]``
    section.

Implementation note
-------------------
This module shares its low-level PDF engine with the shared helpers in
:mod:`_anonymise_shared`.  The scramble-map builder, content-stream
rewriter, and ToUnicode CMap parser live in that shared module and are
imported here.

The scope of scrambling is full-page: **every Tj fragment on every page**
is scrambled by default, relying on the exclusion rules in the config file
to protect text that must remain readable.

Public API
----------
    anonymise_pdf(input_path, output_path=None, config_path=None) -> Path
    anonymise_folder(folder_path, pattern="*.pdf", output_dir=None, config_path=None) -> list[Path]

Example
-------
    from pathlib import Path
    from bank_statement_parser.modules.anonymise import anonymise_pdf

    out = anonymise_pdf(Path("tsb_statement.pdf"), config_path=Path("anonymise.toml"))
"""

from __future__ import annotations

import random
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pdfplumber
import pikepdf

from bank_statement_parser.modules._anonymise_shared import (
    _ANONYMISED_PREFIX,
    _make_scramble_map,
    _parse_tounicode_cmap,
    _rewrite_page_content_stream,
)
from bank_statement_parser.modules.paths import BASE_CONFIG

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default config path.
_DEFAULT_CONFIG_PATH: Path = BASE_CONFIG / "anonymise.toml"

# Digit characters as a frozenset for fast membership test.
_DIGITS: frozenset[str] = frozenset("0123456789")


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _AnonymiseConfig:
    """Parsed contents of ``anonymise.toml``.

    Args:
        numbers_to_scramble: List of number strings (e.g. ``"11-22-33"``)
            whose digit characters should be scrambled when the string
            appears as a substring of a decoded Tj fragment.  Stored
            lowercased for case-insensitive substring matching.
        words_to_not_scramble: Normalised (lowercase, all-whitespace-stripped)
            set of words/phrases that must not have their letters scrambled.
            Used for exact-token matching and sliding-window phrase detection.
        filename_replacements: Ordered list of ``(search, replacement)`` pairs
            to apply to the output filename stem.
    """

    numbers_to_scramble: list[str]  # lowercased; substring match
    words_to_not_scramble: frozenset[str]  # normalised: lowercase, no whitespace
    filename_replacements: list[tuple[str, str]]  # ordered: longer strings first


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_config(config_path: Path) -> _AnonymiseConfig:
    """Parse ``anonymise.toml`` and return an :class:`_AnonymiseConfig` instance.

    Args:
        config_path: Path to the TOML exclusion config file.

    Returns:
        Parsed :class:`_AnonymiseConfig` with normalised exclusion sets.

    Raises:
        FileNotFoundError: If *config_path* does not exist.
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"anonymise.toml not found at {config_path}.\nCopy project/config/user/anonymise_example.toml to project/config/import/anonymise.toml and fill in your exclusions."
        )

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    raw_numbers: list[str] = data.get("numbers_to_scramble", {}).get("values", [])
    raw_words: list[str] = data.get("words_to_not_scramble", {}).get("exclude", [])
    raw_filename: dict[str, str] = data.get("filename_replacements", {})

    numbers_to_scramble: list[str] = [v.strip().lower() for v in raw_numbers if v.strip()]
    words_to_not_scramble: frozenset[str] = frozenset(_normalise_phrase(w) for w in raw_words)

    # Sort filename replacements longest-first to avoid short-fragment collisions.
    filename_replacements: list[tuple[str, str]] = sorted(raw_filename.items(), key=lambda kv: len(kv[0]), reverse=True)

    return _AnonymiseConfig(
        numbers_to_scramble=numbers_to_scramble,
        words_to_not_scramble=words_to_not_scramble,
        filename_replacements=filename_replacements,
    )


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _normalise_phrase(text: str) -> str:
    """Normalise a phrase for case- and whitespace-insensitive comparison.

    Strips all whitespace characters (spaces, tabs, newlines) and converts to
    lowercase so that ``"Spend & Save Account"`` matches
    ``"spend&saveaccount"`` when the PDF renders it across multiple Tj calls.

    Args:
        text: Raw phrase string.

    Returns:
        Lowercased string with all whitespace removed.
    """
    return re.sub(r"\s+", "", text).lower()


# ---------------------------------------------------------------------------
# Digit scrambling helper
# ---------------------------------------------------------------------------


def _scramble_digits(text: str, digit_map: dict[str, str]) -> str:
    """Replace each digit in *text* using *digit_map*, leaving non-digits unchanged.

    Args:
        text: The decoded fragment text.
        digit_map: Mapping of each digit character ``"0"``-``"9"`` to a
            different digit character produced by :func:`_make_digit_map`.

    Returns:
        The text with every digit character replaced by its mapped digit.
    """
    return "".join(digit_map.get(c, c) for c in text)


def _make_digit_map() -> dict[str, str]:
    """Build a randomised digit-substitution table (0-9 -> different 0-9).

    Each digit is mapped to a *different* digit so that, for example, sort
    code ``11-22-33`` never stays ``11-22-33`` after scrambling.

    Returns:
        Dict mapping each digit character to a different digit character.
    """
    digits = list("0123456789")
    shuffled = digits[:]
    while True:
        random.shuffle(shuffled)
        if all(orig != shuf for orig, shuf in zip(digits, shuffled)):
            break
    return dict(zip(digits, shuffled))


# ---------------------------------------------------------------------------
# Filename handling
# ---------------------------------------------------------------------------


def _anonymise_filename(stem: str, config: _AnonymiseConfig) -> str:
    """Apply filename replacements to *stem*.

    Applies each ``(search, replacement)`` pair from ``config.filename_replacements``
    in order (longest strings first, as stored in :class:`_AnonymiseConfig`).

    Args:
        stem: The filename stem (without extension) to sanitise.
        config: Parsed :class:`_AnonymiseConfig` instance.

    Returns:
        The sanitised stem with matching substrings replaced.
    """
    result = stem
    for search, replacement in config.filename_replacements:
        result = result.replace(search, replacement)
    return result


# ---------------------------------------------------------------------------
# Scramble pair builder (full-page, exclusion-based)
# ---------------------------------------------------------------------------


def _build_full_page_scramble_pairs(
    plumber_page: Any,
    scramble_map: dict[int, int],
    config: _AnonymiseConfig,
) -> list[tuple[str, str]]:
    """Build ``(original, scrambled)`` string pairs for the Latin-1 fallback path.

    Used when the page has no ToUnicode CMaps (e.g. HSBC PDFs with
    WinAnsiEncoding).  Extracts words via pdfplumber and builds pairs at the
    word level.  Only letters are scrambled; digits and symbols are preserved
    unless the word contains a substring listed in ``config.numbers_to_scramble``.

    Args:
        plumber_page: The pdfplumber page to inspect.
        scramble_map: Letter translation table from :func:`_make_scramble_map`.
        config: Parsed :class:`_AnonymiseConfig` exclusion config.

    Returns:
        Ordered list of ``(original_text, scrambled_text)`` pairs, longest first.
    """
    chars = plumber_page.chars
    if not chars:
        return []

    words = plumber_page.extract_words(keep_blank_chars=False, use_text_flow=False)
    if not words:
        return []

    digit_map = _make_digit_map()
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()

    for w in words:
        text: str = w["text"].strip()
        if not text or text in seen:
            continue

        normalised = _normalise_phrase(text)

        # Protected word/phrase — skip entirely.
        if normalised in config.words_to_not_scramble:
            seen.add(text)
            continue

        # Determine whether digit scrambling applies.
        scramble_these_digits = any(num in normalised for num in config.numbers_to_scramble)

        # Build scrambled version: letters via scramble_map, digits via digit_map
        # (only when the fragment contains a listed number), symbols unchanged.
        scrambled_chars: list[str] = []
        for ch in text:
            if ch.isalpha():
                scrambled_chars.append(chr(scramble_map.get(ord(ch), ord(ch))))
            elif ch in _DIGITS and scramble_these_digits:
                scrambled_chars.append(digit_map.get(ch, ch))
            else:
                scrambled_chars.append(ch)
        scrambled = "".join(scrambled_chars)

        if scrambled == text:
            seen.add(text)
            continue

        pairs.append((text, scrambled))
        seen.add(text)

    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def _build_full_page_scramble_bytes_pairs(
    plumber_page: Any,
    pike_page: Any,
    scramble_map: dict[int, int],
    config: _AnonymiseConfig,
) -> list[tuple[bytes, bytes]]:
    """Build raw-bytes ``(original, scrambled)`` pairs for every Tj fragment.

    Operates directly on the PDF content stream to handle PDFs where one
    pdfplumber "word" spans multiple consecutive ``Tj`` operators (e.g. TSB
    statements with custom Type1 font encodings).

    Scrambling rules applied to each decoded fragment:
    - **Letters** (a-z, A-Z) are always scrambled via *scramble_map* unless
      the fragment is listed in ``config.words_to_not_scramble`` (exact match
      after normalisation) or is part of a protected phrase window (sliding
      accumulator over consecutive fragments).
    - **Digits** are scrambled only when the decoded fragment contains a
      substring listed in ``config.numbers_to_scramble``.
    - **Everything else** (symbols, punctuation, spaces) is never scrambled.

    For PDFs with no ``/ToUnicode`` streams the returned list is empty;
    callers fall back to :func:`_build_full_page_scramble_pairs`.

    Args:
        plumber_page: The pdfplumber page (used only to gate on page having chars).
        pike_page: The pikepdf page used to access ``/Resources/Font`` CMaps and
            to parse the content stream.
        scramble_map: Letter translation table from :func:`_make_scramble_map`.
        config: Parsed :class:`_AnonymiseConfig` exclusion config.

    Returns:
        Ordered list of ``(original_raw_bytes, scrambled_raw_bytes)`` pairs,
        longest first.  Empty when no ToUnicode CMap is found.
    """
    # ------------------------------------------------------------------
    # Build per-font forward and reverse maps from ToUnicode CMaps.
    # ------------------------------------------------------------------
    try:
        resources = pike_page.obj.get("/Resources", pikepdf.Dictionary())
        font_dict = resources.get("/Font", pikepdf.Dictionary()) if resources else pikepdf.Dictionary()
    except Exception:
        return []

    forward_maps: dict[str, dict[int, str]] = {}  # font_name -> {glyph_byte -> unicode_char}
    reverse_maps: dict[str, dict[str, int]] = {}  # font_name -> {unicode_char -> glyph_byte}

    for fname in font_dict.keys():
        try:
            f = font_dict[fname]
            to_uni = f.get("/ToUnicode")
            if to_uni is None:
                continue
            fwd = _parse_tounicode_cmap(bytes(to_uni.read_bytes()))
            if not fwd:
                continue
            rev: dict[str, int] = {}
            for gb, uc in fwd.items():
                if uc not in rev:
                    rev[uc] = gb
            forward_maps[str(fname)] = fwd
            reverse_maps[str(fname)] = rev
        except Exception:
            continue

    if not forward_maps:
        return []

    # ------------------------------------------------------------------
    # Parse the content stream — collect (raw_bytes, font_name) per Tj/TJ.
    # ------------------------------------------------------------------
    try:
        instructions = list(pikepdf.parse_content_stream(pike_page))
    except Exception:
        return []

    fragments: list[tuple[bytes, str]] = []
    current_font: str = next(iter(forward_maps))

    for operands, operator in instructions:
        op = str(operator)
        if op == "Tf" and operands:
            try:
                candidate = str(operands[0])
                if candidate in forward_maps:
                    current_font = candidate
            except Exception:
                pass
        elif op == "Tj" and operands:
            try:
                raw = bytes(operands[0])
                if raw:
                    fragments.append((raw, current_font))
            except Exception:
                pass
        elif op == "TJ" and operands:
            try:
                arr = operands[0]
                for item in list(arr):  # type: ignore[arg-type]
                    try:
                        raw = bytes(item)
                        if raw:
                            fragments.append((raw, current_font))
                    except Exception:
                        pass
            except Exception:
                pass

    if not fragments:
        return []

    # ------------------------------------------------------------------
    # Decode each fragment using its font's forward map.
    # ------------------------------------------------------------------
    def _decode_fragment(raw: bytes, font: str) -> str:
        fwd = forward_maps.get(font, {})
        return "".join(fwd.get(b, "") for b in raw)

    decoded_fragments: list[str] = [_decode_fragment(raw, font) for raw, font in fragments]

    # ------------------------------------------------------------------
    # Sliding-window phrase protection.
    # Accumulate consecutive decoded fragments and check whether the joined
    # (normalised) string matches any entry in words_to_not_scramble.
    # ------------------------------------------------------------------
    n = len(decoded_fragments)
    phrase_protected_indices: set[int] = set()

    for start in range(n):
        accumulated = ""
        for end in range(start, n):
            accumulated += decoded_fragments[end]
            normalised = _normalise_phrase(accumulated)
            if normalised in config.words_to_not_scramble:
                for i in range(start, end + 1):
                    phrase_protected_indices.add(i)
                break

    # ------------------------------------------------------------------
    # Build one digit_map per page (shared across all fragments on this page).
    # ------------------------------------------------------------------
    digit_map = _make_digit_map()

    # ------------------------------------------------------------------
    # Build a set of raw-byte sequences that must NEVER be scrambled.
    #
    # A raw-byte sequence may represent the same glyph text at multiple
    # positions on the page — some protected, some not.  Because content-
    # stream rewriting replaces bytes globally (not per-position), if we
    # were to scramble bytes that also appear at a protected index we would
    # corrupt the protected text.  The safe rule is: if ANY occurrence of a
    # given raw-byte sequence is phrase-protected (or exact-token-protected),
    # that byte sequence is left unchanged everywhere on the page.
    # ------------------------------------------------------------------
    protected_raw: set[bytes] = set()
    for idx, (raw, _font) in enumerate(fragments):
        decoded = decoded_fragments[idx]
        if not decoded:
            continue
        if idx in phrase_protected_indices or _normalise_phrase(decoded) in config.words_to_not_scramble:
            protected_raw.add(raw)

    # ------------------------------------------------------------------
    # Build pairs: letters-only scramble + optional digit scramble.
    # ------------------------------------------------------------------
    pairs_bytes: list[tuple[bytes, bytes]] = []
    seen_raw: set[bytes] = set()

    for idx, (raw, font) in enumerate(fragments):
        if raw in seen_raw:
            continue

        decoded = decoded_fragments[idx]
        if not decoded:
            seen_raw.add(raw)
            continue

        # Skip any raw-byte sequence that appears at a protected position.
        if raw in protected_raw:
            seen_raw.add(raw)
            continue

        # Determine whether digit scrambling applies to this fragment.
        normalised_decoded = _normalise_phrase(decoded)
        scramble_these_digits = any(num in normalised_decoded for num in config.numbers_to_scramble)

        # Build the scrambled version character by character.
        rev = reverse_maps.get(font, {})
        scrambled_chars: list[str] = []
        for ch in decoded:
            if ch.isalpha():
                scrambled_chars.append(chr(scramble_map.get(ord(ch), ord(ch))))
            elif ch in _DIGITS and scramble_these_digits:
                scrambled_chars.append(digit_map.get(ch, ch))
            else:
                scrambled_chars.append(ch)
        scrambled = "".join(scrambled_chars)

        if scrambled == decoded:
            seen_raw.add(raw)
            continue

        # Re-encode the scrambled text to raw bytes using the font's reverse map.
        try:
            scrambled_raw = bytes(rev[c] for c in scrambled)
        except KeyError:
            # A character has no glyph in this font — skip this fragment.
            seen_raw.add(raw)
            continue

        if scrambled_raw == raw:
            seen_raw.add(raw)
            continue

        pairs_bytes.append((raw, scrambled_raw))
        seen_raw.add(raw)

    # Longest-first to prevent short fragments matching inside longer ones.
    pairs_bytes.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs_bytes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def anonymise_pdf(
    input_path: Path,
    output_path: Path | None = None,
    config_path: Path | None = None,
) -> Path:
    """Anonymise a single PDF using exclusion-based full-page letter scrambling.

    Every letter on every page is scrambled.  Digits and symbols are left
    unchanged unless they match a ``[numbers_to_scramble]`` entry.  Text
    listed in ``[words_to_not_scramble]`` is preserved as-is.

    Args:
        input_path: Path to the source PDF to anonymise.
        output_path: Destination path for the anonymised PDF.  When ``None``,
            the output filename is derived from *input_path* by applying
            ``filename_replacements`` from the config and prepending
            ``anonymised_``.
        config_path: Path to the ``anonymise.toml`` exclusion config.  When
            ``None``, uses the default project config.

    Returns:
        Path to the anonymised output PDF.

    Raises:
        FileNotFoundError: If *input_path* or the config file does not exist.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_path}")

    resolved_config = Path(config_path) if config_path is not None else _DEFAULT_CONFIG_PATH
    config = _load_config(resolved_config)

    if output_path is None:
        clean_stem = _anonymise_filename(input_path.stem, config)
        output_path = input_path.with_name(f"{_ANONYMISED_PREFIX}{clean_stem}{input_path.suffix}")
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    scramble_map = _make_scramble_map()
    total_pairs = 0

    with pdfplumber.open(str(input_path)) as plumber_doc:
        pike_doc = pikepdf.open(str(input_path))
        try:
            for plumber_page, pike_page in zip(plumber_doc.pages, pike_doc.pages):
                # Try bytes-pair approach first (custom-encoded fonts like TSB).
                scramble_bytes = _build_full_page_scramble_bytes_pairs(plumber_page, pike_page, scramble_map, config)
                if scramble_bytes:
                    _rewrite_page_content_stream(
                        pike_page,
                        [],
                        pike_doc,
                        scramble_y_range=None,
                        scramble_bytes_pairs=scramble_bytes,
                    )
                    total_pairs += len(scramble_bytes)
                else:
                    # Fallback: Latin-1 string-pair approach (HSBC / WinAnsiEncoding fonts).
                    scramble_pairs = _build_full_page_scramble_pairs(plumber_page, scramble_map, config)
                    if scramble_pairs:
                        _rewrite_page_content_stream(
                            pike_page,
                            [],
                            pike_doc,
                            scramble_pairs=scramble_pairs,
                            scramble_y_range=None,
                        )
                        total_pairs += len(scramble_pairs)

            pike_doc.save(str(output_path), compress_streams=True)
        finally:
            pike_doc.close()

    print(f"Anonymised: {input_path.name} -> {output_path.name} ({total_pairs} scramble pair(s))")
    return output_path


def anonymise_folder(
    folder_path: Path,
    pattern: str = "*.pdf",
    output_dir: Path | None = None,
    config_path: Path | None = None,
) -> list[Path]:
    """Anonymise all PDFs matching *pattern* in *folder_path* using exclusion-based scrambling.

    Skips any PDF whose stem already starts with ``anonymised_`` to avoid
    re-processing previously anonymised files.

    Args:
        folder_path: Directory to search for PDFs.
        pattern: Glob pattern used to find PDFs within *folder_path*.
            Defaults to ``"*.pdf"``.
        output_dir: Directory to write anonymised PDFs into.  When ``None``,
            each output file is written alongside its source.
        config_path: Path to the ``anonymise.toml`` config file.  When
            ``None``, uses the default project config.

    Returns:
        List of paths to the anonymised output PDFs, in the order processed.

    Raises:
        FileNotFoundError: If *folder_path* or the config file does not exist.
    """
    folder_path = Path(folder_path)
    if not folder_path.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    resolved_config = Path(config_path) if config_path is not None else _DEFAULT_CONFIG_PATH
    # Validate config exists once before iterating, so we fail fast.
    config = _load_config(resolved_config)

    pdfs = sorted(p for p in folder_path.glob(pattern) if not p.stem.startswith(_ANONYMISED_PREFIX))

    if not pdfs:
        print(f"No PDFs matching '{pattern}' found in {folder_path}")
        return []

    outputs: list[Path] = []
    for pdf in pdfs:
        if output_dir is not None:
            clean_stem = _anonymise_filename(pdf.stem, config)
            out: Path | None = Path(output_dir) / f"{_ANONYMISED_PREFIX}{clean_stem}{pdf.suffix}"
        else:
            out = None  # anonymise_pdf will default to alongside the source
        outputs.append(anonymise_pdf(pdf, output_path=out, config_path=resolved_config))

    return outputs
