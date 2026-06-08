"""
anonymise — thin shim delegating PDF anonymisation to uk-bank-statement-anonymiser.

Install the optional dependency to use this module::

    pip install uk-bank-statement-parser[anonymise]

Public API
----------
anonymise_pdf(
    input_path: str | Path,
    output_path: str | Path | None = None,
    always_anonymise_path: str | Path | None = None,
    never_anonymise_path: str | Path | None = None,
    debug: bool = False,
) -> Path
"""

from __future__ import annotations

from pathlib import Path

try:
    from bank_statement_anonymiser import anonymise_pdf as _anonymise_pdf
except ImportError as _err:
    raise ImportError(
        "PDF anonymisation requires the uk-bank-statement-anonymiser package.\n"
        "Install it with:  pip install uk-bank-statement-parser[anonymise]"
    ) from _err


def anonymise_pdf(
    input_path: str | Path,
    output_path: str | Path | None = None,
    always_anonymise_path: str | Path | None = None,
    never_anonymise_path: str | Path | None = None,
    debug: bool = False,
) -> Path:
    """Anonymise a single bank statement PDF.

    Delegates entirely to ``bank_statement_anonymiser.anonymise_pdf``.

    Parameters
    ----------
    input_path:
        Path to the source PDF.

    output_path:
        Destination path for the anonymised PDF.  If omitted, the output is
        written to the same directory as the input with the filename prefix
        ``anonymised_`` prepended.  It is strongly recommended to supply an
        explicit output path that does not contain any sensitive information
        (e.g. account numbers or names that may appear in the original filename).

    always_anonymise_path:
        Optional path to a user ``always_anonymise.toml`` file.  Entries here
        force specific strings to a known replacement value and take priority
        over the bundled system file.

    never_anonymise_path:
        Optional path to a user ``never_anonymise.toml`` file.  Phrases listed
        here are preserved exactly as-is during the scramble pass and are merged
        with the bundled system file.

    debug:
        When ``True``, print diagnostic information about config loading,
        fragment classification, and scramble pairs to stdout.

    Returns
    -------
    Path
        The path to the anonymised output file.
    """
    return _anonymise_pdf(
        input_path,
        output_path,
        always_anonymise_path=always_anonymise_path,
        never_anonymise_path=never_anonymise_path,
        debug=debug,
    )
