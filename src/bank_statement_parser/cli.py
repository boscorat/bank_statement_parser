"""
cli — command-line interface for bank_statement_parser.

Entry points are registered in pyproject.toml under [project.scripts].
Add new subcommands here.

Current subcommands
-------------------
anonymise   Anonymise one PDF or all PDFs in a folder.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _cmd_anonymise(args: argparse.Namespace) -> int:
    """Handler for the ``anonymise`` subcommand.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    from bank_statement_parser.modules.anonymise import anonymise_folder, anonymise_pdf

    config_path: Path | None = Path(args.config) if args.config else None

    if args.folder:
        target = Path(args.target)
        if not target.is_dir():
            print(f"Error: '{target}' is not a directory. Use --folder only with a directory path.", file=sys.stderr)
            return 1
        outputs = anonymise_folder(
            folder_path=target,
            pattern=args.pattern,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            config_path=config_path,
        )
        print(f"Done — {len(outputs)} file(s) anonymised.")
    else:
        target = Path(args.target)
        if not target.is_file():
            print(f"Error: '{target}' is not a file. Pass a PDF path or use --folder.", file=sys.stderr)
            return 1
        output_path = Path(args.output) if args.output else None
        anonymise_pdf(input_path=target, output_path=output_path, config_path=config_path)
        print("Done.")

    return 0


def main() -> None:
    """Entry point for the bank_statement_parser CLI.

    Parses sys.argv and dispatches to the appropriate subcommand handler.
    """
    parser = argparse.ArgumentParser(
        prog="bsp",
        description="bank_statement_parser — command-line tools",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # anonymise subcommand
    # ------------------------------------------------------------------
    anon = subparsers.add_parser(
        "anonymise",
        help="Anonymise one PDF or all PDFs in a folder.",
        description=(
            "Replace personally identifiable information in HSBC bank statement PDFs "
            "with dummy values, writing an anonymised copy alongside the original. "
            "Driven by anonymise.toml in the project config directory."
        ),
    )
    anon.add_argument(
        "target",
        metavar="PATH",
        help="PDF file to anonymise, or a folder when --folder is set.",
    )
    anon.add_argument(
        "--folder",
        action="store_true",
        default=False,
        help="Treat PATH as a directory and anonymise all matching PDFs inside it.",
    )
    anon.add_argument(
        "--pattern",
        metavar="GLOB",
        default="*.pdf",
        help="Glob pattern for PDF discovery when --folder is used (default: '*.pdf').",
    )
    anon.add_argument(
        "--output",
        metavar="OUT_FILE",
        default=None,
        help="Output path for single-file mode (default: <stem>_anonymised.pdf alongside input).",
    )
    anon.add_argument(
        "--output-dir",
        metavar="OUT_DIR",
        dest="output_dir",
        default=None,
        help="Output directory for --folder mode (default: alongside each source file).",
    )
    anon.add_argument(
        "--config",
        metavar="CONFIG_TOML",
        default=None,
        help="Path to a custom anonymise.toml (default: project config directory).",
    )

    args = parser.parse_args()

    if args.command == "anonymise":
        sys.exit(_cmd_anonymise(args))
