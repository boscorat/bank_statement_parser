"""
cli — command-line interface for bank_statement_parser.

Entry points are registered in pyproject.toml under [project.scripts].
Add new subcommands here.

Current subcommands
-------------------
anonymise   Anonymise one PDF or all PDFs in a folder (exclusion-based full scrambling).
forex       Fetch daily USD-based exchange rates and persist them to the project database.
process     Parse bank statement PDFs, persist data, and export reports.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _cmd_forex(args: argparse.Namespace) -> int:
    """Handler for the ``forex`` subcommand.

    Fetches daily USD-based exchange rates for all currencies in DimAccount
    (plus any extras) and writes them to the ``exchange_rates`` table.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    from bank_statement_parser.modules.forex import get_exchange_rates

    project_path = Path(args.project).resolve() if args.project else Path.cwd() / "bsp_project"

    extra: list[str] | None = args.currencies if args.currencies else None
    # Prefer explicit --api-key arg, then BSP_FOREX_API_KEY env var.
    api_key: str | None = args.api_key or os.environ.get("BSP_FOREX_API_KEY") or None

    try:
        get_exchange_rates(
            project_path=project_path,
            extra_currencies=extra,
            api_key=api_key,
        )
    except Exception as exc:
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    return 0


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


def _cmd_process(args: argparse.Namespace) -> int:
    """Handler for the ``process`` subcommand.

    Discovers PDF files, creates a :class:`StatementBatch`, persists the
    results, optionally copies source PDFs into the project tree, and
    exports reports.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    from bank_statement_parser.modules.paths import ProjectPaths
    from bank_statement_parser.modules.statements import StatementBatch

    # -- resolve PDF directory and discover files ----------------------------
    pdfs_dir = Path(args.pdfs).resolve() if args.pdfs else Path.cwd()
    if not pdfs_dir.is_dir():
        print(f"Error: '{pdfs_dir}' is not a directory.", file=sys.stderr)
        return 1

    pdfs = sorted(pdfs_dir.glob(args.pattern))
    if not pdfs:
        print(f"Error: no PDFs found in '{pdfs_dir}' matching '{args.pattern}'.", file=sys.stderr)
        return 1

    # -- resolve project path ------------------------------------------------
    project_path = Path(args.project).resolve() if args.project else Path.cwd() / "bsp_project"
    project_path.mkdir(parents=True, exist_ok=True)

    print(f"Project:  {project_path}")
    print(f"PDFs:     {len(pdfs)} file(s) in {pdfs_dir}")

    # -- create and run the batch (processing starts in __init__) ------------
    batch = StatementBatch(
        pdfs=pdfs,
        company_key=args.company,
        account_key=args.account,
        turbo=not args.no_turbo,
        project_path=project_path,
    )

    # -- persist data --------------------------------------------------------
    batch.update_data(datadestination=args.data)

    # -- copy source PDFs into project statements/ tree ----------------------
    if not args.no_copy:
        copied = batch.copy_statements_to_project()
        print(f"Copied:   {len(copied)} PDF(s) into project statements/ tree.")

    # -- export reports ------------------------------------------------------
    if not args.no_export:
        batch.export(
            filetype=args.export_format,
            type=args.export_type,
            batch_id=args.batch_id if args.batch_id else None,
            filename_timestamp=args.filename_timestamp,
        )

    # -- cleanup temp files --------------------------------------------------
    batch.delete_temp_files()

    # -- summary -------------------------------------------------------------
    paths = ProjectPaths.resolve(project_path)
    print("")
    print(f"Done — processed {batch.pdf_count} PDF(s) ({batch.errors} error(s)) in {batch.duration_secs:.1f}s.")
    print("")
    print(f"Database: {paths.project_db}")
    print(f"Exports:  {paths.exports}")

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
    # forex subcommand
    # ------------------------------------------------------------------
    forex_parser = subparsers.add_parser(
        "forex",
        help="Fetch daily USD-based exchange rates and persist to the project database.",
        description=(
            "Fetches daily USD-based exchange rates for all currencies found in "
            "DimAccount plus any extras specified, forward-fills weekend and "
            "holiday gaps, and writes the results to the exchange_rates table. "
            "Provider and API key can be configured via forex_api_config.toml in "
            "the project config directory."
        ),
    )
    forex_parser.add_argument(
        "--project",
        metavar="PATH",
        default=None,
        help="Project folder path (default: ./bsp_project/ in CWD).",
    )
    forex_parser.add_argument(
        "--currencies",
        metavar="CODE",
        nargs="+",
        default=None,
        help="Additional ISO 4217 currency codes to fetch (e.g. --currencies AED SAR).",
    )
    forex_parser.add_argument(
        "--api-key",
        metavar="KEY",
        dest="api_key",
        default=None,
        help="Override the API key from forex_api_config.toml. Prefer setting the BSP_FOREX_API_KEY environment variable instead to avoid key exposure in shell history.",
    )

    # ------------------------------------------------------------------
    # anonymise subcommand
    # ------------------------------------------------------------------
    anon = subparsers.add_parser(
        "anonymise",
        help="Anonymise one PDF or all PDFs in a folder.",
        description=(
            "Start from a completely scrambled PDF (every letter replaced) and "
            "use anonymise.toml to specify exclusions — text that should remain "
            "readable (transaction type codes, account descriptions, etc.) and "
            "numbers that should be scrambled. Driven by anonymise.toml in the "
            "project config directory."
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
        help="Output path for single-file mode (default: anonymised_<stem>.pdf alongside input).",
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

    # ------------------------------------------------------------------
    # process subcommand
    # ------------------------------------------------------------------
    proc = subparsers.add_parser(
        "process",
        help="Parse bank statement PDFs, persist data, and export reports.",
        description=(
            "Discover PDF bank statements, extract transaction data, persist "
            "results to Parquet and/or SQLite, copy source PDFs into the "
            "project tree, and export reports as Excel, CSV, JSON, and/or "
            "CSV reporting feeds. "
            "A project folder is created automatically if it does not exist."
        ),
    )
    proc.add_argument(
        "--project",
        metavar="PATH",
        default=None,
        help="Project folder path. Created if absent (default: ./bsp_project/ in CWD).",
    )
    proc.add_argument(
        "--pdfs",
        metavar="PATH",
        default=None,
        help="Folder to scan for PDF files (default: current working directory).",
    )
    proc.add_argument(
        "--pattern",
        metavar="GLOB",
        default="**/*.pdf",
        help="Glob pattern for PDF discovery (default: '**/*.pdf').",
    )
    proc.add_argument(
        "--no-turbo",
        action="store_true",
        default=False,
        dest="no_turbo",
        help="Disable parallel processing (turbo is enabled by default).",
    )
    proc.add_argument(
        "--company",
        metavar="KEY",
        default=None,
        help="Company key for config lookup (default: auto-detect from PDF).",
    )
    proc.add_argument(
        "--account",
        metavar="KEY",
        default=None,
        help="Account key for config lookup (default: auto-detect from PDF).",
    )
    proc.add_argument(
        "--data",
        choices=["parquet", "database", "both"],
        default="both",
        help="Persistence target for update_data() (default: 'both').",
    )
    proc.add_argument(
        "--export-format",
        choices=["excel", "csv", "json", "all", "reporting"],
        default="all",
        dest="export_format",
        help="Export file format (default: 'all').",
    )
    proc.add_argument(
        "--export-type",
        choices=["single", "multi"],
        default="single",
        dest="export_type",
        help=(
            "Export preset. 'single' (default) exports a single flat "
            "transactions table. 'multi' exports separate star-schema "
            "tables (accounts, calendar, statements, transactions, "
            "balances, gaps) intended for loading into an external database."
        ),
    )
    proc.add_argument(
        "--no-export",
        action="store_true",
        default=False,
        dest="no_export",
        help="Skip the export step entirely.",
    )
    proc.add_argument(
        "--no-copy",
        action="store_true",
        default=False,
        dest="no_copy",
        help="Skip copying source PDFs into the project statements/ directory.",
    )
    proc.add_argument(
        "--batch-id",
        metavar="ID",
        dest="batch_id",
        default=None,
        help="Filter exports to a single batch identifier (default: export all batches).",
    )
    proc.add_argument(
        "--filename-timestamp",
        action="store_true",
        default=False,
        dest="filename_timestamp",
        help=(
            "Append a human-readable timestamp (yyyymmddHHMMSS) to exported filenames. "
            "For multi exports (CSV/JSON) a timestamped sub-folder is created instead."
        ),
    )

    args = parser.parse_args()

    if args.command == "forex":
        sys.exit(_cmd_forex(args))
    elif args.command == "anonymise":
        sys.exit(_cmd_anonymise(args))
    elif args.command == "process":
        sys.exit(_cmd_process(args))
