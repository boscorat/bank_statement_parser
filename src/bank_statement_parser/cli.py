"""
cli — command-line interface for bank_statement_parser.

Entry points are registered in pyproject.toml under [project.scripts].
Add new subcommands here.

Current subcommands
-------------------
anonymise   Anonymise one PDF or all PDFs in a folder.
process     Parse bank statement PDFs, persist data, and export reports.
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
    from bank_statement_parser.modules.paths import get_paths
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
            datasource=args.export_data,
            filetype=args.export_format,
            type=args.export_type,
        )

    # -- cleanup temp files --------------------------------------------------
    batch.delete_temp_files()

    # -- summary -------------------------------------------------------------
    paths = get_paths(project_path)
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

    # ------------------------------------------------------------------
    # process subcommand
    # ------------------------------------------------------------------
    proc = subparsers.add_parser(
        "process",
        help="Parse bank statement PDFs, persist data, and export reports.",
        description=(
            "Discover PDF bank statements, extract transaction data, persist "
            "results to Parquet and/or SQLite, copy source PDFs into the "
            "project tree, and export reports as Excel and/or CSV. "
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
        "--export-data",
        choices=["parquet", "database"],
        default="database",
        dest="export_data",
        help="Data source for export (default: 'database').",
    )
    proc.add_argument(
        "--export-format",
        choices=["excel", "csv", "both"],
        default="both",
        dest="export_format",
        help="Export file format (default: 'both').",
    )
    proc.add_argument(
        "--export-type",
        choices=["full", "simple"],
        default="simple",
        dest="export_type",
        help=(
            "Export preset. 'simple' (default) exports a single flat "
            "transactions table. 'full' exports separate star-schema "
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

    args = parser.parse_args()

    if args.command == "anonymise":
        sys.exit(_cmd_anonymise(args))
    elif args.command == "process":
        sys.exit(_cmd_process(args))
