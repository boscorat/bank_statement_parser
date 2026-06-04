"""
generate_test_metadata.py — Auto-generate JSON metadata sidecars for test PDFs.

Processes all PDFs in test_data directories and writes expected outcomes
(result status, balances, transaction counts) to .json sidecar files.

Usage:
    python scripts/generate_test_metadata.py              # Generate for both good and bad
    python scripts/generate_test_metadata.py --good       # Good PDFs only
    python scripts/generate_test_metadata.py --bad        # Bad PDFs only

Output:
    One .json file per PDF, same directory, with expected result/outcome/figures.
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

import polars as pl

from bank_statement_parser.modules.paths import validate_or_initialise_project
from bank_statement_parser.modules.statements import StatementBatch
from bank_statement_parser.testing import _pdf_dir

_REPO_ROOT = Path(__file__).parent.parent
_TESTS_DIR = _REPO_ROOT / "tests"
_TEMP_PROJECT_DIR_GOOD = _TESTS_DIR / "_temp_gen_project_good"
_TEMP_PROJECT_DIR_BAD = _TESTS_DIR / "_temp_gen_project_bad"


def _prepare_temp_project(project_dir: Path) -> None:
    """Create and initialize a temporary project directory."""
    if project_dir.exists():
        shutil.rmtree(project_dir)
    project_dir.mkdir(parents=True, exist_ok=True)
    validate_or_initialise_project(project_dir)


def _generate_metadata_for_good_pdfs() -> int:
    """Generate metadata for good PDFs. Returns count of successful generations."""
    pdf_dir = _pdf_dir("good")
    if pdf_dir is None or not pdf_dir.exists():
        print(f"⚠️  Good PDF directory not found (requires access to boscorat/bank-statement-data)")
        return 0

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        print(f"⚠️  No PDFs found in {pdf_dir}")
        return 0

    _prepare_temp_project(_TEMP_PROJECT_DIR_GOOD)
    successful = 0

    for pdf_path in pdfs:
        try:
            # Process just this PDF
            batch = StatementBatch(
                pdfs=[pdf_path],
                turbo=False,  # Single PDF, no need for turbo
                project_path=_TEMP_PROJECT_DIR_GOOD,
            )
            batch.update_data()

            if not batch.processed_pdfs:
                print(f"✗ {pdf_path.name}: no result generated")
                continue

            pdf_result = batch.processed_pdfs[0]

            # Build metadata dict
            metadata = {
                "expected_result": pdf_result.result,
                "expected_outcome": pdf_result.outcome,
            }

            # For SUCCESS or REVIEW, extract financial data
            if hasattr(pdf_result.payload, "statement_info"):
                stmt_info = pdf_result.payload.statement_info
                metadata.update({
                    "expected_filename": stmt_info.filename_new,
                    "expected_statement_date": stmt_info.statement_date.isoformat(),
                    "expected_account": stmt_info.account,
                    "expected_id_account": stmt_info.id_account,
                    "expected_opening_balance": str(stmt_info.opening_balance),
                    "expected_closing_balance": str(stmt_info.closing_balance),
                    "expected_payments_in": str(stmt_info.payments_in),
                    "expected_payments_out": str(stmt_info.payments_out),
                })

                # Extract transaction count from parquet
                try:
                    parquet_df = pl.read_parquet(pdf_result.payload.parquet_files.statement_lines)
                    metadata["expected_transaction_count"] = str(parquet_df.height)
                except Exception as e:
                    print(f"⚠️  {pdf_path.name}: could not read transaction count: {e}")
                    metadata["expected_transaction_count"] = "0"

            metadata["description"] = "Auto-generated from successful processing"

            # Write metadata sidecar
            metadata_path = pdf_path.with_suffix(".json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            print(f"✓ {pdf_path.name} → {metadata_path.name}")
            successful += 1

        except Exception as e:
            print(f"✗ {pdf_path.name}: {type(e).__name__}: {e}")

    # Cleanup
    if _TEMP_PROJECT_DIR_GOOD.exists():
        shutil.rmtree(_TEMP_PROJECT_DIR_GOOD)

    return successful


def _generate_metadata_for_bad_pdfs() -> int:
    """Generate metadata for bad PDFs. Returns count of successful generations."""
    pdf_dir = _pdf_dir("bad")
    if pdf_dir is None or not pdf_dir.exists():
        print(f"⚠️  Bad PDF directory not found (requires access to boscorat/bank-statement-data)")
        return 0

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        print(f"⚠️  No PDFs found in {pdf_dir}")
        return 0

    _prepare_temp_project(_TEMP_PROJECT_DIR_BAD)
    successful = 0

    for pdf_path in pdfs:
        try:
            # Process just this PDF
            batch = StatementBatch(
                pdfs=[pdf_path],
                turbo=False,
                project_path=_TEMP_PROJECT_DIR_BAD,
            )
            batch.update_data()

            if not batch.processed_pdfs:
                print(f"✗ {pdf_path.name}: no result generated")
                continue

            pdf_result = batch.processed_pdfs[0]

            # For bad PDFs, just capture result and outcome
            metadata = {
                "expected_result": pdf_result.result,
                "expected_outcome": pdf_result.outcome,
                "description": "Auto-generated from test run - expected to fail gracefully",
            }

            # Write metadata sidecar
            metadata_path = pdf_path.with_suffix(".json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            print(f"✓ {pdf_path.name} → {metadata_path.name}")
            successful += 1

        except Exception as e:
            print(f"✗ {pdf_path.name}: {type(e).__name__}: {e}")

    # Cleanup
    if _TEMP_PROJECT_DIR_BAD.exists():
        shutil.rmtree(_TEMP_PROJECT_DIR_BAD)

    return successful


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate JSON metadata sidecars for test PDFs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Output: One .json file per PDF, same directory, with expected result/outcome/figures.",
    )
    parser.add_argument(
        "--good",
        action="store_true",
        help="Generate metadata for good PDFs only",
    )
    parser.add_argument(
        "--bad",
        action="store_true",
        help="Generate metadata for bad PDFs only",
    )

    args = parser.parse_args()

    # Default: generate both
    do_good = args.good or (not args.good and not args.bad)
    do_bad = args.bad or (not args.good and not args.bad)

    total_good = 0
    total_bad = 0

    print("=" * 70)
    print("Generating test metadata sidecars...")
    print("=" * 70)

    if do_good:
        print("\n📄 Processing good PDFs...")
        total_good = _generate_metadata_for_good_pdfs()
        print(f"Generated {total_good} metadata files for good PDFs\n")

    if do_bad:
        print("📄 Processing bad PDFs...")
        total_bad = _generate_metadata_for_bad_pdfs()
        print(f"Generated {total_bad} metadata files for bad PDFs\n")

    total = total_good + total_bad
    print("=" * 70)
    if total > 0:
        print(f"✓ Successfully generated {total} metadata files")
        return 0
    else:
        print("✗ No metadata files generated")
        return 1


if __name__ == "__main__":
    sys.exit(main())
