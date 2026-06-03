#!/usr/bin/env python3
"""
test_runner.py — Convenient pytest helper for common test scenarios.

This script provides simple commands for running tests with different markers,
making it easier for developers to run the right tests for their workflow.

Usage:
    # Run all tests (default)
    python test_runner.py

    # Run only synthetic tests (fastest, local development)
    python test_runner.py synthetic

    # Run integration tests with BSP comparison
    python test_runner.py integration

    # Run with verbose output
    python test_runner.py synthetic -v

    # Run with coverage report
    python test_runner.py synthetic --cov

Examples:
    # Development workflow (fast synthetic tests)
    python test_runner.py synthetic -v

    # CI workflow (all tests)
    python test_runner.py all -v

    # Release workflow (anonymised + integration)
    python test_runner.py anonymised -v

For openstan-specific tests:
    # Contract validation
    python test_runner.py contract -v

    # Integration with BSP
    python test_runner.py integration -v
"""

import argparse
import subprocess
import sys
from pathlib import Path


class TestRunner:
    """Helper for running pytest with marker-based filtering."""

    PROFILES = {
        "all": {
            "description": "Run all tests (full suite)",
            "marker": None,
            "args": [],
        },
        "synthetic": {
            "description": "Run only synthetic PDF tests (fastest)",
            "marker": "synthetic",
            "args": [],
        },
        "anonymised": {
            "description": "Run only anonymised PDF tests (requires SSH)",
            "marker": "anonymised",
            "args": [],
        },
        "integration": {
            "description": "Run integration tests (BSP comparison)",
            "marker": "integration",
            "args": [],
        },
        "unit": {
            "description": "Run unit tests only (isolated components)",
            "marker": "unit",
            "args": [],
        },
        "contract": {
            "description": "Run BSP contract validation tests (openstan only)",
            "marker": "integration and test_bsp_contract",
            "args": [],
        },
        "quick": {
            "description": "Run fast tests (synthetic + unit)",
            "marker": "synthetic or unit",
            "args": [],
        },
        "slow": {
            "description": "Run slow tests only",
            "marker": "slow",
            "args": [],
        },
        "not-slow": {
            "description": "Run all tests except slow ones",
            "marker": "not slow",
            "args": [],
        },
    }

    def __init__(self):
        self.repo_root = Path(__file__).parent
        self.tests_dir = self.repo_root / "tests"

    def run(self, profile: str, extra_args: list[str]) -> int:
        """Run pytest with the specified profile and extra arguments.

        Args:
            profile: Profile name (synthetic, integration, etc.)
            extra_args: Additional pytest arguments

        Returns:
            Exit code from pytest
        """
        if profile not in self.PROFILES:
            self._print_error(f"Unknown profile: {profile}")
            self._print_profiles()
            return 1

        config = self.PROFILES[profile]
        print(f"Profile: {config['description']}")
        print(f"Tests directory: {self.tests_dir}")
        print()

        cmd = ["pytest", str(self.tests_dir)]

        if config["marker"]:
            cmd.extend(["-m", config["marker"]])

        cmd.extend(extra_args)

        print(f"Running: {' '.join(cmd)}")
        print("-" * 80)

        return subprocess.call(cmd)

    def _print_profiles(self):
        """Print available test profiles."""
        print("\nAvailable profiles:")
        print()
        for name, config in self.PROFILES.items():
            marker_info = f" (marker: {config['marker']})" if config["marker"] else ""
            print(f"  {name:<20} {config['description']}{marker_info}")
        print()

    @staticmethod
    def _print_error(msg: str):
        """Print error message to stderr."""
        print(f"ERROR: {msg}", file=sys.stderr)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run tests with pytest marker-based filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_runner.py                          # Run all tests
  python test_runner.py synthetic -v             # Verbose synthetic tests
  python test_runner.py integration --cov        # Integration tests with coverage
  python test_runner.py quick --tb=short         # Fast tests with short tracebacks
  python test_runner.py all -k "test_parse"      # All tests matching pattern
        """,
    )

    runner = TestRunner()

    parser.add_argument(
        "profile",
        nargs="?",
        default="all",
        help=f"Test profile to run. Options: {', '.join(runner.PROFILES.keys())}",
    )

    parser.add_argument(
        "--profiles",
        action="store_true",
        help="Show available profiles and exit",
    )

    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional arguments to pass to pytest (e.g., -v, --cov, -k pattern)",
    )

    args = parser.parse_args()

    if args.profiles:
        runner._print_profiles()
        return 0

    # Run tests with the selected profile
    return runner.run(args.profile, args.pytest_args)


if __name__ == "__main__":
    sys.exit(main())
