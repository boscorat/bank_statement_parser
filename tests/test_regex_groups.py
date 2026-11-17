# This file is part of bank_statement_parser.
#
# Copyright (c) 2026 Jason Farrar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Unit tests for regex_groups field capture group extraction feature.

Tests the ability to extract specific regex capture groups instead of entire matches,
enabling splitting of single PDF columns into multiple extracted fields.
"""

import polars as pl

from bank_statement_parser.modules.data import Field
from bank_statement_parser.modules.statement_functions import patmatch


class TestRegexGroups:
    """Test regex_groups field extraction."""

    def test_patmatch_extract_group_1_natwest_payment_type(self) -> None:
        """Test extraction of group 1 (payment_type) from NatWest combined field."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB",
                    "Direct Debit CHRISTIAN AG POV",
                    "Automated Credit LIEVES/FARR SNOOKER DRINKS FP 28/12/25 1937",
                ]
            }
        ).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        field = Field(
            field="payment_type",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=1,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=20,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Group 1 should extract only the payment type (title case portion)
        assert result["value_pattern"][0] == "Card Transaction"
        assert result["value_pattern"][1] == "Direct Debit"
        assert result["value_pattern"][2] == "Automated Credit"

    def test_patmatch_extract_group_2_natwest_description(self) -> None:
        """Test extraction of group 2 (description) from NatWest combined field."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB",
                    "Direct Debit CHRISTIAN AG POV",
                    "Automated Credit LIEVES/FARR SNOOKER DRINKS FP 28/12/25 1937",
                ]
            }
        ).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        field = Field(
            field="description",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=2,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=100,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Group 2 should extract only the description (uppercase/numbers portion)
        assert "0940 15JAN26 C WOODIES LEEDS GB" in result["value_pattern"][0]
        assert result["value_pattern"][1] == "CHRISTIAN AG POV"
        assert "LIEVES/FARR" in result["value_pattern"][2]

    def test_patmatch_regex_groups_backward_compatibility(self) -> None:
        """Test that fields without regex_groups default to group 0 (entire match)."""
        data = pl.DataFrame({"value_strip": ["Card Transaction 0940 15JAN26 C WOODIES LEEDS GB"]}).lazy()

        pattern = r".+"

        field = Field(
            field="combined",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=None,  # Defaults to group 0
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=100,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # When regex_groups is None, should extract entire match (group 0)
        assert result["value_pattern"][0] == "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB"

    def test_patmatch_regex_groups_excludes_footer_contamination(self) -> None:
        """Test that regex_groups pattern stops before title-case footer text."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    # Valid case
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB",
                    # Contamination: title-case footer bleed
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB National Westminster Bank Plc",
                ]
            }
        ).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        field = Field(
            field="description",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=2,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=100,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Valid row should extract normally
        assert "WOODIES LEEDS GB" in result["value_pattern"][0]
        assert result["success_pattern"][0] is True

        # Contaminated row fails extraction because pattern ends with $
        # and "National Westminster Bank Plc" contains lowercase letters (violates [A-Z0-9_...]+ pattern)
        # So extraction should fail, success=False, value should be empty
        assert result["value_pattern"][1] == ""
        assert result["success_pattern"][1] is False

    def test_patmatch_regex_groups_rejects_brought_forward(self) -> None:
        """Test that row starting with 'BROUGHT FORWARD' doesn't match pattern."""
        data = pl.DataFrame({"value_strip": ["BROUGHT FORWARD Card Transaction 0940 24TDS25C HJSVQX"]}).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        field = Field(
            field="payment_type",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=1,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=20,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Pattern should not match (BROUGHT FORWARD is all uppercase, not title case)
        # Extraction should fail, success=False, value should be empty
        assert result["value_pattern"][0] == ""
        assert result["success_pattern"][0] is False


class TestRegexGroupsIntegration:
    """Integration tests for regex_groups with realistic NatWest data."""

    def test_both_groups_same_pattern_natwest(self) -> None:
        """Test both payment_type and description use same pattern with different regex_groups."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB",
                    "Direct Debit CHRISTIAN AG POV",
                ]
            }
        ).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        # Extract payment_type (group 1)
        field_pt = Field(
            field="payment_type",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=1,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=20,
            date_format=None,
            value_offset=None,
        )

        # Extract description (group 2)
        field_desc = Field(
            field="description",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=2,
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=100,
            date_format=None,
            value_offset=None,
        )

        result_pt = patmatch(data, field_pt, pl.DataFrame(), "test.pdf", spec=None).collect()
        result_desc = patmatch(data, field_desc, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Both should extract successfully
        assert result_pt["success_pattern"][0] is True
        assert result_desc["success_pattern"][0] is True

        # Values should be properly split
        assert result_pt["value_pattern"][0] == "Card Transaction"
        assert "0940 15JAN26" in result_desc["value_pattern"][0]

        assert result_pt["value_pattern"][1] == "Direct Debit"
        assert result_desc["value_pattern"][1] == "CHRISTIAN AG POV"

    def test_patmatch_extract_multiple_groups_with_separator(self) -> None:
        """Test extraction of multiple groups concatenated with space separator."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "21st Sep 2026 to 19th Oct 2026",
                    "1st Jan 2026 to 31st Dec 2026",
                ]
            }
        ).lazy()

        # Pattern extracts end date components: day, month, year
        # Month pattern matches 3+ letter names (Sep, Oct, Jan, Dec, etc.)
        pattern = r"^\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]{2,}\s+\d{4}\s+to\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Z][a-z]{2,})\s+(\d{4})$"

        field = Field(
            field="statement_date",
            column=0,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=[1, 2, 3],  # Extract groups 1, 2, 3
            regex_groups_separator=" ",  # Join with spaces
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=None,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Should concatenate groups with spaces, skipping ordinals
        assert result["value_pattern"][0] == "19 Oct 2026"
        assert result["value_pattern"][1] == "31 Dec 2026"
        assert result["success_pattern"][0] is True
        assert result["success_pattern"][1] is True

    def test_patmatch_extract_multiple_groups_no_separator(self) -> None:
        """Test extraction of multiple groups concatenated without separator."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "UK12345678ABC",
                ]
            }
        ).lazy()

        # Pattern extracts account components: country, account, type
        pattern = r"^([A-Z]{2})(\d{8})([A-Z]{3})$"

        field = Field(
            field="account_info",
            column=0,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=[1, 2, 3],
            regex_groups_separator="",  # Explicit empty separator
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=None,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Should concatenate groups without separator
        assert result["value_pattern"][0] == "UK12345678ABC"
        assert result["success_pattern"][0] is True

    def test_patmatch_backward_compatible_int_regex_groups(self) -> None:
        """Ensure existing int regex_groups behavior is unchanged."""
        data = pl.DataFrame(
            {
                "value_strip": [
                    "Card Transaction 0940 15JAN26 C WOODIES LEEDS GB",
                    "Direct Debit CHRISTIAN AG POV",
                ]
            }
        ).lazy()

        pattern = r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([A-Z0-9_\-/*.,&'\s]+)$"

        field = Field(
            field="payment_type",
            column=3,
            vital=True,
            type="string",
            string_pattern=pattern,
            regex_groups=1,  # Single int (existing behavior)
            regex_groups_separator=None,  # Ignored for int
            cell=None,
            strip_characters_start=None,
            strip_characters_end=None,
            currency_override=None,
            numeric_modifier=None,
            string_max_length=20,
            date_format=None,
            value_offset=None,
        )

        result = patmatch(data, field, pl.DataFrame(), "test.pdf", spec=None).collect()

        # Should extract group 1 only (unchanged behavior)
        assert result["value_pattern"][0] == "Card Transaction"
        assert result["value_pattern"][1] == "Direct Debit"
