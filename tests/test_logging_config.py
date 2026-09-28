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

"""
test_logging_config — unit tests for the logging factory and verbosity switching.

Covers:
- Logger caching (same name returns same instance)
- Default verbosity is "normal" (INFO level)
- set_verbosity("verbose") switches to DEBUG
- set_verbosity("normal") switches back to INFO
- Invalid values are ignored
- New loggers created after set_verbosity get the correct level
"""

import logging
from collections.abc import Iterator

import pytest

from bank_statement_parser.modules import logging_config
from bank_statement_parser.modules.logging_config import get_logger, get_verbosity, set_verbosity


@pytest.fixture(autouse=True)
def reset_verbosity() -> Iterator[None]:
    """Reset verbosity to normal before and after each test."""
    set_verbosity("normal")
    yield
    set_verbosity("normal")


class TestLoggerCaching:
    """Verify that get_logger returns cached instances."""

    def test_same_name_returns_same_instance(self) -> None:
        a = get_logger("test.cache")
        b = get_logger("test.cache")
        assert a is b

    def test_different_names_return_different_instances(self) -> None:
        a = get_logger("test.cache.a")
        b = get_logger("test.cache.b")
        assert a is not b

    def test_cached_logger_is_in_internal_cache(self) -> None:
        logger = get_logger("test.cache.internal")
        assert logging_config._LOGGERS["test.cache.internal"] is logger


class TestDefaultVerbosity:
    """Verify default verbosity is normal (INFO)."""

    def test_default_verbosity_is_normal(self) -> None:
        assert get_verbosity() == "normal"

    def test_new_logger_defaults_to_info(self) -> None:
        logger = get_logger("test.default.level")
        assert logger.level == logging.INFO


class TestSetVerbosity:
    """Verify verbosity switching behavior."""

    def test_set_verbose_changes_level(self) -> None:
        set_verbosity("verbose")
        assert get_verbosity() == "verbose"

    def test_set_normal_changes_level(self) -> None:
        set_verbosity("verbose")
        set_verbosity("normal")
        assert get_verbosity() == "normal"

    def test_verbose_sets_debug_on_existing_loggers(self) -> None:
        logger = get_logger("test.verbose.existing")
        set_verbosity("verbose")
        assert logger.level == logging.DEBUG

    def test_normal_sets_info_on_existing_loggers(self) -> None:
        logger = get_logger("test.normal.existing")
        set_verbosity("verbose")
        set_verbosity("normal")
        assert logger.level == logging.INFO

    def test_new_logger_gets_verbose_level(self) -> None:
        set_verbosity("verbose")
        logger = get_logger("test.verbose.new")
        assert logger.level == logging.DEBUG

    def test_new_logger_gets_normal_level(self) -> None:
        set_verbosity("verbose")
        set_verbosity("normal")
        logger = get_logger("test.normal.new")
        assert logger.level == logging.INFO

    def test_invalid_verbosity_is_ignored(self) -> None:
        set_verbosity("verbose")
        set_verbosity("bogus")  # type: ignore[arg-type]
        assert get_verbosity() == "verbose"

    def test_invalid_verbosity_does_not_change_levels(self) -> None:
        logger = get_logger("test.invalid.level")
        set_verbosity("bogus")  # type: ignore[arg-type]
        assert logger.level == logging.INFO


class TestLoggerPropagation:
    """Verify loggers propagate to root."""

    def test_loggers_propagate(self) -> None:
        logger = get_logger("test.propagate")
        assert logger.propagate is True


class TestConsumerConfiguration:
    """Verify get_logger does not override consumer-configured loggers."""

    def test_get_logger_preserves_consumer_level(self) -> None:
        """If consumer sets a level before get_logger, it is preserved."""
        name = "test.consumer.configured"
        raw_logger = logging.getLogger(name)
        raw_logger.setLevel(logging.WARNING)
        try:
            logger = get_logger(name)
            assert logger.level == logging.WARNING
        finally:
            raw_logger.setLevel(logging.NOTSET)

    def test_get_logger_sets_level_when_notset(self) -> None:
        """If consumer has not set a level, get_logger applies verbosity."""
        name = "test.consumer.unconfigured"
        raw_logger = logging.getLogger(name)
        raw_logger.setLevel(logging.NOTSET)
        logger = get_logger(name)
        assert logger.level == logging.INFO
