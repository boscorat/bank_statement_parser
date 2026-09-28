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

"""Logging configuration for bank_statement_parser.

This module provides a logger factory for use by consuming applications
(e.g., openstan). The library itself does not configure any handlers —
callers are responsible for setting up handlers (file, console, etc.)
via Python's logging module.
"""

import logging
from typing import Literal

__all__: list[str] = ["get_logger", "get_verbosity", "set_verbosity"]

Verbosity = Literal["normal", "verbose"]

# Module-level cache of loggers to avoid duplicate configuration
_LOGGERS: dict[str, logging.Logger] = {}
_VERBOSITY: Verbosity = "normal"


def get_logger(name: str) -> logging.Logger:
    """Get or create a logger for the given module name.

    Args:
        name: Module name (typically ``__name__``). Logger will be cached
            and reused for subsequent calls with the same name.

    Returns:
        Configured logger instance. The logger's level is determined by the
        current verbosity setting (see :func:`set_verbosity`).

    Example:
        .. code-block:: python

            from bank_statement_parser import get_logger

            logger = get_logger(__name__)
            logger.info("Processing statement: %s", pdf_path)
            try:
                result = statement.get_results()
            except ValueError as e:
                logger.error("Extraction failed", exc_info=True)
    """
    if name not in _LOGGERS:
        logger = logging.getLogger(name)
        # Propagate to root logger; handlers are added by consuming application
        logger.propagate = True
        logger.setLevel(logging.DEBUG if _VERBOSITY == "verbose" else logging.INFO)
        _LOGGERS[name] = logger

    return _LOGGERS[name]


def set_verbosity(verbosity: Verbosity) -> None:
    """Set the verbosity level for all bank_statement_parser loggers.

    Args:
        verbosity: Either "normal" (INFO level) or "verbose" (DEBUG level).
            Invalid values are silently ignored.

    Note:
        This affects loggers created via :func:`get_logger`. Existing
        logger references will be updated immediately.
    """
    global _VERBOSITY

    if verbosity not in ("normal", "verbose"):
        return

    _VERBOSITY = verbosity

    # Update level for all existing loggers
    for logger in _LOGGERS.values():
        level = logging.DEBUG if verbosity == "verbose" else logging.INFO
        logger.setLevel(level)


def get_verbosity() -> str:
    """Return the current verbosity setting.

    Returns:
        Either "normal" or "verbose".
    """
    return _VERBOSITY
