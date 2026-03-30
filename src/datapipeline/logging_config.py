"""Logging configuration for the data platform."""

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with timestamp, name, level, and message format.

    Args:
        level: Logging level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
