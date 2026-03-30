"""Tests for application configuration loading."""

import os

import pytest

from datapipeline.config import Settings


def test_settings_defaults():
    """Default DATABASE_URL contains expected host and database name."""
    s = Settings()
    assert "dataplatform" in s.database_url


def test_settings_override_via_env(monkeypatch):
    """Environment variables override default values."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pw@host/db")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("CHUNK_SIZE", "500")
    s = Settings()
    assert s.database_url == "postgresql+asyncpg://user:pw@host/db"
    assert s.log_level == "DEBUG"
    assert s.chunk_size == 500


def test_configure_logging_does_not_raise():
    """configure_logging runs without errors for all standard levels."""
    from datapipeline.logging_config import configure_logging

    for level in ("DEBUG", "INFO", "WARNING", "ERROR"):
        configure_logging(level)  # must not raise
