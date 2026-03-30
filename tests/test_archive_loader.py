"""Tests for the ZIP archive loader."""

import zipfile
from pathlib import Path

import pytest

from datapipeline.ingestion.archive_loader import ArchiveLoader

FIXTURES = Path(__file__).parent / "fixtures"


def test_archive_yields_csv_records(tmp_path):
    """ZIP containing one CSV yields all rows from that CSV."""
    zip_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(FIXTURES / "sample.csv", arcname="orders.csv")
    records = list(ArchiveLoader().load(zip_path))
    assert len(records) == 3
    assert records[0]["order_id"] == "ORD-001"


def test_archive_yields_from_multiple_files(tmp_path):
    """ZIP containing CSV and JSON files yields records from both."""
    zip_path = tmp_path / "multi.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(FIXTURES / "sample.csv",  arcname="orders.csv")
        zf.write(FIXTURES / "sample.json", arcname="events.json")
    records = list(ArchiveLoader().load(zip_path))
    # 3 from CSV + 3 from JSON
    assert len(records) == 6


def test_archive_skips_unsupported_files(tmp_path):
    """Unsupported files inside the ZIP (e.g. .md, .pdf) are silently skipped."""
    zip_path = tmp_path / "mixed.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(FIXTURES / "sample.csv", arcname="orders.csv")
        zf.writestr("readme.md",  "This file should be ignored")
        zf.writestr("schema.pdf", "This file should also be ignored")
    records = list(ArchiveLoader().load(zip_path))
    assert len(records) == 3
