"""Tests for the plain-text loader."""

from pathlib import Path

import pytest

from datapipeline.ingestion.text_loader import TextLoader
from datapipeline.ingestion.detector import detect_format


def test_text_loader_yields_one_record(tmp_path):
    """A .txt file yields exactly one record containing the full text."""
    f = tmp_path / "orders.txt"
    f.write_text("Alice ordered 2 Widgets on 2024-01-01 for $9.99.")
    loader = TextLoader()
    records = list(loader.load(f))
    assert len(records) == 1


def test_text_loader_raw_text_key(tmp_path):
    """The record contains _raw_text with the full file content."""
    content = "Order ORD-001: customer Bob from DE, 3 Gadgets at $24.50 on 2024-02-20."
    f = tmp_path / "notes.txt"
    f.write_text(content)
    loader = TextLoader()
    record = list(loader.load(f))[0]
    assert record["_raw_text"] == content


def test_text_loader_source_file_key(tmp_path):
    """The record contains _source_file with the path as a string."""
    f = tmp_path / "data.txt"
    f.write_text("some text")
    loader = TextLoader()
    record = list(loader.load(f))[0]
    assert record["_source_file"] == str(f)


def test_text_loader_empty_file(tmp_path):
    """An empty .txt file still yields one record (with empty _raw_text)."""
    f = tmp_path / "empty.txt"
    f.write_text("")
    loader = TextLoader()
    records = list(loader.load(f))
    assert len(records) == 1
    assert records[0]["_raw_text"] == ""


def test_detector_recognises_txt():
    """The format detector maps .txt to the 'text' format key."""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".txt") as tmp:
        assert detect_format(Path(tmp.name)) == "text"
