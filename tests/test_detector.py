"""Tests for file format detection and discovery."""

import pytest

from datapipeline.exceptions import IngestionError
from datapipeline.ingestion.detector import detect_format, discover_files


@pytest.mark.parametrize("filename,expected", [
    ("orders.csv",   "csv"),
    ("sales.xlsx",   "excel"),
    ("sales.xls",    "excel"),
    ("events.json",  "json"),
    ("stream.ndjson","ndjson"),
    ("stream.jsonl", "ndjson"),
    ("legacy.xml",   "xml"),
    ("archive.zip",  "zip"),
])
def test_detect_known_formats(tmp_path, filename, expected):
    """Known extensions map to the correct format key."""
    f = tmp_path / filename
    f.touch()
    assert detect_format(f) == expected


def test_detect_unknown_raises(tmp_path):
    """Unknown extension raises IngestionError."""
    f = tmp_path / "data.parquet"
    f.touch()
    with pytest.raises(IngestionError, match="Unsupported"):
        detect_format(f)


def test_discover_files_returns_supported_only(tmp_path):
    """discover_files returns only files with supported extensions."""
    (tmp_path / "orders.csv").touch()
    (tmp_path / "events.json").touch()
    (tmp_path / "notes.txt").touch()    # .txt IS now supported — included
    (tmp_path / "report.pdf").touch()   # unsupported — excluded
    (tmp_path / "notes.md").touch()     # unsupported — excluded

    found = discover_files(tmp_path)
    names = [p.name for p in found]
    assert "orders.csv" in names
    assert "events.json" in names
    assert "notes.txt" in names         # text format is supported
    assert "report.pdf" not in names
    assert "notes.md" not in names


def test_discover_files_sorted(tmp_path):
    """discover_files returns files in sorted order."""
    for name in ("z.csv", "a.json", "m.xml"):
        (tmp_path / name).touch()
    found = [p.name for p in discover_files(tmp_path)]
    assert found == sorted(found)


def test_discover_files_ignores_directories(tmp_path):
    """Subdirectories are not included in the result."""
    (tmp_path / "subdir").mkdir()
    (tmp_path / "data.csv").touch()
    found = discover_files(tmp_path)
    assert all(p.is_file() for p in found)
