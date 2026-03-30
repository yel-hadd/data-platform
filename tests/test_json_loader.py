"""Tests for JSON and NDJSON loaders."""

from pathlib import Path

import pytest

from datapipeline.exceptions import IngestionError
from datapipeline.ingestion.json_loader import JSONLoader, NDJSONLoader

FIXTURES = Path(__file__).parent / "fixtures"


def test_json_array_returns_all_records():
    """sample.json with 3 objects yields exactly 3 records."""
    loader = JSONLoader()
    records = list(loader.load(FIXTURES / "sample.json"))
    assert len(records) == 3
    assert records[0]["orderId"] == "ORD-J001"


def test_json_single_object(tmp_path):
    """A JSON file containing a single object (not array) yields 1 record."""
    f = tmp_path / "single.json"
    f.write_text('{"orderId": "X", "customer": "Y"}')
    loader = JSONLoader()
    records = list(loader.load(f))
    assert len(records) == 1
    assert records[0]["orderId"] == "X"


def test_json_invalid_raises(tmp_path):
    """Malformed JSON raises IngestionError."""
    f = tmp_path / "bad.json"
    f.write_text("{not valid json")
    with pytest.raises(IngestionError, match="Invalid JSON"):
        list(JSONLoader().load(f))


def test_ndjson_streams_all_records():
    """sample.ndjson with 5 lines yields exactly 5 records."""
    loader = NDJSONLoader()
    records = list(loader.load(FIXTURES / "sample.ndjson"))
    assert len(records) == 5


def test_ndjson_skips_blank_lines(tmp_path):
    """Blank lines in NDJSON are skipped, not treated as errors."""
    f = tmp_path / "sparse.ndjson"
    f.write_text('{"a": 1}\n\n{"b": 2}\n\n')
    records = list(NDJSONLoader().load(f))
    assert len(records) == 2


def test_ndjson_invalid_line_raises(tmp_path):
    """An invalid JSON line raises IngestionError with the line number."""
    f = tmp_path / "bad.ndjson"
    f.write_text('{"a": 1}\n{bad line}\n')
    with pytest.raises(IngestionError, match="line 2"):
        list(NDJSONLoader().load(f))
