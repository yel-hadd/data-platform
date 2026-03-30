"""Ingestion package: loader registry mapping format keys to loader classes."""

from ..exceptions import IngestionError
from .archive_loader import ArchiveLoader
from .base import BaseLoader, RawRecord
from .csv_loader import CSVLoader
from .excel_loader import ExcelLoader
from .json_loader import JSONLoader, NDJSONLoader
from .text_loader import TextLoader
from .xml_loader import XMLLoader

_LOADERS: dict[str, type[BaseLoader]] = {
    "csv": CSVLoader,
    "json": JSONLoader,
    "ndjson": NDJSONLoader,
    "xml": XMLLoader,
    "excel": ExcelLoader,
    "zip": ArchiveLoader,
    "text": TextLoader,
}


def get_loader(fmt: str) -> BaseLoader:
    """Return an instantiated loader for *fmt*.

    Args:
        fmt: Format key (csv, json, ndjson, xml, excel, zip).

    Raises:
        IngestionError: If no loader is registered for *fmt*.
    """
    cls = _LOADERS.get(fmt)
    if cls is None:
        raise IngestionError(f"No loader registered for format: {fmt!r}")
    return cls()


__all__ = ["get_loader", "BaseLoader", "RawRecord"]
