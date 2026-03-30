"""File format detection: maps file extensions to loader format keys."""

from pathlib import Path

from ..exceptions import IngestionError

# Maps lowercase file extension → internal format key used by the loader registry.
_EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
    ".ndjson": "ndjson",
    ".jsonl": "ndjson",
    ".xml": "xml",
    ".zip": "zip",
    ".txt": "text",
}


def detect_format(path: Path) -> str:
    """Return the format key for *path* based on its extension.

    Args:
        path: File to inspect.

    Returns:
        One of: csv, excel, json, ndjson, xml, zip.

    Raises:
        IngestionError: If the extension is not supported.
    """
    fmt = _EXTENSION_MAP.get(path.suffix.lower())
    if fmt is None:
        raise IngestionError(f"Unsupported file type: {path.suffix!r} ({path.name})")
    return fmt


def discover_files(directory: Path) -> list[Path]:
    """Return all supported data files in *directory* (non-recursive).

    Args:
        directory: Directory to scan.

    Returns:
        Sorted list of paths whose extensions are in _EXTENSION_MAP.
    """
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() in _EXTENSION_MAP
    )
