"""JSON and NDJSON loaders: supports both array-style JSON and line-delimited JSON."""

import json
from collections.abc import Iterator
from pathlib import Path

from ..exceptions import IngestionError
from .base import BaseLoader, RawRecord


class JSONLoader(BaseLoader):
    """Loads a JSON file containing a top-level array of objects."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield each element of the JSON array as a RawRecord.

        Args:
            path: Path to a .json file containing an array or single object.

        Raises:
            IngestionError: If the file is not valid JSON.
        """
        try:
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            raise IngestionError(f"Invalid JSON in {path.name}: {exc}") from exc
        yield from (data if isinstance(data, list) else [data])


class NDJSONLoader(BaseLoader):
    """Streams NDJSON (Newline-Delimited JSON) line by line for memory efficiency."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield one RawRecord per non-empty line in the NDJSON file.

        Args:
            path: Path to a .ndjson or .jsonl file.

        Raises:
            IngestionError: If any line is not valid JSON.
        """
        with path.open(encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise IngestionError(
                        f"Invalid JSON on line {lineno} of {path.name}: {exc}"
                    ) from exc
