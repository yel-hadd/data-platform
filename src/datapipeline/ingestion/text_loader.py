"""Text loader: reads plain-text files for unstructured AI extraction."""

from collections.abc import Iterator
from pathlib import Path

from .base import BaseLoader, RawRecord


class TextLoader(BaseLoader):
    """Reads an entire .txt file and yields it as a single raw record.

    The record contains the full file content under the ``_raw_text`` key.
    Downstream processing (see ``unstructured_parser``) hands this text to a
    PydanticAI agent to extract structured order data.
    """

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield one record containing the full text content of *path*.

        Args:
            path: Path to the .txt file.

        Yields:
            A single RawRecord with ``_raw_text`` and ``_source_file`` keys.
        """
        content = path.read_text(encoding="utf-8")
        yield {"_raw_text": content, "_source_file": str(path)}
