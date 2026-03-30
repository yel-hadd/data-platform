"""CSV loader: reads CSV files in configurable chunks for memory efficiency."""

from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from ..config import settings
from .base import BaseLoader, RawRecord


class CSVLoader(BaseLoader):
    """Reads CSV files in chunks of *chunk_size* rows using pandas."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield one RawRecord per CSV row, processing the file in chunks.

        Args:
            path: Path to the .csv file.

        Yields:
            RawRecord dicts with all values as strings (or None for empty cells).
        """
        for chunk in pd.read_csv(path, chunksize=settings.chunk_size, dtype=str):
            # Convert to object dtype first so NaN cells become Python None.
            yield from (
                chunk.astype(object)
                .where(chunk.notna(), other=None)
                .to_dict(orient="records")
            )
