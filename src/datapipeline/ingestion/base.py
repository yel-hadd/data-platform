"""Abstract base loader and shared type alias for raw ingestion records."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path

# Raw record: a flat dict mapping column names to primitive values.
RawRecord = dict[str, str | int | float | None]


class BaseLoader(ABC):
    """Contract that every format-specific loader must implement."""

    @abstractmethod
    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield raw records from *path* one at a time.

        Args:
            path: Path to the data file.

        Yields:
            One RawRecord per logical row in the source.
        """
