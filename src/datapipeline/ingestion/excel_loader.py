"""Excel loader: reads .xlsx/.xls files using pandas and openpyxl."""

from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from .base import BaseLoader, RawRecord


class ExcelLoader(BaseLoader):
    """Reads the first sheet of an Excel workbook into RawRecord dicts."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield one RawRecord per data row in the first Excel sheet.

        All values are read as strings. Empty cells become None.

        Args:
            path: Path to the .xlsx or .xls file.
        """
        df = pd.read_excel(path, dtype=str)
        yield from (
            df.astype(object)
            .where(df.notna(), other=None)
            .to_dict(orient="records")
        )
