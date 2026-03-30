"""ZIP archive loader: extracts and delegates to per-format loaders."""

import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

from .base import BaseLoader, RawRecord
from .detector import detect_format


class ArchiveLoader(BaseLoader):
    """Extracts a ZIP archive to a temp directory and loads each contained file."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield records from every supported file inside the ZIP archive.

        Files with unsupported extensions inside the archive are silently skipped.

        Args:
            path: Path to the .zip file.
        """
        # Import here to avoid a circular dependency with the registry in __init__.py.
        from . import get_loader

        with zipfile.ZipFile(path) as zf, tempfile.TemporaryDirectory() as tmp:
            zf.extractall(tmp)
            for entry in sorted(Path(tmp).iterdir()):
                if not entry.is_file():
                    continue
                try:
                    fmt = detect_format(entry)
                except Exception:
                    continue  # skip unsupported files inside the archive
                yield from get_loader(fmt).load(entry)
