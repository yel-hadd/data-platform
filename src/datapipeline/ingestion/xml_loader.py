"""XML loader: streams <order> elements using iterparse for memory efficiency."""

import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path

from ..exceptions import IngestionError
from .base import BaseLoader, RawRecord


class XMLLoader(BaseLoader):
    """Parses XML files expecting a root element containing <order> children."""

    def load(self, path: Path) -> Iterator[RawRecord]:
        """Yield one RawRecord per <order> element in the XML file.

        Uses iterparse with elem.clear() to keep memory usage constant regardless
        of file size — the parsed element is discarded immediately after yielding.

        Args:
            path: Path to the .xml file.

        Raises:
            IngestionError: If the file is not well-formed XML.
        """
        try:
            for _event, elem in ET.iterparse(path, events=("end",)):
                if elem.tag == "order":
                    yield {child.tag: child.text for child in elem}
                    # Free the element from memory once yielded.
                    elem.clear()
        except ET.ParseError as exc:
            raise IngestionError(f"Malformed XML in {path.name}: {exc}") from exc
