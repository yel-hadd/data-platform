"""Custom exceptions for the data platform pipeline."""


class IngestionError(Exception):
    """Raised when a file cannot be read or parsed."""


class RowValidationError(Exception):
    """Raised for a single invalid row; carries the raw row data and failure reason."""

    def __init__(self, row: dict, reason: str) -> None:
        self.row = row
        self.reason = reason
        super().__init__(reason)


class StorageError(Exception):
    """Raised when a database write operation fails."""
