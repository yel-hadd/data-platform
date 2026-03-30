"""SQLAlchemy ORM mapped classes for the data platform schema."""

from datetime import UTC, date, datetime
from decimal import Decimal

from pgvector.sqlalchemy import Vector
from sqlalchemy import Date, DateTime, Numeric, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class Order(Base):
    """Persisted canonical order record."""

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    customer_name: Mapped[str] = mapped_column(String, nullable=False)
    country: Mapped[str] = mapped_column(String, index=True, nullable=False)
    product: Mapped[str] = mapped_column(String, index=True, nullable=False)
    quantity: Mapped[int] = mapped_column(nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    ingestion_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class IngestionError(Base):
    """Record of a row that failed validation or normalisation."""

    __tablename__ = "ingestion_errors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_file: Mapped[str] = mapped_column(String, nullable=False)
    row_data: Mapped[str] = mapped_column(Text, nullable=False)  # JSON-serialised
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DocumentChunk(Base):
    """Persisted text excerpt with its pgvector embedding for semantic search."""

    __tablename__ = "document_chunks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_file: Mapped[str] = mapped_column(String, nullable=False)
    # Raw text excerpt stored alongside its embedding for result display.
    content: Mapped[str] = mapped_column(Text, nullable=False)
    # 1536-dim vector produced by text-embedding-3-small.
    embedding: Mapped[list] = mapped_column(Vector(1536), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
