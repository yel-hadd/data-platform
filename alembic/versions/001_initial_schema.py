"""Initial schema: orders, ingestion_errors, document_chunks.

Revision ID: 001
Revises: (none)
Create Date: 2024-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension — idempotent, safe to run more than once.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "orders",
        sa.Column("id",                  sa.Integer,             primary_key=True, autoincrement=True),
        sa.Column("order_id",            sa.String,              nullable=False),
        sa.Column("customer_name",       sa.String,              nullable=False),
        sa.Column("country",             sa.String,              nullable=False),
        sa.Column("product",             sa.String,              nullable=False),
        sa.Column("quantity",            sa.Integer,             nullable=False),
        sa.Column("price",               sa.Numeric(12, 2),      nullable=False),
        sa.Column("order_date",          sa.Date,                nullable=False),
        sa.Column("source_type",         sa.String,              nullable=False),
        sa.Column(
            "ingestion_timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_orders_order_id", "orders", ["order_id"], unique=True)
    op.create_index("ix_orders_country",  "orders", ["country"])
    op.create_index("ix_orders_product",  "orders", ["product"])

    op.create_table(
        "ingestion_errors",
        sa.Column("id",          sa.Integer,             primary_key=True, autoincrement=True),
        sa.Column("source_file", sa.String,              nullable=False),
        sa.Column("row_data",    sa.Text,                nullable=False),
        sa.Column("reason",      sa.Text,                nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "document_chunks",
        sa.Column("id",          sa.Integer,             primary_key=True, autoincrement=True),
        sa.Column("source_file", sa.String,              nullable=False),
        sa.Column("content",     sa.Text,                nullable=False),
        # vector(1536) requires the pgvector extension created above.
        sa.Column("embedding",   sa.Text,                nullable=False),  # rendered as vector by pgvector
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    # Rename embedding column type to vector(1536) — pgvector DDL via raw SQL.
    op.execute(
        "ALTER TABLE document_chunks "
        "ALTER COLUMN embedding TYPE vector(1536) "
        "USING embedding::vector"
    )

    # IVFFlat cosine index for fast approximate nearest-neighbour search.
    op.execute(
        "CREATE INDEX ix_document_chunks_embedding "
        "ON document_chunks USING ivfflat (embedding vector_cosine_ops) "
        "WITH (lists = 100)"
    )


def downgrade() -> None:
    op.drop_table("document_chunks")
    op.drop_index("ix_orders_product",  table_name="orders")
    op.drop_index("ix_orders_country",  table_name="orders")
    op.drop_index("ix_orders_order_id", table_name="orders")
    op.drop_table("orders")
    op.drop_table("ingestion_errors")
