-- Data Platform schema — mirrored from SQLAlchemy ORM for Docker init and reference.
-- Alembic manages this schema in production; this file bootstraps the Docker container.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS orders (
    id               SERIAL PRIMARY KEY,
    order_id         VARCHAR NOT NULL UNIQUE,
    customer_name    VARCHAR NOT NULL,
    country          VARCHAR NOT NULL,
    product          VARCHAR NOT NULL,
    quantity         INTEGER NOT NULL,
    price            NUMERIC(12, 2) NOT NULL,
    order_date       DATE NOT NULL,
    source_type      VARCHAR NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_orders_order_id  ON orders(order_id);
CREATE INDEX IF NOT EXISTS ix_orders_country   ON orders(country);
CREATE INDEX IF NOT EXISTS ix_orders_product   ON orders(product);

CREATE TABLE IF NOT EXISTS ingestion_errors (
    id          SERIAL PRIMARY KEY,
    source_file VARCHAR NOT NULL,
    row_data    TEXT NOT NULL,
    reason      TEXT NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS document_chunks (
    id          SERIAL PRIMARY KEY,
    source_file VARCHAR NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1536) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- IVFFlat cosine index for approximate nearest-neighbour search.
CREATE INDEX IF NOT EXISTS ix_document_chunks_embedding
    ON document_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
