# Advanced Python Data Platform

A production-ready, async multi-format data ingestion, validation, storage, and analytics platform built with Python 3.13, FastAPI, SQLAlchemy 2, Pydantic v2, and Docker Compose.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Objective](#2-business-objective)
3. [Main Features](#3-main-features)
4. [Architecture](#4-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Directory Structure](#6-directory-structure)
7. [Installation & Setup](#7-installation--setup)
8. [Configuration](#8-configuration)
9. [Running with Docker Compose](#9-running-with-docker-compose)
10. [Running the Pipeline (CLI)](#10-running-the-pipeline-cli)
11. [Running the API](#11-running-the-api)
12. [Running the Tests](#12-running-the-tests)
13. [Supported File Formats](#13-supported-file-formats)
14. [Data Normalisation & Validation](#14-data-normalisation--validation)
15. [Analytics API](#15-analytics-api)
16. [Unstructured Parser & Semantic Search](#16-unstructured-parser--semantic-search)
17. [Database Migrations (Alembic)](#17-database-migrations-alembic)
18. [Parquet Export](#18-parquet-export)
19. [Datasets Used](#19-datasets-used)
20. [Known Limitations & Areas for Improvement](#20-known-limitations--areas-for-improvement)

---

## 1. Project Overview

This platform ingests tabular order data from heterogeneous file formats, validates and normalises every row into a canonical `OrderRecord` schema, persists valid records to PostgreSQL, and surfaces aggregate analytics via a FastAPI REST API. It also supports AI-driven extraction from unstructured text files using PydanticAI and stores text embeddings in PostgreSQL via pgvector for semantic search.

Invalid rows are **never silently dropped**: they are written to an `ingestion_errors` table with the original row data and failure reason for full audit and replay capability.

---

## 2. Business Objective

A company receives sales and order data from multiple heterogeneous sources — legacy XML systems, Excel spreadsheets, CSV exports, JSON event streams, and plain-text summaries. This platform provides a single, automated pipeline that:

- eliminates manual data wrangling by auto-detecting and parsing each format,
- enforces data quality through strict Pydantic validation before any record reaches the database,
- delivers consistent analytical metrics (revenue, top customers, trends) via a stable API consumed by dashboards and reporting tools,
- enables AI-powered querying of unstructured order summaries through semantic search.

---

## 3. Main Features

- **Multi-format ingestion** — CSV (chunked), Excel, JSON, NDJSON, XML, ZIP archives, and unstructured `.txt` files
- **Canonical schema** — all sources normalised to a single `OrderRecord` Pydantic model before storage
- **Zero silent failures** — invalid rows isolated to `ingestion_errors` table with full context
- **Async concurrent pipeline** — all files processed in parallel via `asyncio.gather` with per-file sessions
- **FastAPI analytics API** — revenue, product, customer, and trend metrics over HTTP
- **AI unstructured parser** — PydanticAI + OpenAI GPT extracts structured orders from free-form text
- **Semantic search** — pgvector stores text embeddings; cosine-similarity search exposed via API
- **Parquet export** — Hive-partitioned by country for Spark/DuckDB/Pandas compatibility
- **Alembic migrations** — version-controlled schema evolution for production deployments
- **Docker Compose** — one command brings up PostgreSQL (with pgvector), the API, and the pipeline worker
- **93 automated tests** — full coverage of all loaders, validation, pipeline, API, and AI features

---

## 4. Architecture

```mermaid
flowchart TD
    A[data/raw/] --> B[FileDetector]
    B --> C{Format?}
    C -->|csv / xlsx / json / xml / zip| D[Format Loader]
    C -->|txt| E[TextLoader]
    D --> F[Normaliser]
    F --> G[Pydantic v2 OrderRecord]
    G -->|valid| H[(orders)]
    G -->|invalid| I[(ingestion_errors)]
    E --> P[PydanticAI Agent\ngpt-4o-mini]
    P -->|extracted orders| H
    E --> V[OpenAI Embeddings\ntext-embedding-3-small]
    V --> J[(document_chunks\npgvector)]
    H --> K[write_parquet]
    K --> L[data/exports/\ncountry=XX/]
    H --> M[FastAPI]
    J --> M
    M --> N[GET /metrics/*]
    M --> O[GET /metrics/semantic-search]
```

The pipeline processes all discovered files **concurrently** with `asyncio.gather`, creating a dedicated database session per file to avoid transaction conflicts.

---

## 5. Technology Stack

| Component | Library / Version |
|---|---|
| Language | Python 3.13 |
| Web framework | FastAPI ≥ 0.115 |
| Database driver | asyncpg ≥ 0.30 (PostgreSQL) |
| ORM | SQLAlchemy 2 async |
| Migrations | Alembic ≥ 1.14 |
| Validation & settings | Pydantic v2 + pydantic-settings |
| AI extraction | PydanticAI ≥ 1.0 |
| AI embeddings | OpenAI `text-embedding-3-small` |
| Vector search | pgvector ≥ 0.3 |
| Data processing | pandas ≥ 2.2, pyarrow ≥ 18 |
| Excel | openpyxl ≥ 3.1 |
| CLI | Typer ≥ 0.12 |
| ASGI server | uvicorn |
| Package manager | uv |
| Testing | pytest, pytest-asyncio, aiosqlite, httpx, pytest-mock |
| Database (dev/prod) | PostgreSQL 16 + pgvector |
| Containerisation | Docker Compose |

---

## 6. Directory Structure

```
data-platform/
├── src/datapipeline/
│   ├── __init__.py
│   ├── __main__.py              # python -m datapipeline entry point
│   ├── cli.py                   # Typer CLI: run / api / generate-data / migrate
│   ├── config.py                # pydantic-settings: all env vars
│   ├── exceptions.py            # IngestionError, RowValidationError, StorageError
│   ├── logging_config.py        # root logger setup
│   ├── ingestion/
│   │   ├── __init__.py          # loader registry + get_loader()
│   │   ├── base.py              # BaseLoader ABC + RawRecord type alias
│   │   ├── detector.py          # detect_format() + discover_files()
│   │   ├── csv_loader.py        # CSV with pandas chunking
│   │   ├── json_loader.py       # JSON array + NDJSON streaming
│   │   ├── xml_loader.py        # XML iterparse
│   │   ├── excel_loader.py      # xlsx/xls via pandas + openpyxl
│   │   ├── archive_loader.py    # ZIP extraction + delegation
│   │   ├── text_loader.py       # .txt files → raw text record
│   │   └── unstructured_parser.py  # PydanticAI extraction + embedding storage
│   ├── validation/
│   │   └── models.py            # OrderRecord (Pydantic v2 canonical schema)
│   ├── transform/
│   │   └── normalize.py         # source-field-to-canonical remapping
│   ├── storage/
│   │   ├── __init__.py          # insert_orders(), insert_error()
│   │   ├── database.py          # async engine, session factory, init_db(), run_migrations()
│   │   ├── orm_models.py        # Order, IngestionError, DocumentChunk ORM classes
│   │   └── parquet_writer.py    # write_parquet() with Hive partitioning
│   ├── analytics/
│   │   ├── reports.py           # revenue_by_country/product, top_customers, trend
│   │   └── semantic_search.py   # pgvector cosine-similarity query
│   ├── pipeline/
│   │   └── orchestrator.py      # run_pipeline() with asyncio.gather
│   └── api/
│       ├── deps.py              # get_session() FastAPI dependency
│       ├── main.py              # FastAPI app + lifespan
│       └── routes/
│           ├── health.py        # GET /health
│           └── metrics.py       # GET /metrics/* endpoints
├── alembic/
│   ├── env.py                   # async Alembic environment
│   ├── __init__.py
│   └── versions/
│       └── 001_initial_schema.py
├── tests/
│   ├── conftest.py              # async_engine, async_session, sample_orders fixtures
│   ├── fixtures/                # sample.csv, .json, .ndjson, .xml
│   ├── test_api.py
│   ├── test_archive_loader.py
│   ├── test_config.py
│   ├── test_csv_loader.py
│   ├── test_detector.py
│   ├── test_excel_loader.py
│   ├── test_json_loader.py
│   ├── test_normalize.py
│   ├── test_parquet_writer.py
│   ├── test_pipeline.py
│   ├── test_reports.py
│   ├── test_semantic_search.py
│   ├── test_storage.py
│   ├── test_text_loader.py
│   ├── test_unstructured_parser.py
│   ├── test_validation.py
│   └── test_xml_loader.py
├── data/
│   ├── raw/                     # drop input files here (or use generate-data)
│   ├── processed/               # reserved for future transformed outputs
│   └── exports/                 # Parquet output (country-partitioned)
├── sql/
│   └── init.sql                 # PostgreSQL schema + pgvector extension (Docker init)
├── alembic.ini
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 7. Installation & Setup

**Requirements:** [uv](https://docs.astral.sh/uv/) and Python 3.13.

```bash
# Enter the project directory
cd data-platform

# Create virtual environment and install all dependencies (including dev)
uv sync --extra dev

# Verify installation
uv run python -c "import datapipeline; print('OK')"
```

---

## 8. Configuration

Copy `.env.example` to `.env` and edit as needed:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://pipeline:pipeline@localhost:5432/dataplatform` | Async PostgreSQL connection string |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `DATA_DIR` | `data/raw` | Directory scanned for input files |
| `CHUNK_SIZE` | `10000` | Rows read per pandas chunk for large CSV files |
| `OPENAI_API_KEY` | *(unset)* | Required only for unstructured `.txt` file processing |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | OpenAI embedding model used for pgvector storage |
| `EXTRACTION_MODEL` | `openai:gpt-4o-mini` | PydanticAI model used for structured data extraction from text |

All variables can be set in `.env` or as standard environment variables. The `OPENAI_API_KEY` is only required if you process `.txt` files; all other functionality works without it.

---

## 9. Running with Docker Compose

```bash
# Start PostgreSQL (pgvector), API, and pipeline worker
docker compose up --build

# Start only the database (for local development)
docker compose up postgres -d

# Run a one-shot pipeline against local data
docker compose run --rm pipeline

# Run Alembic migrations inside the container
docker compose run --rm pipeline uv run datapipeline migrate
```

Services:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 | PostgreSQL 16 + pgvector extension |
| `api` | 8000 | FastAPI analytics API |
| `pipeline` | — | One-shot ingestion run (exits when done) |

API interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 10. Running the Pipeline (CLI)

```bash
# Generate synthetic sample data in all supported formats
uv run datapipeline generate-data --output-dir data/raw

# Run the ingestion pipeline (requires a running PostgreSQL)
uv run datapipeline run --data-dir data/raw

# Run Alembic schema migrations
uv run datapipeline migrate

# Show all available commands
uv run datapipeline --help
```

The `run` command:
1. discovers all supported files in `DATA_DIR`,
2. processes them concurrently,
3. writes valid records to `orders`, errors to `ingestion_errors`,
4. exports a Parquet snapshot to `data/exports/`.

---

## 11. Running the API

```bash
# Start the API server
uv run datapipeline api --host 0.0.0.0 --port 8000

# Or via uvicorn directly
uv run uvicorn datapipeline.api.main:app --reload
```

Interactive docs available at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## 12. Running the Tests

All tests run against an **in-memory SQLite database** — no external services or API keys required.

```bash
# Run the full test suite
uv run pytest -v

# Run a specific module
uv run pytest tests/test_pipeline.py -v

# Short traceback on failure
uv run pytest --tb=short
```

**93 tests** covering:

| Area | Tests |
|---|---|
| Config loading and env override | `test_config.py` |
| All format loaders + edge cases | `test_csv/json/xml/excel/archive/text_loader.py` |
| File detection and discovery | `test_detector.py` |
| Field normalisation for every source | `test_normalize.py` |
| Pydantic validation (valid + invalid) | `test_validation.py` |
| Async DB insert and error persistence | `test_storage.py` |
| Parquet export with partitioning | `test_parquet_writer.py` |
| Async pipeline orchestration | `test_pipeline.py` |
| Analytics queries | `test_reports.py` |
| FastAPI endpoints | `test_api.py` |
| PydanticAI extraction (mocked) | `test_unstructured_parser.py` |
| Semantic search endpoint (mocked) | `test_semantic_search.py` |

---

## 13. Supported File Formats

| Extension | Format | Loader | Notes |
|---|---|---|---|
| `.csv` | CSV | `CSVLoader` | Chunked with pandas (`CHUNK_SIZE` rows) |
| `.xlsx` / `.xls` | Excel | `ExcelLoader` | First sheet; via pandas + openpyxl |
| `.json` | JSON array | `JSONLoader` | Top-level array or single object |
| `.ndjson` / `.jsonl` | NDJSON | `NDJSONLoader` | One JSON object per line; streamed |
| `.xml` | XML | `XMLLoader` | `<order>` children via `iterparse` |
| `.zip` | ZIP archive | `ArchiveLoader` | Delegates to inner-file loaders recursively |
| `.txt` | Unstructured text | `TextLoader` + PydanticAI | AI extraction via GPT; requires `OPENAI_API_KEY` |

Files with unsupported extensions are silently skipped by `discover_files()`.

---

## 14. Data Normalisation & Validation

Every loaded row is normalised to a canonical `OrderRecord` shape before validation:

```python
class OrderRecord(BaseModel):
    order_id:            str       # non-empty
    customer_name:       str       # non-empty
    country:             str       # 2–100 chars
    product:             str       # non-empty
    quantity:            int       # > 0
    price:               Decimal   # >= 0
    order_date:          date
    source_type:         str       # loader format key
    ingestion_timestamp: datetime  # auto-generated at load time
```

Each source format maps its own column names to the canonical schema via `transform/normalize.py`. For example:

| Source | `order_id` field | `customer_name` field |
|---|---|---|
| CSV | `order_id` | `customer_name` |
| Excel | `Order ID` | `Customer` |
| JSON | `orderId` | `customer` |
| XML | `<order_id>` | `<customer_name>` |

Invalid rows (failed Pydantic validation) are written to the `ingestion_errors` table — never discarded silently.

---

## 15. Analytics API

Base URL: `http://localhost:8000`

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service liveness check |
| GET | `/metrics/revenue-by-country` | Total revenue per country, sorted descending |
| GET | `/metrics/revenue-by-product` | Total revenue per product, sorted descending |
| GET | `/metrics/top-customers?limit=N` | Top N customers by total spend (default 10) |
| GET | `/metrics/revenue-trend` | Daily revenue totals in chronological order |
| GET | `/metrics/semantic-search?q=...&limit=N` | Semantic search over ingested text documents |

Example:

```bash
curl http://localhost:8000/metrics/revenue-by-country
# [{"country": "US", "revenue": 12450.50}, ...]

curl "http://localhost:8000/metrics/semantic-search?q=large+order+from+Germany&limit=3"
# [{"source_file": "...", "content": "...", "similarity": 0.91}, ...]
```

---

## 16. Unstructured Parser & Semantic Search

When a `.txt` file is placed in `data/raw/`, the pipeline activates a two-step AI workflow:

```mermaid
flowchart LR
    T[.txt file] --> L[TextLoader\nreads raw text]
    L --> A[PydanticAI Agent\nEXTRACTION_MODEL]
    A -->|list of ExtractedOrder| V[Pydantic validation]
    V -->|valid OrderRecord| DB[(orders table)]
    V -->|invalid| E[(ingestion_errors)]
    L --> EMB[OpenAI Embeddings\nEMBEDDING_MODEL]
    EMB -->|vector 1536-dim| PG[(document_chunks\npgvector)]
    PG -->|cosine similarity| API[GET /metrics/semantic-search]
```

**Extraction** — a `PydanticAI` agent (configurable via `EXTRACTION_MODEL`) reads the full text and returns a list of structured `ExtractedOrder` objects, which are then validated against `OrderRecord` and inserted into the `orders` table. Partially extractable orders that fail strict validation are logged to `ingestion_errors`.

**Embedding & storage** — the raw text is embedded using the OpenAI `EMBEDDING_MODEL` and stored as a 1536-dimensional vector in the `document_chunks` table (pgvector IVFFlat index, cosine distance).

**Semantic search** — `GET /metrics/semantic-search?q=your+query` embeds the query and returns the most similar stored documents ranked by cosine similarity.

Both steps require `OPENAI_API_KEY` to be set. If the key is absent, structured ingestion formats continue to work normally and the API returns a `503` for semantic search requests.

---

## 17. Database Migrations (Alembic)

Schema changes are managed with Alembic for production deployments:

```bash
# Apply all pending migrations
uv run datapipeline migrate

# Or call Alembic directly
uv run alembic upgrade head

# Check current revision
uv run alembic current

# Generate a new migration after ORM model changes
uv run alembic revision --autogenerate -m "describe_change"
```

The initial migration (`alembic/versions/001_initial_schema.py`) creates:
- `orders` table
- `ingestion_errors` table
- `document_chunks` table with `vector(1536)` column and IVFFlat cosine index
- enables `CREATE EXTENSION IF NOT EXISTS vector`

The Docker Compose setup applies `sql/init.sql` automatically on first start, which is equivalent to running migrations from scratch.

---

## 18. Parquet Export

After a pipeline run, all validated records are exported to `data/exports/` partitioned by country (Hive layout):

```
data/exports/
├── country=DE/
│   └── part-0.parquet
├── country=FR/
│   └── part-0.parquet
└── country=US/
    └── part-0.parquet
```

This layout is natively readable by Spark, DuckDB, Pandas, and any tool that understands Hive partitioning:

```python
import pandas as pd
df = pd.read_parquet("data/exports/", filters=[("country", "=", "FR")])
```

The pipeline CLI (`datapipeline run`) writes a Parquet snapshot automatically after each ingestion run.

---

## 19. Datasets Used

The `generate-data` CLI command creates synthetic sample data in every supported format:

| File | Format | Contents |
|---|---|---|
| `data/raw/orders.csv` | CSV | 100 order rows with varied countries and products |
| `data/raw/sales.xlsx` | Excel | Same schema, Excel column naming conventions |
| `data/raw/events.json` | JSON | Array of order events in camelCase JSON |
| `data/raw/stream.ndjson` | NDJSON | Same records, one JSON object per line |
| `data/raw/legacy.xml` | XML | `<orders><order>...</order></orders>` legacy format |
| `data/raw/archive.zip` | ZIP | ZIP containing a nested CSV file |
| `data/raw/unstructured_orders.txt` | Text | Natural-language order descriptions for AI extraction |

All sources contain the same business entity (orders) in format-specific representations. Generate them with:

```bash
uv run datapipeline generate-data --output-dir data/raw
```

---

## 20. Known Limitations & Areas for Improvement

| Area | Current state | Potential improvement |
|---|---|---|
| **AI extraction cost** | One OpenAI API call per `.txt` file | Batch or cache embeddings to reduce cost |
| **pgvector index** | IVFFlat with `lists=100` (approximate) | Switch to HNSW for better recall at scale |
| **Alembic autogenerate** | `Vector` column type requires manual DDL | Custom type comparator for full autogenerate support |
| **CI pipeline** | No automated CI | Add GitHub Actions: lint → test → build Docker image |
| **Authentication** | API is unauthenticated | Add API key or OAuth2 middleware |
| **Retry / backoff** | No retry on DB or OpenAI failures | Add `tenacity`-based retry decorator |
| **Streaming large files** | XML uses iterparse; CSV uses chunking | Explore async generators for all loaders |
| **Multi-table schema** | Single `orders` table | Add `customers` and `products` dimension tables |
| **Monitoring** | Structured logs only | Add Prometheus metrics + Grafana dashboard |
