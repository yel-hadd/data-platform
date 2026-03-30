"""Alembic environment — async migration runner for the data platform."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

# The config object gives access to values in alembic.ini.
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import the shared Base so Alembic can autogenerate migrations from ORM models.
# Import orm_models early so all mapped classes are registered on Base.metadata.
from datapipeline.storage.orm_models import Base  # noqa: E402
import datapipeline.storage.orm_models  # noqa: F401 — registers all models

target_metadata = Base.metadata


def _get_url() -> str:
    """Return the async DATABASE_URL from application settings."""
    from datapipeline.config import settings
    return settings.database_url


def do_run_migrations(connection):
    """Execute migrations synchronously within an async connection context."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine and run all pending migrations."""
    url = _get_url()
    engine = create_async_engine(url, echo=False)
    async with engine.connect() as conn:
        await conn.run_sync(do_run_migrations)
    await engine.dispose()


def run_migrations_online() -> None:
    """Entry point called by Alembic when running in 'online' mode."""
    asyncio.run(run_async_migrations())


def run_migrations_offline() -> None:
    """Generate SQL scripts without a live DB connection (for review/audit)."""
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
