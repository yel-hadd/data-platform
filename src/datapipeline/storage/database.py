"""Async SQLAlchemy engine and session factory."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..config import settings
from .orm_models import Base

engine = create_async_engine(settings.database_url, pool_pre_ping=True)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, expire_on_commit=False
)


async def init_db() -> None:
    """Create all ORM-defined tables if they do not already exist.

    Called once at application startup via the FastAPI lifespan handler.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def run_migrations() -> None:
    """Run Alembic migrations programmatically (upgrade to head).

    Intended for use in the ``migrate`` CLI command and production startup.
    Tests bypass this and use ``init_db()`` directly against SQLite.
    """
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")


async def get_session() -> AsyncSession:  # pragma: no cover
    """Async generator that yields a database session; used as a FastAPI dependency."""
    async with AsyncSessionLocal() as session:
        yield session
