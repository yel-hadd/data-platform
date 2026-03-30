"""FastAPI dependency: provides an async database session per request."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from ..storage.database import AsyncSessionLocal


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield a database session; auto-closed after the request completes."""
    async with AsyncSessionLocal() as session:
        yield session
