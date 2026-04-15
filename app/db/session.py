from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    return create_async_engine(database_url, echo=echo, pool_pre_ping=True)


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


def get_sessionmaker(database_url: str, *, echo: bool = False) -> async_sessionmaker[AsyncSession]:
    """Convenience helper: build engine + async sessionmaker."""
    engine = create_engine(database_url, echo=echo)
    return create_sessionmaker(engine)

