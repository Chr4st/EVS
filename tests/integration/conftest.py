"""Integration test fixtures: real Postgres via testcontainers, async session, and HTTP client."""

from collections.abc import AsyncGenerator, Generator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.postgres import PostgresContainer

from app.db.base import Base
from app.db.session import get_session
from app.main import app


@pytest.fixture(scope="session")
def postgres_url() -> Generator[str, None, None]:
    """Start a Postgres container for the entire test session."""
    with PostgresContainer("postgres:16-alpine") as pg:
        sync_url = pg.get_connection_url()
        yield sync_url.replace("psycopg2", "asyncpg")


@pytest.fixture
async def async_engine(postgres_url: str) -> AsyncGenerator[AsyncEngine, None]:
    """Create a fresh async engine per test to avoid event-loop conflicts."""
    engine = create_async_engine(postgres_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def db_session(async_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Provide an async DB session for direct repository/service assertions."""
    factory = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
async def client(async_engine: AsyncEngine) -> AsyncGenerator[AsyncClient, None]:
    """HTTP client wired to the FastAPI app with a test-database session override."""
    factory = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

    async def _override_session() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
