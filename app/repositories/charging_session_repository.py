from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.charging_session import ChargingSession
from app.domain.schemas.charging_session import ChargingSessionCreate


class ChargingSessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_batch(
        self, sessions: list[ChargingSessionCreate]
    ) -> int:
        """Insert sessions, skipping duplicates on session_id conflict. Returns inserted count."""
        if not sessions:
            return 0

        values = [s.model_dump() for s in sessions]
        stmt = insert(ChargingSession).values(values)
        stmt = stmt.on_conflict_do_nothing(index_elements=["session_id"])
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount  # type: ignore[return-value]

    async def get_by_session_id(self, session_id: str) -> ChargingSession | None:
        stmt = select(ChargingSession).where(ChargingSession.session_id == session_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_source(
        self, source_dataset: str, *, limit: int = 100, offset: int = 0
    ) -> Sequence[ChargingSession]:
        stmt = (
            select(ChargingSession)
            .where(ChargingSession.source_dataset == source_dataset)
            .order_by(ChargingSession.arrival_ts)
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return result.scalars().all()

    async def count_by_source(self, source_dataset: str) -> int:
        from sqlalchemy import func

        stmt = select(func.count()).where(
            ChargingSession.source_dataset == source_dataset
        )
        result = await self._session.execute(stmt)
        return result.scalar_one()
