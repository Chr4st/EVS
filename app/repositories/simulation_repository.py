"""Repository for persisting simulation runs and load timeseries."""

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.simulation import LoadTimeseries, SimulationRun


class SimulationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_run(
        self,
        scenario_name: str,
        policy_name: str,
        sessions_count: int,
        peak_load_kw: float,
        total_energy_kwh: float,
        completion_rate: float,
        average_load_kw: float,
        load_factor: float,
    ) -> SimulationRun:
        run = SimulationRun(
            scenario_name=scenario_name,
            policy_name=policy_name,
            sessions_count=sessions_count,
            peak_load_kw=peak_load_kw,
            total_energy_kwh=total_energy_kwh,
            completion_rate=completion_rate,
            average_load_kw=average_load_kw,
            load_factor=load_factor,
        )
        self._session.add(run)
        await self._session.flush()
        return run

    async def insert_load_timeseries(
        self,
        run_id: int,
        records: list[dict[str, object]],
    ) -> int:
        """Insert load timeseries records. Returns count inserted."""
        if not records:
            return 0

        objects = [
            LoadTimeseries(
                run_id=run_id,
                timestamp=r["timestamp"],  # type: ignore[arg-type]
                station_id=r.get("station_id"),  # type: ignore[arg-type]
                load_kw=r["load_kw"],  # type: ignore[arg-type]
            )
            for r in records
        ]
        self._session.add_all(objects)
        await self._session.flush()
        return len(objects)

    async def get_run(self, run_id: int) -> SimulationRun | None:
        stmt = select(SimulationRun).where(SimulationRun.id == run_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_fleet_load(self, run_id: int) -> Sequence[LoadTimeseries]:
        stmt = (
            select(LoadTimeseries)
            .where(LoadTimeseries.run_id == run_id)
            .where(LoadTimeseries.station_id.is_(None))
            .order_by(LoadTimeseries.timestamp)
        )
        result = await self._session.execute(stmt)
        return result.scalars().all()

    async def get_station_load(
        self, run_id: int, station_id: str | None = None
    ) -> Sequence[LoadTimeseries]:
        stmt = (
            select(LoadTimeseries)
            .where(LoadTimeseries.run_id == run_id)
            .where(LoadTimeseries.station_id.is_not(None))
        )
        if station_id is not None:
            stmt = stmt.where(LoadTimeseries.station_id == station_id)
        stmt = stmt.order_by(LoadTimeseries.station_id, LoadTimeseries.timestamp)
        result = await self._session.execute(stmt)
        return result.scalars().all()
