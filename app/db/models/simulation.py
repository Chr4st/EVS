from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.utils.time import utc_now


class SimulationRun(Base):
    __tablename__ = "simulation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scenario_name: Mapped[str] = mapped_column(Text, nullable=False)
    policy_name: Mapped[str] = mapped_column(Text, nullable=False)
    sessions_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    peak_load_kw: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_energy_kwh: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    completion_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    average_load_kw: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    load_factor: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class LoadTimeseries(Base):
    __tablename__ = "load_timeseries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("simulation_runs.id"), nullable=False, index=True
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    station_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    load_kw: Mapped[float] = mapped_column(Float, nullable=False)
