from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.utils.time import utc_now


class ChargingSession(Base):
    __tablename__ = "charging_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    source_dataset: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    source_record_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    station_id: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    port_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    vehicle_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    arrival_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    departure_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    energy_requested_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    energy_delivered_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_charge_rate_kw: Mapped[float | None] = mapped_column(Float, nullable=True)
    average_charge_rate_kw: Mapped[float | None] = mapped_column(Float, nullable=True)

    session_duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    charging_duration_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)

    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    validation_errors: Mapped[dict] = mapped_column(JSONB, nullable=False, default=list)  # type: ignore[assignment]
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)  # type: ignore[assignment]

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    __table_args__ = (
        Index("ix_charging_sessions_source_arrival", "source_dataset", "arrival_ts"),
    )
