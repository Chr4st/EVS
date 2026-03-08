from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.utils.time import utc_now


class ChargingSessionCreate(BaseModel):
    """Schema for creating a new charging session record."""

    model_config = ConfigDict(strict=False)

    session_id: str
    source_dataset: str
    source_record_id: str | None = None

    station_id: str | None = None
    port_id: str | None = None
    vehicle_id: str | None = None

    arrival_ts: datetime
    departure_ts: datetime

    energy_requested_kwh: float | None = None
    energy_delivered_kwh: float | None = None
    max_charge_rate_kw: float | None = None
    average_charge_rate_kw: float | None = None

    session_duration_minutes: int
    charging_duration_minutes: int | None = None

    is_valid: bool = True
    validation_errors: list[dict[str, Any]] = Field(default_factory=list)
    raw_payload: dict[str, Any]


class ChargingSessionRead(BaseModel):
    """Schema for reading a charging session from the database."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    session_id: str
    source_dataset: str
    source_record_id: str | None = None

    station_id: str | None = None
    port_id: str | None = None
    vehicle_id: str | None = None

    arrival_ts: datetime
    departure_ts: datetime

    energy_requested_kwh: float | None = None
    energy_delivered_kwh: float | None = None
    max_charge_rate_kw: float | None = None
    average_charge_rate_kw: float | None = None

    session_duration_minutes: int
    charging_duration_minutes: int | None = None

    is_valid: bool
    validation_errors: list[dict[str, Any]]
    raw_payload: dict[str, Any]

    created_at: datetime
    updated_at: datetime


class IngestionRequest(BaseModel):
    """Request body for triggering ingestion."""

    source_dataset: str
    source_path: str


class IngestionResponse(BaseModel):
    """Response from an ingestion run."""

    run_id: int
    source_dataset: str
    status: str
    records_seen: int
    records_inserted: int
    records_invalid: int
    started_at: datetime
    completed_at: datetime | None = None
