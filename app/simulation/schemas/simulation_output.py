"""Pydantic schemas for simulation API layer."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SimulationRunRequest(BaseModel):
    """Request to start a simulation run."""

    source_dataset: str
    policy_name: str
    scenario_name: str = "default"
    interval_minutes: int = 5


class SimulationRunResponse(BaseModel):
    """Response for a completed simulation run."""

    model_config = ConfigDict(from_attributes=True)

    run_id: int
    scenario_name: str
    policy_name: str
    sessions_count: int
    peak_load_kw: float
    total_energy_kwh: float
    completion_rate: float
    average_load_kw: float
    load_factor: float
    created_at: datetime


class LoadTimeseriesPoint(BaseModel):
    """Single point in a load timeseries."""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    station_id: str | None
    load_kw: float


class SimulationResultsResponse(BaseModel):
    """Full results for a simulation run."""

    run: SimulationRunResponse
    fleet_load_curve: list[LoadTimeseriesPoint]
    station_load_curves: list[LoadTimeseriesPoint]


class PolicyInfo(BaseModel):
    """Info about an available charging policy."""

    name: str
    description: str
