"""Simulation API endpoints."""

import polars as pl
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.charging_session import ChargingSession
from app.db.session import get_session
from app.repositories.simulation_repository import SimulationRepository
from app.simulation.aggregation.load_aggregator import (
    compute_fleet_load_curve,
    compute_station_load_curves,
    decisions_to_frame,
)
from app.simulation.engine.baseline_engine import SimulationConfig, run_simulation
from app.simulation.engine.charging_policy import get_policy, list_policies
from app.simulation.metrics.baseline_metrics import compute_metrics
from app.simulation.schemas.simulation_output import (
    LoadTimeseriesPoint,
    PolicyInfo,
    SimulationResultsResponse,
    SimulationRunRequest,
    SimulationRunResponse,
)

router = APIRouter(prefix="/simulation", tags=["simulation"])

POLICY_DESCRIPTIONS = {
    "immediate_charge": "Charge at max rate immediately upon arrival.",
    "uniform_spread": "Spread energy evenly across the charging window.",
    "randomized_delay": "Delay start randomly within slack, then charge at max rate.",
}


@router.get("/policies")
async def get_policies() -> list[PolicyInfo]:
    """List available charging policies."""
    return [
        PolicyInfo(name=name, description=POLICY_DESCRIPTIONS.get(name, ""))
        for name in list_policies()
    ]


@router.post("/run", response_model=SimulationRunResponse)
async def run_sim(
    request: SimulationRunRequest,
    db: AsyncSession = Depends(get_session),  # noqa: B008
) -> SimulationRunResponse:
    """Run a baseline charging simulation."""
    # Load sessions from DB
    stmt = select(ChargingSession).where(
        ChargingSession.source_dataset == request.source_dataset
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No sessions found for dataset '{request.source_dataset}'",
        )

    # Convert ORM objects to Polars DataFrame
    records = [
        {
            "session_id": r.session_id,
            "station_id": r.station_id,
            "arrival_ts": r.arrival_ts,
            "departure_ts": r.departure_ts,
            "energy_requested_kwh": r.energy_requested_kwh,
            "energy_delivered_kwh": r.energy_delivered_kwh,
            "max_charge_rate_kw": r.max_charge_rate_kw,
            "session_duration_minutes": r.session_duration_minutes,
            "is_valid": r.is_valid,
        }
        for r in rows
    ]
    sessions_df = pl.DataFrame(records)

    # Get policy
    try:
        policy = get_policy(request.policy_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Run simulation
    config = SimulationConfig(
        policy=policy,
        interval_minutes=request.interval_minutes,
        scenario_name=request.scenario_name,
    )
    sim_result = run_simulation(sessions_df, config)

    # Compute metrics
    metrics = compute_metrics(sim_result, sessions_df)

    # Compute load curves
    decisions_df = decisions_to_frame(sim_result.decisions)
    fleet_load = compute_fleet_load_curve(decisions_df)
    station_load = compute_station_load_curves(decisions_df, sessions_df)

    # Persist
    repo = SimulationRepository(db)
    run = await repo.create_run(
        scenario_name=sim_result.scenario_name,
        policy_name=sim_result.policy_name,
        sessions_count=sim_result.sessions_count,
        peak_load_kw=metrics.peak_load_kw,
        total_energy_kwh=metrics.total_energy_kwh,
        completion_rate=metrics.completion_rate,
        average_load_kw=metrics.average_load_kw,
        load_factor=metrics.load_factor,
    )

    # Persist fleet load (station_id=None for fleet aggregate)
    fleet_records = [
        {"timestamp": row["timestamp"], "station_id": None, "load_kw": row["total_load_kw"]}
        for row in fleet_load.iter_rows(named=True)
    ]
    await repo.insert_load_timeseries(run.id, fleet_records)

    # Persist station load
    station_records = [
        {"timestamp": row["timestamp"], "station_id": row["station_id"], "load_kw": row["load_kw"]}
        for row in station_load.iter_rows(named=True)
    ]
    await repo.insert_load_timeseries(run.id, station_records)

    await db.commit()

    return SimulationRunResponse(
        run_id=run.id,
        scenario_name=run.scenario_name,
        policy_name=run.policy_name,
        sessions_count=run.sessions_count,
        peak_load_kw=run.peak_load_kw,
        total_energy_kwh=run.total_energy_kwh,
        completion_rate=run.completion_rate,
        average_load_kw=run.average_load_kw,
        load_factor=run.load_factor,
        created_at=run.created_at,
    )


@router.get("/{run_id}/results", response_model=SimulationResultsResponse)
async def get_results(
    run_id: int,
    db: AsyncSession = Depends(get_session),  # noqa: B008
) -> SimulationResultsResponse:
    """Get full results for a simulation run."""
    repo = SimulationRepository(db)

    run = await repo.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Simulation run {run_id} not found")

    fleet_load = await repo.get_fleet_load(run_id)
    station_load = await repo.get_station_load(run_id)

    return SimulationResultsResponse(
        run=SimulationRunResponse(
            run_id=run.id,
            scenario_name=run.scenario_name,
            policy_name=run.policy_name,
            sessions_count=run.sessions_count,
            peak_load_kw=run.peak_load_kw,
            total_energy_kwh=run.total_energy_kwh,
            completion_rate=run.completion_rate,
            average_load_kw=run.average_load_kw,
            load_factor=run.load_factor,
            created_at=run.created_at,
        ),
        fleet_load_curve=[
            LoadTimeseriesPoint(
                timestamp=ts.timestamp, station_id=ts.station_id, load_kw=ts.load_kw
            )
            for ts in fleet_load
        ],
        station_load_curves=[
            LoadTimeseriesPoint(
                timestamp=ts.timestamp, station_id=ts.station_id, load_kw=ts.load_kw
            )
            for ts in station_load
        ],
    )
