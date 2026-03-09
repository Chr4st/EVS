"""Baseline charging simulator engine.

Steps through a time index, applies a charging policy to each active session,
and accumulates charging decisions into a complete simulation result.
"""

from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 — used at runtime in dataclass fields

import polars as pl

from app.simulation.engine.charging_policy import ChargingDecision, ChargingPolicy
from app.simulation.engine.fleet_state import (
    FleetSnapshot,
    build_fleet_snapshot,
    build_session_frame,
)
from app.simulation.engine.time_index import build_time_index, infer_time_bounds
from app.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class SimulationConfig:
    """Immutable simulation configuration."""

    policy: ChargingPolicy
    interval_minutes: int = 5
    scenario_name: str = "default"


@dataclass(frozen=True)
class SimulationResult:
    """Immutable result of a simulation run."""

    scenario_name: str
    policy_name: str
    sessions_count: int
    decisions: list[ChargingDecision]
    snapshots: list[FleetSnapshot]
    time_index: pl.DataFrame


@dataclass
class EnergyTracker:
    """Mutable tracker for energy delivered per session during simulation."""

    delivered: dict[str, float] = field(default_factory=dict)

    def add(self, session_id: str, energy_kwh: float) -> None:
        self.delivered[session_id] = self.delivered.get(session_id, 0.0) + energy_kwh

    def get(self, session_id: str) -> float:
        return self.delivered.get(session_id, 0.0)

    def snapshot(self) -> dict[str, float]:
        return dict(self.delivered)


def run_simulation(
    sessions: pl.DataFrame,
    config: SimulationConfig,
) -> SimulationResult:
    """Execute a baseline charging simulation.

    Args:
        sessions: DataFrame of normalized charging sessions from Feature 1.
            Must contain: session_id, station_id, arrival_ts, departure_ts,
            energy_requested_kwh, energy_delivered_kwh, max_charge_rate_kw,
            session_duration_minutes.
        config: Simulation configuration.

    Returns:
        SimulationResult with all charging decisions and fleet snapshots.
    """
    # Filter to valid sessions only
    if "is_valid" in sessions.columns:
        sessions = sessions.filter(pl.col("is_valid"))

    session_frame = build_session_frame(sessions)
    sessions_count = len(session_frame)

    if sessions_count == 0:
        logger.warning("No valid sessions for simulation")
        empty_time_index = pl.DataFrame({
            "timestamp": [],
            "step_index": [],
            "interval_hours": [],
        })
        return SimulationResult(
            scenario_name=config.scenario_name,
            policy_name=config.policy.name,
            sessions_count=0,
            decisions=[],
            snapshots=[],
            time_index=empty_time_index,
        )

    start_ts, end_ts = infer_time_bounds(session_frame)
    time_index = build_time_index(start_ts, end_ts, config.interval_minutes)

    logger.info(
        "Starting simulation: %s policy=%s sessions=%d steps=%d",
        config.scenario_name,
        config.policy.name,
        sessions_count,
        len(time_index),
    )

    tracker = EnergyTracker()
    all_decisions: list[ChargingDecision] = []
    all_snapshots: list[FleetSnapshot] = []

    for row in time_index.iter_rows(named=True):
        ts: datetime = row["timestamp"]
        step_idx: int = row["step_index"]
        interval_hours: float = row["interval_hours"]

        snapshot = build_fleet_snapshot(
            session_frame, ts, step_idx, tracker.snapshot()
        )
        all_snapshots.append(snapshot)

        for session_state in snapshot.active_sessions:
            decision = config.policy.decide(session_state, ts, interval_hours)
            all_decisions.append(decision)
            tracker.add(decision.session_id, decision.energy_delivered_kwh)

    logger.info(
        "Simulation complete: decisions=%d snapshots=%d",
        len(all_decisions),
        len(all_snapshots),
    )

    return SimulationResult(
        scenario_name=config.scenario_name,
        policy_name=config.policy.name,
        sessions_count=sessions_count,
        decisions=all_decisions,
        snapshots=all_snapshots,
        time_index=time_index,
    )
