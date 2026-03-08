"""Baseline simulation metrics.

Pure functions that compute key performance indicators from simulation results.
"""

from dataclasses import dataclass

import polars as pl

from app.simulation.engine.baseline_engine import SimulationResult
from app.simulation.engine.charging_policy import ChargingDecision


@dataclass(frozen=True)
class BaselineMetrics:
    """Immutable metrics for a simulation run."""

    peak_load_kw: float
    total_energy_kwh: float
    completion_rate: float
    average_load_kw: float
    load_factor: float  # average / peak
    sessions_completed: int
    sessions_total: int


def _compute_energy_per_session(
    decisions: list[ChargingDecision],
) -> dict[str, float]:
    """Sum energy delivered per session across all timesteps."""
    totals: dict[str, float] = {}
    for d in decisions:
        totals[d.session_id] = totals.get(d.session_id, 0.0) + d.energy_delivered_kwh
    return totals


def _compute_completion_rate(
    energy_delivered: dict[str, float],
    energy_targets: dict[str, float],
    threshold: float = 0.95,
) -> tuple[int, int, float]:
    """Compute fraction of sessions that received >= threshold of requested energy.

    Returns (completed, total, rate).
    """
    if not energy_targets:
        return 0, 0, 0.0

    completed = 0
    for sid, target in energy_targets.items():
        delivered = energy_delivered.get(sid, 0.0)
        if target <= 0 or delivered / target >= threshold:
            completed += 1

    total = len(energy_targets)
    rate = completed / total if total > 0 else 0.0
    return completed, total, rate


def compute_metrics(
    result: SimulationResult,
    sessions: pl.DataFrame,
) -> BaselineMetrics:
    """Compute baseline metrics from a simulation result.

    Args:
        result: Output of run_simulation.
        sessions: Original sessions DataFrame (for energy targets).
    """
    if not result.decisions:
        return BaselineMetrics(
            peak_load_kw=0.0,
            total_energy_kwh=0.0,
            completion_rate=0.0,
            average_load_kw=0.0,
            load_factor=0.0,
            sessions_completed=0,
            sessions_total=result.sessions_count,
        )

    # Total load per timestep
    load_by_step: dict[int, float] = {}
    for snapshot in result.snapshots:
        load_by_step[snapshot.step_index] = 0.0

    for d in result.decisions:
        # Find step index by timestamp
        for snap in result.snapshots:
            if snap.timestamp == d.timestamp:
                load_by_step[snap.step_index] = (
                    load_by_step.get(snap.step_index, 0.0) + d.charge_rate_kw
                )
                break

    loads = list(load_by_step.values())
    peak_load = max(loads) if loads else 0.0
    avg_load = sum(loads) / len(loads) if loads else 0.0
    load_factor = avg_load / peak_load if peak_load > 0 else 0.0

    # Energy totals
    energy_per_session = _compute_energy_per_session(result.decisions)
    total_energy = sum(energy_per_session.values())

    # Completion rate
    energy_targets: dict[str, float] = {}
    if "energy_requested_kwh" in sessions.columns:
        for row in sessions.iter_rows(named=True):
            target = row.get("energy_requested_kwh")
            if target is not None and target > 0:
                energy_targets[row["session_id"]] = target

    completed, total, rate = _compute_completion_rate(
        energy_per_session, energy_targets
    )

    return BaselineMetrics(
        peak_load_kw=round(peak_load, 2),
        total_energy_kwh=round(total_energy, 2),
        completion_rate=round(rate, 4),
        average_load_kw=round(avg_load, 2),
        load_factor=round(load_factor, 4),
        sessions_completed=completed,
        sessions_total=result.sessions_count,
    )
