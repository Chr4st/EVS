"""Fleet state builder.

Constructs a view of all active EV sessions at each simulation timestep,
tracking remaining energy demand, remaining time, and max charge rate.
"""

from dataclasses import dataclass
from datetime import datetime

import polars as pl


@dataclass(frozen=True)
class SessionState:
    """Immutable snapshot of a single EV session at a given timestep."""

    session_id: str
    station_id: str | None
    arrival_ts: datetime
    departure_ts: datetime
    energy_requested_kwh: float
    energy_delivered_so_far_kwh: float
    max_charge_rate_kw: float
    remaining_energy_kwh: float
    remaining_minutes: float


@dataclass(frozen=True)
class FleetSnapshot:
    """Immutable snapshot of fleet state at a single timestep."""

    timestamp: datetime
    step_index: int
    active_sessions: list[SessionState]


DEFAULT_MAX_RATE_KW = 7.2  # Level 2 default
DEFAULT_ENERGY_KWH = 30.0  # Reasonable default if missing


def build_session_frame(sessions: pl.DataFrame) -> pl.DataFrame:
    """Prepare sessions for simulation by filling defaults and adding tracking columns.

    Returns a new DataFrame with standardized columns ready for simulation.
    """
    return sessions.select([
        pl.col("session_id"),
        pl.col("station_id"),
        pl.col("arrival_ts"),
        pl.col("departure_ts"),
        pl.col("energy_requested_kwh")
        .fill_null(
            pl.when(pl.col("energy_delivered_kwh").is_not_null())
            .then(pl.col("energy_delivered_kwh"))
            .otherwise(pl.lit(DEFAULT_ENERGY_KWH))
        )
        .alias("energy_target_kwh"),
        pl.col("energy_delivered_kwh").fill_null(0.0).alias("energy_baseline_kwh"),
        pl.col("max_charge_rate_kw")
        .fill_null(pl.lit(DEFAULT_MAX_RATE_KW))
        .alias("max_rate_kw"),
        pl.col("session_duration_minutes"),
    ])


def get_active_sessions_at(
    session_frame: pl.DataFrame,
    timestamp: datetime,
) -> pl.DataFrame:
    """Filter sessions that are active (plugged in) at the given timestamp."""
    return session_frame.filter(
        (pl.col("arrival_ts") <= timestamp)
        & (pl.col("departure_ts") > timestamp)
    )


def build_fleet_snapshot(
    session_frame: pl.DataFrame,
    timestamp: datetime,
    step_index: int,
    delivered_energy: dict[str, float],
) -> FleetSnapshot:
    """Build an immutable fleet snapshot at a given timestep.

    Args:
        session_frame: Prepared session DataFrame.
        timestamp: Current simulation time.
        step_index: Current step index.
        delivered_energy: Mapping of session_id -> energy delivered so far (kWh).
    """
    active = get_active_sessions_at(session_frame, timestamp)

    states: list[SessionState] = []
    for row in active.iter_rows(named=True):
        sid = row["session_id"]
        delivered = delivered_energy.get(sid, 0.0)
        target = row["energy_target_kwh"]
        remaining_energy = max(0.0, target - delivered)

        departure: datetime = row["departure_ts"]
        remaining_minutes = max(0.0, (departure - timestamp).total_seconds() / 60.0)

        states.append(SessionState(
            session_id=sid,
            station_id=row["station_id"],
            arrival_ts=row["arrival_ts"],
            departure_ts=departure,
            energy_requested_kwh=target,
            energy_delivered_so_far_kwh=delivered,
            max_charge_rate_kw=row["max_rate_kw"],
            remaining_energy_kwh=remaining_energy,
            remaining_minutes=remaining_minutes,
        ))

    return FleetSnapshot(
        timestamp=timestamp,
        step_index=step_index,
        active_sessions=states,
    )
