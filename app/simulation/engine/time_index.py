"""Time index builder for simulation.

Creates a discrete time grid at configurable resolution (e.g. 5-minute intervals)
over which the charging simulation steps.
"""

from datetime import UTC, datetime, timedelta

import polars as pl


def build_time_index(
    start_ts: datetime,
    end_ts: datetime,
    interval_minutes: int = 5,
) -> pl.DataFrame:
    """Build a time index DataFrame with evenly spaced UTC timestamps.

    Args:
        start_ts: Simulation start time (inclusive).
        end_ts: Simulation end time (inclusive).
        interval_minutes: Resolution in minutes.

    Returns:
        DataFrame with columns: [timestamp, step_index, interval_hours].
    """
    if end_ts <= start_ts:
        raise ValueError("end_ts must be after start_ts")
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")

    # Ensure UTC
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=UTC)
    if end_ts.tzinfo is None:
        end_ts = end_ts.replace(tzinfo=UTC)

    timestamps: list[datetime] = []
    current = start_ts
    while current <= end_ts:
        timestamps.append(current)
        current += timedelta(minutes=interval_minutes)

    interval_hours = interval_minutes / 60.0

    return pl.DataFrame({
        "timestamp": timestamps,
        "step_index": list(range(len(timestamps))),
        "interval_hours": [interval_hours] * len(timestamps),
    })


def infer_time_bounds(sessions: pl.DataFrame) -> tuple[datetime, datetime]:
    """Infer simulation time bounds from session arrival/departure times.

    Returns (earliest_arrival, latest_departure) as UTC datetimes.
    """
    arrival_min = sessions["arrival_ts"].min()
    departure_max = sessions["departure_ts"].max()

    if arrival_min is None or departure_max is None:
        raise ValueError("Cannot infer time bounds: no valid sessions")

    return arrival_min, departure_max
