"""Load aggregation from simulation decisions.

Aggregates individual charging decisions into per-station and fleet-wide
load curves as Polars DataFrames.
"""

import polars as pl

from app.simulation.engine.charging_policy import ChargingDecision


def decisions_to_frame(decisions: list[ChargingDecision]) -> pl.DataFrame:
    """Convert a list of ChargingDecisions to a Polars DataFrame."""
    if not decisions:
        return pl.DataFrame({
            "session_id": [],
            "timestamp": [],
            "charge_rate_kw": [],
            "energy_delivered_kwh": [],
        })

    return pl.DataFrame({
        "session_id": [d.session_id for d in decisions],
        "timestamp": [d.timestamp for d in decisions],
        "charge_rate_kw": [d.charge_rate_kw for d in decisions],
        "energy_delivered_kwh": [d.energy_delivered_kwh for d in decisions],
    })


def compute_fleet_load_curve(decisions_df: pl.DataFrame) -> pl.DataFrame:
    """Compute total fleet load at each timestep.

    Returns DataFrame with columns: [timestamp, total_load_kw, total_energy_kwh, active_sessions].
    """
    if decisions_df.is_empty():
        return pl.DataFrame({
            "timestamp": [],
            "total_load_kw": [],
            "total_energy_kwh": [],
            "active_sessions": [],
        })

    return (
        decisions_df
        .group_by("timestamp")
        .agg([
            pl.col("charge_rate_kw").sum().alias("total_load_kw"),
            pl.col("energy_delivered_kwh").sum().alias("total_energy_kwh"),
            pl.col("session_id").count().alias("active_sessions"),
        ])
        .sort("timestamp")
    )


def compute_station_load_curves(
    decisions_df: pl.DataFrame,
    sessions_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute per-station load at each timestep.

    Joins decisions with session station_id to produce station-level aggregation.

    Returns DataFrame with columns: [timestamp, station_id, load_kw, energy_kwh].
    """
    if decisions_df.is_empty():
        return pl.DataFrame({
            "timestamp": [],
            "station_id": [],
            "load_kw": [],
            "energy_kwh": [],
        })

    station_map = sessions_df.select(["session_id", "station_id"]).unique()

    enriched = decisions_df.join(station_map, on="session_id", how="left")

    return (
        enriched
        .group_by(["timestamp", "station_id"])
        .agg([
            pl.col("charge_rate_kw").sum().alias("load_kw"),
            pl.col("energy_delivered_kwh").sum().alias("energy_kwh"),
        ])
        .sort(["station_id", "timestamp"])
    )
