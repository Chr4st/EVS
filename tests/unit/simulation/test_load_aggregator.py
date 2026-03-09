"""Tests for load aggregation."""

from datetime import UTC, datetime

import polars as pl

from app.simulation.engine.charging_policy import ChargingDecision
from app.simulation.aggregation.load_aggregator import (
    compute_fleet_load_curve,
    compute_station_load_curves,
    decisions_to_frame,
)


def _make_decisions() -> list[ChargingDecision]:
    ts1 = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
    ts2 = datetime(2020, 8, 1, 8, 5, tzinfo=UTC)
    return [
        ChargingDecision("s1", ts1, 5.0, 0.42),
        ChargingDecision("s2", ts1, 3.0, 0.25),
        ChargingDecision("s1", ts2, 5.0, 0.42),
        ChargingDecision("s2", ts2, 0.0, 0.0),
    ]


def _make_sessions_df() -> pl.DataFrame:
    return pl.DataFrame({
        "session_id": ["s1", "s2"],
        "station_id": ["st1", "st2"],
    })


class TestDecisionsToFrame:
    def test_converts(self) -> None:
        df = decisions_to_frame(_make_decisions())
        assert len(df) == 4
        assert "session_id" in df.columns
        assert "charge_rate_kw" in df.columns

    def test_empty(self) -> None:
        df = decisions_to_frame([])
        assert len(df) == 0


class TestFleetLoadCurve:
    def test_aggregates_by_timestep(self) -> None:
        df = decisions_to_frame(_make_decisions())
        fleet = compute_fleet_load_curve(df)

        assert len(fleet) == 2
        # First timestep: 5 + 3 = 8 kW
        row0 = fleet.row(0, named=True)
        assert row0["total_load_kw"] == 8.0
        assert row0["active_sessions"] == 2

        # Second timestep: 5 + 0 = 5 kW
        row1 = fleet.row(1, named=True)
        assert row1["total_load_kw"] == 5.0

    def test_empty(self) -> None:
        df = decisions_to_frame([])
        fleet = compute_fleet_load_curve(df)
        assert len(fleet) == 0


class TestStationLoadCurves:
    def test_aggregates_by_station(self) -> None:
        decisions_df = decisions_to_frame(_make_decisions())
        sessions_df = _make_sessions_df()
        station = compute_station_load_curves(decisions_df, sessions_df)

        assert len(station) > 0
        assert "station_id" in station.columns
        station_ids = station["station_id"].unique().to_list()
        assert "st1" in station_ids
        assert "st2" in station_ids
