"""Tests for fleet state builder."""

from datetime import UTC, datetime

import polars as pl

from app.simulation.engine.fleet_state import (
    build_fleet_snapshot,
    build_session_frame,
    get_active_sessions_at,
)


def _make_sessions_df() -> pl.DataFrame:
    return pl.DataFrame({
        "session_id": ["s1", "s2", "s3"],
        "station_id": ["st1", "st1", "st2"],
        "arrival_ts": [
            datetime(2020, 8, 1, 8, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 9, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 10, 0, tzinfo=UTC),
        ],
        "departure_ts": [
            datetime(2020, 8, 1, 12, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 17, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 14, 0, tzinfo=UTC),
        ],
        "energy_requested_kwh": [20.0, 40.0, 30.0],
        "energy_delivered_kwh": [20.0, 40.0, 30.0],
        "max_charge_rate_kw": [6.6, 7.2, 6.6],
        "session_duration_minutes": [240, 480, 240],
    })


class TestBuildSessionFrame:
    def test_creates_expected_columns(self) -> None:
        df = _make_sessions_df()
        result = build_session_frame(df)

        assert "session_id" in result.columns
        assert "energy_target_kwh" in result.columns
        assert "max_rate_kw" in result.columns
        assert len(result) == 3

    def test_fills_missing_energy(self) -> None:
        df = pl.DataFrame({
            "session_id": ["s1"],
            "station_id": ["st1"],
            "arrival_ts": [datetime(2020, 8, 1, 8, 0, tzinfo=UTC)],
            "departure_ts": [datetime(2020, 8, 1, 12, 0, tzinfo=UTC)],
            "energy_requested_kwh": [None],
            "energy_delivered_kwh": [15.0],
            "max_charge_rate_kw": [None],
            "session_duration_minutes": [240],
        })
        result = build_session_frame(df)

        assert result["energy_target_kwh"][0] == 15.0  # Falls back to delivered
        assert result["max_rate_kw"][0] == 7.2  # Default


class TestGetActiveSessions:
    def test_filters_correctly(self) -> None:
        df = build_session_frame(_make_sessions_df())

        # At 8:30, only s1 is active
        active = get_active_sessions_at(df, datetime(2020, 8, 1, 8, 30, tzinfo=UTC))
        assert len(active) == 1
        assert active["session_id"][0] == "s1"

        # At 10:30, s2 and s3 are active (s1 departed by then or at boundary)
        active = get_active_sessions_at(df, datetime(2020, 8, 1, 10, 30, tzinfo=UTC))
        assert len(active) == 2

    def test_no_active(self) -> None:
        df = build_session_frame(_make_sessions_df())
        active = get_active_sessions_at(df, datetime(2020, 8, 1, 18, 0, tzinfo=UTC))
        assert len(active) == 0


class TestBuildFleetSnapshot:
    def test_snapshot_tracks_delivered_energy(self) -> None:
        df = build_session_frame(_make_sessions_df())
        ts = datetime(2020, 8, 1, 9, 30, tzinfo=UTC)

        snapshot = build_fleet_snapshot(df, ts, 0, {"s1": 10.0})

        s1_state = next(s for s in snapshot.active_sessions if s.session_id == "s1")
        assert s1_state.energy_delivered_so_far_kwh == 10.0
        assert s1_state.remaining_energy_kwh == 10.0  # 20 - 10

    def test_snapshot_timestamp(self) -> None:
        df = build_session_frame(_make_sessions_df())
        ts = datetime(2020, 8, 1, 9, 0, tzinfo=UTC)

        snapshot = build_fleet_snapshot(df, ts, 5, {})
        assert snapshot.timestamp == ts
        assert snapshot.step_index == 5
