"""Tests for baseline simulation engine."""

from datetime import UTC, datetime

import polars as pl

from app.simulation.engine.baseline_engine import SimulationConfig, run_simulation
from app.simulation.engine.charging_policy import (
    ImmediateChargePolicy,
    UniformSpreadPolicy,
)


def _make_test_sessions() -> pl.DataFrame:
    return pl.DataFrame({
        "session_id": ["s1", "s2"],
        "station_id": ["st1", "st1"],
        "arrival_ts": [
            datetime(2020, 8, 1, 8, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 9, 0, tzinfo=UTC),
        ],
        "departure_ts": [
            datetime(2020, 8, 1, 12, 0, tzinfo=UTC),
            datetime(2020, 8, 1, 13, 0, tzinfo=UTC),
        ],
        "energy_requested_kwh": [20.0, 15.0],
        "energy_delivered_kwh": [20.0, 15.0],
        "max_charge_rate_kw": [6.6, 6.6],
        "session_duration_minutes": [240, 240],
        "is_valid": [True, True],
    })


class TestRunSimulation:
    def test_immediate_charge_produces_decisions(self) -> None:
        sessions = _make_test_sessions()
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=15,
            scenario_name="test_immediate",
        )
        result = run_simulation(sessions, config)

        assert result.scenario_name == "test_immediate"
        assert result.policy_name == "immediate_charge"
        assert result.sessions_count == 2
        assert len(result.decisions) > 0
        assert len(result.snapshots) > 0

    def test_uniform_spread_produces_decisions(self) -> None:
        sessions = _make_test_sessions()
        config = SimulationConfig(
            policy=UniformSpreadPolicy(),
            interval_minutes=15,
            scenario_name="test_uniform",
        )
        result = run_simulation(sessions, config)

        assert result.policy_name == "uniform_spread"
        assert len(result.decisions) > 0

    def test_filters_invalid_sessions(self) -> None:
        sessions = _make_test_sessions().with_columns(
            pl.Series("is_valid", [True, False])
        )
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=60,
        )
        result = run_simulation(sessions, config)

        assert result.sessions_count == 1
        session_ids = {d.session_id for d in result.decisions}
        assert "s2" not in session_ids

    def test_empty_sessions(self) -> None:
        sessions = _make_test_sessions().filter(pl.lit(False))
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=15,
        )
        result = run_simulation(sessions, config)

        assert result.sessions_count == 0
        assert result.decisions == []

    def test_energy_conservation(self) -> None:
        """Total energy delivered should not exceed total requested."""
        sessions = _make_test_sessions()
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=5,
        )
        result = run_simulation(sessions, config)

        total_requested = sessions["energy_requested_kwh"].sum()
        total_delivered = sum(d.energy_delivered_kwh for d in result.decisions)

        assert total_delivered <= total_requested + 0.01  # Small float tolerance
