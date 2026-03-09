"""Tests for baseline metrics computation."""

from datetime import UTC, datetime

import polars as pl

from app.simulation.engine.baseline_engine import SimulationConfig, run_simulation
from app.simulation.engine.charging_policy import ImmediateChargePolicy
from app.simulation.metrics.baseline_metrics import (
    _compute_completion_rate,
    _compute_energy_per_session,
    compute_metrics,
)
from app.simulation.engine.charging_policy import ChargingDecision


def _make_sessions() -> pl.DataFrame:
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


class TestEnergyPerSession:
    def test_sums_correctly(self) -> None:
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        decisions = [
            ChargingDecision("s1", ts, 5.0, 2.0),
            ChargingDecision("s1", ts, 5.0, 3.0),
            ChargingDecision("s2", ts, 3.0, 1.5),
        ]
        result = _compute_energy_per_session(decisions)
        assert result["s1"] == 5.0
        assert result["s2"] == 1.5


class TestCompletionRate:
    def test_all_completed(self) -> None:
        delivered = {"s1": 20.0, "s2": 15.0}
        targets = {"s1": 20.0, "s2": 15.0}
        completed, total, rate = _compute_completion_rate(delivered, targets)
        assert completed == 2
        assert rate == 1.0

    def test_partial_completion(self) -> None:
        delivered = {"s1": 20.0, "s2": 5.0}
        targets = {"s1": 20.0, "s2": 15.0}
        completed, total, rate = _compute_completion_rate(delivered, targets)
        assert completed == 1
        assert rate == 0.5

    def test_empty(self) -> None:
        _, _, rate = _compute_completion_rate({}, {})
        assert rate == 0.0


class TestComputeMetrics:
    def test_full_simulation_metrics(self) -> None:
        sessions = _make_sessions()
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=15,
        )
        result = run_simulation(sessions, config)
        metrics = compute_metrics(result, sessions)

        assert metrics.peak_load_kw > 0
        assert metrics.total_energy_kwh > 0
        assert 0 <= metrics.completion_rate <= 1.0
        assert 0 <= metrics.load_factor <= 1.0
        assert metrics.sessions_total == 2

    def test_empty_simulation(self) -> None:
        sessions = _make_sessions().filter(pl.lit(False))
        config = SimulationConfig(
            policy=ImmediateChargePolicy(),
            interval_minutes=15,
        )
        result = run_simulation(sessions, config)
        metrics = compute_metrics(result, sessions)

        assert metrics.peak_load_kw == 0.0
        assert metrics.total_energy_kwh == 0.0
        assert metrics.sessions_total == 0
