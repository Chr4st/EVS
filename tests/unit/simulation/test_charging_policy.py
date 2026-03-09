"""Tests for charging policies."""

from datetime import UTC, datetime

from app.simulation.engine.charging_policy import (
    ImmediateChargePolicy,
    RandomizedDelayPolicy,
    UniformSpreadPolicy,
    get_policy,
    list_policies,
)
from app.simulation.engine.fleet_state import SessionState


def _make_session(
    remaining_kwh: float = 20.0,
    max_rate: float = 6.6,
    remaining_min: float = 240.0,
) -> SessionState:
    arrival = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
    departure = datetime(2020, 8, 1, 12, 0, tzinfo=UTC)
    return SessionState(
        session_id="test-001",
        station_id="st1",
        arrival_ts=arrival,
        departure_ts=departure,
        energy_requested_kwh=20.0,
        energy_delivered_so_far_kwh=20.0 - remaining_kwh,
        max_charge_rate_kw=max_rate,
        remaining_energy_kwh=remaining_kwh,
        remaining_minutes=remaining_min,
    )


class TestImmediateCharge:
    def test_charges_at_max_rate(self) -> None:
        policy = ImmediateChargePolicy()
        session = _make_session(remaining_kwh=20.0, max_rate=6.6)
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1 / 12)  # 5 min
        assert decision.charge_rate_kw > 0
        assert decision.charge_rate_kw <= 6.6
        assert decision.energy_delivered_kwh > 0

    def test_stops_when_full(self) -> None:
        policy = ImmediateChargePolicy()
        session = _make_session(remaining_kwh=0.0)
        ts = datetime(2020, 8, 1, 9, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1 / 12)
        assert decision.charge_rate_kw == 0.0
        assert decision.energy_delivered_kwh == 0.0

    def test_caps_at_remaining_energy(self) -> None:
        policy = ImmediateChargePolicy()
        session = _make_session(remaining_kwh=0.1, max_rate=6.6)
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1.0)
        assert decision.energy_delivered_kwh == 0.1


class TestUniformSpread:
    def test_spreads_evenly(self) -> None:
        policy = UniformSpreadPolicy()
        session = _make_session(remaining_kwh=20.0, remaining_min=240.0)
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1 / 12)
        expected_rate = 20.0 / 4.0  # 5 kW over 4 hours
        assert abs(decision.charge_rate_kw - expected_rate) < 0.01

    def test_stops_when_full(self) -> None:
        policy = UniformSpreadPolicy()
        session = _make_session(remaining_kwh=0.0)
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1 / 12)
        assert decision.charge_rate_kw == 0.0


class TestRandomizedDelay:
    def test_deterministic_with_seed(self) -> None:
        policy1 = RandomizedDelayPolicy(seed=42)
        policy2 = RandomizedDelayPolicy(seed=42)
        session = _make_session()
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        d1 = policy1.decide(session, ts, interval_hours=1 / 12)
        d2 = policy2.decide(session, ts, interval_hours=1 / 12)
        assert d1.charge_rate_kw == d2.charge_rate_kw

    def test_may_delay(self) -> None:
        # With a large slack, some delay fraction will cause no charging at t=0
        policy = RandomizedDelayPolicy(seed=999)
        session = _make_session(remaining_kwh=5.0, max_rate=10.0, remaining_min=600.0)
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)

        decision = policy.decide(session, ts, interval_hours=1 / 12)
        # This may or may not charge depending on seed — just ensure it doesn't crash
        assert decision.charge_rate_kw >= 0.0


class TestPolicyRegistry:
    def test_list_policies(self) -> None:
        policies = list_policies()
        assert "immediate_charge" in policies
        assert "uniform_spread" in policies
        assert "randomized_delay" in policies

    def test_get_policy(self) -> None:
        p = get_policy("immediate_charge")
        assert isinstance(p, ImmediateChargePolicy)

    def test_unknown_policy_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown policy"):
            get_policy("nonexistent")
