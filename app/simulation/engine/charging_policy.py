"""Baseline charging policies.

Each policy is a pure function that takes a SessionState and simulation parameters,
and returns a ChargingDecision (the charging rate in kW for this timestep).
"""

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from app.simulation.engine.fleet_state import SessionState


@dataclass(frozen=True)
class ChargingDecision:
    """Immutable charging rate decision for one session at one timestep."""

    session_id: str
    timestamp: datetime
    charge_rate_kw: float
    energy_delivered_kwh: float  # Energy delivered in this interval


class ChargingPolicy(ABC):
    """Abstract base for charging policies."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Policy identifier."""

    @abstractmethod
    def decide(
        self,
        session: SessionState,
        timestamp: datetime,
        interval_hours: float,
    ) -> ChargingDecision:
        """Decide charging rate for a session at a given timestep."""


class ImmediateChargePolicy(ChargingPolicy):
    """Charge at max rate immediately upon arrival until energy target met."""

    @property
    def name(self) -> str:
        return "immediate_charge"

    def decide(
        self,
        session: SessionState,
        timestamp: datetime,
        interval_hours: float,
    ) -> ChargingDecision:
        if session.remaining_energy_kwh <= 0:
            return ChargingDecision(
                session_id=session.session_id,
                timestamp=timestamp,
                charge_rate_kw=0.0,
                energy_delivered_kwh=0.0,
            )

        max_energy_this_step = session.max_charge_rate_kw * interval_hours
        energy = min(session.remaining_energy_kwh, max_energy_this_step)
        rate = energy / interval_hours if interval_hours > 0 else 0.0

        return ChargingDecision(
            session_id=session.session_id,
            timestamp=timestamp,
            charge_rate_kw=rate,
            energy_delivered_kwh=energy,
        )


class UniformSpreadPolicy(ChargingPolicy):
    """Spread energy demand evenly across the entire charging window."""

    @property
    def name(self) -> str:
        return "uniform_spread"

    def decide(
        self,
        session: SessionState,
        timestamp: datetime,
        interval_hours: float,
    ) -> ChargingDecision:
        if session.remaining_energy_kwh <= 0 or session.remaining_minutes <= 0:
            return ChargingDecision(
                session_id=session.session_id,
                timestamp=timestamp,
                charge_rate_kw=0.0,
                energy_delivered_kwh=0.0,
            )

        remaining_hours = session.remaining_minutes / 60.0
        uniform_rate = session.remaining_energy_kwh / remaining_hours
        capped_rate = min(uniform_rate, session.max_charge_rate_kw)
        energy = min(capped_rate * interval_hours, session.remaining_energy_kwh)

        return ChargingDecision(
            session_id=session.session_id,
            timestamp=timestamp,
            charge_rate_kw=capped_rate,
            energy_delivered_kwh=energy,
        )


class RandomizedDelayPolicy(ChargingPolicy):
    """Delay charging start by a random fraction of the slack window, then charge at max rate.

    The delay fraction is computed once per session and cached.
    """

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)
        self._delay_fractions: dict[str, float] = {}

    @property
    def name(self) -> str:
        return "randomized_delay"

    def _get_delay_fraction(self, session_id: str) -> float:
        if session_id not in self._delay_fractions:
            self._delay_fractions[session_id] = self._rng.random()
        return self._delay_fractions[session_id]

    def decide(
        self,
        session: SessionState,
        timestamp: datetime,
        interval_hours: float,
    ) -> ChargingDecision:
        if session.remaining_energy_kwh <= 0:
            return ChargingDecision(
                session_id=session.session_id,
                timestamp=timestamp,
                charge_rate_kw=0.0,
                energy_delivered_kwh=0.0,
            )

        # Compute how long charging actually takes at max rate
        charging_hours = session.energy_requested_kwh / session.max_charge_rate_kw
        total_window_hours = (
            (session.departure_ts - session.arrival_ts).total_seconds() / 3600.0
        )
        slack_hours = max(0.0, total_window_hours - charging_hours)

        # Delay start by a random fraction of slack
        delay_fraction = self._get_delay_fraction(session.session_id)
        delay_hours = slack_hours * delay_fraction

        elapsed_hours = (timestamp - session.arrival_ts).total_seconds() / 3600.0

        if elapsed_hours < delay_hours:
            return ChargingDecision(
                session_id=session.session_id,
                timestamp=timestamp,
                charge_rate_kw=0.0,
                energy_delivered_kwh=0.0,
            )

        max_energy = session.max_charge_rate_kw * interval_hours
        energy = min(session.remaining_energy_kwh, max_energy)
        rate = energy / interval_hours if interval_hours > 0 else 0.0

        return ChargingDecision(
            session_id=session.session_id,
            timestamp=timestamp,
            charge_rate_kw=rate,
            energy_delivered_kwh=energy,
        )


POLICY_REGISTRY: dict[str, type[ChargingPolicy]] = {
    "immediate_charge": ImmediateChargePolicy,
    "uniform_spread": UniformSpreadPolicy,
    "randomized_delay": RandomizedDelayPolicy,
}


def get_policy(name: str, **kwargs: int | None) -> ChargingPolicy:
    """Instantiate a charging policy by name."""
    cls = POLICY_REGISTRY.get(name)
    if cls is None:
        available = ", ".join(sorted(POLICY_REGISTRY.keys()))
        raise ValueError(f"Unknown policy '{name}'. Available: {available}")
    return cls(**kwargs) if kwargs else cls()


def list_policies() -> list[str]:
    """Return available policy names."""
    return sorted(POLICY_REGISTRY.keys())
