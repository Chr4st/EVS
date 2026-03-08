"""Tests for time utilities."""

from datetime import UTC, datetime, timezone, timedelta

from app.utils.time import ensure_utc, utc_now


class TestUtcNow:
    def test_returns_utc(self) -> None:
        now = utc_now()
        assert now.tzinfo == UTC


class TestEnsureUtc:
    def test_naive_becomes_utc(self) -> None:
        naive = datetime(2020, 1, 1, 12, 0, 0)
        result = ensure_utc(naive)
        assert result.tzinfo == UTC

    def test_utc_stays_utc(self) -> None:
        aware = datetime(2020, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = ensure_utc(aware)
        assert result.tzinfo == UTC
        assert result == aware

    def test_other_tz_converts(self) -> None:
        est = timezone(timedelta(hours=-5))
        aware = datetime(2020, 1, 1, 12, 0, 0, tzinfo=est)
        result = ensure_utc(aware)
        assert result.tzinfo == UTC
        assert result.hour == 17
