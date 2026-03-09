"""Tests for time index builder."""

from datetime import UTC, datetime

import polars as pl
import pytest

from app.simulation.engine.time_index import build_time_index, infer_time_bounds


class TestBuildTimeIndex:
    def test_basic_index(self) -> None:
        start = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        end = datetime(2020, 8, 1, 9, 0, tzinfo=UTC)
        result = build_time_index(start, end, interval_minutes=15)

        assert len(result) == 5  # 8:00, 8:15, 8:30, 8:45, 9:00
        assert "timestamp" in result.columns
        assert "step_index" in result.columns
        assert "interval_hours" in result.columns
        assert result["step_index"].to_list() == [0, 1, 2, 3, 4]
        assert result["interval_hours"][0] == 0.25

    def test_five_minute_resolution(self) -> None:
        start = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        end = datetime(2020, 8, 1, 8, 30, tzinfo=UTC)
        result = build_time_index(start, end, interval_minutes=5)

        assert len(result) == 7  # 0, 5, 10, 15, 20, 25, 30

    def test_invalid_range_raises(self) -> None:
        ts = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="end_ts must be after"):
            build_time_index(ts, ts, interval_minutes=5)

    def test_invalid_interval_raises(self) -> None:
        start = datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        end = datetime(2020, 8, 1, 9, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="interval_minutes must be positive"):
            build_time_index(start, end, interval_minutes=0)

    def test_naive_timestamps_get_utc(self) -> None:
        start = datetime(2020, 8, 1, 8, 0)
        end = datetime(2020, 8, 1, 9, 0)
        result = build_time_index(start, end, interval_minutes=30)

        assert len(result) == 3


class TestInferTimeBounds:
    def test_basic_bounds(self) -> None:
        df = pl.DataFrame({
            "arrival_ts": [
                datetime(2020, 8, 1, 8, 0, tzinfo=UTC),
                datetime(2020, 8, 1, 9, 0, tzinfo=UTC),
            ],
            "departure_ts": [
                datetime(2020, 8, 1, 12, 0, tzinfo=UTC),
                datetime(2020, 8, 1, 17, 0, tzinfo=UTC),
            ],
        })
        start, end = infer_time_bounds(df)

        assert start == datetime(2020, 8, 1, 8, 0, tzinfo=UTC)
        assert end == datetime(2020, 8, 1, 17, 0, tzinfo=UTC)
