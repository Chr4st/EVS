"""Tests for UrbanEV loader normalization."""

from datetime import datetime
from pathlib import Path

from app.ingestion.loaders.urbanev_loader import (
    UrbanevLoader,
    _parse_charging_time_minutes,
    _parse_datetime,
    normalize_urbanev_record,
)


class TestParseChargingTime:
    def test_hh_mm_ss(self) -> None:
        assert _parse_charging_time_minutes("3:30:00") == 210

    def test_zero(self) -> None:
        assert _parse_charging_time_minutes("0:00:00") == 0

    def test_with_seconds(self) -> None:
        assert _parse_charging_time_minutes("1:00:30") == 61

    def test_none(self) -> None:
        assert _parse_charging_time_minutes(None) is None

    def test_empty(self) -> None:
        assert _parse_charging_time_minutes("") is None

    def test_invalid(self) -> None:
        assert _parse_charging_time_minutes("invalid") is None


class TestParseDatetime:
    def test_iso_format(self) -> None:
        result = _parse_datetime("2020-08-01T08:00:00")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_us_format(self) -> None:
        result = _parse_datetime("08/01/2020 08:00")
        assert result is not None

    def test_none(self) -> None:
        assert _parse_datetime(None) is None

    def test_empty(self) -> None:
        assert _parse_datetime("") is None


class TestNormalizeUrbanevRecord:
    def test_valid_record(self) -> None:
        record = {
            "Station_Name": "Downtown Station",
            "Start_Date___Time": "2020-08-01T08:00:00",
            "End_Date___Time": "2020-08-01T12:00:00",
            "Total_kWh": 25.5,
            "Charging_Time__hh_mm_ss_": "3:30:00",
            "EVSE_ID": "EVSE001",
            "Port_Number": "1",
        }
        col_map = {
            "arrival": "Start_Date___Time",
            "departure": "End_Date___Time",
            "energy": "Total_kWh",
            "charging_time": "Charging_Time__hh_mm_ss_",
            "station": "Station_Name",
            "port": "Port_Number",
        }

        result = normalize_urbanev_record(record, 0, col_map)

        assert result["session_id"] == "urbanev-0"
        assert result["source_dataset"] == "urbanev"
        assert result["station_id"] == "Downtown Station"
        assert result["port_id"] == "1"
        assert result["energy_delivered_kwh"] == 25.5
        assert result["charging_duration_minutes"] == 210
        assert result["session_duration_minutes"] == 240
        assert isinstance(result["arrival_ts"], datetime)
        assert isinstance(result["departure_ts"], datetime)
        assert result["raw_payload"] == record


class TestUrbanevLoader:
    def test_load_and_normalize(self, urbanev_sample_path: Path) -> None:
        loader = UrbanevLoader()
        assert loader.dataset_name == "urbanev"

        lf = loader.load_and_normalize(str(urbanev_sample_path))
        df = lf.collect()

        assert len(df) == 5
        assert "session_id" in df.columns
        assert all(v == "urbanev" for v in df["source_dataset"].to_list())

    def test_iter_batches(self, urbanev_sample_path: Path) -> None:
        loader = UrbanevLoader()
        batches = list(loader.iter_batches(str(urbanev_sample_path), batch_size=3))

        assert len(batches) == 2
        assert len(batches[0]) == 3
        assert len(batches[1]) == 2
