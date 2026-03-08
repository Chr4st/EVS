"""Tests for ACN loader normalization."""

from datetime import datetime
from pathlib import Path

from app.ingestion.loaders.acn_loader import AcnLoader, normalize_acn_record


class TestNormalizeAcnRecord:
    def test_valid_record(self) -> None:
        record = {
            "_id": "abc123",
            "connectionTime": "2020-08-01T08:30:00",
            "disconnectTime": "2020-08-01T17:00:00",
            "doneChargingTime": "2020-08-01T12:30:00",
            "kWhDelivered": 30.5,
            "kWhRequested": 35.0,
            "userID": "user_001",
            "stationID": "CA-001",
            "spaceID": "space_A1",
            "timezone": "America/Los_Angeles",
            "maxRate": 6.6,
        }
        result = normalize_acn_record(record)

        assert result["session_id"] == "acn-abc123"
        assert result["source_dataset"] == "acn"
        assert result["source_record_id"] == "abc123"
        assert result["station_id"] == "CA-001"
        assert result["port_id"] == "space_A1"
        assert result["vehicle_id"] == "user_001"
        assert isinstance(result["arrival_ts"], datetime)
        assert isinstance(result["departure_ts"], datetime)
        assert result["arrival_ts"].tzinfo is not None
        assert result["energy_delivered_kwh"] == 30.5
        assert result["energy_requested_kwh"] == 35.0
        assert result["max_charge_rate_kw"] == 6.6
        assert result["session_duration_minutes"] > 0
        assert result["charging_duration_minutes"] is not None
        assert result["raw_payload"] == record

    def test_missing_timestamps(self) -> None:
        record = {
            "_id": "xyz",
            "connectionTime": None,
            "disconnectTime": "2020-08-02T15:00:00",
            "timezone": "America/Los_Angeles",
        }
        result = normalize_acn_record(record)

        assert result["arrival_ts"] is None
        assert result["session_duration_minutes"] == 0

    def test_zero_duration_session(self) -> None:
        record = {
            "_id": "zero",
            "connectionTime": "2020-08-02T10:00:00",
            "disconnectTime": "2020-08-02T10:00:00",
            "timezone": "America/Los_Angeles",
        }
        result = normalize_acn_record(record)

        assert result["session_duration_minutes"] == 0

    def test_preserves_raw_payload(self) -> None:
        record = {"_id": "raw_test", "extra_field": "should_be_preserved"}
        result = normalize_acn_record(record)

        assert result["raw_payload"]["extra_field"] == "should_be_preserved"

    def test_invalid_kwh(self) -> None:
        record = {
            "_id": "bad_kwh",
            "connectionTime": "2020-08-01T08:00:00",
            "disconnectTime": "2020-08-01T09:00:00",
            "kWhDelivered": "not_a_number",
            "timezone": "UTC",
        }
        result = normalize_acn_record(record)

        assert result["energy_delivered_kwh"] is None


class TestAcnLoader:
    def test_load_and_normalize(self, acn_sample_path: Path) -> None:
        loader = AcnLoader()
        assert loader.dataset_name == "acn"

        lf = loader.load_and_normalize(str(acn_sample_path))
        df = lf.collect()

        assert len(df) == 4
        assert "session_id" in df.columns
        assert "source_dataset" in df.columns
        assert all(v == "acn" for v in df["source_dataset"].to_list())

    def test_iter_batches(self, acn_sample_path: Path) -> None:
        loader = AcnLoader()
        batches = list(loader.iter_batches(str(acn_sample_path), batch_size=2))

        assert len(batches) == 2
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2
