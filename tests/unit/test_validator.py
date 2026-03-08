"""Tests for session validator."""

from datetime import UTC, datetime

from app.ingestion.validate.session_validator import validate_batch, validate_session


def _make_valid_record() -> dict:
    return {
        "session_id": "test-001",
        "source_dataset": "test",
        "source_record_id": "001",
        "station_id": "station_1",
        "port_id": "port_1",
        "vehicle_id": "vehicle_1",
        "arrival_ts": datetime(2020, 8, 1, 8, 0, tzinfo=UTC),
        "departure_ts": datetime(2020, 8, 1, 17, 0, tzinfo=UTC),
        "energy_requested_kwh": 35.0,
        "energy_delivered_kwh": 30.5,
        "max_charge_rate_kw": 6.6,
        "average_charge_rate_kw": 3.4,
        "session_duration_minutes": 540,
        "charging_duration_minutes": 240,
        "is_valid": True,
        "validation_errors": [],
        "raw_payload": {},
    }


class TestValidateSession:
    def test_valid_record_passes(self) -> None:
        record = _make_valid_record()
        result = validate_session(record)

        assert result["is_valid"] is True
        assert result["validation_errors"] == []

    def test_does_not_mutate_input(self) -> None:
        record = _make_valid_record()
        original_errors = record["validation_errors"]
        validate_session(record)

        assert record["validation_errors"] is original_errors

    def test_missing_arrival(self) -> None:
        record = _make_valid_record()
        record["arrival_ts"] = None
        result = validate_session(record)

        assert result["is_valid"] is False
        error_fields = [e["field"] for e in result["validation_errors"]]
        assert "arrival_ts" in error_fields

    def test_missing_departure(self) -> None:
        record = _make_valid_record()
        record["departure_ts"] = None
        result = validate_session(record)

        assert result["is_valid"] is False
        error_fields = [e["field"] for e in result["validation_errors"]]
        assert "departure_ts" in error_fields

    def test_departure_before_arrival(self) -> None:
        record = _make_valid_record()
        record["departure_ts"] = datetime(2020, 8, 1, 7, 0, tzinfo=UTC)
        result = validate_session(record)

        assert result["is_valid"] is False
        errors = result["validation_errors"]
        assert any("departure must be after arrival" in e["error"] for e in errors)

    def test_departure_equals_arrival(self) -> None:
        record = _make_valid_record()
        record["departure_ts"] = record["arrival_ts"]
        result = validate_session(record)

        assert result["is_valid"] is False

    def test_zero_duration(self) -> None:
        record = _make_valid_record()
        record["session_duration_minutes"] = 0
        result = validate_session(record)

        assert result["is_valid"] is False
        error_fields = [e["field"] for e in result["validation_errors"]]
        assert "session_duration_minutes" in error_fields

    def test_negative_energy(self) -> None:
        record = _make_valid_record()
        record["energy_delivered_kwh"] = -5.0
        result = validate_session(record)

        assert result["is_valid"] is False
        error_fields = [e["field"] for e in result["validation_errors"]]
        assert "energy_delivered_kwh" in error_fields

    def test_negative_charge_rate(self) -> None:
        record = _make_valid_record()
        record["max_charge_rate_kw"] = -1.0
        result = validate_session(record)

        assert result["is_valid"] is False

    def test_none_optional_fields_ok(self) -> None:
        record = _make_valid_record()
        record["energy_requested_kwh"] = None
        record["energy_delivered_kwh"] = None
        record["max_charge_rate_kw"] = None
        record["average_charge_rate_kw"] = None
        record["charging_duration_minutes"] = None
        result = validate_session(record)

        assert result["is_valid"] is True

    def test_multiple_errors(self) -> None:
        record = _make_valid_record()
        record["arrival_ts"] = None
        record["departure_ts"] = None
        record["session_duration_minutes"] = -1
        result = validate_session(record)

        assert result["is_valid"] is False
        assert len(result["validation_errors"]) >= 3


class TestValidateBatch:
    def test_batch_validation(self) -> None:
        valid = _make_valid_record()
        invalid = _make_valid_record()
        invalid["arrival_ts"] = None

        results = validate_batch([valid, invalid])

        assert len(results) == 2
        assert results[0]["is_valid"] is True
        assert results[1]["is_valid"] is False
