"""Session validator. Pure functions that check canonical schema records for data quality issues.

Invalid records are NOT discarded — they are marked with is_valid=False and
validation_errors populated for auditability.
"""

from datetime import datetime
from typing import Any


def _check_required_timestamp(
    field_name: str, value: Any
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if value is None:
        errors.append({"field": field_name, "error": "missing required timestamp"})
    elif not isinstance(value, datetime):
        errors.append({"field": field_name, "error": f"invalid type: {type(value).__name__}"})
    return errors


def _check_non_negative(
    field_name: str, value: Any
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if value is not None:
        try:
            if float(value) < 0:
                errors.append({"field": field_name, "error": "must be non-negative"})
        except (ValueError, TypeError):
            errors.append({"field": field_name, "error": f"invalid numeric value: {value}"})
    return errors


def validate_session(record: dict[str, Any]) -> dict[str, Any]:
    """Validate a single normalized session record. Returns a new record with
    is_valid and validation_errors updated. Does NOT mutate the input."""
    errors: list[dict[str, str]] = []

    # Required timestamps
    arrival = record.get("arrival_ts")
    departure = record.get("departure_ts")

    errors.extend(_check_required_timestamp("arrival_ts", arrival))
    errors.extend(_check_required_timestamp("departure_ts", departure))

    # Temporal ordering
    if (
        isinstance(arrival, datetime)
        and isinstance(departure, datetime)
        and departure <= arrival
    ):
        errors.append({
            "field": "departure_ts",
            "error": "departure must be after arrival",
        })

    # Session duration
    duration = record.get("session_duration_minutes")
    if duration is None or duration <= 0:
        errors.append({
            "field": "session_duration_minutes",
            "error": "must be positive",
        })

    # Non-negative energy/rate fields
    errors.extend(_check_non_negative("energy_requested_kwh", record.get("energy_requested_kwh")))
    errors.extend(_check_non_negative("energy_delivered_kwh", record.get("energy_delivered_kwh")))
    errors.extend(_check_non_negative("max_charge_rate_kw", record.get("max_charge_rate_kw")))
    errors.extend(
        _check_non_negative("average_charge_rate_kw", record.get("average_charge_rate_kw"))
    )
    errors.extend(
        _check_non_negative("charging_duration_minutes", record.get("charging_duration_minutes"))
    )

    is_valid = len(errors) == 0

    return {
        **record,
        "is_valid": is_valid,
        "validation_errors": errors,
    }


def validate_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate a list of records. Returns new list with validation applied."""
    return [validate_session(r) for r in records]
