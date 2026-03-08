"""Loader for ACN (Adaptive Charging Network) dataset from Caltech.

Supports JSON files exported from the ACN-Data API (https://ev.caltech.edu/api/v1/sessions/).
Each record typically contains:
  _id, connectionTime, disconnectTime, doneChargingTime, kWhDelivered,
  userID, stationID, spaceID, siteID, clusterID, timezone, etc.
"""

import json
from datetime import UTC, datetime

import polars as pl

from app.ingestion.interfaces.session_loader import SessionLoader

DATASET_NAME = "acn"


def _parse_acn_timestamp(ts_str: str | None, tz: str | None) -> datetime | None:
    """Parse ACN timestamp strings (ISO-like format) to UTC datetime."""
    if ts_str is None or ts_str == "":
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            import zoneinfo

            if tz:
                tzinfo = zoneinfo.ZoneInfo(tz)
                dt = dt.replace(tzinfo=tzinfo)
            else:
                dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except (ValueError, KeyError):
        return None


def _compute_duration_minutes(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    delta = (end - start).total_seconds()
    return max(0, int(delta / 60))


def _build_session_id(record_id: str) -> str:
    return f"acn-{record_id}"


def normalize_acn_record(record: dict) -> dict:
    """Normalize a single ACN JSON record to canonical schema. Pure function."""
    tz = record.get("timezone", "America/Los_Angeles")
    record_id = str(record.get("_id", ""))

    arrival = _parse_acn_timestamp(record.get("connectionTime"), tz)
    departure = _parse_acn_timestamp(record.get("disconnectTime"), tz)
    done_charging = _parse_acn_timestamp(record.get("doneChargingTime"), tz)

    session_duration = _compute_duration_minutes(arrival, departure)
    charging_duration = _compute_duration_minutes(arrival, done_charging)

    kwh_delivered = record.get("kWhDelivered")
    if kwh_delivered is not None:
        try:
            kwh_delivered = float(kwh_delivered)
        except (ValueError, TypeError):
            kwh_delivered = None

    kwh_requested = record.get("kWhRequested")
    if kwh_requested is not None:
        try:
            kwh_requested = float(kwh_requested)
        except (ValueError, TypeError):
            kwh_requested = None

    max_rate = record.get("maxRate")
    if max_rate is not None:
        try:
            max_rate = float(max_rate)
        except (ValueError, TypeError):
            max_rate = None

    return {
        "session_id": _build_session_id(record_id),
        "source_dataset": DATASET_NAME,
        "source_record_id": record_id,
        "station_id": record.get("stationID"),
        "port_id": record.get("spaceID"),
        "vehicle_id": record.get("userID"),
        "arrival_ts": arrival,
        "departure_ts": departure,
        "energy_requested_kwh": kwh_requested,
        "energy_delivered_kwh": kwh_delivered,
        "max_charge_rate_kw": max_rate,
        "average_charge_rate_kw": None,
        "session_duration_minutes": session_duration or 0,
        "charging_duration_minutes": charging_duration,
        "is_valid": True,  # Will be set by validator
        "validation_errors": [],
        "raw_payload": record,
    }


class AcnLoader(SessionLoader):
    @property
    def dataset_name(self) -> str:
        return DATASET_NAME

    def load_raw(self, source_path: str) -> pl.LazyFrame:
        """Load ACN JSON file. Supports JSON arrays or newline-delimited JSON."""
        with open(source_path) as f:
            content = f.read().strip()

        if content.startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(line) for line in content.splitlines() if line.strip()]

        if not records:
            return pl.LazyFrame()

        return pl.LazyFrame(records)

    def normalize(self, raw: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize by collecting and mapping each row through the pure normalize function."""
        df = raw.collect()
        if df.is_empty():
            return pl.LazyFrame()

        records = df.to_dicts()
        normalized = [normalize_acn_record(r) for r in records]
        return pl.LazyFrame(normalized)
