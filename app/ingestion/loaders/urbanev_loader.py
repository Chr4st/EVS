"""Loader for UrbanEV dataset.

Source: https://github.com/IntelligentSystemsLab/UrbanEV
CSV files with columns like:
  Start_Date___Time, End_Date___Time, Total_kWh, Charging_Time__hh_mm_ss_,
  EVSE_ID, Port_Number, Station_Name, Address, City, State_Province, Country, etc.
"""

from datetime import UTC, datetime

import polars as pl

from app.ingestion.interfaces.session_loader import SessionLoader

DATASET_NAME = "urbanev"

# Common column name variations in UrbanEV CSVs
ARRIVAL_COLS = ["Start_Date___Time", "Start Date / Time", "start_time", "Start"]
DEPARTURE_COLS = ["End_Date___Time", "End Date / Time", "end_time", "End"]
ENERGY_COLS = ["Total_kWh", "Total kWh", "Energy (kWh)", "total_kwh"]
CHARGING_TIME_COLS = ["Charging_Time__hh_mm_ss_", "Charging Time (hh:mm:ss)", "charging_time"]
STATION_COLS = ["Station_Name", "Station Name", "station_name", "EVSE_ID"]
PORT_COLS = ["Port_Number", "Port Number", "port_number", "Port"]
ADDRESS_COLS = ["Address", "address"]


def _find_column(df: pl.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name from candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _parse_charging_time_minutes(val: str | None) -> int | None:
    """Parse 'hh:mm:ss' or 'h:mm:ss' format to minutes."""
    if val is None or val == "":
        return None
    try:
        parts = val.strip().split(":")
        if len(parts) == 3:
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
            return hours * 60 + minutes + (1 if seconds > 0 else 0)
        if len(parts) == 2:
            hours, minutes = int(parts[0]), int(parts[1])
            return hours * 60 + minutes
    except (ValueError, IndexError):
        pass
    return None


def _parse_datetime(val: str | None) -> datetime | None:
    """Parse various datetime string formats to UTC datetime."""
    if val is None or val == "":
        return None
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        pass
    # Try common formats
    for fmt in [
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ]:
        try:
            dt = datetime.strptime(val, fmt)
            return dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _compute_duration_minutes(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    delta = (end - start).total_seconds()
    return max(0, int(delta / 60))


def normalize_urbanev_record(record: dict, row_index: int, col_map: dict[str, str]) -> dict:
    """Normalize a single UrbanEV record to canonical schema. Pure function."""
    arrival_col = col_map.get("arrival")
    departure_col = col_map.get("departure")
    energy_col = col_map.get("energy")
    charging_time_col = col_map.get("charging_time")
    station_col = col_map.get("station")
    port_col = col_map.get("port")

    arrival_raw = str(record.get(arrival_col, "")) if arrival_col else None
    departure_raw = str(record.get(departure_col, "")) if departure_col else None

    arrival = _parse_datetime(arrival_raw)
    departure = _parse_datetime(departure_raw)

    session_duration = _compute_duration_minutes(arrival, departure)

    energy = None
    if energy_col and record.get(energy_col) is not None:
        try:
            energy = float(record[energy_col])
        except (ValueError, TypeError):
            energy = None

    charging_minutes = None
    if charging_time_col and record.get(charging_time_col) is not None:
        charging_minutes = _parse_charging_time_minutes(str(record[charging_time_col]))

    station_id = None
    if station_col:
        station_id = str(record.get(station_col, "")) or None

    port_id = None
    if port_col:
        port_id = str(record.get(port_col, "")) or None

    record_id = str(row_index)
    session_id = f"urbanev-{record_id}"

    return {
        "session_id": session_id,
        "source_dataset": DATASET_NAME,
        "source_record_id": record_id,
        "station_id": station_id,
        "port_id": port_id,
        "vehicle_id": None,
        "arrival_ts": arrival,
        "departure_ts": departure,
        "energy_requested_kwh": None,
        "energy_delivered_kwh": energy,
        "max_charge_rate_kw": None,
        "average_charge_rate_kw": None,
        "session_duration_minutes": session_duration or 0,
        "charging_duration_minutes": charging_minutes,
        "is_valid": True,  # Will be set by validator
        "validation_errors": [],
        "raw_payload": record,
    }


class UrbanevLoader(SessionLoader):
    @property
    def dataset_name(self) -> str:
        return DATASET_NAME

    def load_raw(self, source_path: str) -> pl.LazyFrame:
        """Load UrbanEV CSV file."""
        return pl.scan_csv(source_path, try_parse_dates=False, infer_schema_length=1000)

    def normalize(self, raw: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize by detecting columns and mapping each row."""
        df = raw.collect()
        if df.is_empty():
            return pl.LazyFrame()

        col_map = {
            "arrival": _find_column(df, ARRIVAL_COLS),
            "departure": _find_column(df, DEPARTURE_COLS),
            "energy": _find_column(df, ENERGY_COLS),
            "charging_time": _find_column(df, CHARGING_TIME_COLS),
            "station": _find_column(df, STATION_COLS),
            "port": _find_column(df, PORT_COLS),
        }

        records = df.to_dicts()
        normalized = [
            normalize_urbanev_record(r, i, col_map)
            for i, r in enumerate(records)
        ]
        return pl.LazyFrame(normalized)
