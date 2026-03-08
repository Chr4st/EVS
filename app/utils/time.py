from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(dt: datetime) -> datetime:
    """Convert a datetime to UTC. If naive, assume UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
