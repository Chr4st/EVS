"""E2E test: full ACN ingestion pipeline via the API."""

from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.charging_session import ChargingSession
from app.db.models.ingestion_run import IngestionRun

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


async def test_ingest_acn_full_pipeline(client: AsyncClient, db_session: AsyncSession) -> None:
    """POST /ingestion/sessions with ACN fixture -> verify response + DB state.

    ACN sample has 4 records: 2 valid, 2 invalid (zero-duration + missing timestamp).
    """
    resp = await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "acn",
            "source_path": str(FIXTURES_DIR / "acn_sample.json"),
        },
    )

    assert resp.status_code == 200
    body = resp.json()

    # Verify response envelope
    assert body["status"] == "completed"
    assert body["source_dataset"] == "acn"
    assert body["records_seen"] == 4
    assert body["records_inserted"] == 4  # all persisted (invalid ones too)
    assert body["records_invalid"] == 2
    assert body["run_id"] >= 1
    assert body["started_at"] is not None
    assert body["completed_at"] is not None

    # Verify DB: ingestion_run record
    run_result = await db_session.execute(select(IngestionRun))
    runs = run_result.scalars().all()
    assert len(runs) == 1
    assert runs[0].status == "completed"
    assert runs[0].records_seen == 4

    # Verify DB: charging_session records
    sessions_result = await db_session.execute(
        select(ChargingSession).order_by(ChargingSession.session_id)
    )
    sessions = sessions_result.scalars().all()
    assert len(sessions) == 4

    # Check a valid session has correct fields
    valid_sessions = [s for s in sessions if s.is_valid]
    assert len(valid_sessions) == 2
    for s in valid_sessions:
        assert s.source_dataset == "acn"
        assert s.arrival_ts is not None
        assert s.departure_ts is not None
        assert s.raw_payload is not None
        assert s.validation_errors == []

    # Check invalid sessions have error details
    invalid_sessions = [s for s in sessions if not s.is_valid]
    assert len(invalid_sessions) == 2
    for s in invalid_sessions:
        assert len(s.validation_errors) > 0


async def test_ingest_acn_populates_energy_fields(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Verify energy and rate fields are mapped correctly for valid ACN records."""
    await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "acn",
            "source_path": str(FIXTURES_DIR / "acn_sample.json"),
        },
    )

    result = await db_session.execute(
        select(ChargingSession)
        .where(ChargingSession.is_valid.is_(True))
        .order_by(ChargingSession.arrival_ts)
    )
    sessions = result.scalars().all()

    first = sessions[0]
    assert first.energy_delivered_kwh == pytest.approx(30.5)
    assert first.energy_requested_kwh == pytest.approx(35.0)
    assert first.max_charge_rate_kw == pytest.approx(6.6)
    assert first.station_id is not None
    assert first.session_duration_minutes > 0
