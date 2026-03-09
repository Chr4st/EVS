"""E2E test: full UrbanEV ingestion pipeline via the API."""

from pathlib import Path

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.charging_session import ChargingSession
from app.db.models.ingestion_run import IngestionRun

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


async def test_ingest_urbanev_full_pipeline(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """POST /ingestion/sessions with UrbanEV fixture -> verify response + DB state.

    UrbanEV sample has 5 records: 3 valid, 2 invalid (zero-duration + missing end time).
    """
    resp = await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "urbanev",
            "source_path": str(FIXTURES_DIR / "urbanev_sample.csv"),
        },
    )

    assert resp.status_code == 200
    body = resp.json()

    assert body["status"] == "completed"
    assert body["source_dataset"] == "urbanev"
    assert body["records_seen"] == 5
    assert body["records_inserted"] == 5  # all persisted (invalid ones too)
    assert body["records_invalid"] == 2
    assert body["run_id"] >= 1
    assert body["completed_at"] is not None

    # Verify DB: ingestion_run record
    run_result = await db_session.execute(select(IngestionRun))
    runs = run_result.scalars().all()
    assert len(runs) == 1
    assert runs[0].status == "completed"

    # Verify DB: charging_session records
    sessions_result = await db_session.execute(select(ChargingSession))
    sessions = sessions_result.scalars().all()
    assert len(sessions) == 5

    valid_sessions = [s for s in sessions if s.is_valid]
    assert len(valid_sessions) == 3

    invalid_sessions = [s for s in sessions if not s.is_valid]
    assert len(invalid_sessions) == 2
    for s in invalid_sessions:
        assert len(s.validation_errors) > 0


async def test_ingest_urbanev_populates_energy_and_station(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Verify energy and station fields are mapped correctly for valid UrbanEV records."""
    await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "urbanev",
            "source_path": str(FIXTURES_DIR / "urbanev_sample.csv"),
        },
    )

    result = await db_session.execute(
        select(ChargingSession)
        .where(ChargingSession.is_valid.is_(True))
        .order_by(ChargingSession.arrival_ts)
    )
    sessions = result.scalars().all()

    first = sessions[0]
    assert first.energy_delivered_kwh == pytest.approx(25.5)
    assert first.station_id is not None
    assert first.source_dataset == "urbanev"
    assert first.session_duration_minutes > 0
    assert first.charging_duration_minutes is not None
    assert first.raw_payload is not None
