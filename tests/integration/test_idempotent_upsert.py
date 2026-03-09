"""E2E test: idempotent upsert — re-ingesting the same data does not duplicate records."""

from pathlib import Path

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.charging_session import ChargingSession

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


async def test_double_ingest_acn_no_duplicates(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Ingesting ACN data twice should not create duplicate sessions."""
    payload = {
        "source_dataset": "acn",
        "source_path": str(FIXTURES_DIR / "acn_sample.json"),
    }

    # First ingestion
    resp1 = await client.post("/ingestion/sessions", json=payload)
    assert resp1.status_code == 200
    body1 = resp1.json()
    assert body1["records_inserted"] == 4

    # Second ingestion — same data
    resp2 = await client.post("/ingestion/sessions", json=payload)
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["records_inserted"] == 0  # all skipped as duplicates
    assert body2["records_seen"] == 4

    # Verify DB has exactly 4 records, not 8
    count_result = await db_session.execute(
        select(func.count()).select_from(ChargingSession)
    )
    total = count_result.scalar_one()
    assert total == 4


async def test_double_ingest_urbanev_no_duplicates(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    """Ingesting UrbanEV data twice should not create duplicate sessions."""
    payload = {
        "source_dataset": "urbanev",
        "source_path": str(FIXTURES_DIR / "urbanev_sample.csv"),
    }

    resp1 = await client.post("/ingestion/sessions", json=payload)
    assert resp1.status_code == 200
    assert resp1.json()["records_inserted"] == 5

    resp2 = await client.post("/ingestion/sessions", json=payload)
    assert resp2.status_code == 200
    assert resp2.json()["records_inserted"] == 0

    count_result = await db_session.execute(
        select(func.count()).select_from(ChargingSession)
    )
    assert count_result.scalar_one() == 5
