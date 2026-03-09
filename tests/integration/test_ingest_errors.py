"""E2E tests for ingestion error handling."""

from pathlib import Path

from httpx import AsyncClient

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


async def test_invalid_dataset_returns_400(client: AsyncClient) -> None:
    """Unknown dataset name should return 400 with a clear error message."""
    resp = await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "nonexistent_dataset",
            # Use a file that exists so the path check passes
            "source_path": str(FIXTURES_DIR / "acn_sample.json"),
        },
    )

    assert resp.status_code == 400
    detail = resp.json()["detail"].lower()
    assert "nonexistent_dataset" in detail or "unknown" in detail


async def test_missing_source_file_returns_400(client: AsyncClient) -> None:
    """Non-existent source path should return 400."""
    resp = await client.post(
        "/ingestion/sessions",
        json={
            "source_dataset": "acn",
            "source_path": "/tmp/does_not_exist_12345.json",
        },
    )

    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"].lower()


async def test_missing_request_body_returns_422(client: AsyncClient) -> None:
    """Missing required fields should return 422 validation error."""
    resp = await client.post("/ingestion/sessions", json={})

    assert resp.status_code == 422


async def test_missing_source_dataset_returns_422(client: AsyncClient) -> None:
    """Missing source_dataset field should return 422."""
    resp = await client.post(
        "/ingestion/sessions",
        json={"source_path": "/tmp/some_file.json"},
    )

    assert resp.status_code == 422
