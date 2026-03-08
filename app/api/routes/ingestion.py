from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.domain.schemas.charging_session import IngestionRequest, IngestionResponse
from app.ingestion.services.ingestion_service import run_ingestion

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


@router.post("/sessions", response_model=IngestionResponse)
async def ingest_sessions(
    request: IngestionRequest,
    db: AsyncSession = Depends(get_session),  # noqa: B008
) -> IngestionResponse:
    """Trigger ingestion of charging sessions from a dataset source."""
    source_path = Path(request.source_path)
    if not source_path.exists():
        detail = f"Source path not found: {request.source_path}"
        raise HTTPException(status_code=400, detail=detail)

    try:
        run = await run_ingestion(db, request.source_dataset, request.source_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return IngestionResponse(
        run_id=run.id,
        source_dataset=run.source_dataset,
        status=run.status,
        records_seen=run.records_seen,
        records_inserted=run.records_inserted,
        records_invalid=run.records_invalid,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )
