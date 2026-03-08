"""Orchestrates the ingestion pipeline: load -> normalize -> validate -> persist."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.ingestion_run import IngestionRun
from app.domain.schemas.charging_session import ChargingSessionCreate
from app.ingestion.loaders import get_loader
from app.ingestion.validate.session_validator import validate_batch
from app.repositories.charging_session_repository import ChargingSessionRepository
from app.utils.logging import get_logger
from app.utils.time import utc_now

logger = get_logger(__name__)

BATCH_SIZE = 5000


async def run_ingestion(
    db: AsyncSession,
    source_dataset: str,
    source_path: str,
) -> IngestionRun:
    """Execute a full ingestion run for a given dataset and source path."""
    loader = get_loader(source_dataset)
    repo = ChargingSessionRepository(db)

    # Create ingestion run record
    run = IngestionRun(
        source_dataset=source_dataset,
        source_path=source_path,
        status="running",
    )
    db.add(run)
    await db.flush()
    logger.info("Ingestion run %d started for %s from %s", run.id, source_dataset, source_path)

    total_seen = 0
    total_inserted = 0
    total_invalid = 0

    try:
        for batch_dicts in loader.iter_batches(source_path, batch_size=BATCH_SIZE):
            total_seen += len(batch_dicts)

            # Validate
            validated = validate_batch(batch_dicts)

            # Count invalid
            batch_invalid = sum(1 for r in validated if not r["is_valid"])
            total_invalid += batch_invalid

            # Convert to Pydantic models for persistence
            sessions = [ChargingSessionCreate(**r) for r in validated]

            # Persist
            inserted = await repo.upsert_batch(sessions)
            total_inserted += inserted

            logger.info(
                "Run %d: batch processed — seen=%d, inserted=%d, invalid=%d",
                run.id,
                len(batch_dicts),
                inserted,
                batch_invalid,
            )

        run.status = "completed"
    except Exception as exc:
        logger.error("Ingestion run %d failed: %s", run.id, exc)
        run.status = "failed"
        run.notes = str(exc)
        raise
    finally:
        run.completed_at = utc_now()
        run.records_seen = total_seen
        run.records_inserted = total_inserted
        run.records_invalid = total_invalid
        await db.commit()

    logger.info(
        "Ingestion run %d completed: seen=%d, inserted=%d, invalid=%d",
        run.id,
        total_seen,
        total_inserted,
        total_invalid,
    )
    return run
