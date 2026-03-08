# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

EVS — EV flexibility orchestration platform. Ingests, normalizes, validates, and persists EV charging session data from multiple open-source datasets.

## Build & Test Commands

- Install: `uv venv --python 3.12 .venv && source .venv/bin/activate && uv pip install -e ".[dev]"`
- Test all: `python -m pytest tests/ -v`
- Test unit only: `python -m pytest tests/unit/ -v`
- Lint: `ruff check app/ tests/`
- Lint fix: `ruff check --fix app/ tests/`
- Format: `black app/ tests/`
- Type check: `mypy app/`
- Run app: `uvicorn app.main:app --reload`
- Docker: `docker-compose up`
- Migrate: `alembic upgrade head`

## Architecture

- `app/api/routes/` — FastAPI endpoints (health, ingestion)
- `app/db/models/` — SQLAlchemy models (ChargingSession, IngestionRun)
- `app/db/migrations/` — Alembic migrations
- `app/domain/schemas/` — Pydantic schemas for API/domain layer
- `app/ingestion/interfaces/` — Abstract SessionLoader interface
- `app/ingestion/loaders/` — Dataset-specific loaders (ACN, UrbanEV) + registry
- `app/ingestion/validate/` — Pure validation functions
- `app/ingestion/services/` — Ingestion orchestration service
- `app/repositories/` — Repository pattern for DB operations
- `app/utils/` — Time, logging utilities

## Key Design Decisions

- Loaders implement `SessionLoader` ABC — add new datasets by creating a new loader
- Validation never drops records — invalid rows get `is_valid=false` with errors in JSONB
- Raw payloads preserved in `raw_payload` JSONB column for auditability
- Idempotent upsert on `session_id` — safe to re-ingest
- Polars for data processing, async SQLAlchemy + asyncpg for DB
