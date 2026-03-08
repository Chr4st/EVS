# EVS — EV Flexibility Orchestration Platform

A data-driven platform for ingesting, normalizing, and managing EV charging session data from multiple open-source datasets. Built as the foundation for admissible-control-based flexibility estimation and grid-aware charging optimization.

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- Docker & Docker Compose (for PostgreSQL)

### Setup

```bash
# Clone and enter repo
git clone https://github.com/Chr4st/EVS.git && cd EVS

# Create virtual environment and install
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Copy environment config
cp .env.example .env

# Start PostgreSQL
docker-compose up -d db

# Run migrations
alembic upgrade head

# Start the API
uvicorn app.main:app --reload
```

### Verify

```bash
curl http://localhost:8000/health
# {"status": "ok"}
```

## Ingesting Data

### Via API

```bash
curl -X POST http://localhost:8000/ingestion/sessions \
  -H "Content-Type: application/json" \
  -d '{"source_dataset": "acn", "source_path": "/path/to/acn_sessions.json"}'
```

### Supported Datasets

| Dataset | Format | Loader |
|---------|--------|--------|
| ACN (Caltech) | JSON | `acn` |
| UrbanEV | CSV | `urbanev` |

### Adding a New Dataset Loader

1. Create `app/ingestion/loaders/your_loader.py`
2. Implement the `SessionLoader` interface (`load_raw`, `normalize`)
3. Register in `app/ingestion/loaders/__init__.py`

## Development

```bash
# Run tests
python -m pytest tests/ -v

# Lint
ruff check app/ tests/

# Format
black app/ tests/

# Type check
mypy app/
```

## Project Structure

```
app/
  main.py                          # FastAPI application
  config.py                        # Settings via pydantic-settings
  api/routes/                      # HTTP endpoints
  db/models/                       # SQLAlchemy ORM models
  db/migrations/                   # Alembic migrations
  domain/schemas/                  # Pydantic schemas
  ingestion/
    interfaces/session_loader.py   # Abstract loader interface
    loaders/                       # Dataset-specific loaders
    validate/                      # Validation logic
    services/                      # Ingestion orchestration
  repositories/                    # Data access layer
  utils/                           # Shared utilities
tests/
  fixtures/                        # Sample data files
  unit/                            # Unit tests
  integration/                     # Integration tests
```

## Tech Stack

- **API**: FastAPI + Pydantic v2
- **Database**: PostgreSQL + SQLAlchemy 2.0 (async) + Alembic
- **Data Processing**: Polars + PyArrow
- **Tooling**: Ruff, Black, MyPy, Pytest
