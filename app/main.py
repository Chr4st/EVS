from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.ingestion import router as ingestion_router
from app.api.routes.simulation import router as simulation_router

app = FastAPI(title="EVS", version="0.1.0", description="EV flexibility orchestration platform")

app.include_router(health_router, tags=["health"])
app.include_router(ingestion_router)
app.include_router(simulation_router)
