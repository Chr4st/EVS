from fastapi import FastAPI

from app.api.routes.health import router as health_router

app = FastAPI(title="EVS", version="0.1.0", description="EV flexibility orchestration platform")

app.include_router(health_router, tags=["health"])
