from fastapi import APIRouter

from app.api.routes.dimensions import router as dimensions_router
from app.api.routes.performance import router as performance_router

api_router = APIRouter(prefix="/api")
api_router.include_router(performance_router, prefix="/performance", tags=["performance"])
api_router.include_router(dimensions_router, prefix="/dimensions", tags=["dimensions"])
