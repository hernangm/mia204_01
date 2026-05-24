from fastapi import APIRouter

from app.api.v1.endpoints.forecast import router as forecast_router
from app.api.v1.endpoints.wells import router as wells_router

api_router = APIRouter()
api_router.include_router(wells_router)
api_router.include_router(forecast_router)
