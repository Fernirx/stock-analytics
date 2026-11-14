from fastapi import APIRouter
from app.api.modules.market_data.router import router as market_data_router
from app.api.modules.analytics.router import router as analytics_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(market_data_router)
api_router.include_router(analytics_router)
