from fastapi import APIRouter, Query
from app.api.modules.analytics.service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])
service = AnalyticsService()


@router.get("/overview")
def get_overview(symbol: str = Query("BTCUSDT", description="Mã giao dịch")):
    return service.get_overview(symbol=symbol.upper())
