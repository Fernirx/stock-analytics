import asyncio
from typing import Optional
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from app.api.modules.market_data.service import MarketDataService

router = APIRouter(prefix="/market-data", tags=["Market Data"])
service = MarketDataService()


@router.get("/symbols")
def get_symbols():
    return {"symbols": service.get_symbols()}


@router.get("/ticks")
def get_ticks(
    symbol: Optional[str] = Query(None, description="Lọc theo mã"),
    limit: int = Query(50, ge=1, le=500, description="Số lượng bản ghi")
):
    ticks = service.get_latest_ticks(symbol=symbol, limit=limit)
    return {"data": ticks, "total": len(ticks)}


@router.get("/candles/{symbol}")
def get_candles(
    symbol: str,
    timeframe: str = Query("1m", description="Khung thời gian nến (2s, 1m, 5m, 15m, 1h)"),
    interval: Optional[int] = Query(None, description="Khung nến theo phút (tương thích cũ)"),
    limit: int = Query(200, ge=1, le=1000, description="Số lượng nến")
):
    tf = f"{interval}m" if interval is not None else timeframe
    candles = service.get_candles(symbol=symbol.upper(), timeframe=tf, limit=limit)
    return {"symbol": symbol.upper(), "timeframe": tf, "data": candles}


@router.websocket("/ws")
async def websocket_ticks(websocket: WebSocket):
    """Bắn dữ liệu khớp lệnh thời gian thực cho Web Dashboard qua WebSocket dùng cơ chế Checkpoint Streaming."""
    await websocket.accept()
    last_time = None
    try:
        while True:
            try:
                if last_time is None:
                    # Lần đầu tiên kết nối: lấy mốc thời gian của lệnh mới nhất
                    init_ticks = service.get_latest_ticks(limit=1)
                    if init_ticks:
                        last_time = init_ticks[0]["trade_time"]
                else:
                    # Checkpoint Streaming: chỉ lấy đúng những lệnh mới sinh ra kể từ last_time
                    new_ticks = service.get_latest_ticks(since=last_time)
                    if new_ticks:
                        last_time = new_ticks[-1]["trade_time"]
                        await websocket.send_json({"type": "ticks", "data": new_ticks})
            except Exception:
                pass
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
