import json
from typing import Optional
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from app.api.modules.market_data.broadcaster import broadcaster
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
    """Bắn dữ liệu khớp lệnh và Overview thời gian thực dùng In-Memory Broadcast Hub (Zero Client DB Query)."""
    await broadcaster.connect(websocket)
    broadcaster.start_engine(service)

    try:
        # Snapshot ban đầu: Gửi các lệnh gần nhất cho client hiển thị ngay lập tức
        init_ticks = service.get_latest_ticks(limit=15)
        if init_ticks:
            await websocket.send_json({"type": "ticks", "data": list(reversed(init_ticks))})

        # Client giữ kết nối chờ Server Push dữ liệu (hoàn toàn không query Database)
        while True:
            text = await websocket.receive_text()
            try:
                msg = json.loads(text)
                if msg.get("action") == "switch_symbol" and msg.get("symbol"):
                    await broadcaster.set_symbol(websocket, msg["symbol"])
            except Exception:
                pass
    except WebSocketDisconnect:
        await broadcaster.disconnect(websocket)
    except Exception:
        await broadcaster.disconnect(websocket)
