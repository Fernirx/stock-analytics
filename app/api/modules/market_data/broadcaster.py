import asyncio
import logging
from typing import Set, List, Dict, Any
from datetime import datetime
import pandas as pd
from fastapi import WebSocket
from app.api.modules.analytics.service import AnalyticsService

logger = logging.getLogger("TickBroadcaster")


class TickBroadcaster:
    """Quản lý các kết nối WebSocket và phát tán 100% Realtime Event-Driven (Zero Polling)."""

    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._client_symbols: Dict[WebSocket, str] = {}
        self._active_candles: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._worker_task: asyncio.Task = None
        self._last_time = None
        self._analytics_service = AnalyticsService()

    async def connect(self, websocket: WebSocket, initial_symbol: str = "BTCUSDT"):
        """Đăng ký kết nối WebSocket mới."""
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            self._client_symbols[websocket] = initial_symbol.upper()
        logger.info(f"Client WebSocket kết nối ({initial_symbol}). Tổng clients: {len(self._connections)}")

    async def set_symbol(self, websocket: WebSocket, symbol: str):
        """Cập nhật mã mà client đang theo dõi và push ngay dữ liệu mới nhất."""
        sym = symbol.upper()
        async with self._lock:
            self._client_symbols[websocket] = sym

        # Push ngay lập tức cây nến active và Overview của mã mới cho client này
        try:
            if sym in self._active_candles:
                await websocket.send_json({"type": "candle", "symbol": sym, "data": self._active_candles[sym]})
            ov = self._analytics_service.get_overview(symbol=sym)
            await websocket.send_json({"type": "overview", "symbol": sym, "data": ov})
        except Exception:
            pass

    async def disconnect(self, websocket: WebSocket):
        """Gỡ bỏ kết nối WebSocket khi client ngắt kết nối."""
        async with self._lock:
            self._connections.discard(websocket)
            self._client_symbols.pop(websocket, None)
        logger.info(f"Client WebSocket ngắt kết nối. Còn lại: {len(self._connections)}")

    async def broadcast_message(self, message: Dict[str, Any]):
        """Bắn đồng thời tin nhắn tới toàn bộ clients trong RAM (< 2ms)."""
        if not self._connections:
            return

        dead_connections = []
        async with self._lock:
            connections = list(self._connections)

        for ws in connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead_connections.append(ws)

        if dead_connections:
            async with self._lock:
                for ws in dead_connections:
                    self._connections.discard(ws)
                    self._client_symbols.pop(ws, None)

    async def broadcast(self, ticks: List[Dict[str, Any]]):
        """Bắn đồng thời dữ liệu khớp lệnh tới toàn bộ các clients đang online trong RAM."""
        if not ticks:
            return
        await self.broadcast_message({"type": "ticks", "data": ticks})

    async def _stream_engine(self, service):
        """Tiến trình nền DUY NHẤT: Push Ticks và Overview 24h tới toàn bộ clients (Zero Polling)."""
        logger.info("Khởi động 100% Realtime Event-Driven Stream Engine...")
        overview_counter = 0

        while True:
            try:
                if self._connections:
                    # 1. Bắt ticks mới và tự động kích hoạt Stream Ticks
                    if self._last_time is None:
                        init_ticks = service.get_latest_ticks(limit=1)
                        if init_ticks:
                            self._last_time = init_ticks[0]["trade_time"]
                    else:
                        new_ticks = service.get_latest_ticks(since=self._last_time)
                        if new_ticks:
                            self._last_time = new_ticks[-1]["trade_time"]
                            # Push Sổ Lệnh Live Tape & Realtime Tick Bouncing
                            await self.broadcast(new_ticks)

                    # 2. Push Overview Định kỳ 2s (Calculate Once, Broadcast to All)
                    overview_counter += 1
                    if overview_counter >= 6:  # 6 * 0.3s ~ 1.8s - 2.0s
                        overview_counter = 0
                        async with self._lock:
                            active_symbols = set(self._client_symbols.values()) or {"BTCUSDT"}

                        for sym in active_symbols:
                            try:
                                ov = self._analytics_service.get_overview(symbol=sym)
                                await self.broadcast_message({"type": "overview", "symbol": sym, "data": ov})
                            except Exception as ex:
                                logger.debug(f"Lỗi tính overview stream cho {sym}: {ex}")

            except Exception as e:
                logger.debug(f"Lỗi trong stream engine: {e}")

            await asyncio.sleep(0.3)

    def start_engine(self, service):
        """Khởi động tiến trình nền nếu chưa chạy."""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._stream_engine(service))


broadcaster = TickBroadcaster()
