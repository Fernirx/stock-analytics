import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pandas as pd
from vnstock import Market
from app.core.constants import DEFAULT_SYMBOLS

logger = logging.getLogger("VnstockClient")


class VnstockClient:
    def __init__(self, symbols=None):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.market = Market(source="binance")
        self.last_trade_ids = {}
        self.last_prices = {}
        self._lock = threading.Lock()

    def _fetch_symbol_ticks(self, symbol: str):
        """Kéo các lệnh khớp thực tế mới nhất cho một mã cụ thể từ sàn Binance."""
        ticks = []
        try:
            df = self.market.crypto(symbol).trades()
            if not hasattr(df, "empty") or df.empty or "id" not in df.columns:
                return ticks

            df["id"] = df["id"].astype(int)
            df = df.sort_values("id")

            with self._lock:
                last_id = self.last_trade_ids.get(symbol)

            if last_id is not None:
                new_trades = df[df["id"] > last_id]
                if new_trades.empty:
                    # Không có giao dịch mới phát sinh trong chu kỳ này -> Bỏ qua, không fake lệnh
                    return ticks
                selected = new_trades.tail(20)
            else:
                # Khởi động lần đầu: chỉ lấy 3 lệnh gần nhất để làm mốc checkpoint
                selected = df.tail(3)

            for _, row in selected.iterrows():
                price = float(row.get("price") or 0)
                volume = float(row.get("volume") or 0)
                side = str(row.get("side") or "buy").lower()
                match_type = "B" if "buy" in side else "S"

                # Parse timestamp thực tế từ sàn Binance theo chuẩn UTC gốc
                raw_time = row.get("time")
                if raw_time is not None:
                    dt = pd.to_datetime(raw_time)
                    if dt.tzinfo is not None:
                        dt = dt.tz_convert("UTC").tz_localize(None)
                    trade_time = dt.to_pydatetime()
                else:
                    trade_time = datetime.utcnow()

                if price > 0 and volume > 0:
                    ticks.append({
                        "symbol": symbol,
                        "price": price,
                        "volume": volume,
                        "match_type": match_type,
                        "trade_time": trade_time
                    })

            # Cập nhật checkpoint an toàn trong luồng
            with self._lock:
                self.last_trade_ids[symbol] = int(selected.iloc[-1]["id"])
                self.last_prices[symbol] = float(selected.iloc[-1]["price"])

            return ticks

        except Exception as e:
            logger.warning(f"[{symbol}] Lỗi khi kéo ticks từ Binance API: {e}")
            return ticks

    def fetch_latest_ticks(self):
        """Kéo dữ liệu khớp lệnh đồng thời cho các mã được cấu hình bằng ThreadPool."""
        ticks = []
        workers = min(len(self.symbols), 8)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(self._fetch_symbol_ticks, self.symbols)
            for res in results:
                ticks.extend(res)
        return ticks
