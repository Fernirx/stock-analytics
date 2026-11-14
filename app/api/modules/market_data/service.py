from datetime import datetime
import pandas as pd
from vnstock import Market
from app.core.constants import DEFAULT_SYMBOLS
from app.core.database import get_clickhouse_client


class MarketDataService:
    def __init__(self):
        self.market = Market(source="binance")
        self._candle_cache = {}
        self._candle_cache_time = {}

    @property
    def client(self):
        return get_clickhouse_client()

    def get_symbols(self):
        query = "SELECT DISTINCT symbol FROM stock_analytics.stock_ticks"
        try:
            result = self.client.query(query)
            symbols = [row[0] for row in result.result_rows if row[0]]
            return symbols if len(symbols) >= 4 else DEFAULT_SYMBOLS
        except Exception:
            return DEFAULT_SYMBOLS

    def get_latest_ticks(self, symbol: str = None, limit: int = 50, since: str = None):
        """Lấy các lệnh khớp gần nhất hoặc lấy các lệnh mới kể từ mốc thời gian since."""
        conditions = []
        params = {}
        if symbol:
            conditions.append("symbol = %(symbol)s")
            params["symbol"] = symbol
        if since:
            conditions.append("trade_time > %(since)s")
            params["since"] = since

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_clause = "ORDER BY trade_time ASC" if since else "ORDER BY trade_time DESC"
        limit_clause = "" if since else f"LIMIT {limit}"

        query = f"""
            SELECT 
                symbol,
                price,
                volume,
                match_type,
                toString(trade_time) AS trade_time
            FROM stock_analytics.stock_ticks
            {where_clause}
            {order_clause}
            {limit_clause}
        """
        result = self.client.query(query, parameters=params)
        columns = ["symbol", "price", "volume", "match_type", "trade_time"]
        return [dict(zip(columns, row)) for row in result.result_rows]

    def get_candles(self, symbol: str, timeframe: str = "1m", limit: int = 120):
        """Tổng hợp nến OHLCV từ Binance và cập nhật tick realtime mới nhất từ ClickHouse."""
        tf_str = str(timeframe).lower().strip()
        binance_tf_map = {
            "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "60m": "1h"
        }
        b_interval = binance_tf_map.get(tf_str, "1m")
        cache_key = f"{symbol.upper()}_{b_interval}"
        now = datetime.now()

        # Giới hạn số nến chuẩn theo từng khung thời gian để chỉ lấy dữ liệu gần nhất trong ngày
        tf_limit_map = {
            "1m": 120,   # 120 phút = 2 giờ gần nhất của hôm nay
            "5m": 72,    # 72 nến = 6 giờ gần nhất của hôm nay
            "15m": 48,   # 48 nến = 12 giờ gần nhất của hôm nay
            "1h": 24     # 24 nến = 24 giờ qua
        }
        effective_limit = min(limit, tf_limit_map.get(b_interval, 120))

        candles = []
        if cache_key in self._candle_cache and (now - self._candle_cache_time.get(cache_key, now)).total_seconds() < 3:
            candles = list(self._candle_cache[cache_key])
        else:
            try:
                df = self.market.crypto(symbol.upper()).ohlcv(interval=b_interval)
                if hasattr(df, "empty") and not df.empty:
                    parsed = []
                    for _, r in df.iterrows():
                        ts = int(pd.to_datetime(r["time"]).timestamp())
                        op = float(r["open"])
                        hi = float(r["high"])
                        lo = float(r["low"])
                        cl = float(r["close"])
                        vo = float(r["volume"])
                        dec = 5 if cl < 0.1 else (4 if cl < 1 else 2)
                        parsed.append({
                            "time": ts,
                            "open": round(op, dec),
                            "high": round(hi, dec),
                            "low": round(lo, dec),
                            "close": round(cl, dec),
                            "volume": round(vo, 4)
                        })
                    self._candle_cache[cache_key] = parsed
                    self._candle_cache_time[cache_key] = now
                    candles = list(parsed)
            except Exception:
                candles = []

        if not candles:
            # Dự phòng ClickHouse nếu Binance API quá tải
            interval_sql = "INTERVAL 1 MINUTE"
            if b_interval == "5m": interval_sql = "INTERVAL 5 MINUTE"
            elif b_interval == "15m": interval_sql = "INTERVAL 15 MINUTE"
            elif b_interval == "1h": interval_sql = "INTERVAL 1 HOUR"

            query = f"""
                SELECT
                    toUnixTimestamp(toStartOfInterval(trade_time, {interval_sql})) AS time,
                    argMin(price, trade_time) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    argMax(price, trade_time) AS close,
                    sum(volume) AS volume
                FROM stock_analytics.stock_ticks
                WHERE symbol = %(symbol)s
                GROUP BY time
                ORDER BY time ASC
                LIMIT %(limit)s
            """
            result = self.client.query(query, parameters={"symbol": symbol.upper(), "limit": effective_limit})
            columns = ["time", "open", "high", "low", "close", "volume"]
            candles = [dict(zip(columns, row)) for row in result.result_rows]

        # Cập nhật giá nến mới nhất bằng tick gần nhất từ ClickHouse
        if candles:
            try:
                latest_tick_query = """
                    SELECT price, volume FROM stock_analytics.stock_ticks
                    WHERE symbol = %(symbol)s
                    ORDER BY trade_time DESC
                    LIMIT 1
                """
                last_res = self.client.query(latest_tick_query, parameters={"symbol": symbol.upper()}).result_rows
                if last_res:
                    last_price = float(last_res[0][0])
                    dec = 5 if last_price < 0.1 else (4 if last_price < 1 else 2)
                    last_c = dict(candles[-1])
                    last_c["close"] = round(last_price, dec)
                    last_c["high"] = max(last_c["high"], round(last_price, dec))
                    last_c["low"] = min(last_c["low"], round(last_price, dec))
                    candles[-1] = last_c
            except Exception:
                pass

        return candles[-effective_limit:]
