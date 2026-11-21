import logging
from datetime import datetime
import pandas as pd
from vnstock import Market
from app.core.constants import DEFAULT_SYMBOLS
from app.core.database import get_clickhouse_client

logger = logging.getLogger("MarketDataService")


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
            conditions.append("stock_ticks.symbol = %(symbol)s")
            params["symbol"] = symbol
        if since:
            conditions.append("stock_ticks.trade_time > %(since)s")
            params["since"] = since

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_clause = "ORDER BY stock_ticks.trade_time ASC" if since else "ORDER BY stock_ticks.trade_time DESC"
        limit_clause = f"LIMIT {limit}" if not since else "LIMIT 200"

        # Trả về cả trade_time chuẩn UTC (cho so sánh filter/since) và trade_time_vn (cho hiển thị sổ lệnh)
        query = f"""
            SELECT 
                symbol,
                price,
                volume,
                match_type,
                toString(stock_ticks.trade_time) AS trade_time,
                substring(toString(toTimeZone(stock_ticks.trade_time, 'Asia/Ho_Chi_Minh')), 12, 8) AS trade_time_vn
            FROM stock_analytics.stock_ticks
            {where_clause}
            {order_clause}
            {limit_clause}
        """
        result = self.client.query(query, parameters=params)
        columns = ["symbol", "price", "volume", "match_type", "trade_time", "trade_time_vn"]
        return [dict(zip(columns, row)) for row in result.result_rows]

    def get_candles(self, symbol: str, timeframe: str = "1m", limit: int = 120):
        """Truy vấn 100% nến OHLCV từ ClickHouse Continuous Aggregation stock_candles_1m (Zero External API)."""
        tf_str = str(timeframe).lower().strip()
        interval_map = {
            "1m": "INTERVAL 1 MINUTE",
            "5m": "INTERVAL 5 MINUTE",
            "15m": "INTERVAL 15 MINUTE",
            "1h": "INTERVAL 1 HOUR",
            "60m": "INTERVAL 1 HOUR",
        }
        interval_sql = interval_map.get(tf_str, "INTERVAL 1 MINUTE")
        b_interval = tf_str if tf_str in ["1m", "5m", "15m", "1h"] else "1m"

        tf_limit_map = {
            "1m": 120,   # 120 nến = 2 giờ
            "5m": 72,    # 72 nến = 6 giờ
            "15m": 48,   # 48 nến = 12 giờ
            "1h": 24     # 24 nến = 24 giờ
        }
        effective_limit = min(limit, tf_limit_map.get(b_interval, 120))

        ch_candle_query = f"""
            SELECT * FROM (
                SELECT
                    toUnixTimestamp(toStartOfInterval(candle_time, {interval_sql})) AS time,
                    argMinMerge(open_state) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    argMaxMerge(close_state) AS close,
                    sum(volume) AS volume
                FROM stock_analytics.stock_candles_1m
                WHERE symbol = %(symbol)s
                GROUP BY time
                ORDER BY time DESC
                LIMIT %(limit)s
            ) ORDER BY time ASC
        """
        candles = []
        try:
            ch_res = self.client.query(ch_candle_query, parameters={"symbol": symbol.upper(), "limit": effective_limit})
            if ch_res and ch_res.result_rows:
                for r in ch_res.result_rows:
                    cl = float(r[4])
                    dec = 5 if cl < 0.1 else (4 if cl < 1 else 2)
                    candles.append({
                        "time": int(r[0]),
                        "open": round(float(r[1]), dec),
                        "high": round(float(r[2]), dec),
                        "low": round(float(r[3]), dec),
                        "close": round(cl, dec),
                        "volume": round(float(r[5]), 4)
                    })
        except Exception as e:
            logger.error(f"Lỗi query nến từ ClickHouse: {e}")

        # Cập nhật giá nến mới nhất bằng tick gần nhất từ ClickHouse
        if candles:
            try:
                latest_tick_query = """
                    SELECT price, volume FROM stock_analytics.stock_ticks
                    WHERE symbol = %(symbol)s
                    ORDER BY stock_ticks.trade_time DESC
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

        return candles
