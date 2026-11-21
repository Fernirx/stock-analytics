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
            conditions.append("symbol = %(symbol)s")
            params["symbol"] = symbol
        if since:
            conditions.append("trade_time > %(since)s")
            params["since"] = since

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_clause = "ORDER BY trade_time ASC" if since else "ORDER BY trade_time DESC"
        # Bảo vệ chặn tràn bộ nhớ: luôn có LIMIT ngay cả khi streaming theo since
        limit_clause = f"LIMIT {limit}" if not since else "LIMIT 200"

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
        """Tổng hợp nến OHLCV: Ưu tiên bảng AggregatingMergeTree stock_candles_1m của ClickHouse."""
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
        cache_key = f"{symbol.upper()}_{b_interval}"
        now = datetime.now()

        tf_limit_map = {
            "1m": 120,   # 120 phút = 2 giờ gần nhất
            "5m": 72,    # 72 nến = 6 giờ gần nhất
            "15m": 48,   # 48 nến = 12 giờ gần nhất
            "1h": 24     # 24 nến = 24 giờ qua
        }
        effective_limit = min(limit, tf_limit_map.get(b_interval, 120))
        candles = []

        # -------------------------------------------------------------
        # 1. ƯU TIÊN SỐ 1: Đọc trực tiếp từ bảng Continuous Aggregation stock_candles_1m
        # -------------------------------------------------------------
        try:
            ch_candle_query = f"""
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
                ORDER BY time ASC
                LIMIT %(limit)s
            """
            ch_res = self.client.query(ch_candle_query, parameters={"symbol": symbol.upper(), "limit": effective_limit})
            if ch_res and len(ch_res.result_rows) >= 3:
                cols = ["time", "open", "high", "low", "close", "volume"]
                candles = [
                    {
                        "time": int(r[0]),
                        "open": round(float(r[1]), 5 if float(r[4]) < 0.1 else (4 if float(r[4]) < 1 else 2)),
                        "high": round(float(r[2]), 5 if float(r[4]) < 0.1 else (4 if float(r[4]) < 1 else 2)),
                        "low": round(float(r[3]), 5 if float(r[4]) < 0.1 else (4 if float(r[4]) < 1 else 2)),
                        "close": round(float(r[4]), 5 if float(r[4]) < 0.1 else (4 if float(r[4]) < 1 else 2)),
                        "volume": round(float(r[5]), 4)
                    }
                    for r in ch_res.result_rows
                ]
        except Exception as e:
            logger.debug(f"Không thể đọc từ stock_candles_1m (chưa có bảng hoặc dữ liệu): {e}")

        # -------------------------------------------------------------
        # 2. DỰ PHÒNG: Nếu ClickHouse mới khởi động chưa đủ nến -> lấy từ Cache / Binance
        # -------------------------------------------------------------
        if not candles:
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

        # -------------------------------------------------------------
        # 3. DỰ PHÒNG CUỐI CÙNG: Tổng hợp trực tiếp từ bảng raw stock_ticks
        # -------------------------------------------------------------
        if not candles:
            try:
                raw_candle_query = f"""
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
                raw_res = self.client.query(raw_candle_query, parameters={"symbol": symbol.upper(), "limit": effective_limit})
                columns = ["time", "open", "high", "low", "close", "volume"]
                candles = [dict(zip(columns, row)) for row in raw_res.result_rows]
            except Exception:
                candles = []

        # -------------------------------------------------------------
        # 4. Cập nhật nến mới nhất bằng tick gần nhất từ ClickHouse
        # -------------------------------------------------------------
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
