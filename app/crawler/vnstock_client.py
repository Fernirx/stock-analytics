from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from vnstock import Market
from app.core.constants import DEFAULT_SYMBOLS


class VnstockClient:
    def __init__(self, symbols=None):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.market = Market(source="binance")
        self.last_trade_ids = {}
        self.last_prices = {}

    def _fetch_symbol_ticks(self, symbol: str):
        """Kéo ticks cho một mã cụ thể."""
        ticks = []
        try:
            df = self.market.crypto(symbol).trades()
            if hasattr(df, "empty") and not df.empty and "id" in df.columns:
                df["id"] = df["id"].astype(int)
                last_id = self.last_trade_ids.get(symbol)
                if last_id is not None:
                    new_trades = df[df["id"] > last_id]
                    selected = new_trades.tail(5) if not new_trades.empty else df.tail(1)
                else:
                    selected = df.tail(2)

                if not selected.empty:
                    now = datetime.now()
                    for _, row in selected.iterrows():
                        price = float(row.get("price") or 0)
                        volume = float(row.get("volume") or 0)
                        side = str(row.get("side") or "buy").lower()
                        match_type = "B" if "buy" in side else "S"

                        if price > 0 and volume > 0:
                            ticks.append({
                                "symbol": symbol,
                                "price": price,
                                "volume": volume,
                                "match_type": match_type,
                                "trade_time": now
                            })

                    if "id" in selected.columns:
                        self.last_trade_ids[symbol] = int(selected.iloc[-1]["id"])
                    self.last_prices[symbol] = float(selected.iloc[-1]["price"])
                    return ticks
        except Exception:
            pass

        try:
            price = 0.0
            price_data = self.market.crypto(symbol).last_price()
            if isinstance(price_data, (int, float)):
                price = float(price_data)
            elif hasattr(price_data, "iloc") and not price_data.empty:
                val = price_data.iloc[0]
                price = float(val.get("price") or val.get("last_price") or val)
            elif isinstance(price_data, dict):
                price = float(price_data.get("price") or price_data.get("last_price") or 0)

            if price > 0:
                prev_p = self.last_prices.get(symbol, price)
                match_type = "B" if price >= prev_p else "S"
                self.last_prices[symbol] = price
                vol_map = {
                    "BTCUSDT": 0.02, "ETHUSDT": 0.5, "SOLUSDT": 3.0, "BNBUSDT": 1.0,
                    "DOGEUSDT": 100.0, "XRPUSDT": 50.0, "ADAUSDT": 60.0, "AVAXUSDT": 10.0
                }
                volume = vol_map.get(symbol, 1.0)

                ticks.append({
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "match_type": match_type,
                    "trade_time": datetime.now()
                })
        except Exception:
            pass

        return ticks

    def fetch_latest_ticks(self):
        """Kéo dữ liệu khớp lệnh đồng thời cho các mã được cấu hình."""
        ticks = []
        with ThreadPoolExecutor(max_workers=len(self.symbols)) as executor:
            results = executor.map(self._fetch_symbol_ticks, self.symbols)
            for res in results:
                ticks.extend(res)
        return ticks
