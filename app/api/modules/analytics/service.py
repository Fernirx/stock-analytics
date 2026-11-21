from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from app.core.database import get_clickhouse_client


class AnalyticsService:
    @property
    def client(self):
        return get_clickhouse_client()

    def _fetch_kpi(self, params):
        kpi_query = """
            SELECT 
                count() AS total_trades,
                sum(volume) AS total_volume,
                round(ifNull(sum(price * volume) / nullIf(sum(volume), 0), 0), 5) AS vwap,
                min(price) AS min_price,
                max(price) AS max_price,
                argMax(price, trade_time) AS last_price,
                round(ifNull((argMax(price, trade_time) - argMin(price, trade_time)) / nullIf(argMin(price, trade_time), 0) * 100, 0), 2) AS price_change_pct,
                round(sumIf(volume, match_type = 'B'), 4) AS buy_vol,
                round(sumIf(volume, match_type = 'S'), 4) AS sell_vol
            FROM stock_analytics.stock_ticks
            WHERE symbol = %(symbol)s
              AND trade_time >= %(start_time)s
              AND trade_time <= %(end_time)s
        """
        kpi_res = self.client.query(kpi_query, parameters=params).result_rows
        kpi = {
            "total_trades": 0, "total_volume": 0.0, "vwap": 0.0,
            "min_price": 0.0, "max_price": 0.0, "last_price": 0.0,
            "price_change_pct": 0.0, "buy_vol": 0.0, "sell_vol": 0.0
        }
        if kpi_res and kpi_res[0]:
            r = kpi_res[0]
            kpi = {
                "total_trades": int(r[0] or 0),
                "total_volume": round(float(r[1] or 0), 4),
                "vwap": float(r[2] or 0.0),
                "min_price": float(r[3] or 0.0),
                "max_price": float(r[4] or 0.0),
                "last_price": float(r[5] or 0.0),
                "price_change_pct": float(r[6] or 0.0),
                "buy_vol": round(float(r[7] or 0), 4),
                "sell_vol": round(float(r[8] or 0), 4),
            }
        return kpi

    def _fetch_top(self, market_params):
        try:
            # Ưu tiên query bảng nén stock_orderflow_5m (SummingMergeTree)
            top_query = """
                SELECT 
                    symbol,
                    round(sum(total_vol), 4) AS coin_volume,
                    round(sum(turnover), 2) AS turnover,
                    round(sum(turnover) / nullIf(sum(total_vol), 0), 2) AS last_price,
                    0.0 AS change_pct
                FROM stock_analytics.stock_orderflow_5m
                WHERE bucket_time >= %(start_time)s AND bucket_time <= %(end_time)s
                GROUP BY symbol
                ORDER BY turnover DESC LIMIT 10
            """
            top_res = self.client.query(top_query, parameters=market_params).result_rows
            if top_res:
                return [
                    {
                        "symbol": r[0], "coin_volume": float(r[1] or 0),
                        "turnover": float(r[2] or 0), "volume": float(r[2] or 0),
                        "last_price": float(r[3] or 0), "change_pct": float(r[4] or 0)
                    }
                    for r in top_res
                ]
        except Exception:
            pass

        # Fallback bảng raw stock_ticks nếu bảng orderflow chưa có data
        fallback_query = """
            SELECT symbol, round(sum(volume), 4), round(sum(price * volume), 2),
                   round(argMax(price, trade_time), 2), 0.0
            FROM stock_analytics.stock_ticks
            WHERE trade_time >= %(start_time)s AND trade_time <= %(end_time)s
            GROUP BY symbol ORDER BY 3 DESC LIMIT 10
        """
        res = self.client.query(fallback_query, parameters=market_params).result_rows
        return [
            {"symbol": r[0], "coin_volume": float(r[1] or 0), "turnover": float(r[2] or 0),
             "volume": float(r[2] or 0), "last_price": float(r[3] or 0), "change_pct": float(r[4] or 0)}
            for r in res
        ]

    def _fetch_sankey(self, market_params):
        try:
            sankey_query = """
                SELECT symbol, match_type, round(sum(price * volume), 2) AS flow_val
                FROM stock_analytics.stock_ticks
                WHERE trade_time >= %(start_time)s AND trade_time <= %(end_time)s
                GROUP BY symbol, match_type
            """
            sankey_res = self.client.query(sankey_query, parameters=market_params).result_rows
            nodes_set = {"Dòng Tiền Thị Trường", "Lệnh Mua (Buy)", "Lệnh Bán (Sell)"}
            links, sym_totals = [], {}
            for r in sankey_res:
                sym, m_type, val = r[0], r[1], float(r[2] or 0)
                coin_name = sym.replace("USDT", "")
                nodes_set.add(coin_name)
                sym_totals[coin_name] = sym_totals.get(coin_name, 0.0) + val
                target = "Lệnh Mua (Buy)" if m_type == "B" else "Lệnh Bán (Sell)"
                links.append({"source": coin_name, "target": target, "value": max(0.1, round(val, 2))})
            for coin, tot in sym_totals.items():
                links.append({"source": "Dòng Tiền Thị Trường", "target": coin, "value": max(0.1, round(tot, 2))})
            return {"nodes": [{"name": n} for n in nodes_set], "links": links}
        except Exception:
            return {"nodes": [], "links": []}

    def _fetch_dist(self, params, kpi):
        min_p = float(kpi.get("min_price") or 0.0)
        max_p = float(kpi.get("max_price") or 0.0)
        diff = max_p - min_p
        if min_p > 0 and diff > 0:
            step = diff / 8
            dec = 5 if min_p < 0.1 else (4 if min_p < 1.0 else 2)
            dist_params = {**params, "min_p": min_p, "step": step, "dec": dec}
            dist_query = """
                SELECT 
                    round(%(min_p)s + (floor((price - %(min_p)s) / %(step)s) * %(step)s), %(dec)s) AS price_bin,
                    round(sum(volume), 2) AS volume
                FROM stock_analytics.stock_ticks
                WHERE symbol = %(symbol)s AND trade_time >= %(start_time)s AND trade_time <= %(end_time)s
                GROUP BY price_bin ORDER BY price_bin ASC
            """
            dist_res = self.client.query(dist_query, parameters=dist_params).result_rows
            return [{"price": float(r[0]), "volume": float(r[1] or 0)} for r in dist_res]
        return []

    def get_overview(self, symbol: str = "BTCUSDT"):
        anchor_query = """
            SELECT max(max_time) FROM system.parts 
            WHERE database = 'stock_analytics' AND table = 'stock_ticks' AND active = 1
        """
        try:
            anchor_res = self.client.query(anchor_query).result_rows
            anchor_time = anchor_res[0][0] if (anchor_res and anchor_res[0][0]) else None
        except Exception:
            anchor_time = None

        if not anchor_time:
            anchor_time = datetime.now()

        start_time = anchor_time - timedelta(days=1)
        end_time = anchor_time
        params = {"symbol": symbol, "start_time": start_time, "end_time": end_time}
        market_params = {"start_time": start_time, "end_time": end_time}

        # BẮN SONG SONG CÁC TRUY VẤN ĐỘC LẬP BẰNG THREAD POOL EXECUTOR
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_kpi = executor.submit(self._fetch_kpi, params)
            future_top = executor.submit(self._fetch_top, market_params)
            future_sankey = executor.submit(self._fetch_sankey, market_params)

            kpi = future_kpi.result()
            future_dist = executor.submit(self._fetch_dist, params, kpi)

            top_symbols = future_top.result()
            sankey_data = future_sankey.result()
            dist_data = future_dist.result()

        total_vol = (kpi["buy_vol"] + kpi["sell_vol"]) or 1.0
        pie_data = [
            {"name": "Lực Mua (Buy)", "value": kpi["buy_vol"], "percent": round(kpi["buy_vol"] / total_vol * 100, 1)},
            {"name": "Lực Bán (Sell)", "value": kpi["sell_vol"], "percent": round(kpi["sell_vol"] / total_vol * 100, 1)}
        ]

        return {
            "symbol": symbol,
            "kpi": kpi,
            "top_ranking": top_symbols,
            "pie_ratio": pie_data,
            "sankey": sankey_data,
            "distribution": dist_data
        }
