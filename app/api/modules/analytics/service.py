from datetime import datetime, timedelta
from app.core.database import get_clickhouse_client


class AnalyticsService:
    @property
    def client(self):
        return get_clickhouse_client()

    def get_overview(self, symbol: str = "BTCUSDT"):
        # 0. Xác định anchor_time từ metadata của active parts trong ClickHouse
        anchor_query = """
            SELECT max(max_time) 
            FROM system.parts 
            WHERE database = 'stock_analytics' AND table = 'stock_ticks' AND active = 1
        """
        anchor_res = self.client.query(anchor_query).result_rows
        anchor_time = anchor_res[0][0] if (anchor_res and anchor_res[0][0]) else None

        # Fallback an toàn nếu chưa có metadata part
        if not anchor_time:
            fallback_res = self.client.query("SELECT max(trade_time) FROM stock_analytics.stock_ticks").result_rows
            anchor_time = fallback_res[0][0] if (fallback_res and fallback_res[0][0]) else None

        # Trả về payload mặc định nếu bảng hoàn toàn rỗng
        if not anchor_time:
            return {
                "symbol": symbol,
                "kpi": {
                    "total_trades": 0, "total_volume": 0.0, "vwap": 0.0,
                    "min_price": 0.0, "max_price": 0.0, "last_price": 0.0,
                    "price_change_pct": 0.0, "buy_vol": 0.0, "sell_vol": 0.0
                },
                "top_ranking": [],
                "pie_ratio": [
                    {"name": "Lực Mua (Buy)", "value": 0.0, "percent": 0.0},
                    {"name": "Lực Bán (Sell)", "value": 0.0, "percent": 0.0}
                ],
                "sankey": {"nodes": [], "links": []},
                "distribution": []
            }

        start_time = anchor_time - timedelta(days=1)
        end_time = anchor_time

        params = {
            "symbol": symbol,
            "start_time": start_time,
            "end_time": end_time
        }
        market_params = {
            "start_time": start_time,
            "end_time": end_time
        }

        # 1. KPI Metrics (24h Window) - Tận dụng Primary Index (symbol, trade_time)
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
            row = kpi_res[0]
            kpi = {
                "total_trades": int(row[0] or 0),
                "total_volume": round(float(row[1] or 0), 4),
                "vwap": float(row[2] or 0.0),
                "min_price": float(row[3] or 0.0),
                "max_price": float(row[4] or 0.0),
                "last_price": float(row[5] or 0.0),
                "price_change_pct": float(row[6] or 0.0),
                "buy_vol": round(float(row[7] or 0), 4),
                "sell_vol": round(float(row[8] or 0), 4),
            }

        # 2. Top Ranking & Bar Chart (Xếp hạng thanh khoản 24h theo USD Turnover)
        top_query = """
            SELECT 
                symbol,
                round(sum(volume), 4) AS coin_volume,
                round(sum(price * volume), 2) AS turnover,
                round(argMax(price, trade_time), 2) AS last_price,
                round(ifNull((argMax(price, trade_time) - argMin(price, trade_time)) / nullIf(argMin(price, trade_time), 0) * 100, 0), 2) AS change_pct
            FROM stock_analytics.stock_ticks
            WHERE trade_time >= %(start_time)s
              AND trade_time <= %(end_time)s
            GROUP BY symbol
            ORDER BY turnover DESC
            LIMIT 10
        """
        top_res = self.client.query(top_query, parameters=market_params).result_rows
        top_symbols = [
            {
                "symbol": r[0],
                "coin_volume": float(r[1] or 0),
                "turnover": float(r[2] or 0),
                "volume": float(r[2] or 0),  # Giữ nguyên giá trị turnover cho volume để tương thích Frontend
                "last_price": float(r[3] or 0),
                "change_pct": float(r[4] or 0)
            }
            for r in top_res
        ]

        # 3. Pie Chart (Tỷ lệ lực Mua vs Bán)
        total_vol = (kpi["buy_vol"] + kpi["sell_vol"]) or 1.0
        pie_data = [
            {"name": "Lực Mua (Buy)", "value": kpi["buy_vol"], "percent": round(kpi["buy_vol"] / total_vol * 100, 1)},
            {"name": "Lực Bán (Sell)", "value": kpi["sell_vol"], "percent": round(kpi["sell_vol"] / total_vol * 100, 1)}
        ]

        # 4. Sankey Diagram (Dòng tiền 24h: Tổng Market -> Mã -> Lực Mua / Lực Bán)
        sankey_query = """
            SELECT 
                symbol,
                match_type,
                round(sum(price * volume), 2) AS flow_val
            FROM stock_analytics.stock_ticks
            WHERE trade_time >= %(start_time)s
              AND trade_time <= %(end_time)s
            GROUP BY symbol, match_type
        """
        sankey_res = self.client.query(sankey_query, parameters=market_params).result_rows
        
        nodes_set = {"Dòng Tiền Thị Trường", "Lệnh Mua (Buy)", "Lệnh Bán (Sell)"}
        links = []
        sym_totals = {}

        for r in sankey_res:
            sym, m_type, val = r[0], r[1], float(r[2] or 0)
            coin_name = sym.replace("USDT", "")
            nodes_set.add(coin_name)
            sym_totals[coin_name] = sym_totals.get(coin_name, 0.0) + val
            target_node = "Lệnh Mua (Buy)" if m_type == "B" else "Lệnh Bán (Sell)"
            links.append({"source": coin_name, "target": target_node, "value": max(0.1, round(val, 2))})

        for coin, tot in sym_totals.items():
            links.append({"source": "Dòng Tiền Thị Trường", "target": coin, "value": max(0.1, round(tot, 2))})

        sankey_data = {
            "nodes": [{"name": n} for n in nodes_set],
            "links": links
        }

        # 5. Distribution (Phân bổ khối lượng 24h theo vùng giá - Parameter Binding)
        min_p = float(kpi.get("min_price") or 0.0)
        max_p = float(kpi.get("max_price") or 0.0)
        diff = max_p - min_p

        if min_p > 0 and diff > 0:
            n_bins = 8
            step = diff / n_bins
            dec = 5 if min_p < 0.1 else (4 if min_p < 1.0 else (3 if min_p < 10.0 else (2 if min_p < 100.0 else 1)))
            dist_params = {
                **params,
                "min_p": min_p,
                "step": step,
                "dec": dec
            }
            dist_query = """
                SELECT 
                    round(%(min_p)s + (floor((price - %(min_p)s) / %(step)s) * %(step)s), %(dec)s) AS price_bin,
                    round(sum(volume), 2) AS volume
                FROM stock_analytics.stock_ticks
                WHERE symbol = %(symbol)s
                  AND trade_time >= %(start_time)s
                  AND trade_time <= %(end_time)s
                GROUP BY price_bin
                ORDER BY price_bin ASC
            """
            dist_res = self.client.query(dist_query, parameters=dist_params).result_rows
            distribution_data = [
                {"price": float(r[0]), "volume": float(r[1] or 0)} for r in dist_res
            ]
        elif min_p > 0:
            distribution_data = [{"price": min_p, "volume": float(kpi.get("total_volume") or 0)}]
        else:
            distribution_data = []

        return {
            "symbol": symbol,
            "kpi": kpi,
            "top_ranking": top_symbols,
            "pie_ratio": pie_data,
            "sankey": sankey_data,
            "distribution": distribution_data
        }
