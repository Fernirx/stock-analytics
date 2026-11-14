import json
import time
from datetime import datetime
import pytest
import requests
from app.core.config import settings
from app.core.database import get_clickhouse_client
from app.crawler.producer import get_kafka_producer


def get_api_base_url():
    for base in ["http://localhost:8000", "http://app:8000", f"http://localhost:{settings.APP_PORT}"]:
        try:
            r = requests.get(f"{base}/health", timeout=2)
            if r.status_code == 200:
                return base
        except Exception:
            continue
    return "http://localhost:8000"


def test_clickhouse_connection():
    """Kiểm tra kết nối và schema bảng ClickHouse."""
    client = get_clickhouse_client()
    res = client.query("SELECT count() FROM stock_analytics.stock_ticks")
    assert res is not None
    assert len(res.result_rows) == 1
    total_ticks = res.result_rows[0][0]
    assert total_ticks >= 0


def test_kafka_to_clickhouse_ingestion():
    """Kiểm tra toàn vẹn luồng Ingestion: Producer -> Kafka -> ClickHouse Materialized View."""
    producer = get_kafka_producer()
    client = get_clickhouse_client()

    test_symbol = "BTCUSDT"
    test_price = 77777.77
    test_volume = 0.5432
    test_side = "B"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "symbol": test_symbol,
        "price": test_price,
        "volume": test_volume,
        "match_type": test_side,
        "trade_time": now_str,
    }

    producer.send(settings.KAFKA_TOPIC_TICKS, value=payload)
    producer.flush()

    query = f"""
        SELECT count() 
        FROM stock_analytics.stock_ticks 
        WHERE symbol = '{test_symbol}' AND price = {test_price}
    """
    found = False
    for _ in range(10):
        time.sleep(1.0)
        res = client.query(query).result_rows
        if res and res[0][0] >= 1:
            found = True
            break

    assert found, "ClickHouse chưa nhận được message từ Kafka Materialized View trong vòng 10 giây!"


def test_fastapi_market_data_endpoints():
    """Kiểm tra các endpoints REST API của FastAPI trả về đúng cấu trúc."""
    base_url = get_api_base_url()

    # 1. Health check
    res_health = requests.get(f"{base_url}/health", timeout=3)
    assert res_health.status_code == 200
    assert res_health.json().get("status") == "healthy"

    # 2. Symbols endpoint
    res_symbols = requests.get(f"{base_url}/api/v1/market-data/symbols", timeout=3)
    assert res_symbols.status_code == 200
    symbols = res_symbols.json().get("symbols", [])
    assert len(symbols) >= 1
    assert "BTCUSDT" in symbols

    # 3. Latest ticks endpoint
    res_ticks = requests.get(f"{base_url}/api/v1/market-data/ticks?symbol=BTCUSDT&limit=5", timeout=3)
    assert res_ticks.status_code == 200
    data = res_ticks.json().get("data", [])
    assert len(data) >= 1
    assert "price" in data[0]
    assert "volume" in data[0]

    # 4. Analytics Overview endpoint
    res_overview = requests.get(f"{base_url}/api/v1/analytics/overview?symbol=BTCUSDT", timeout=5)
    assert res_overview.status_code == 200
    overview = res_overview.json()
    assert "kpi" in overview
    assert "top_ranking" in overview
    assert "pie_ratio" in overview
    assert "sankey" in overview
    assert "distribution" in overview
    assert overview["kpi"]["total_trades"] > 0
