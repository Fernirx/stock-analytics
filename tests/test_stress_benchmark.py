import argparse
import json
import random
import sys
import time
from datetime import datetime
import pytest
from app.core.config import settings
from app.core.database import get_clickhouse_client
from app.crawler.producer import get_kafka_producer


def run_benchmark(count: int = 5000):
    producer = get_kafka_producer()
    client = get_clickhouse_client()

    symbols = {
        "BTCUSDT": 69200.0,
        "ETHUSDT": 1950.0,
        "SOLUSDT": 135.0,
        "BNBUSDT": 590.0,
        "DOGEUSDT": 0.0645,
        "XRPUSDT": 0.58,
        "ADAUSDT": 0.35,
        "AVAXUSDT": 27.5,
    }
    sym_list = list(symbols.keys())
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n[+] Bắt đầu Stress Test với {count} messages...")
    start_time = time.time()

    for _ in range(count):
        sym = random.choice(sym_list)
        base = symbols[sym]
        p = round(base * (1 + random.uniform(-0.001, 0.001)), 5 if base < 1 else 2)
        v = round(random.uniform(0.01, 2.0), 4)
        side = random.choice(["B", "S"])
        payload = {
            "symbol": sym,
            "price": p,
            "volume": v,
            "match_type": side,
            "trade_time": now_str,
        }
        producer.send(settings.KAFKA_TOPIC_TICKS, value=payload)

    producer.flush()
    elapsed = max(0.001, time.time() - start_time)
    producer_rate = count / elapsed

    # Đợi ClickHouse hoàn tất consume & commit (poll tối đa 10s)
    ingested_count = 0
    for _ in range(10):
        time.sleep(1.0)
        res = client.query(f"""
            SELECT count() 
            FROM stock_analytics.stock_ticks 
            WHERE trade_time = '{now_str}'
        """).result_rows
        ingested_count = res[0][0] if res else 0
        if ingested_count >= count:
            break

    print("=" * 65)
    print("               KẾT QUẢ KIỂM THỬ HIỆU NĂNG (BENCHMARK)")
    print("=" * 65)
    print(f" [*] Tổng số messages kiểm thử     : {count:,}")
    print(f" [*] Thời gian gửi (Producer)       : {elapsed:.2f}s")
    print(f" [*] Tốc độ gửi (Producer Rate)     : {producer_rate:,.0f} msgs/giây")
    print(f" [*] ClickHouse đã tiêu thụ         : {ingested_count:,} messages")
    print(f" [*] Trạng thái Ingestion           : {'HOÀN TẤT 100%' if ingested_count >= count else 'ĐANG XỬ LÝ'}")
    print("=" * 65)

    return {
        "count": count,
        "elapsed": elapsed,
        "rate": producer_rate,
        "ingested": ingested_count,
    }


def test_stress_throughput():
    """Pytest case kiểm tra ngưỡng chịu tải tối thiểu."""
    result = run_benchmark(count=3000)
    assert result["rate"] >= 1000, "Tốc độ gửi Kafka quá chậm (< 1,000 msgs/s)!"
    assert result["ingested"] >= 3000, "ClickHouse chưa nuốt đủ số lượng message kiểm thử!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy Stress Test Kafka & ClickHouse")
    parser.add_argument("--count", type=int, default=5000, help="Số lượng message cần bắn (mặc định: 5000)")
    args = parser.parse_args()
    run_benchmark(count=args.count)
