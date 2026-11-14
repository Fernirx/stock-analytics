import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime

# Tắt log rác từ các thư viện kafka và urllib3
logging.getLogger("kafka").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("clickhouse_connect").setLevel(logging.ERROR)

# Đảm bảo import được module app khi chạy file trực tiếp
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from app.core.config import settings
from app.core.database import get_clickhouse_client
from app.crawler.producer import get_kafka_producer


def get_current_lag(bootstrap_servers: str, group_id: str, topic: str) -> int:
    """Truy vấn trực tiếp Kafka Broker để tính Consumer Lag tức thời."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        tp = TopicPartition(topic, 0)

        group_offsets = admin.list_consumer_group_offsets(group_id)
        end_offsets = consumer.end_offsets([tp])

        admin.close()
        consumer.close()

        committed = group_offsets[tp].offset if tp in group_offsets else 0
        end = end_offsets.get(tp, 0)
        return max(0, end - committed)
    except Exception:
        return 0


def run_benchmark(count: int = 5000):
    """Thực thi kiểm thử tải dồn dập, đo Peak Consumer Lag và Recovery Time."""
    producer = get_kafka_producer()
    client = get_clickhouse_client()
    server = f"localhost:{settings.KAFKA_PORT}"

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

    print(f"\n[+] Bắt đầu Stress Test: Bắn dồn {count:,} messages vào topic '{settings.KAFKA_TOPIC_TICKS}'...")
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
        producer.send(settings.KAFKA_TOPIC_TICKS, key=sym.encode("utf-8"), value=payload)

    producer.flush()
    elapsed = max(0.001, time.time() - start_time)
    producer_rate = count / elapsed

    # 1. Đo PEAK LAG ngay khi Producer vừa xả xong toàn bộ tin
    peak_lag = get_current_lag(server, "clickhouse_ticks_consumer", settings.KAFKA_TOPIC_TICKS)

    # 2. Bấm giờ đo RECOVERY TIME: Mất bao lâu để ClickHouse kéo Lag về mức an toàn (< 50)
    recovery_start = time.time()
    recovery_time = 0.0
    ingested_count = 0

    for _ in range(15):
        time.sleep(0.5)
        current_lag = get_current_lag(server, "clickhouse_ticks_consumer", settings.KAFKA_TOPIC_TICKS)
        if current_lag <= 50 and recovery_time == 0.0:
            recovery_time = time.time() - recovery_start

        res = client.query(f"""
            SELECT count() 
            FROM stock_analytics.stock_ticks 
            WHERE trade_time = '{now_str}'
        """).result_rows
        ingested_count = res[0][0] if res else 0

        if ingested_count >= count and current_lag <= 50:
            if recovery_time == 0.0:
                recovery_time = time.time() - recovery_start
            break

    if recovery_time == 0.0:
        recovery_time = time.time() - recovery_start

    print("=" * 72)
    print("             KẾT QUẢ KIỂM THỬ HIỆU NĂNG & CONSUMER LAG")
    print("=" * 72)
    print(f" [*] Tổng số messages kiểm thử       : {count:,}")
    print(f" [*] Thời gian gửi (Producer)         : {elapsed:.2f}s")
    print(f" [*] Tốc độ gửi (Producer Rate)       : {producer_rate:,.0f} msgs/giây")
    print(f" [*] Peak Consumer Lag (Đỉnh điểm)   : {peak_lag:,} messages")
    print(f" [*] Recovery Time (Thời gian xả đệm): {recovery_time:.2f}s")
    print(f" [*] ClickHouse đã tiêu thụ           : {ingested_count:,} messages")
    print(f" [*] Trạng thái Ingestion             : {'HOÀN TẤT 100%' if ingested_count >= count else 'ĐANG XỬ LÝ'}")
    print("=" * 72)

    return {
        "count": count,
        "producer_rate": producer_rate,
        "peak_lag": peak_lag,
        "recovery_time": recovery_time,
        "ingested": ingested_count,
    }


def test_stress_throughput():
    """Pytest case kiểm tra ngưỡng chịu tải và Consumer Lag."""
    result = run_benchmark(count=3000)
    assert result["producer_rate"] >= 1000, "Tốc độ gửi Kafka quá chậm (< 1,000 msgs/s)!"
    assert result["ingested"] >= 3000, "ClickHouse chưa nuốt đủ số lượng message kiểm thử!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy Stress Test Kafka & ClickHouse")
    parser.add_argument("--count", type=int, default=5000, help="Số lượng message cần bắn (mặc định: 5000)")
    args = parser.parse_args()
    run_benchmark(count=args.count)
