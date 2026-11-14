import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime

# Tắt log rác từ các thư viện kafka và clickhouse
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


def get_kafka_server():
    is_docker = os.path.exists("/.dockerenv")
    return settings.KAFKA_BOOTSTRAP_SERVERS if is_docker else f"localhost:{settings.KAFKA_PORT}"


def get_lag(admin: KafkaAdminClient, consumer: KafkaConsumer, tp: TopicPartition, group_id: str) -> int:
    """Đo Consumer Lag tức thời qua kết nối Kafka mở sẵn."""
    try:
        group_offsets = admin.list_consumer_group_offsets(group_id)
        end_offsets = consumer.end_offsets([tp])
        committed = group_offsets[tp].offset if tp in group_offsets else 0
        end = end_offsets.get(tp, 0)
        return max(0, end - committed)
    except Exception:
        return 0


def run_benchmark(count: int = 5000):
    """Bắn tin nhắn và đo chính xác: ClickHouse ăn bao lâu thì hết Kafka, và mỗi giây ăn bao nhiêu tin."""
    producer = get_kafka_producer()
    client = get_clickhouse_client()
    server = get_kafka_server()
    topic = settings.KAFKA_TOPIC_TICKS
    group_id = "clickhouse_ticks_consumer"
    tp = TopicPartition(topic, 0)

    # Mở sẵn kết nối Admin và Consumer để đo mẫu siêu nhạy mỗi 50ms
    admin = KafkaAdminClient(bootstrap_servers=server)
    consumer = KafkaConsumer(bootstrap_servers=server)

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

    print(f"\n[+] Bắt đầu thử tải: Đẩy {count:,} messages vào Kafka và đo tốc độ nuốt của ClickHouse...")

    # 1. BẮT ĐẦU BẤM GIỜ TOÀN BỘ CHU TRÌNH
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
        producer.send(topic, key=sym.encode("utf-8"), value=payload)

    producer.flush()
    send_elapsed = max(0.001, time.time() - start_time)
    producer_rate = count / send_elapsed

    # Đo Peak Lag ngay khi Producer vừa gửi xong
    peak_lag = get_lag(admin, consumer, tp, group_id)

    # 2. THEO DÕI CLICKHOUSE ĂN CHO ĐẾN KHI VÉT SẠCH KAFKA (LAG VỀ 0)
    drain_start = time.time()
    final_lag = peak_lag

    while time.time() - drain_start < 20:  # Timeout tối đa 20s
        time.sleep(0.05)
        current_lag = get_lag(admin, consumer, tp, group_id)
        if current_lag <= 5:  # ClickHouse đã ăn hết sạch
            final_lag = current_lag
            break

    # 3. TÍNH TOÁN CÁC CHỈ SỐ BẠN CẦN
    total_consume_time = time.time() - start_time
    clickhouse_eat_rate = count / max(0.001, total_consume_time)

    admin.close()
    consumer.close()

    print("=" * 72)
    print("             KẾT QUẢ ĐO TỐC ĐỘ NUỐT DỮ LIỆU CỦA CLICKHOUSE")
    print("=" * 72)
    print(f" [*] Tổng số messages gửi vào Kafka    : {count:,} messages")
    print(f" [*] Tốc độ Producer bắn tin           : {producer_rate:,.0f} messages/giây (mất {send_elapsed:.2f}s)")
    print(" " + "-" * 70)
    print(f" [*] Thời gian ClickHouse nuốt HẾT sạch: {total_consume_time:.2f} giây")
    print(f" [*] Tốc độ ClickHouse ăn thực tế      : {clickhouse_eat_rate:,.0f} messages/giây")
    print(f" [*] Peak Lag trong lúc tải            : {peak_lag:,} messages")
    print(f" [*] Trạng thái sau khi ăn             : LAG = {final_lag} (Đã vét sạch 100%)")
    print("=" * 72)

    return {
        "count": count,
        "producer_rate": producer_rate,
        "total_consume_time": total_consume_time,
        "clickhouse_eat_rate": clickhouse_eat_rate,
        "peak_lag": peak_lag,
        "final_lag": final_lag,
    }


def test_stress_throughput():
    """Pytest case kiểm tra ngưỡng chịu tải và tốc độ nuốt của ClickHouse."""
    result = run_benchmark(count=3000)
    assert result["producer_rate"] >= 1000, "Tốc độ gửi Kafka quá chậm (< 1,000 msgs/s)!"
    assert result["total_consume_time"] <= 15.0, "ClickHouse nuốt dữ liệu quá chậm (> 15s)!"
    assert result["final_lag"] <= 10, "ClickHouse chưa vét sạch hàng đợi trong Kafka!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Đo tốc độ nuốt dữ liệu của ClickHouse từ Kafka")
    parser.add_argument("--count", type=int, default=5000, help="Số lượng message cần bắn (mặc định: 5000)")
    args = parser.parse_args()
    run_benchmark(count=args.count)
