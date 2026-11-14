import argparse
import json
import logging
import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Mute verbose logs from libraries
logging.getLogger("kafka").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("clickhouse_connect").setLevel(logging.ERROR)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from app.core.config import settings
from app.core.database import get_clickhouse_client
from app.crawler.producer import get_kafka_producer


def get_kafka_server():
    is_docker = os.path.exists("/.dockerenv")
    return settings.KAFKA_BOOTSTRAP_SERVERS if is_docker else f"localhost:{settings.KAFKA_PORT}"


def get_api_base_url():
    is_docker = os.path.exists("/.dockerenv")
    return f"http://app:{settings.APP_PORT}" if is_docker else f"http://localhost:{settings.APP_PORT}"


def get_percentile(data: list, p: float) -> float:
    """Tính toán phân vị Percentile (P50, P95, P99) từ danh sách số liệu thực tế."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_data[int(k)])
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return float(d0 + d1)


def execute_api_dashboard_request(base_url: str, symbol: str = "BTCUSDT", session: requests.Session = None) -> float:
    """Thực thi gọi REST API Overview của FastAPI và đo Round-Trip Latency (ms)."""
    s = session or requests
    t0 = time.time()
    res = s.get(f"{base_url}/api/v1/analytics/overview?symbol={symbol}", timeout=10)
    res.raise_for_status()
    return (time.time() - t0) * 1000.0


def run_mixed_workload_benchmark(count: int = 20000, read_iterations: int = 30):
    client = get_clickhouse_client()
    producer = get_kafka_producer()
    server = get_kafka_server()
    base_url = get_api_base_url()
    topic = settings.KAFKA_TOPIC_TICKS

    admin = KafkaAdminClient(bootstrap_servers=server)
    consumer = KafkaConsumer(bootstrap_servers=server)
    tp = TopicPartition(topic, 0)

    symbols = {
        "BTCUSDT": 69200.0, "ETHUSDT": 1950.0, "SOLUSDT": 135.0, "BNBUSDT": 590.0,
        "DOGEUSDT": 0.0645, "XRPUSDT": 0.58, "ADAUSDT": 0.35, "AVAXUSDT": 27.5
    }
    sym_list = list(symbols.keys())

    print("\n" + "=" * 76)
    print("        BẮT ĐẦU KIỂM THỬ TẢI HỖN HỢP THỰC TẾ (MIXED WORKLOAD BENCHMARK)")
    print(f"        Mục tiêu đo: FastAPI REST API Latency + ClickHouse Ingestion")
    print(f"        Target Endpoint: {base_url}/api/v1/analytics/overview")
    print("=" * 76)

    # ---------------------------------------------------------
    # PHA 1: ĐO BASELINE (CHỈ ĐỌC API KHI HỆ THỐNG RẢNH)
    # ---------------------------------------------------------
    print(f"\n[Pha 1] Đang đo Baseline API Latency ({read_iterations} lần gọi API khi không có luồng ghi)...")
    baseline_latencies = []
    base_session = requests.Session()
    for _ in range(read_iterations):
        lat = execute_api_dashboard_request(base_url, symbol="BTCUSDT", session=base_session)
        baseline_latencies.append(lat)
        time.sleep(0.05)

    base_p50 = get_percentile(baseline_latencies, 50)
    base_p95 = get_percentile(baseline_latencies, 95)
    base_p99 = get_percentile(baseline_latencies, 99)
    print(f" -> Baseline API Latency: P50 = {base_p50:.2f}ms | P95 = {base_p95:.2f}ms | P99 = {base_p99:.2f}ms")

    # ---------------------------------------------------------
    # PHA 2: CHẠY ĐỒNG THỜI LUỒNG GHI VÀ LUỒNG ĐỌC API (UNDER LOAD)
    # ---------------------------------------------------------
    print(f"\n[Pha 2] Kích hoạt tải hỗn hợp: Bắn {count:,} messages (Ghi) đồng thời liên tục gọi API (Đọc)...")
    load_latencies = []
    stop_read_worker = False

    def read_worker():
        worker_session = requests.Session()
        while not stop_read_worker:
            sym = random.choice(["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"])
            try:
                lat = execute_api_dashboard_request(base_url, symbol=sym, session=worker_session)
                load_latencies.append(lat)
            except Exception:
                pass
            time.sleep(0.02)

    # Bắt đầu luồng đọc song song
    executor = ThreadPoolExecutor(max_workers=2)
    read_future = executor.submit(read_worker)

    # Bắt đầu luồng ghi (Producer)
    write_start = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _ in range(count):
        sym = random.choice(sym_list)
        base = symbols[sym]
        p = round(base * (1 + random.uniform(-0.001, 0.001)), 5 if base < 1 else 2)
        v = round(random.uniform(0.01, 2.0), 4)
        side = random.choice(["B", "S"])
        payload = {
            "symbol": sym, "price": p, "volume": v, "match_type": side, "trade_time": now_str
        }
        producer.send(topic, key=sym.encode("utf-8"), value=payload)

    producer.flush()
    producer_send_time = time.time() - write_start
    producer_rate = count / max(0.001, producer_send_time)

    # Đợi ClickHouse nuốt sạch tin trong Kafka (Lag về 0)
    drain_start = time.time()
    while time.time() - drain_start < 25:
        time.sleep(0.05)
        offsets = admin.list_consumer_group_offsets("clickhouse_ticks_consumer")
        end_offsets = consumer.end_offsets([tp])
        committed = offsets[tp].offset if tp in offsets else 0
        lag = max(0, end_offsets[tp] - committed)
        if lag <= 5:
            break

    total_ingestion_time = time.time() - write_start
    insert_throughput = count / max(0.001, total_ingestion_time)

    # Dừng luồng đọc
    stop_read_worker = True
    executor.shutdown(wait=True)

    load_p50 = get_percentile(load_latencies, 50)
    load_p95 = get_percentile(load_latencies, 95)
    load_p99 = get_percentile(load_latencies, 99)

    # ---------------------------------------------------------
    # PHA 3: ĐỐI CHIẾU VỚI SYSTEM TABLES CỦA CLICKHOUSE
    # ---------------------------------------------------------
    print("\n[Pha 3] Đồng bộ SYSTEM FLUSH LOGS và đối chiếu dữ liệu với ClickHouse System Tables...")
    try:
        client.command("SYSTEM FLUSH LOGS")
    except Exception:
        pass
    time.sleep(1.0)

    # 1. Đo Rows Read & Query Latency từ system.query_log (lọc các query xuất phát từ service)
    q_log_res = client.query("""
        SELECT 
            round(avg(query_duration_ms), 2) AS avg_duration_ms,
            round(quantile(0.95)(query_duration_ms), 2) AS p95_duration_ms,
            round(avg(read_rows), 0) AS avg_read_rows,
            round(avg(result_rows), 0) AS avg_result_rows
        FROM system.query_log
        WHERE type = 'QueryFinish' 
          AND query LIKE '%stock_ticks%' 
          AND query NOT LIKE '%system%'
          AND event_time >= now() - INTERVAL 5 MINUTE
    """).result_rows

    sys_avg_latency = q_log_res[0][0] if q_log_res and q_log_res[0][0] else 0.0
    sys_p95_latency = q_log_res[0][1] if q_log_res and q_log_res[0][1] else 0.0
    sys_read_rows = int(q_log_res[0][2] or 0) if q_log_res else 0
    sys_result_rows = int(q_log_res[0][3] or 0) if q_log_res else 0

    # 2. Đo Insert Latency từ system.part_log
    part_log_res = client.query("""
        SELECT 
            round(avg(duration_ms), 2) AS avg_insert_latency_ms,
            max(duration_ms) AS max_insert_latency_ms,
            sum(rows) AS total_rows_inserted
        FROM system.part_log
        WHERE table = 'stock_ticks' 
          AND event_type = 'NewPart'
          AND event_time >= now() - INTERVAL 5 MINUTE
    """).result_rows

    avg_insert_latency = part_log_res[0][0] if part_log_res and part_log_res[0][0] else 0.0
    max_insert_latency = part_log_res[0][1] if part_log_res and part_log_res[0][1] else 0.0

    # 3. Đo Storage Size & Compression Ratio thực tế từ system.parts
    storage_res = client.query("""
        SELECT 
            sum(rows) AS total_rows,
            sum(bytes_on_disk) AS bytes_on_disk,
            sum(data_compressed_bytes) AS compressed_bytes,
            sum(data_uncompressed_bytes) AS uncompressed_bytes,
            round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) AS comp_ratio
        FROM system.parts
        WHERE table = 'stock_ticks' AND active = 1
    """).result_rows

    total_table_rows = storage_res[0][0] or 0
    compressed_bytes = storage_res[0][2] or 0
    comp_ratio = storage_res[0][4] or 1.0
    compressed_mb = compressed_bytes / (1024 * 1024)

    admin.close()
    consumer.close()

    # ---------------------------------------------------------
    # PHA 4: XUẤT BÁO CÁO TOÀN DIỆN MIXED WORKLOAD
    # ---------------------------------------------------------
    print("\n" + "=" * 76)
    print("          BÁO CÁO KẾT QUẢ KIỂM THỬ TẢI HỖN HỢP (MIXED WORKLOAD BENCHMARK)")
    print("=" * 76)
    print(f" [1] INSERT PERFORMANCE (GHI DỮ LIỆU)")
    print(f"     [*] Tổng số messages nạp kiểm thử  : {count:,} messages")
    print(f"     [*] Producer Send Rate             : {producer_rate:,.0f} msgs/giây ({producer_send_time:.2f}s)")
    print(f"     [*] ClickHouse Insert Throughput   : {insert_throughput:,.0f} rows/giây (Toàn chu trình {total_ingestion_time:.2f}s)")
    print(f"     [*] ClickHouse Insert Latency      : Trung bình {avg_insert_latency:.1f} ms | Max {max_insert_latency} ms (đối chiếu system.part_log)")
    print(" " + "-" * 74)
    print(f" [2] FASTAPI REST API LATENCY (ĐỘ TRỄ API OVERVIEW THỰC TẾ)")
    print(f"     [*] Số lượt gọi API đồng thời      : {len(load_latencies)} calls")
    print(f"     [*] BASELINE (Lúc hệ thống rảnh)   : P50 = {base_p50:.2f} ms | P95 = {base_p95:.2f} ms | P99 = {base_p99:.2f} ms")
    print(f"     [*] UNDER LOAD (Vừa Ghi vừa Đọc)   : P50 = {load_p50:.2f} ms | P95 = {load_p95:.2f} ms | P99 = {load_p99:.2f} ms")
    print(f"     [*] Mức độ ảnh hưởng do tải ghi    : +{max(0.0, load_p50 - base_p50):.2f} ms ({((load_p50 / max(0.001, base_p50)) - 1) * 100:+.1f}%)")
    print(" " + "-" * 74)
    print(f" [3] CLICKHOUSE INTERNAL ENGINE EFFICIENCY (HIỆU QUẢ LÕI CLICKHOUSE)")
    print(f"     [*] ClickHouse Internal Latency    : Trung bình {sys_avg_latency:.2f} ms | P95 = {sys_p95_latency:.2f} ms")
    print(f"     [*] Rows Read trung bình           : {sys_read_rows:,} rows / query")
    print(f"     [*] Rows Returned trung bình       : {sys_result_rows:,} rows (Tỷ lệ lọc: {sys_read_rows / max(1, sys_result_rows):,.1f}x)")
    print(" " + "-" * 74)
    print(f" [4] STORAGE & COMPRESSION (LƯU TRỮ TRÊN ĐĨA)")
    print(f"     [*] Tổng số dòng dữ liệu hiện tại  : {total_table_rows:,} rows")
    print(f"     [*] Dung lượng nén trên đĩa        : {compressed_mb:.2f} MB")
    print(f"     [*] Compression Ratio thực tế      : {comp_ratio}x (Tiết kiệm {(1 - 1/comp_ratio)*100:.1f}% dung lượng đĩa)")
    print("=" * 76 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy kiểm thử tải hỗn hợp (Mixed Workload Benchmark)")
    parser.add_argument("--count", type=int, default=20000, help="Số lượng tin nhắn kiểm thử nạp (mặc định: 20000)")
    parser.add_argument("--reads", type=int, default=30, help="Số lượt truy vấn baseline (mặc định: 30)")
    args = parser.parse_args()
    run_mixed_workload_benchmark(count=args.count, read_iterations=args.reads)
