import json
import logging
import os
import time
from kafka import KafkaProducer
from app.core.config import settings
from app.core.schemas.tick import TickSchema
from app.crawler.vnstock_client import VnstockClient

# Tắt log chi tiết của kafka-python
logging.getLogger("kafka").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CrawlerProducer")

def get_kafka_producer():
    is_docker = os.path.exists("/.dockerenv")
    if is_docker:
        candidate_servers = [settings.KAFKA_BOOTSTRAP_SERVERS, f"localhost:{settings.KAFKA_PORT}"]
    else:
        candidate_servers = [f"localhost:{settings.KAFKA_PORT}", settings.KAFKA_BOOTSTRAP_SERVERS]

    for server in candidate_servers:
        try:
            producer = KafkaProducer(
                bootstrap_servers=server,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
            )
            logger.info(f"Connected to Kafka broker at: {server}")
            return producer
        except Exception:
            continue
    raise ConnectionError(f"Could not connect to Kafka at {candidate_servers}")


def check_and_seed_historical_data(producer, topic):
    """Kiểm tra nếu ClickHouse chưa có đủ dữ liệu nến lịch sử (Cold-Start) -> Tự động bắn mồi vào Kafka."""
    try:
        from app.core.database import get_clickhouse_client
        ch = get_clickhouse_client()
        res = ch.query("SELECT min(cnt) FROM (SELECT count() as cnt FROM stock_analytics.stock_candles_1m GROUP BY symbol)").result_rows
        min_count = res[0][0] if (res and res[0][0]) else 0
        if min_count >= 100:
            logger.info(f"ClickHouse mỗi mã đã có {min_count} nến trong stock_candles_1m. Bỏ qua bước Seeding.")
            return

        logger.info(f"ClickHouse chỉ có {min_count} nến/mã (Cold-Start < 100). Bắt đầu nạp mồi 120 nến lịch sử qua Kafka...")
        client = VnstockClient()
        seed_ticks = client.fetch_historical_ticks_for_seeding(limit=120)
        logger.info(f"Đã tạo {len(seed_ticks)} ticks mồi cho {len(client.symbols)} mã. Đang bắn vào Kafka topic '{topic}'...")

        for tick_data in seed_ticks:
            tick = TickSchema(**tick_data)
            payload = tick.model_dump(mode="json")
            producer.send(topic, key=tick.symbol.encode("utf-8"), value=payload)

        producer.flush()
        logger.info(f"Đã bắn xong toàn bộ ticks mồi vào Kafka! Nghỉ 3s để ClickHouse Materialized Views tiêu thụ xong...")
        time.sleep(3)
    except Exception as e:
        logger.warning(f"Lỗi trong quá trình Seeding qua Kafka: {e}")


def run_crawler():
    producer = get_kafka_producer()
    client = VnstockClient()
    topic = settings.KAFKA_TOPIC_TICKS
    interval = settings.CRAWL_INTERVAL_SECONDS

    # 1. Kích hoạt Historical Data Seeding Pipeline nếu ClickHouse đang trống
    check_and_seed_historical_data(producer, topic)

    logger.info(f"Starting crawler worker. Target topic: {topic}. Interval: {interval}s")

    while True:
        try:
            ticks = client.fetch_latest_ticks()
            for tick_data in ticks:
                tick = TickSchema(**tick_data)
                payload = tick.model_dump(mode="json")
                producer.send(topic, key=tick.symbol.encode("utf-8"), value=payload)
                logger.info(f"Sent tick: {tick.symbol} | Price: {tick.price} | Time: {tick.trade_time}")

            producer.flush()
        except Exception as e:
            logger.error(f"Error in crawl cycle: {e}")

        time.sleep(interval)


if __name__ == "__main__":
    run_crawler()
