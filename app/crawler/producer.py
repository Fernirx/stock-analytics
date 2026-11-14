import json
import logging
import time
from kafka import KafkaProducer
from app.core.config import settings
from app.core.schemas.tick import TickSchema
from app.crawler.vnstock_client import VnstockClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CrawlerProducer")

def get_kafka_producer():
    candidate_servers = [settings.KAFKA_BOOTSTRAP_SERVERS, f"localhost:{settings.KAFKA_PORT}"]
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


def run_crawler():
    producer = get_kafka_producer()
    client = VnstockClient()
    topic = settings.KAFKA_TOPIC_TICKS
    interval = settings.CRAWL_INTERVAL_SECONDS

    logger.info(f"Starting crawler worker. Target topic: {topic}. Interval: {interval}s")

    while True:
        try:
            ticks = client.fetch_latest_ticks()
            for tick_data in ticks:
                tick = TickSchema(**tick_data)
                payload = tick.model_dump(mode="json")
                producer.send(topic, value=payload)
                logger.info(f"Sent tick: {tick.symbol} | Price: {tick.price} | Time: {tick.trade_time}")

            producer.flush()
        except Exception as e:
            logger.error(f"Error in crawl cycle: {e}")

        time.sleep(interval)


if __name__ == "__main__":
    run_crawler()
