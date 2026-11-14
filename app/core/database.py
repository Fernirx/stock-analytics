import logging
import os
import clickhouse_connect
from app.core.config import settings

logging.getLogger("clickhouse_connect").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger = logging.getLogger("Database")


def get_clickhouse_client():
    is_docker = os.path.exists("/.dockerenv")
    if is_docker:
        candidate_hosts = [settings.CLICKHOUSE_HOST, "localhost"]
    else:
        candidate_hosts = ["localhost", settings.CLICKHOUSE_HOST]

    for host in candidate_hosts:
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=settings.CLICKHOUSE_HTTP_PORT,
                database=settings.CLICKHOUSE_DB,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD,
                connect_timeout=3,
            )
            client.command("SELECT 1")
            return client
        except Exception:
            continue
    raise ConnectionError(f"Không thể kết nối tới ClickHouse tại {candidate_hosts}")
