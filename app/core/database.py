import logging
import os
import threading
import clickhouse_connect
from app.core.config import settings

logging.getLogger("clickhouse_connect").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger = logging.getLogger("Database")

_client = None
_client_lock = threading.Lock()


def get_clickhouse_client():
    """Lấy ClickHouse Client theo mô hình Singleton, tái sử dụng Connection Pool."""
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:
            return _client

        is_docker = os.path.exists("/.dockerenv")
        candidate_hosts = [settings.CLICKHOUSE_HOST, "localhost"] if is_docker else ["localhost", settings.CLICKHOUSE_HOST]

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
                logger.info(f"Kết nối thành công tới ClickHouse tại {host}:{settings.CLICKHOUSE_HTTP_PORT}")
                _client = client
                return _client
            except Exception:
                continue

        raise ConnectionError(f"Không thể kết nối tới ClickHouse tại {candidate_hosts}")


def close_clickhouse_client():
    """Đóng kết nối ClickHouse khi tắt server."""
    global _client
    with _client_lock:
        if _client is not None:
            try:
                _client.close()
            except Exception:
                pass
            _client = None
