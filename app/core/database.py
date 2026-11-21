import logging
import os
import threading
import clickhouse_connect
from app.core.config import settings

logging.getLogger("clickhouse_connect").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger = logging.getLogger("Database")

# Dùng Thread-Local: Mỗi luồng sở hữu 1 client riêng, an toàn đa luồng 100%
_local = threading.local()


def get_clickhouse_client():
    """Lấy ClickHouse Client riêng cho từng luồng (Thread-Local Client), tối ưu connection và thread-safe."""
    if hasattr(_local, "client") and _local.client is not None:
        return _local.client

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
            _local.client = client
            return _local.client
        except Exception:
            continue

    raise ConnectionError(f"Không thể kết nối tới ClickHouse tại {candidate_hosts}")


def close_clickhouse_client():
    """Đóng kết nối ClickHouse của luồng hiện tại."""
    if hasattr(_local, "client") and _local.client is not None:
        try:
            _local.client.close()
        except Exception:
            pass
        _local.client = None
