from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APP_PORT: int = 8000

    KAFKA_PORT: int = 9092
    KAFKA_UI_PORT: int = 8080
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_TOPIC_TICKS: str = "stock_ticks"
    CRAWL_INTERVAL_SECONDS: int = 2

    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_HTTP_PORT: int = 8123
    CLICKHOUSE_TCP_PORT: int = 9000
    CLICKHOUSE_DB: str = "stock_analytics"
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""


settings = Settings()
