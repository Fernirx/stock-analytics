CREATE DATABASE IF NOT EXISTS stock_analytics;

USE stock_analytics;

CREATE TABLE IF NOT EXISTS stock_analytics.stock_ticks (
    symbol LowCardinality(String),
    price Float64,
    volume Float64,
    match_type LowCardinality(String),
    trade_time DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_time)
ORDER BY (symbol, trade_time);

CREATE TABLE IF NOT EXISTS stock_analytics.stock_ticks_kafka (
    symbol String,
    price Float64,
    volume Float64,
    match_type String,
    trade_time DateTime
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'stock_ticks',
    kafka_group_name = 'clickhouse_ticks_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.stock_ticks_mv
TO stock_analytics.stock_ticks AS
SELECT
    symbol,
    price,
    volume,
    match_type,
    trade_time
FROM stock_analytics.stock_ticks_kafka;
