CREATE DATABASE IF NOT EXISTS stock_analytics;

USE stock_analytics;

-- ====================================================================
-- 1. RAW TICKS STORAGE (Lưu vết dữ liệu khớp lệnh thô cho Live Tape)
-- ====================================================================
CREATE TABLE IF NOT EXISTS stock_analytics.stock_ticks (
    symbol LowCardinality(String),
    price Float64,
    volume Float64,
    match_type LowCardinality(String),
    trade_time DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_time)
ORDER BY (symbol, trade_time);

-- ====================================================================
-- 2. KAFKA INGESTION ENGINE (Đọc stream từ Apache Kafka)
-- ====================================================================
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
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 100;

-- MV 1: Pipe trung chuyển Raw Ticks sang bảng stock_ticks
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.stock_ticks_mv
TO stock_analytics.stock_ticks AS
SELECT
    symbol,
    price,
    volume,
    match_type,
    trade_time
FROM stock_analytics.stock_ticks_kafka;

-- ====================================================================
-- 3. REALTIME 1M CANDLESTICK AGGREGATOR (Tự động tính nến 1 phút OHLCV)
-- Sử dụng AggregatingMergeTree với các hàm Aggregate State Combinators
-- ====================================================================
CREATE TABLE IF NOT EXISTS stock_analytics.stock_candles_1m (
    symbol LowCardinality(String),
    candle_time DateTime,
    open_state AggregateFunction(argMin, Float64, DateTime),
    high SimpleAggregateFunction(max, Float64),
    low SimpleAggregateFunction(min, Float64),
    close_state AggregateFunction(argMax, Float64, DateTime),
    volume SimpleAggregateFunction(sum, Float64),
    trades_count SimpleAggregateFunction(sum, UInt32)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(candle_time)
ORDER BY (symbol, candle_time);

-- MV 2: Continuous Aggregation tính nến OHLCV trực tiếp từ luồng Kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.stock_candles_1m_mv
TO stock_analytics.stock_candles_1m AS
SELECT
    symbol,
    toStartOfInterval(trade_time, INTERVAL 1 MINUTE) AS candle_time,
    argMinState(price, trade_time) AS open_state,
    max(price) AS high,
    min(price) AS low,
    argMaxState(price, trade_time) AS close_state,
    sum(volume) AS volume,
    count() AS trades_count
FROM stock_analytics.stock_ticks_kafka
GROUP BY symbol, candle_time;

-- ====================================================================
-- 4. REALTIME ORDER FLOW & CVD ROLLUP (Phân tích áp lực Mua/Bán & VWAP 5 phút)
-- Sử dụng SummingMergeTree tự động cộng dồn doanh số và khối lượng
-- ====================================================================
CREATE TABLE IF NOT EXISTS stock_analytics.stock_orderflow_5m (
    symbol LowCardinality(String),
    bucket_time DateTime,
    turnover Float64,        -- sum(price * volume)
    buy_vol Float64,         -- sumIf(volume, match_type = 'B')
    sell_vol Float64,        -- sumIf(volume, match_type = 'S')
    total_vol Float64,       -- sum(volume)
    trades_count UInt32
) ENGINE = SummingMergeTree((turnover, buy_vol, sell_vol, total_vol, trades_count))
PARTITION BY toYYYYMM(bucket_time)
ORDER BY (symbol, bucket_time);

-- MV 3: Continuous Aggregation tính dòng tiền vi mô trực tiếp từ luồng Kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.stock_orderflow_5m_mv
TO stock_analytics.stock_orderflow_5m AS
SELECT
    symbol,
    toStartOfInterval(trade_time, INTERVAL 5 MINUTE) AS bucket_time,
    sum(price * volume) AS turnover,
    sumIf(volume, match_type = 'B') AS buy_vol,
    sumIf(volume, match_type = 'S') AS sell_vol,
    sum(volume) AS total_vol,
    count() AS trades_count
FROM stock_analytics.stock_ticks_kafka
GROUP BY symbol, bucket_time;
