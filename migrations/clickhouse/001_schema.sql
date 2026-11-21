CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, event_id);

CREATE TABLE IF NOT EXISTS cinema.kafka_events
AS cinema.raw_events
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,
    kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest_events
TO cinema.raw_events AS SELECT * FROM cinema.kafka_events;

CREATE TABLE IF NOT EXISTS cinema.aggregate_values
(
    date Date,
    metric LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, version, metric, dimension);

CREATE TABLE IF NOT EXISTS cinema.aggregate_runs
(
    date Date,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY date;

CREATE VIEW IF NOT EXISTS cinema.daily_metrics AS
SELECT date, metric, dimension, value, computed_at
FROM cinema.aggregate_values
WHERE (date, version) IN
    (SELECT date, max(version) FROM cinema.aggregate_runs GROUP BY date);
