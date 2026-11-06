-- Raw events, one row per SDK event. Kept 30 days for ad-hoc analysis and
-- for rebuilding aggregates. Partitioned by day so TTL drops whole parts.
CREATE TABLE IF NOT EXISTS proview.events_raw
(
    event_id      String,
    event_type    LowCardinality(String),
    impression_id String,
    banner_id     UInt64,
    placement     LowCardinality(String),
    event_time    DateTime64(3, 'UTC'),
    received_at   DateTime64(3, 'UTC'),
    duration_ms   UInt32,
    visitor_id    String,
    country       LowCardinality(String),
    device        LowCardinality(String),
    page_host     LowCardinality(String),
    inserted_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (banner_id, event_type, event_time)
TTL toDateTime(event_time) + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 1000;
