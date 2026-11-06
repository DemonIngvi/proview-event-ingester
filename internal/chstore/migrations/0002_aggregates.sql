-- Aggregator database: pre-aggregated slices read by proview-analytics.
-- Materialized views update them on every insert into events_raw, so the
-- aggregates are consistent with raw data without a separate batch job.

CREATE TABLE IF NOT EXISTS proview_agg.banner_stats_1m
(
    bucket      DateTime('UTC'),
    event_type  LowCardinality(String),
    banner_id   UInt64,
    placement   LowCardinality(String),
    country     LowCardinality(String),
    device      LowCardinality(String),
    events      SimpleAggregateFunction(sum, UInt64),
    duration_ms SimpleAggregateFunction(sum, UInt64),
    visitors    AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (bucket, event_type, banner_id, placement, country, device)
TTL bucket + INTERVAL 14 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS proview_agg.banner_stats_1h
(
    bucket      DateTime('UTC'),
    event_type  LowCardinality(String),
    banner_id   UInt64,
    placement   LowCardinality(String),
    country     LowCardinality(String),
    device      LowCardinality(String),
    events      SimpleAggregateFunction(sum, UInt64),
    duration_ms SimpleAggregateFunction(sum, UInt64),
    visitors    AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (bucket, event_type, banner_id, placement, country, device)
TTL bucket + INTERVAL 730 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS proview_agg.mv_banner_stats_1m
TO proview_agg.banner_stats_1m AS
SELECT
    toStartOfMinute(event_time) AS bucket,
    event_type, banner_id, placement, country, device,
    count()                   AS events,
    sum(toUInt64(duration_ms)) AS duration_ms,
    uniqState(visitor_id)     AS visitors
FROM proview.events_raw
GROUP BY bucket, event_type, banner_id, placement, country, device;

CREATE MATERIALIZED VIEW IF NOT EXISTS proview_agg.mv_banner_stats_1h
TO proview_agg.banner_stats_1h AS
SELECT
    toStartOfHour(event_time) AS bucket,
    event_type, banner_id, placement, country, device,
    count()                   AS events,
    sum(toUInt64(duration_ms)) AS duration_ms,
    uniqState(visitor_id)     AS visitors
FROM proview.events_raw
GROUP BY bucket, event_type, banner_id, placement, country, device;
